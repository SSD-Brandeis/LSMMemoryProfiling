#pragma once

#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>

#include <rocksdb/memtablerep.h>

// Tracks the last WINDOW_SIZE operations in a lock-free ring buffer and
// implements MemtableAdvisor so DynamicMemtableFactory can query the cost
// model without being coupled to application code.
//
// Because only a fixed-size window of recent ops is considered, the cost
// model adapts to workload shifts rather than being dominated by the initial
// bulk-load phase forever.
struct WorkloadMonitor : public ROCKSDB_NAMESPACE::MemtableAdvisor {
  // Number of recent operations the cost model looks at.
  // Larger → smoother / slower adaptation.  Smaller → faster response.
  static constexpr size_t WINDOW_SIZE = 12000; //[MAKE IT CONFIGURABLE THROUGH BUFFER SIZE IN PAGES AND ENTRIES PER PAGE]

  // Operation types recorded into the ring buffer.
  enum class OpType : int8_t {
    Empty       = 0,  // uninitialised slot
    Insert      = 1,
    Update      = 2,
    PointDelete = 3,
    RangeDelete = 4,
    PointQuery  = 5,
    RangeQuery  = 6,
    kCount      = 7,  // sentinel — keep last
  };

  // Computed distribution snapshot over the current window.
  struct WindowStats {
    uint64_t writes;        // Insert + Update + PointDelete + RangeDelete
    uint64_t point_queries;
    uint64_t range_queries;
    uint64_t total;

    double write_ratio()       const { return total ? double(writes)        / double(total) : 0.0; }
    double point_query_ratio() const { return total ? double(point_queries) / double(total) : 0.0; }
    // Fraction of all reads that are range scans (0 when no reads present).
    double range_read_ratio()  const {
      const uint64_t reads = point_queries + range_queries;
      return reads ? double(range_queries) / double(reads) : 0.0;
    }
  };

  WorkloadMonitor() : n_entries_(1024), bucket_count_(50000) { Reset(); }

  // Call once after argument parsing, before the workload loop.
  //   n_entries:    expected entries per memtable at flush
  //                 (= entries_per_page × buffer_size_in_pages).
  //   bucket_count: hash table bucket count for hash-based memtables.
  // These drive the cost model in SelectMemtableType so thresholds are
  // derived from the actual buffer geometry rather than hardcoded ratios.
  void Configure(size_t n_entries, size_t bucket_count) {
    n_entries_    = n_entries    > 0 ? n_entries    : 1;
    bucket_count_ = bucket_count > 0 ? bucket_count : 1;
  }

  // --- recording API (one call per executed operation) -------------------

  void RecordInsert()      { record(OpType::Insert);      }
  void RecordUpdate()      { record(OpType::Update);      }
  void RecordPointDelete() { record(OpType::PointDelete); }
  void RecordRangeDelete() { record(OpType::RangeDelete); }
  void RecordPointQuery()  { record(OpType::PointQuery);  }
  void RecordRangeQuery()  { record(OpType::RangeQuery);  }

  // Clear the ring buffer (e.g. to force a cold-start re-evaluation).
  void Reset() {
    for (auto& slot : ring_)
      slot.store(static_cast<int8_t>(OpType::Empty), std::memory_order_relaxed);
    write_pos_.store(0, std::memory_order_relaxed);
  }

  // Compute the distribution over the current window without selecting a type.
  WindowStats ComputeStats() const {
    uint64_t counts[static_cast<size_t>(OpType::kCount)] = {};
    for (const auto& slot : ring_) {
      const int8_t raw = slot.load(std::memory_order_relaxed);
      if (raw > 0 && raw < static_cast<int8_t>(OpType::kCount))
        counts[raw]++;
    }

    WindowStats s;
    s.writes        = counts[idx(OpType::Insert)]
                    + counts[idx(OpType::Update)]
                    + counts[idx(OpType::PointDelete)]
                    + counts[idx(OpType::RangeDelete)];
    s.point_queries = counts[idx(OpType::PointQuery)];
    s.range_queries = counts[idx(OpType::RangeQuery)];
    s.total         = s.writes + s.point_queries + s.range_queries;
    return s;
  }

  // --- MemtableAdvisor ----------------------------------------------------

  // Selects the memtable type by computing expected cost per op for each
  // candidate structure using the actual n (entries per memtable) and B
  // (bucket count) supplied via Configure().
  //
  //   Structure       insert         point-query         range-scan
  //   SkipList        O(log n)       O(log n)            O(log n + k)
  //   UnsortedVector  O(1)           O(n)                O(n·log n)
  //   HashLinkList    O(log n·1.2)   O(n/B + cache_pen)  O(n·log n)
  //
  // HashLinkList (RocksDB HashLinkedListRep) keeps a sorted linked list per
  // bucket.  Each operation hashes the key and accesses a random bucket.
  //
  // Cache penalty: the bucket-pointer array is B×8 bytes.  When that array
  // exceeds the L2 cache, every bucket access is a cache miss costing roughly
  // logn comparison-units.  Example: B=100 000 → 800 KB array >> 256 KB L2
  // → cache_penalty ≈ logn.  B=1 000 → 8 KB array fits in L1 → penalty ≈ 0.
  //
  // This means: when the dynamic binary is configured with a large B (e.g.
  // 100 000) the effective HashLinkList lookup cost equals SkipList or worse,
  // so SkipList is preferred.  Only a well-sized B (B ≈ n) makes HL faster.
  //
  // Write overhead: HL writes (hash + sorted insert + alloc) cost ≈1.2×logn,
  // slightly more than SkipList.  HL therefore only wins when reads are
  // frequent enough to offset this, i.e. when w < 1/1.2 ≈ 0.83.
  //
  // Returns a type id in [1, 9] matching the memtable_factory enum.
  int SelectMemtableType(bool /*has_prefix*/) const override {
    const WindowStats s = ComputeStats();
    if (s.total == 0) return 1;  // window not yet warm → SkipList default

    const double w  = s.write_ratio();
    const double pq = s.point_query_ratio();
    const double rq = 1.0 - w - pq;   // range query fraction of total ops

    const double n        = static_cast<double>(n_entries_);
    const double B        = static_cast<double>(bucket_count_);
    const double logn     = std::log2(std::max(n, 2.0));
    const double logn_over_B = std::log2(std::max(n / B, 2.0));
    const double n_over_B = n / B;

    // Cache-miss penalty for HashLinkList bucket access.
    // The bucket array is B×8 bytes; fraction exceeding a 256 KB L2 cache
    // causes random cache misses, each costing ~logn comparison-units.
    const double kL2Bytes      = 256.0 * 1024.0;
    const double cache_penalty = std::min(1.0, B * 8.0 / kL2Bytes) * logn;
    // const double c_hl_lookup   = n_over_B + cache_penalty;  // effective PQ cost

    // Expected cost per op (proportional units):
    const double c_skiplist = w * logn + pq * logn + rq * (3 * n/2) * logn;
    const double c_unsorted = w        + pq * (n/2)           + rq * n * logn;
    const double c_hashlink = w * logn
                            + pq * logn_over_B
                            + rq * n * logn;
    const double c_hashskiplist = w * logn * 1.02
                            + pq * logn_over_B
                            + rq * n * logn;
    const double c_hashvector = w * n * logn
                            + pq * logn_over_B
                            + rq * n * logn;

    double best = c_skiplist;
    int    type = 1;                                          // SkipList
    if (c_unsorted < best) { best = c_unsorted; type = 5; }  // UnsortedVector
    if (c_hashvector < best) { best = c_hashvector; type = 9; }  // HashVector
    if (c_hashskiplist < best) { best = c_hashskiplist; type = 3; }  // HashSkipList
    if (c_hashlink < best) { best = c_hashlink; type = 4; }  // HashLinkList
    return type;
  }

 private:
  size_t n_entries_;
  size_t bucket_count_;
  std::array<std::atomic<int8_t>, WINDOW_SIZE> ring_;
  std::atomic<uint64_t> write_pos_{0};

  static constexpr size_t idx(OpType t) { return static_cast<size_t>(t); }

  void record(OpType op) {
    const uint64_t pos =
        write_pos_.fetch_add(1, std::memory_order_relaxed) % WINDOW_SIZE;
    ring_[pos].store(static_cast<int8_t>(op), std::memory_order_relaxed);
  }
};

// Process-wide singleton shared by config_options.h and run_workload.cc.
inline WorkloadMonitor& GlobalWorkloadMonitor() {
  static WorkloadMonitor instance;
  return instance;
}
