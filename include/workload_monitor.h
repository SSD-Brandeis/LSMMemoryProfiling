#pragma once

#include <array>
#include <atomic>
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
  static constexpr size_t WINDOW_SIZE = 100;

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

  WorkloadMonitor() { Reset(); }

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

  // Cost-model selector called by DynamicMemtableFactory on every flush.
  // Applies threshold rules derived from the asymptotic cost of each structure.
  //
  //   Structure       insert       point-query    range-scan
  //   SkipList        O(log n)     O(log n)       O(log n + k)
  //   SortedVector    O(n)         O(n)           O(n)
  //   UnsortedVector  O(1)         O(n)           O(n )
  //   HashSkipList    O(log n/B)   O(log n/B)     O(n log n)  [prefix]
  //   HashLinkList    O(1)         O(n/B)         O(n log n)  [prefix]
  //   HashVector      O(1)         O(n/B)         O(n log n)  [prefix]
  //   SimpleSkipList  ~SkipList, lower constant
  //
  // Returns a type id in [1, 9] matching the memtable_factory enum.
  int SelectMemtableType(bool has_prefix) const override {
    const WindowStats s = ComputeStats();
    if (s.total == 0) return 1;  // no window data yet → SkipList default

    const double wr  = s.write_ratio();
    const double pqr = s.point_query_ratio();
    const double rqr = s.range_read_ratio();

    // rule 1: almost pure writes → cheapest-insert structure
    if (wr > 0.95)
      return 5;  // UnsortedVector

    // rule 2: write-heavy (80-95 %) → skip-list variants, avoid O(n) insert
    if (wr > 0.80 && pqr > 0.05) {
      return 3;  // HashSkipList
    }

    // rule 3: range-scan dominant → sorted structure for cheap range seek
    if (rqr > 0.60) {
      // if (wr < 0.20) return 6;  // SortedVector (near-read-only)
      return 9;                  // hashvector
    }

    // rule 4: point-query dominant with prefix → hash bucket lookup
    // if (pqr > 0.50 && has_prefix) return 4;  // HashVector
    if (pqr > 0.30) return 4;  // HashLinkedlist
    
    return 1;  // SkipList default
  }

 private:
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
