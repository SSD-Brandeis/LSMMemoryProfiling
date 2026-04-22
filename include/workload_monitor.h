#pragma once

#include <atomic>
#include <cstdint>

#include <rocksdb/memtablerep.h>

// Tracks per-epoch operation counts and implements the MemtableAdvisor
// interface so DynamicMemtableFactory can query the cost model directly.
// All record methods are wait-free via relaxed atomics; SelectMemtableType()
// is a pure read and safe to call concurrently.
struct WorkloadMonitor : public ROCKSDB_NAMESPACE::MemtableAdvisor {
  // --- counters ---
  std::atomic<uint64_t> inserts{0};
  std::atomic<uint64_t> updates{0};
  std::atomic<uint64_t> point_deletes{0};
  std::atomic<uint64_t> range_deletes{0};
  std::atomic<uint64_t> point_queries{0};
  std::atomic<uint64_t> range_queries{0};

  void RecordInsert()      { inserts.fetch_add(1,       std::memory_order_relaxed); }
  void RecordUpdate()      { updates.fetch_add(1,       std::memory_order_relaxed); }
  void RecordPointDelete() { point_deletes.fetch_add(1, std::memory_order_relaxed); }
  void RecordRangeDelete() { range_deletes.fetch_add(1, std::memory_order_relaxed); }
  void RecordPointQuery()  { point_queries.fetch_add(1, std::memory_order_relaxed); }
  void RecordRangeQuery()  { range_queries.fetch_add(1, std::memory_order_relaxed); }

  // Reset all counters (e.g. at the start of a new profiling epoch).
  void Reset() {
    inserts.store(0,       std::memory_order_relaxed);
    updates.store(0,       std::memory_order_relaxed);
    point_deletes.store(0, std::memory_order_relaxed);
    range_deletes.store(0, std::memory_order_relaxed);
    point_queries.store(0, std::memory_order_relaxed);
    range_queries.store(0, std::memory_order_relaxed);
  }

  // MemtableAdvisor implementation — called by DynamicMemtableFactory.
  //
  // Cost model: we identify the dominant operation term by comparing
  // dimensionless ratios against empirically-chosen thresholds.
  //
  //   SkipList        insert O(log n)  pq O(log n)   rq O(log n + k)
  //   SortedVector    insert O(n)      pq O(log n)   rq O(log n + k)
  //   UnsortedVector  insert O(1)      pq O(n)       rq O(n log n)
  //   HashSkipList    insert O(log n/B) pq O(log n/B) rq O(n log n) [prefix]
  //   HashLinkList    insert O(1)      pq O(n/B)     rq O(n log n) [prefix]
  //   HashVector      insert O(1)      pq O(n/B)     rq O(n log n) [prefix]
  //   SimpleSkipList  ~SkipList with lower per-node constant
  //
  // Returns a type id in [1, 9] matching the memtable_factory enum.
  int SelectMemtableType(bool has_prefix) const override {
    const uint64_t w  = inserts.load(std::memory_order_relaxed)
                      + updates.load(std::memory_order_relaxed)
                      + point_deletes.load(std::memory_order_relaxed)
                      + range_deletes.load(std::memory_order_relaxed);
    const uint64_t pq = point_queries.load(std::memory_order_relaxed);
    const uint64_t rq = range_queries.load(std::memory_order_relaxed);
    const uint64_t total = w + pq + rq;

    // No observations yet — return the balanced default.
    if (total == 0) return 1; // SkipList

    const double wr  = double(w)  / double(total);
    // fraction of reads that are range scans
    const double rqr = (pq + rq) > 0 ? double(rq) / double(pq + rq) : 0.0;
    // point-query share of all ops
    const double pqr = double(pq) / double(total);

    // --- rule 1: almost pure writes (insert cost dominates) ---
    // O(1) append structures; range-scan cost is negligible.
    if (wr > 0.95) {
      return has_prefix ? 9  // HashVector:      O(1) insert + hash bucket
                        : 5; // UnsortedVector:  O(1) append
    }

    // --- rule 2: write-heavy (80–95 %) ---
    // Skip-list variants keep O(log n) insert and avoid SortedVector's O(n).
    // SimpleSkipList has lower per-node overhead than the standard inline
    // skip list and is better suited to a single-writer workload.
    if (wr > 0.80) {
      if (has_prefix && pqr > 0.05)
        return 3; // HashSkipList: prefix point-queries alongside writes
      return 8;   // SimpleSkipList: lighter O(log n) insert
    }

    // --- rule 3: range-scan dominant ---
    // Sorted structures give O(log n) seek to the range start.
    if (rqr > 0.60) {
      if (wr < 0.20)
        return 6; // SortedVector: near-read-only, cheap binary-search seek
      return 1;   // SkipList: O(log n) insert + O(log n + k) range scan
    }

    // --- rule 4: point-query dominant with prefix support ---
    // HashLinkList gives O(1) hash bucket + O(n/B) scan within prefix.
    if (pqr > 0.50 && has_prefix)
      return 4; // HashLinkList

    // --- default: balanced skip list ---
    // Supports concurrent inserts, O(log n) for all operation types.
    return 1; // SkipList
  }
};

// Process-wide singleton — shared by config_options.h (factory construction)
// and run_workload.cc (operation recording).
inline WorkloadMonitor& GlobalWorkloadMonitor() {
  static WorkloadMonitor instance;
  return instance;
}
