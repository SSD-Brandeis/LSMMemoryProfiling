// run_workload_multithread.cc
//
// Multithreaded counterpart to run_workload.cc, built for the R2.W2
// scalability-vs-thread-count experiments. Structurally this is a copy of
// runWorkload(): same DBEnv/configOptions setup, same DB::Open, and the same
// per-line operation dispatch (identical switch-case semantics for
// I/U/D/P|Q/S|SC/R/M). The difference is execution: env->num_client_threads
// worker threads are spawned against one shared DB* handle, each replaying
// its own workload shard file ("shard_<i>.txt" in the current directory) and
// issuing real rocksdb::DB API calls (Put/Get/Delete/NewIterator/...)
// concurrently — the same client-facing API surface a real multi-client
// deployment would exercise.
//
// Each thread owns its own Buffer instances (workload_t<i>.log /
// stats_t<i>.log) since Buffer is not internally synchronized for concurrent
// writers. GlobalWorkloadMonitor()'s counters are atomic and are safe to
// update from all threads; Configure() is called once up front, before any
// thread starts.
//
// Consecutive I/U lines are coalesced into a WriteBatch (flushed every
// kBatchSize entries, and immediately before any op that reads or deletes,
// to preserve read-your-own-writes ordering for mixed workloads). A pure
// per-line db->Put() issues one single-entry write group per call; at
// microsecond-scale memtable insert costs, RocksDB's write-group
// leader/follower coordination then dominates the measurement instead of
// the memtable itself, which is what this harness is trying to isolate.
// Batching is the standard RocksDB bulk-load practice, not a measurement
// trick — see rocksdb/write_batch.h.
#include "run_workload_multithread.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>
#include <tuple>
#include <vector>

#include "config_options.h"
#include "rocksdb/write_batch.h"
#include "utils.h"
#include "workload_monitor.h"

namespace {

// Per-thread result, aggregated after all threads join.
struct ThreadResult {
  unsigned long ops = 0;
};

// Replays one shard file against the shared db. Mirrors the per-line
// dispatch in runWorkload() exactly (same operation codes / semantics).
//
// When loop_enabled is set, the shard wraps back to its own first line on
// EOF and keeps going until the shared `deadline` passes, instead of
// stopping after one pass. This is the fixed-duration, ops-completed
// methodology (matches db_bench's timed benchmarks): with only a few
// million total ops, per-run fixed costs (DB::Open, dropping the system
// page cache, background-thread warmup) are large enough relative to a
// single short pass that thread-count comparisons become noisy; measuring
// a longer, fixed wall-clock window instead amortizes that fixed cost away.
void RunShard(int thread_idx, DB *db, const WriteOptions &write_options,
              const ReadOptions &read_options, bool use_prefix_seek,
              std::unique_ptr<DBEnv> &env, bool loop_enabled,
              std::chrono::steady_clock::time_point deadline,
              ThreadResult *result) {
  std::string shard_path = "shard_" + std::to_string(thread_idx) + ".txt";
  std::ifstream workload_file(shard_path);
  if (!workload_file) {
    std::cerr << "Thread " << thread_idx << ": failed to open " << shard_path
              << std::endl;
    return;
  }

  std::string workload_log = "workload_t" + std::to_string(thread_idx) + ".log";
  std::string stats_log = "stats_t" + std::to_string(thread_idx) + ".log";
  std::shared_ptr<Buffer> buffer = std::make_unique<Buffer>(workload_log);
  std::unique_ptr<Buffer> stats = std::make_unique<Buffer>(stats_log);

#ifdef PER_OP_TIMER
  unsigned long inserts_exec_time = 0, updates_exec_time = 0, pq_exec_time = 0,
                pdelete_exec_time = 0, rq_exec_time = 0, merge_exec_time = 0;
#endif // PER_OP_TIMER

  static constexpr size_t kBatchSize = 256;
  WriteBatch pending_batch;
  size_t pending_count = 0;
  Status s;
  // Attributed to "insert time"; individual entries within a flushed batch
  // are not separately timed (see file header comment).
  auto flush_pending = [&]() {
    if (pending_count == 0) return;
#ifdef PER_OP_TIMER
    auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
    s = db->Write(write_options, &pending_batch);
#ifdef PER_OP_TIMER
    auto stop = std::chrono::high_resolution_clock::now();
    inserts_exec_time +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start)
            .count();
#endif // PER_OP_TIMER
    pending_batch.Clear();
    pending_count = 0;
  };

  std::string line;
  unsigned long ith_op = 0;
  while (true) {
    if (!std::getline(workload_file, line) || line.empty()) {
      if (!loop_enabled) break;
      workload_file.clear();
      workload_file.seekg(0, std::ios::beg);
      if (!std::getline(workload_file, line) || line.empty()) {
        break; // empty shard; nothing to loop over
      }
    }

    std::istringstream stream(line);
    char operation;
    stream >> operation;

    switch (operation) {
      // [Insert]
    case 'I': {
      std::string key, value;
      stream >> key >> value;
      pending_batch.Put(key, value);
      GlobalWorkloadMonitor().RecordInsert();
      if (++pending_count >= kBatchSize) flush_pending();
      break;
    }
      // [Update]
    case 'U': {
      std::string key, value;
      stream >> key >> value;
      pending_batch.Put(key, value);
      GlobalWorkloadMonitor().RecordUpdate();
      if (++pending_count >= kBatchSize) flush_pending();
      break;
    }
      // [PointDelete]
    case 'D': {
      flush_pending();
      std::string key;
      stream >> key;

#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
      s = db->Delete(write_options, key);
      GlobalWorkloadMonitor().RecordPointDelete();
#ifdef PER_OP_TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "D: " << duration.count() << std::endl;
      pdelete_exec_time += duration.count();
#endif // PER_OP_TIMER
      break;
    }
      // [ProbePointQuery]
    case 'P':
    case 'Q': {
      flush_pending();
      std::string key, value;
      stream >> key;

#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
      s = db->Get(read_options, key, &value);
      GlobalWorkloadMonitor().RecordPointQuery();
      if (s.IsNotFound()) {
        (*buffer) << "PQ: " << key << ", Not Found" << std::endl;
      } else if (!s.ok()) {
        (*buffer) << "PQ: Error reading key " << key << ": " << s.ToString()
                  << std::endl;
      }
#ifdef PER_OP_TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "Q: " << duration.count() << std::endl;
      pq_exec_time += duration.count();
#endif // PER_OP_TIMER
      break;
    }
      // [ScanRangeQuery] — same "S"/"SC" formats as runWorkload().
    case 'S': {
      flush_pending();
      const bool is_count_scan = (stream.peek() == 'C');
      if (is_count_scan) stream.get();

      std::string start_key;
      stream >> start_key;

      ReadOptions scan_read_options = ReadOptions(read_options);
      scan_read_options.total_order_seek = !use_prefix_seek;

      Iterator *it = db->NewIterator(scan_read_options);

#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER

      if (is_count_scan) {
        uint64_t scan_len;
        stream >> scan_len;
        uint64_t steps = 0;
        for (it->Seek(start_key); it->Valid() && steps < scan_len;
             it->Next(), ++steps) {
        }
      } else {
        std::string end_key;
        stream >> end_key;
        if (env->common_prefix_len > 0) {
          const size_t prefix_bytes =
              std::min((size_t)env->common_prefix_len, start_key.size());
          end_key = start_key.substr(0, prefix_bytes) +
                    end_key.substr(prefix_bytes);
        }
        for (it->Seek(start_key); it->Valid(); it->Next()) {
          if (it->key().ToString() >= end_key) {
            break;
          }
        }
      }

      if (!it->status().ok()) {
        (*buffer) << it->status().ToString() << std::endl << std::flush;
      }
#ifdef PER_OP_TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "S: " << duration.count() << std::endl;
      rq_exec_time += duration.count();
#endif // PER_OP_TIMER
      GlobalWorkloadMonitor().RecordRangeQuery();
      delete it;
      break;
    }
      // [RangeDelete]
    case 'R': {
      flush_pending();
      std::string start_key, end_key;
      stream >> start_key >> end_key;
      s = db->DeleteRange(write_options, start_key, end_key);
      GlobalWorkloadMonitor().RecordRangeDelete();
      break;
    }
      // [ReadModifyWrite]
    case 'M': {
      flush_pending();
#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
      std::string start_key, end_key;
      stream >> start_key >> end_key;
      s = db->Merge(write_options, start_key, end_key);
      GlobalWorkloadMonitor().RecordUpdate();
#ifdef PER_OP_TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "M: " << duration.count() << std::endl;
      merge_exec_time += duration.count();
#endif // PER_OP_TIMER
      break;
    }
    default:
      (*buffer) << "ERROR: Case match NOT found !!" << std::endl;
      break;
    }

    ith_op += 1;

    // Checked every op: clock_gettime is on the order of tens of
    // nanoseconds, negligible next to any real DB operation (disk-bound
    // point reads run in the hundreds of microseconds to milliseconds; even
    // batched memtable inserts are microseconds). A coarser interval here
    // previously let disk-bound read runs overshoot the deadline by
    // several seconds per thread before the first check landed.
    if (loop_enabled && std::chrono::steady_clock::now() >= deadline) break;
  }
  flush_pending();

#ifdef PER_OP_TIMER
  (*buffer) << "=====================" << std::endl;
  (*buffer) << "Inserts Execution Time: " << inserts_exec_time << std::endl;
  (*buffer) << "Updates Execution Time: " << updates_exec_time << std::endl;
  (*buffer) << "PointQuery Execution Time: " << pq_exec_time << std::endl;
  (*buffer) << "PointDelete Execution Time: " << pdelete_exec_time << std::endl;
  (*buffer) << "RangeQuery Execution Time: " << rq_exec_time << std::endl;
  (*buffer) << "Merge Execution Time: " << merge_exec_time << std::endl;
#endif // PER_OP_TIMER

  buffer->flush();
  stats->flush();
  result->ops = ith_op;
}

} // namespace

int runWorkloadMultithread(std::unique_ptr<DBEnv> &env) {
  const unsigned int T = std::max(1u, env->num_client_threads);

  // Multi-writer memtable inserts require this; every memtable factory
  // exercised by this harness supports it (see MemTableRepFactory::
  // IsInsertConcurrentlySupported() overrides). This is a safety net on top
  // of --concurrent_memtable_write (parse_arguments.h), which callers should
  // still pass explicitly so it shows up in logs/manifests rather than only
  // being implied here.
  if (T > 1) {
    env->allow_concurrent_memtable_write = true;
  }

  GlobalWorkloadMonitor().Configure(
      env->entries_per_page * env->buffer_size_in_pages, env->bucket_count);

  DB *db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;

  configOptions(env, &options, &table_options, &write_options, &read_options,
               &flush_options);

  std::shared_ptr<Buffer> summary_buffer =
      std::make_unique<Buffer>("workload.log");

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(env->kDBPath, options);
    std::cerr << "Destroying database ... done" << std::endl;
  }

  PrintExperimentalSetup(env, summary_buffer);
  std::cerr << "Multithreaded run: " << T << " client thread(s)" << std::endl;

  Status s = DB::Open(options, env->kDBPath, &db);
  if (!s.ok()) {
    // assert() is compiled out under this project's Release build
    // (CMAKE_BUILD_TYPE Release => NDEBUG), so an invalid option
    // combination (e.g. unordered_write=1 with
    // concurrent_memtable_write=0) would otherwise silently continue with
    // a null db and segfault on first use instead of failing cleanly.
    std::cerr << "DB::Open failed: " << s.ToString() << std::endl;
    return 1;
  }

  if (env->clear_system_cache) {
#ifdef __linux__
    std::cerr << "Clearing system cache ...";
    std::cerr << system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
              << " done" << std::endl;
#endif
  }

  const bool use_prefix_seek =
      (env->common_prefix_len > 0 &&
       env->common_prefix_len >= env->prefix_length);

  if (env->IsPerfStatEnabled())
    rocksdb::get_perf_context()->Reset();
  if (env->IsIOStatEnabled())
    rocksdb::get_iostats_context()->Reset();

  std::vector<ThreadResult> results(T);
  std::vector<std::thread> threads;
  threads.reserve(T);

#ifdef TOTAL_TIMER
  auto exec_start = std::chrono::high_resolution_clock::now();
#endif // TOTAL_TIMER

  const bool loop_enabled = env->duration_secs > 0;
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(env->duration_secs);

  for (unsigned int t = 0; t < T; t++) {
    threads.emplace_back(RunShard, static_cast<int>(t), db,
                         std::cref(write_options), std::cref(read_options),
                         use_prefix_seek, std::ref(env), loop_enabled,
                         deadline, &results[t]);
  }
  for (auto &th : threads) th.join();

#ifdef TOTAL_TIMER
  auto total_exec_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now() - exec_start)
          .count();
  double total_seconds = total_exec_time / 1e9;
#endif // TOTAL_TIMER

  unsigned long total_ops = 0;
  for (const auto &r : results) total_ops += r.ops;

#ifdef TOTAL_TIMER
  double ops_per_sec = (total_seconds > 0) ? total_ops / total_seconds : 0;
  std::ofstream throughput_csv("throughput.csv");
  throughput_csv << "threads,total_ops,seconds,ops_per_sec\n";
  throughput_csv << T << "," << total_ops << "," << std::fixed
                 << std::setprecision(6) << total_seconds << ","
                 << std::setprecision(1) << ops_per_sec << "\n";
  throughput_csv.close();
  std::cerr << "THROUGHPUT threads=" << T << " total_ops=" << total_ops
            << " seconds=" << total_seconds << " ops_per_sec=" << ops_per_sec
            << std::endl;
#endif // TOTAL_TIMER

#ifdef PROFILE
  (*summary_buffer) << "=====================" << std::endl;
  LogTreeState(db, summary_buffer, env);
#endif // PROFILE

  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  s = db->Close();
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  PrintRocksDBPerfStats(env, summary_buffer, options);
  table_options.block_cache.reset();
  options.table_factory.reset();
  summary_buffer->flush();

  return 0;
}
