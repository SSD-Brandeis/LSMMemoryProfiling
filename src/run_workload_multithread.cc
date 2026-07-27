// run_workload_multithread.cc


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


struct ThreadResult {
  unsigned long ops = 0;
  unsigned long inserts_exec_time = 0;
  unsigned long updates_exec_time = 0;
  unsigned long pq_exec_time = 0;
  unsigned long pdelete_exec_time = 0;
  unsigned long rq_exec_time = 0;
  unsigned long merge_exec_time = 0;
};


void RunShard(const std::string &label, const std::string &shard_path, DB *db,
              const WriteOptions &write_options,
              const ReadOptions &read_options, bool use_prefix_seek,
              std::unique_ptr<DBEnv> &env, ThreadResult *result) {
  std::ifstream workload_file(shard_path);
  if (!workload_file) {
    std::cerr << "Shard " << label << ": failed to open " << shard_path
              << std::endl;
    return;
  }

  std::string workload_log = "workload_t" + label + ".log";
  std::string stats_log = "stats_t" + label + ".log";
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

  auto write_pending_batch = [&]() {
    if (pending_count == 0) return;
    size_t written_count = pending_count;
#ifdef PER_OP_TIMER
    auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
    s = db->Write(write_options, &pending_batch);
#ifdef PER_OP_TIMER
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    (*stats) << "IB: " << duration.count() << " count=" << written_count
            << std::endl;
    inserts_exec_time += duration.count();
#endif // PER_OP_TIMER
    pending_batch.Clear();
    pending_count = 0;
  };

  std::string line;
  unsigned long ith_op = 0;
  while (true) {
    if (!std::getline(workload_file, line) || line.empty()) break;

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
      if (++pending_count >= kBatchSize) write_pending_batch();
      break;
    }
      // [Update]
    case 'U': {
      std::string key, value;
      stream >> key >> value;
      pending_batch.Put(key, value);
      GlobalWorkloadMonitor().RecordUpdate();
      if (++pending_count >= kBatchSize) write_pending_batch();
      break;
    }
      // [PointDelete]
    case 'D': {
      write_pending_batch();
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
      write_pending_batch();
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
      } else {
        (*buffer) << "PQ: " << key << ", " << value << std::endl;
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
      write_pending_batch();
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
      write_pending_batch();
      std::string start_key, end_key;
      stream >> start_key >> end_key;
      s = db->DeleteRange(write_options, start_key, end_key);
      GlobalWorkloadMonitor().RecordRangeDelete();
      break;
    }
      // [ReadModifyWrite]
    case 'M': {
      write_pending_batch();
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
  }
  write_pending_batch();

#ifdef PER_OP_TIMER
  (*buffer) << "=====================" << std::endl;
  (*buffer) << "Inserts Execution Time: " << inserts_exec_time << std::endl;
  (*buffer) << "Updates Execution Time: " << updates_exec_time << std::endl;
  (*buffer) << "PointQuery Execution Time: " << pq_exec_time << std::endl;
  (*buffer) << "PointDelete Execution Time: " << pdelete_exec_time << std::endl;
  (*buffer) << "RangeQuery Execution Time: " << rq_exec_time << std::endl;
  (*buffer) << "Merge Execution Time: " << merge_exec_time << std::endl;
  result->inserts_exec_time = inserts_exec_time;
  result->updates_exec_time = updates_exec_time;
  result->pq_exec_time = pq_exec_time;
  result->pdelete_exec_time = pdelete_exec_time;
  result->rq_exec_time = rq_exec_time;
  result->merge_exec_time = merge_exec_time;
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

  // Load phase: replayed single-threaded, in this same process/DB::Open(),
  // strictly before the T-thread region below starts. Exists so a
  // read/mixed-style "load N keys, then measure M queries against them"
  // experiment never needs a second process to reopen the DB -- since WAL
  // is disabled (disableWAL=true, db_env.h), a process restart would have
  // no way to recover the loaded data short of a real flush to SST, which
  // defeats an in-memory experiment's purpose. Doing the load here keeps
  // everything in one memtable, one process lifetime.
  //

  ThreadResult load_result;
  const bool did_load = !env->load_file.empty();
#ifdef TOTAL_TIMER
  unsigned long load_exec_time = 0;
#endif // TOTAL_TIMER
  if (did_load) {
    std::cerr << "Loading " << env->load_file << " ..." << std::endl;
#ifdef TOTAL_TIMER
    auto load_start = std::chrono::high_resolution_clock::now();
#endif // TOTAL_TIMER
    RunShard("load", env->load_file, db, write_options, read_options,
            use_prefix_seek, env, &load_result);
#ifdef TOTAL_TIMER
    load_exec_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::high_resolution_clock::now() - load_start)
                         .count();
#endif // TOTAL_TIMER
    std::cerr << "Load done: " << load_result.ops << " ops" << std::endl;
  }

  std::vector<ThreadResult> results(T);
  std::vector<std::thread> threads;
  threads.reserve(T);

#ifdef TOTAL_TIMER
  auto exec_start = std::chrono::high_resolution_clock::now();
#endif // TOTAL_TIMER

  for (unsigned int t = 0; t < T; t++) {
    threads.emplace_back(RunShard, std::to_string(t),
                         "shard_" + std::to_string(t) + ".txt", db,
                         std::cref(write_options), std::cref(read_options),
                         use_prefix_seek, std::ref(env), &results[t]);
  }
  for (auto &th : threads) th.join();

#ifdef PER_OP_TIMER
  // Consolidated dense per-op log: every thread's (plus the load phase's,
  // if any) stats_t<label>.log gets merged into one stats_all.log here,
  // each line prefixed with which shard it came from. This runs strictly
  // after all threads have joined -- i.e. after the timed region ends --
  // so it adds no synchronization between threads and cannot perturb any
  // timing recorded above. The per-thread files are left in place too
  // (this is an added convenience view, not a replacement); lines stay in
  // per-thread chronological order but are grouped thread-by-thread, not
  // globally interleaved by wall-clock time (no absolute timestamp is
  // recorded per line, only durations).
  {
    std::ofstream consolidated("stats_all.log");
    auto append_shard = [&](const std::string &label) {
      std::ifstream in("stats_t" + label + ".log");
      std::string line;
      while (std::getline(in, line)) {
        consolidated << "[t" << label << "] " << line << "\n";
      }
    };
    if (!env->load_file.empty()) append_shard("load");
    for (unsigned int t = 0; t < T; t++) append_shard(std::to_string(t));
  }
#endif // PER_OP_TIMER

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

#ifdef TOTAL_TIMER
  // Mirrors run_workload.cc's single-threaded "Workload Execution Time"
  // line -- this is the one run-level (not per-thread) timing quantity
  // that previously had no home in workload.log at all (only in
  // throughput.csv/stderr).
  (*summary_buffer) << "=====================" << std::endl;
  (*summary_buffer) << "Workload Execution Time: " << total_exec_time
                    << std::endl;
  (*summary_buffer) << "Threads: " << T << std::endl;
  (*summary_buffer) << "Total Ops: " << total_ops << std::endl;
  (*summary_buffer) << "Ops Per Sec: " << ops_per_sec << std::endl;
#endif // TOTAL_TIMER

#ifdef PER_OP_TIMER
  // End-to-end per-op-type execution time, same 6 labels run_workload.cc
  // prints for its single thread, here summed across all T timed shard
  // threads -- this is a real, meaningful total: inserts_exec_time already
  // accumulates actual db->Write() time per written batch (write_pending_batch,
  // above), batching only changes what counts as "one operation" for the
  // dense per-op log, not whether the time is tracked. Note this is a SUM
  // of concurrently-elapsed per-thread time, not wall-clock time, so it can
  // (and usually will) exceed Workload Execution Time above when T > 1 --
  // that's expected, not a bug: T threads doing real work in parallel for
  // total_seconds each contribute up to total_seconds of their own time.
  unsigned long total_inserts_exec_time = 0, total_updates_exec_time = 0,
               total_pq_exec_time = 0, total_pdelete_exec_time = 0,
               total_rq_exec_time = 0, total_merge_exec_time = 0;
  for (const auto &r : results) {
    total_inserts_exec_time += r.inserts_exec_time;
    total_updates_exec_time += r.updates_exec_time;
    total_pq_exec_time += r.pq_exec_time;
    total_pdelete_exec_time += r.pdelete_exec_time;
    total_rq_exec_time += r.rq_exec_time;
    total_merge_exec_time += r.merge_exec_time;
  }
  (*summary_buffer) << "=====================" << std::endl;
  (*summary_buffer) << "Inserts Execution Time: " << total_inserts_exec_time
                    << std::endl;
  (*summary_buffer) << "Updates Execution Time: " << total_updates_exec_time
                    << std::endl;
  (*summary_buffer) << "PointQuery Execution Time: " << total_pq_exec_time
                    << std::endl;
  (*summary_buffer) << "PointDelete Execution Time: " << total_pdelete_exec_time
                    << std::endl;
  (*summary_buffer) << "RangeQuery Execution Time: " << total_rq_exec_time
                    << std::endl;
  (*summary_buffer) << "Merge Execution Time: " << total_merge_exec_time
                    << std::endl;

  // The load phase's own timing, shown separately from the T-thread
  // measurement above (not because it isn't measured -- it is, both wall
  // clock and per-op-type -- but because it's a one-time setup step, not
  // part of the per-thread-count throughput this harness plots).
  if (did_load) {
    (*summary_buffer) << "=====================" << std::endl;
    (*summary_buffer) << "[load phase] Execution Time: " << load_exec_time
                      << std::endl;
    (*summary_buffer) << "[load phase] Inserts Execution Time: "
                      << load_result.inserts_exec_time << std::endl;
    (*summary_buffer) << "[load phase] Updates Execution Time: "
                      << load_result.updates_exec_time << std::endl;
    (*summary_buffer) << "[load phase] PointQuery Execution Time: "
                      << load_result.pq_exec_time << std::endl;
    (*summary_buffer) << "[load phase] PointDelete Execution Time: "
                      << load_result.pdelete_exec_time << std::endl;
    (*summary_buffer) << "[load phase] RangeQuery Execution Time: "
                      << load_result.rq_exec_time << std::endl;
    (*summary_buffer) << "[load phase] Merge Execution Time: "
                      << load_result.merge_exec_time << std::endl;
    (*summary_buffer) << "[load phase] Ops: " << load_result.ops
                      << std::endl;
  }
#endif // PER_OP_TIMER

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
