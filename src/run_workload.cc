#include "run_workload.h"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <tuple>

#include "config_options.h"
#include "utils.h"
#include "workload_monitor.h"

std::string buffer_file = "workload.log";
std::string stats_file = "stats.log";

int runWorkload(std::unique_ptr<DBEnv> &env) {
  DB *db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;

  configOptions(env, &options, &table_options, &write_options, &read_options,
                &flush_options);

  std::shared_ptr<Buffer> buffer = std::make_unique<Buffer>(buffer_file);
  std::unique_ptr<Buffer> stats = std::make_unique<Buffer>(stats_file);

  // // Add custom listners
  // std::shared_ptr<CompactionsListner> compaction_listener =
  //     std::make_shared<CompactionsListner>(env);
  // options.listeners.emplace_back(compaction_listener);

  // std::shared_ptr<FlushListner> flush_listener =
  //     std::make_shared<FlushListner>(buffer);
  // options.listeners.emplace_back(flush_listener);

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(env->kDBPath, options);
    std::cerr << "Destroying database ... done" << std::endl;
  }

  PrintExperimentalSetup(env, buffer);

  Status s = DB::Open(options, env->kDBPath, &db);
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  // Clearing the system cache
  if (env->clear_system_cache) {
#ifdef __linux__
    std::cerr << "Clearing system cache ...";
    std::cerr << system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
              << " done" << std::endl;
#endif
  }

  std::ifstream workload_file;
  workload_file.open("workload.txt");
  assert(workload_file);

  size_t total_operations = 0;
  if (env->IsShowProgressEnabled()) {
    std::string line;
    while (std::getline(workload_file, line)) {
      ++total_operations;
    }
  }

  workload_file.clear();
  workload_file.seekg(0, std::ios::beg);

#ifdef PER_OP_TIMER
  unsigned long inserts_exec_time = 0, updates_exec_time = 0, pq_exec_time = 0,
                pdelete_exec_time = 0, rq_exec_time = 0, merge_exec_time = 0;
#endif // PER_OP_TIMER

#ifdef TOTAL_TIMER
  auto exec_start = std::chrono::high_resolution_clock::now();
#endif // TOTAL_TIMER

  if (env->IsPerfStatEnabled())
    rocksdb::get_perf_context()->Reset();
  if (env->IsIOStatEnabled())
    rocksdb::get_iostats_context()->Reset();

  // Precompute whether to disable total_order_seek for prefix-bounded scans.
  // When common_prefix_len == prefix_length, the scan is fully prefix-bounded
  // and hash-based memtables can use the prefix extractor directly.
  const bool use_prefix_seek =
      (env->common_prefix_len > 0 &&
       env->common_prefix_len >= env->prefix_length);

  std::string line;
  unsigned long ith_op = 0;
  while (std::getline(workload_file, line)) {
    if (line.empty())
      break;
    bool is_last_line = (workload_file.peek() == EOF);

    std::istringstream stream(line);
    char operation;
    stream >> operation;

    switch (operation) {
      // [Insert]
    case 'I': {
      std::string key, value;
      stream >> key >> value;

#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
      s = db->Put(write_options, key, value);
      GlobalWorkloadMonitor().RecordInsert();
#ifdef PER_OP_TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "I: " << duration.count() << std::endl;
      inserts_exec_time += duration.count();
#endif // PER_OP_TIMER
      break;
    }
      // [Update]
    case 'U': {
      std::string key, value;
      stream >> key >> value;

#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
      s = db->Put(write_options, key, value);
      GlobalWorkloadMonitor().RecordUpdate();
#ifdef PER_OP_TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "U: " << duration.count() << std::endl;
      updates_exec_time += duration.count();
#endif // PER_OP_TIMER
      break;
    }
      // [PointDelete]
    case 'D': {
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
      std::string key, value;
      stream >> key;

#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
      s = db->Get(read_options, key, &value);
      GlobalWorkloadMonitor().RecordPointQuery();
      // if (s.IsNotFound()) {
      //   std::cout << key << ", Not Found" << std::endl;
      // } else if (s.ok()) {
      //   std::cout << key << ", " << value << std::endl;
      // } else {
      //   std::cout << "Error reading key " << key << ": " << s.ToString()
      //             << std::endl;
      // }

#ifdef PER_OP_TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "Q: " << duration.count() << std::endl;
      pq_exec_time += duration.count();
#endif // PER_OP_TIMER
      break;
    }
      // [ScanRangeQuery]
      // Handles two formats produced by Tectonic:
      //   "S  <start_key> <end_key>"   — StartEnd  (key comparison termination)
      //   "SC <start_key> <scan_len>"  — StartCount (YCSB-style fixed-length scan)
      // After the switch reads 'S', peek at the next character: if it is 'C'
      // consume it and treat the second token as a step count, otherwise treat
      // it as an end key.
    case 'S': {
      // Detect "SC": next char on the stream is 'C' with no whitespace between.
      const bool is_count_scan = (stream.peek() == 'C');
      if (is_count_scan) stream.get(); // consume the 'C'

      std::string start_key;
      stream >> start_key;

      ReadOptions scan_read_options = ReadOptions(read_options);
      scan_read_options.total_order_seek = !use_prefix_seek;

      Iterator *it = db->NewIterator(scan_read_options);
      assert(it->status().ok());

#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER

      if (is_count_scan) {
        // SC <start_key> <scan_len> — iterate exactly scan_len steps.
        uint64_t scan_len;
        stream >> scan_len;

        uint64_t steps = 0;
        for (it->Seek(start_key); it->Valid() && steps < scan_len;
             it->Next(), ++steps) {
        }
      } else {
        // S <start_key> <end_key> — iterate until key >= end_key.
        std::string end_key;
        stream >> end_key;

        // Synthesise a prefix-controlled end key when common_prefix_len > 0.
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
        // std::cout << "Key: " << it->key().ToString()
        //           << " Value: " << it->value().ToString() << std::endl;
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
      std::string start_key, end_key;
      stream >> start_key >> end_key;
      s = db->DeleteRange(write_options, start_key, end_key);
      GlobalWorkloadMonitor().RecordRangeDelete();
      break;
    }
    // [ReadModifyWrite]
    case 'M': {
#ifdef PER_OP_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // PER_OP_TIMER
      std::string start_key, end_key;
      stream >> start_key >> end_key;
      s = db->Merge(write_options, start_key, end_key);
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
    UpdateProgressBar(env, ith_op, total_operations,
                      (int)total_operations * 0.02);
    if (is_last_line)
      break;
  }

#ifdef PROFILE
  (*buffer) << "=====================" << std::endl;
  LogTreeState(db, buffer, env);
  // LogRocksDBStatistics(db, options, buffer);
#endif // PROFILE

#ifdef TOTAL_TIMER
  auto total_exec_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now() - exec_start)
          .count();
#endif // TOTAL_TIMER

#ifdef PER_OP_TIMER
  (*buffer) << "=====================" << std::endl;
#endif // PER_OP_TIMER
#ifdef TOTAL_TIMER
  (*buffer) << "Workload Execution Time: " << total_exec_time << std::endl;
#endif // TOTAL_TIMER
#ifdef PER_OP_TIMER
  (*buffer) << "Inserts Execution Time: " << inserts_exec_time << std::endl;
  (*buffer) << "Updates Execution Time: " << updates_exec_time << std::endl;
  (*buffer) << "PointQuery Execution Time: " << pq_exec_time << std::endl;
  (*buffer) << "PointDelete Execution Time: " << pdelete_exec_time << std::endl;
  (*buffer) << "RangeQuery Execution Time: " << rq_exec_time << std::endl;
  (*buffer) << "Merge Execution Time: " << merge_exec_time << std::endl;
#endif // PER_OP_TIMER

  // tree->BuildStructure(db); //rebuild structure after each input
  // tree->PrintFluidLSM(db);
  // close db
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  s = db->Close();
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  PrintRocksDBPerfStats(env, buffer, options);
  table_options.block_cache.reset();
  options.table_factory.reset();

  // flush final stats and delete ptr
  buffer->flush();
  stats->flush();
#ifdef TOTAL_TIMER
  long long total_seconds = total_exec_time / 1e9;
  std::cerr << "\nExperiment completed in " << total_seconds / 3600 << "h "
            << (total_seconds % 3600) / 60 << "m " << total_seconds % 60 << "s "
            << std::endl;
#endif // TOTAL_TIMER
  return 0;
}
