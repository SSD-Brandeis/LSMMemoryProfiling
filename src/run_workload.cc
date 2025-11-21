#include "run_workload.h"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <tuple>
#include <random>

#include "config_options.h"
#include "utils.h"

// +++  common prefix exp +++
//  generate a random key of a given length
std::string generate_random_key(size_t length) {
    static const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 2);

    static std::random_device rd;
    static std::mt19937 generator(rd());
    std::uniform_int_distribution<int> distribution(0, max_index);

    std::string random_string(length, 0);
    std::generate_n(random_string.begin(), length, [&]() {
        return charset[distribution(generator)];
    });
    return random_string;
}
//  reads workload.txt, modifies scan operations ,
// and overwrites the workload.txt file
void preprocess_workload_inplace(std::unique_ptr<DBEnv> &env) {
    // remember to set this in .sh script bro if we are running common preifix config exp
    // const char* common_prefix_env = std::getenv("COMMON_PREFIX_C");
    // if (common_prefix_env == nullptr) {
    //     return; 
    // }

    // int c = -1;
    // try {
    //     c = std::stoi(common_prefix_env);
    // } catch (...) {
    //     std::cout << "something wrong with Invalid COMMON_PREFIX_C. Workload file will not be modified." << std::endl;
    //     return;
    // }

    // if (c < 0) return;
    // this is for randomly generate RQ for prefix length exp. 
    // Use the prefix_length from the command-line argument as the trigger
    int c = env->prefix_length;

    // If prefix_length is  negative, skip preprocessing.
    // 
    if (c < 0) {
        std::cout << "prefix_length <= 0. Workload file will not be modified." << std::endl;
        return;
    }
    // Phase 1: Read the  original workload 
    std::vector<std::string> workload_lines;
    std::string line;
    std::ifstream workload_in("workload.txt");
    if (!workload_in) {
        std::cout << "Error: Could not open workload.txt for reading." << std::endl;
        exit(1);
    }
    while (std::getline(workload_in, line)) {
        workload_lines.push_back(line);
    }
    workload_in.close();

    // Phase 2: overwrite scan wl 
    for (std::string& current_line : workload_lines) {
        if (current_line.empty() || current_line[0] != 'S') {
            continue; // Skip non-scan lines
        }

        std::istringstream stream(current_line);
        char operation;
        std::string start_key, end_key;
        stream >> operation >> start_key >> end_key;

        size_t key_len = start_key.length();
        int safe_c = (c > key_len) ? key_len : c;

        // --- swap but do not discard ---

        // Generate the first key. This will become the start_key.
        start_key = generate_random_key(key_len);
        
        // Generate the second key based on the first key's prefix.
        // std::string prefix = start_key.substr(0, safe_c);
        // std::string suffix = generate_random_key(key_len - safe_c);
        // end_key = prefix + suffix;
        end_key = generate_random_key(key_len);

        // If the keys are identical or in the wrong order, swap them.
        if (start_key >= end_key) {
            std::swap(start_key, end_key);
        }

        // reconstruct new key
        std::ostringstream oss;
        oss << "S " << start_key << " " << end_key;
        current_line = oss.str();
    }

    // Phase 3: write back to worklaod.txt with generated start/end key
    std::ofstream workload_out("workload.txt", std::ios::trunc);
    if (!workload_out) {
        std::cout << "Error: Could not open workload.txt for writing." << std::endl;
        exit(1);
    }
    for (const std::string& modified_line : workload_lines) {
        workload_out << modified_line << std::endl;
    }
    workload_out.close();
}
// +++ END of common prefix exp +++

std::string buffer_file = "workload.log";
std::string stats_file = "stats.log";
std::string selectvity_file = "selectivity.log";

int runWorkload(std::unique_ptr<DBEnv> &env) {
  // preprocess_workload_inplace(env);
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
  std::unique_ptr<Buffer> selectivity =
      std::make_unique<Buffer>(selectvity_file);

  // Add custom listners
  std::shared_ptr<CompactionsListner> compaction_listener =
      std::make_shared<CompactionsListner>();
  options.listeners.emplace_back(compaction_listener);

  std::shared_ptr<FlushListner> flush_listener =
      std::make_shared<FlushListner>(buffer);
  options.listeners.emplace_back(flush_listener);

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(env->kDBPath, options);
    // std::cout << "Destroying database ... done" << std::endl;
  }

  PrintExperimentalSetup(env, buffer);

  Status s = DB::Open(options, env->kDBPath, &db);
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());

#ifdef DOSTO
  if (env->debugging) {
    tree->SetDebugMode(env->debugging);
    tree->PrintFluidLSM(db);
  }
#endif // DOSTO

  // Clearing the system cache
  if (env->clear_system_cache) {
#ifdef __linux__
    // std::cout << "Clearing system cache ...";
    std::cout << system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
              << "done" << std::endl;
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

#ifdef GET_TIMER
  unsigned long inserts_exec_time = 0, updates_exec_time = 0, pq_exec_time = 0,
                pdelete_exec_time = 0, rq_exec_time = 0;
#endif // GET_TIMER
  auto exec_start = std::chrono::high_resolution_clock::now();

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

#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER
      // std::cout << "put operation: " << std::endl <<std::flush;
      s = db->Put(write_options, key, value);
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "InsertTime: " << duration.count() << std::endl;
      inserts_exec_time += duration.count();
#endif // TIMER
      break;
    }
      // [Update]
    case 'U': {
      std::string key, value;
      stream >> key >> value;

#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER
      s = db->Put(write_options, key, value);
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "UpdateTime: " << duration.count() << std::endl;
      updates_exec_time += duration.count();
#endif // TIMER
      break;
    }
      // [PointDelete]
    case 'D': {
      std::string key;
      stream >> key;

#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER
      s = db->Delete(write_options, key);
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "DeleteTime: " << duration.count() << std::endl;
      pdelete_exec_time += duration.count();
#endif // TIMER
      break;
    }
      // [ProbePointQuery]
    case 'Q': {
      std::string key, value;
      stream >> key;

#ifdef GET_TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // GET_TIMER
      // std::cout << "get operation: " << std::endl <<std::flush;
      s = db->Get(read_options, key, &value);
 
      // std::cout << "Key: " << key << std::endl;
#ifdef GET_TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "GetTime: " << duration.count() << std::endl;
      pq_exec_time += duration.count();
#endif // GET_TIMER
      break;
    }
      // [ScanRangeQuery]
    case 'S': {
      std::string start_key, end_key;
      stream >> start_key >> end_key;

      uint64_t keys_returned = 0, keys_read = 0;
      ReadOptions scan_read_options = ReadOptions(read_options);
      // based on the prefix length X.
      // read X character from the start and end key
      // if both are identical, then set the total_order_seek to false. Otherwise, set it to true.
      // const size_t prefix_length = env->prefix_length;

      // if (start_key.compare(0, prefix_length, end_key, 0, prefix_length) == 0) {
      //   scan_read_options.total_order_seek = false;
      // } else {
      //   scan_read_options.total_order_seek = true;
      // }
      scan_read_options.total_order_seek = true;
      Iterator *it = db->NewIterator(scan_read_options);
      // it->Refresh();
      assert(it->status().ok());

#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER
      // std::cout << "scan operation: " << start_key << " endkey: " << end_key << std::endl <<std::flush;
      for (it->Seek(start_key); it->Valid(); it->Next()) {
        if (it->key().ToString() >= end_key) {
          break;
        }
        // std::cout << "Key: " << it->key().ToString() << std::endl;
        keys_returned++;
      }
      (*selectivity) << "keys_returned: " << keys_returned << ", selectivity: " << (keys_returned/env->num_inserts) << std::endl;
      if (!it->status().ok()) {
        (*buffer) << it->status().ToString() << std::endl << std::flush;
      }
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "ScanTime: " << duration.count() << std::endl;
      rq_exec_time += duration.count();
#endif // TIMER
      delete it;
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
  LogTreeState(db, buffer);
  LogRocksDBStatistics(db, options, buffer);
#endif // PROFILE

  auto total_exec_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now() - exec_start)
          .count();
#ifdef GET_TIMER
  (*buffer) << "=====================" << std::endl;
  (*buffer) << "Workload Execution Time: " << total_exec_time << std::endl;
  (*buffer) << "Inserts Execution Time: " << inserts_exec_time << std::endl;
  (*buffer) << "Updates Execution Time: " << updates_exec_time << std::endl;
  (*buffer) << "PointQuery Execution Time: " << pq_exec_time << std::endl;
  (*buffer) << "PointDelete Execution Time: " << pdelete_exec_time << std::endl;
  (*buffer) << "RangeQuery Execution Time: " << rq_exec_time << std::endl;
#endif // GET_TIMER

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
  (*buffer) << "===========END HERE=========\n";

  // flush final stats and delete ptr
  buffer->flush();
  stats->flush();
  long long total_seconds = total_exec_time / 1e9;
  std::cout << "Experiment completed in " << total_seconds / 3600 << "h "
            << (total_seconds % 3600) / 60 << "m " << total_seconds % 60 << "s "
            << std::endl;
  return 0;
}