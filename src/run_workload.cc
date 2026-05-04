#include "run_workload.h"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <tuple>
#include <vector>
#include <thread>
#include <atomic>

#include "config_options.h"
#include "utils.h"
#include "workload_monitor.h"

std::string buffer_file = "workload.log";
std::string stats_file = "stats.log";

#ifdef PER_OP_TIMER
std::atomic<unsigned long> inserts_exec_time(0);
std::atomic<unsigned long> updates_exec_time(0);
std::atomic<unsigned long> pq_exec_time(0);
std::atomic<unsigned long> pdelete_exec_time(0);
std::atomic<unsigned long> rq_exec_time(0);
std::atomic<unsigned long> merge_exec_time(0);
#endif 

std::atomic<unsigned long> global_ith_op(0);

void processWorkloadSlice(DB *db, const std::vector<std::string> &workload_lines, 
                          size_t start_idx, size_t end_idx, 
                          const WriteOptions &write_options, const ReadOptions &read_options, 
                          std::unique_ptr<Buffer> &stats, std::unique_ptr<DBEnv> &env, 
                          std::shared_ptr<Buffer> &buffer, bool use_prefix_seek, 
                          size_t total_operations) {
    
    unsigned long local_i_time = 0;
    unsigned long local_u_time = 0;
    unsigned long local_pq_time = 0;
    unsigned long local_pd_time = 0;
    unsigned long local_rq_time = 0;
    unsigned long local_m_time = 0;

    for (size_t i = start_idx; i < end_idx; ++i) {
        const std::string &line = workload_lines[i];
        if (line.empty()) continue;

        std::istringstream stream(line);
        char operation;
        stream >> operation;
        Status s;

        switch (operation) {
        case 'I': {
            std::string key, value;
            stream >> key >> value;
#ifdef PER_OP_TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            s = db->Put(write_options, key, value);
#ifdef PER_OP_TIMER
            auto stop = std::chrono::high_resolution_clock::now();
            local_i_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
#endif
            GlobalWorkloadMonitor().RecordInsert();
            break;
        }
        case 'U': {
            std::string key, value;
            stream >> key >> value;
#ifdef PER_OP_TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            s = db->Put(write_options, key, value);
#ifdef PER_OP_TIMER
            auto stop = std::chrono::high_resolution_clock::now();
            local_u_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
#endif
            GlobalWorkloadMonitor().RecordUpdate();
            break;
        }
        case 'D': {
            std::string key;
            stream >> key;
#ifdef PER_OP_TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            s = db->Delete(write_options, key);
#ifdef PER_OP_TIMER
            auto stop = std::chrono::high_resolution_clock::now();
            local_pd_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
#endif
            GlobalWorkloadMonitor().RecordPointDelete();
            break;
        }
        case 'P':
        case 'Q': {
            std::string key, value;
            stream >> key;
#ifdef PER_OP_TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            s = db->Get(read_options, key, &value);
#ifdef PER_OP_TIMER
            auto stop = std::chrono::high_resolution_clock::now();
            local_pq_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
#endif
            GlobalWorkloadMonitor().RecordPointQuery();
            break;
        }
        case 'S': {
            const bool is_count_scan = (stream.peek() == 'C');
            if (is_count_scan) stream.get();
            std::string start_key;
            stream >> start_key;
            ReadOptions scan_read_options = ReadOptions(read_options);
            scan_read_options.total_order_seek = !use_prefix_seek;
            Iterator *it = db->NewIterator(scan_read_options);
#ifdef PER_OP_TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            if (is_count_scan) {
                uint64_t scan_len; stream >> scan_len; uint64_t steps = 0;
                for (it->Seek(start_key); it->Valid() && steps < scan_len; it->Next(), ++steps) {}
            } else {
                std::string end_key; stream >> end_key;
                if (env->common_prefix_len > 0) {
                    const size_t prefix_bytes = std::min((size_t)env->common_prefix_len, start_key.size());
                    end_key = start_key.substr(0, prefix_bytes) + end_key.substr(prefix_bytes);
                }
                for (it->Seek(start_key); it->Valid(); it->Next()) { if (it->key().ToString() >= end_key) break; }
            }
#ifdef PER_OP_TIMER
            auto stop = std::chrono::high_resolution_clock::now();
            local_rq_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
#endif
            if (!it->status().ok()) { (*buffer) << it->status().ToString() << std::endl << std::flush; }
            GlobalWorkloadMonitor().RecordRangeQuery();
            delete it;
            break;
        }
        case 'M': {
            std::string start_key, end_key; stream >> start_key >> end_key;
#ifdef PER_OP_TIMER
            auto start = std::chrono::high_resolution_clock::now();
#endif
            s = db->Merge(write_options, start_key, end_key);
#ifdef PER_OP_TIMER
            auto stop = std::chrono::high_resolution_clock::now();
            local_m_time += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
#endif
            GlobalWorkloadMonitor().RecordUpdate();
            break;
        }
        default: break;
        }

        unsigned long current_op = ++global_ith_op;
        if (env->IsShowProgressEnabled() && (current_op % std::max((size_t)1, total_operations / 50)) == 0) {
            UpdateProgressBar(env, current_op, total_operations, (int)total_operations * 0.02);
        }
    }

#ifdef PER_OP_TIMER
    inserts_exec_time += local_i_time;
    updates_exec_time += local_u_time;
    pq_exec_time += local_pq_time;
    pdelete_exec_time += local_pd_time;
    rq_exec_time += local_rq_time;
    merge_exec_time += local_m_time;
#endif
}

int runWorkload(std::unique_ptr<DBEnv> &env) {
  GlobalWorkloadMonitor().Configure(env->entries_per_page * env->buffer_size_in_pages, env->bucket_count);

  DB *db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;

 
  env->allow_concurrent_memtable_write = true; 
  // env->max_background_jobs = 8; 
  env->max_background_jobs = std::max(2, env->num_threads);

  configOptions(env, &options, &table_options, &write_options, &read_options, &flush_options);

  std::shared_ptr<Buffer> buffer = std::make_unique<Buffer>(buffer_file);
  std::unique_ptr<Buffer> stats = std::make_unique<Buffer>(stats_file);

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(env->kDBPath, options);
    std::cerr << "Destroying database ... done" << std::endl;
  }

  PrintExperimentalSetup(env, buffer);

  Status s = DB::Open(options, env->kDBPath, &db);
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  if (env->clear_system_cache) {
#ifdef __linux__
    std::cerr << "Clearing system cache ...";
    std::cerr << system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'") << " done" << std::endl;
#endif
  }

  std::vector<std::string> workload_lines;
  std::ifstream workload_file("workload.txt");
  assert(workload_file);
  
  std::string file_line;
  while (std::getline(workload_file, file_line)) { if (!file_line.empty()) workload_lines.push_back(file_line); }
  size_t total_operations = workload_lines.size();

  global_ith_op = 0;
#ifdef PER_OP_TIMER
  inserts_exec_time = updates_exec_time = pq_exec_time = pdelete_exec_time = rq_exec_time = merge_exec_time = 0;
#endif 

#ifdef TOTAL_TIMER
  auto exec_start = std::chrono::high_resolution_clock::now();
#endif 

  const bool use_prefix_seek = (env->common_prefix_len > 0 && env->common_prefix_len >= env->prefix_length);
  int num_workers = env->num_threads > 0 ? env->num_threads : 1;
  std::vector<std::thread> workers;
  size_t ops_per_thread = total_operations / num_workers;

  std::cerr << "Spawning " << num_workers << " thread(s) for " << total_operations << " operations..." << std::endl;

  for (int i = 0; i < num_workers; ++i) {
      size_t start_idx = i * ops_per_thread;
      size_t end_idx = (i == num_workers - 1) ? total_operations : (i + 1) * ops_per_thread;
      workers.emplace_back(processWorkloadSlice, db, std::ref(workload_lines), start_idx, end_idx,
                           std::ref(write_options), std::ref(read_options), std::ref(stats), 
                           std::ref(env), std::ref(buffer), use_prefix_seek, total_operations);
  }

  for (auto &t : workers) { if (t.joinable()) t.join(); }
  if (env->IsShowProgressEnabled()) std::cout << std::endl;

#ifdef PROFILE
  (*buffer) << "=====================" << std::endl;
  LogTreeState(db, buffer, env); 
#endif 

#ifdef TOTAL_TIMER
  auto total_exec_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now() - exec_start).count();
#endif 

#ifdef TOTAL_TIMER
  (*buffer) << "Workload Execution Time: " << total_exec_time << std::endl;
#endif 
#ifdef PER_OP_TIMER
  (*buffer) << "Inserts Execution Time: " << inserts_exec_time.load() << std::endl;
  (*buffer) << "Updates Execution Time: " << updates_exec_time.load() << std::endl;
  (*buffer) << "PointQuery Execution Time: " << pq_exec_time.load() << std::endl;
  (*buffer) << "PointDelete Execution Time: " << pdelete_exec_time.load() << std::endl;
  (*buffer) << "RangeQuery Execution Time: " << rq_exec_time.load() << std::endl;
  (*buffer) << "Merge Execution Time: " << merge_exec_time.load() << std::endl;
#endif 

  double total_seconds = total_exec_time / 1e9;
  (*buffer) << "Throughput: " << (long)((double)total_operations / total_seconds) << " ops" << std::endl;

  db->Close();
  PrintRocksDBPerfStats(env, buffer, options);
  buffer->flush();
  stats->flush();

  std::cerr << "Experiment completed in " << total_seconds << "s" << std::endl;
  return 0;
}