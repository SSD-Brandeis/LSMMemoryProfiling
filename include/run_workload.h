#include <rocksdb/db.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/table.h>

#include <fstream>
#include <iomanip>
#include <thread>

#include "aux_time.h"
#include "config_options.h"
#include "db_env.h"
#include "stats.h"

std::string kDBPath = "./db";
std::mutex mtx;
std::condition_variable cv;
bool compaction_complete = false;

struct FlushEvent {
  std::chrono::steady_clock::time_point timestamp_;
  uint64_t num_entries_;
  uint64_t data_size_;
  uint64_t index_size_;
  uint64_t filter_size_;
};

struct CompactionEvent {
  std::chrono::steady_clock::time_point timestamp_;
  uint64_t num_input_files_;
  uint64_t num_output_files_;
  uint64_t num_entries_inp_;
  uint64_t data_size_inp_;
  uint64_t index_size_inp_;
  uint64_t filter_size_inp_;
  uint64_t num_entries_out_;
  uint64_t data_size_out_;
  uint64_t index_size_out_;
  uint64_t filter_size_out_;
};

// std::vector<InsertEvent> insert_events_;
// std::vector<PointQueryEvent> point_query_events_;
// std::vector<FlushEvent> flush_events_;
// std::vector<CompactionEvent> compaction_events_;

using hrc = std::chrono::high_resolution_clock;
using ns = std::chrono::nanoseconds;
using std::chrono::duration_cast;

#define LOG(msg) \
std::cout << __FILE__ << "(" << __LINE__ << "): " << msg << std::endl



/*
 * The compactions can run in background even after the workload is completely
 * executed so, we have to wait for them to complete. Compaction Listener gets
 * notified by the rocksdb API for every compaction that just finishes off.
 * After every compaction we check, if more compactions are required with
 * `WaitForCompaction` function, if not then it signals to close the db
 */
class CompactionsListner : public EventListener {
 public:
  explicit CompactionsListner() {}

  void OnCompactionCompleted(
      DB* /*db*/, const CompactionJobInfo& compaction_job_info) override {
    std::lock_guard<std::mutex> lock(mtx);
    compaction_complete = true;
    cv.notify_one();

    if (compaction_job_info.compaction_reason != CompactionReason::kFlush) {
      uint64_t num_entries_inp = 0;
      uint64_t data_size_inp = 0;
      uint64_t index_size_inp = 0;
      uint64_t filter_size_inp = 0;
      uint64_t num_entries_out = 0;
      uint64_t data_size_out = 0;
      uint64_t index_size_out = 0;
      uint64_t filter_size_out = 0;

      for (const auto& pair : compaction_job_info.table_properties) {
        const std::shared_ptr<const TableProperties>& tp = pair.second;

        if (std::find(compaction_job_info.input_files.begin(),
                      compaction_job_info.input_files.end(),
                      pair.first) != compaction_job_info.input_files.end()) {
          num_entries_inp += tp->num_entries;
          data_size_inp += tp->data_size;
          index_size_inp += tp->index_size;
          filter_size_inp += tp->filter_size;
        } else {
          num_entries_out += tp->num_entries;
          data_size_out += tp->data_size;
          index_size_out += tp->index_size;
          filter_size_out += tp->filter_size;
        }
      }

    //   compaction_events_.push_back(CompactionEvent{
    //       std::chrono::steady_clock::now(),
    //       compaction_job_info.input_files.size(),
    //       compaction_job_info.output_files.size(), num_entries_inp,
    //       data_size_inp, index_size_inp, filter_size_inp, num_entries_out,
    //       data_size_out, index_size_out, filter_size_out});
    }
  }
};

/*
 * The flushes are happening in background and they send a event
 * once they are complete. This is to track the bytes written
 */
class BufferFlushListner : public EventListener {
 public:
  explicit BufferFlushListner() {}

  void OnFlushCompleted(DB* /*db*/,
                        const FlushJobInfo& flush_job_info) override {
    if (flush_job_info.flush_reason == FlushReason::kWriteBufferFull ||
        flush_job_info.flush_reason == FlushReason::kGetLiveFiles) {
      TableProperties tp = flush_job_info.table_properties;
      // flush_events_.push_back(FlushEvent{std::chrono::steady_clock::now(),
      //                                    tp.num_entries, tp.data_size,
      //                                    tp.index_size, tp.filter_size});
    }
  }
};

/*
 * Wait for compactions that are running (or will run) to make the
 * LSM tree in its shape. Check `CompactionListner` for more details.
 */
void WaitForCompactions(rocksdb::DB* db) {
  std::unique_lock<std::mutex> lock(mtx);
  uint64_t num_running_compactions;
  uint64_t pending_compaction_bytes;
  uint64_t num_pending_compactions;

  while (!compaction_complete) {
    // Check if there are ongoing or pending compactions
    db->GetIntProperty("rocksdb.num-running-compactions",
                       &num_running_compactions);
    db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes",
                       &pending_compaction_bytes);
    db->GetIntProperty("rocksdb.compaction-pending", &num_pending_compactions);
    if (num_running_compactions == 0 && pending_compaction_bytes == 0 &&
        num_pending_compactions == 0) {
      break;
    }
    cv.wait(lock);
  }
}

void runWorkload(DBEnv* env) {
  DB* db;
  Options options;
  WriteOptions w_options;
  ReadOptions r_options;
  BlockBasedTableOptions table_options;
  FlushOptions f_options;
  auto start_time_point = std::chrono::steady_clock::now();

  configOptions(env, &options, &table_options, &w_options, &r_options,
                &f_options);

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(kDBPath, options);
    // std::cout << "Destroying database ..." << std::endl;
  }

  std::shared_ptr<CompactionsListner> listener =
      std::make_shared<CompactionsListner>();
  std::shared_ptr<BufferFlushListner> bf_listner =
      std::make_shared<BufferFlushListner>();
  options.listeners.emplace_back(listener);
  options.listeners.emplace_back(bf_listner);

  Status s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  Stats* fade_stats = Stats::getInstance();
  fade_stats->db_open = true;

  int current_level = fade_stats->levels_in_tree;



  const auto TIMES = 500;
  for (int t = 0; t < 500; ++t) {
    if (t % 100 == 0) LOG("===" << t << "===");
    long total_time = 0;
    for (size_t j = 0; j < TIMES; ++j) {
      size_t i = TIMES - j;
      std::string key = "k" + std::to_string(i) + std::string(5, '0');
      std::string val = std::to_string(i) + std::string(5, '0') + std::string(10, 'v');
      auto start = hrc::now();
      db->Put(w_options, key.substr(0, 5), val.substr(0, 11));
      auto duration = duration_cast<ns>(hrc::now() - start);
      total_time += duration.count();
      // workload_stats.add(DBOperation::Insert);
    }
    LOG("Time: " << total_time / 1000000);
    // std::string val;
    // for (size_t i = 0; i < TIMES / 10; ++i) {
    //     std::string key = "k" + std::to_string(i) + std::string(5, '0');
    //     db->Get(r_options, key.substr(0, 5), &val);
    //     workload_stats.add(DBOperation::PointQuery);
    // }
  }

  return;
  // opening workload file for the first time
  ifstream workload_file;
  workload_file.open("workload.txt");
  assert(workload_file);
  // doing a first pass to get the workload size
  uint32_t workload_size = 0;
  std::string line;
  while (std::getline(workload_file, line)) ++workload_size;
  workload_file.close();

  // !YBS-sep09-XX!
  // Clearing the system cache
  if (env->clear_system_cache) {
    // std::cout << "Clearing system cache ..." << std::endl;
    auto _ = system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
  }
  // START stat collection
  if (env->IsPerfIOStatEnabled()) {
    // begin perf/iostat code
    rocksdb::SetPerfLevel(
        rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_perf_context()->ClearPerLevelPerfContext();
    rocksdb::get_perf_context()->EnablePerLevelPerfContext();
    rocksdb::get_iostats_context()->Reset();
    // end perf/iostat code
  }

  // re-opening workload file to execute workload
  workload_file.open("workload.txt");
  assert(workload_file);

  Iterator* it;          // = db->NewIterator(r_options);  // for range reads
  uint32_t counter = 0;  // for progress bar

  // !YBS-sep09-XX!
  my_clock start_time, end_time;
  if (my_clock_get_time(&start_time) == -1)
    std::cerr << "Failed to get experiment start time" << std::endl;

  env->level0_file_num_compaction_trigger = 1;
  env->level0_stop_writes_trigger = 1;

  // time variables for measuring the time taken by the workload
  // std::chrono::nanoseconds start, end;
  std::chrono::time_point<std::chrono::high_resolution_clock> insert_start,
      insert_end;
  std::chrono::time_point<std::chrono::high_resolution_clock> query_start,
      query_end;
  std::chrono::time_point<std::chrono::high_resolution_clock> delete_start,
      delete_end;
  std::chrono::time_point<std::chrono::high_resolution_clock> update_start,
      update_end;
  std::chrono::time_point<std::chrono::high_resolution_clock> range_query_start,
      range_query_end;
  std::chrono::time_point<std::chrono::high_resolution_clock>
      range_delete_start, range_delete_end;
  std::chrono::nanoseconds total_insert_time_elapsed(0);
  std::chrono::nanoseconds total_query_time_elapsed(0);
  std::chrono::nanoseconds total_delete_time_elapsed(0);
  std::chrono::nanoseconds total_update_time_elapsed(0);
  std::chrono::nanoseconds total_range_query_time_elapsed(0);
  std::chrono::nanoseconds total_range_delete_time_elapsed(0);
  auto start = std::chrono::high_resolution_clock::now();

  while (!workload_file.eof()) {
    char instruction;
    long key, start_key, end_key;
    string value;
    workload_file >> instruction;

    switch (instruction) {
      case 'I':  // insert
        workload_file >> key >> value;

        // start measuring the time taken by the insert
        insert_start = std::chrono::high_resolution_clock::now();

        // Put key-value

        s = db->Put(w_options, std::to_string(key), value);
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        assert(s.ok());

        // end measuring the time taken by the insert
        insert_end = std::chrono::high_resolution_clock::now();
        total_insert_time_elapsed +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(insert_end -
                                                                 insert_start);
        // insert_events_.push_back(InsertEvent{std::chrono::steady_clock::now(),
        //                                      insert_end - insert_start});
        counter++;
        fade_stats->inserts_completed++;
        break;

      case 'U':  // update
        workload_file >> key >> value;

        // start measuring the time taken by the update
        update_start = std::chrono::high_resolution_clock::now();

        {
#ifdef PROFILE
          auto update_start_time = std::chrono::high_resolution_clock::now();
#endif  // PROFILE

          // Put key-value
          s = db->Put(w_options, std::to_string(key), value);
          if (!s.ok()) std::cerr << s.ToString() << std::endl;
          assert(s.ok());

#ifdef PROFILE
          auto update_end_time = std::chrono::high_resolution_clock::now();
          std::cout << "UpdateQueryTime: "
                    << std::chrono::duration_cast<std::chrono::nanoseconds>(
                           update_end_time - update_start_time)
                           .count()
                    << std::endl
                    << std::flush;
#endif  // PROFILE
        }

        // end measuring the time taken by the update
        update_end = std::chrono::high_resolution_clock::now();
        total_update_time_elapsed += update_end - update_start;
        counter++;
        fade_stats->updates_completed++;
        break;

      case 'D':  // point delete
        workload_file >> key;

        // start measuring the time taken by the delete
        delete_start = std::chrono::high_resolution_clock::now();
        s = db->Delete(w_options, std::to_string(key));
        assert(s.ok());
        // end measuring the time taken by the delete
        delete_end = std::chrono::high_resolution_clock::now();
        total_delete_time_elapsed += delete_end - delete_start;
        counter++;
        fade_stats->point_deletes_completed++;
        break;

      case 'R':  // range delete
        workload_file >> start_key >> end_key;

        // start measuring the time taken by the range delete
        range_delete_start = std::chrono::high_resolution_clock::now();

        s = db->DeleteRange(w_options, std::to_string(start_key),
                            std::to_string(end_key));
        assert(s.ok());

        // end measuring the time taken by the range delete
        range_delete_end = std::chrono::high_resolution_clock::now();
        total_range_delete_time_elapsed +=
            range_delete_end - range_delete_start;
        counter++;
        fade_stats->range_deletes_completed++;
        break;

      case 'Q':  // probe: point query
        workload_file >> key;

        // start measuring the time taken by the query
        query_start = std::chrono::high_resolution_clock::now();
        s = db->Get(r_options, std::to_string(key), &value);

        // end measuring the time taken by the query
        query_end = std::chrono::high_resolution_clock::now();
        total_query_time_elapsed +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(query_end -
                                                                 query_start);
        // point_query_events_.push_back(PointQueryEvent{
        //     std::chrono::steady_clock::now(), insert_end - insert_start});
        counter++;
        fade_stats->point_queries_completed++;
        break;

      case 'S':  // scan: range query
        workload_file >> start_key >> end_key;

        // start measuring the time taken by the range query
        range_query_start = std::chrono::high_resolution_clock::now();

        {
#ifdef PROFILE
          auto start_time = std::chrono::high_resolution_clock::now();
#endif  // PROFILE

          r_options.total_order_seek = true;
          it = db->NewIterator(r_options);
          it->Refresh();
          assert(it->status().ok());
          for (it->Seek(std::to_string(start_key)); it->Valid(); it->Next()) {
            if (it->key().ToString() >= std::to_string(end_key)) {
              break;
              // std::cout << "found key = " << it->key().ToString() <<
              // std::endl << std::flush;
            }
          }
          if (!it->status().ok()) {
            std::cerr << it->status().ToString() << std::endl;
          }
          r_options.total_order_seek = false;

#ifdef PROFILE
          auto end_time = std::chrono::high_resolution_clock::now();
          std::cout << "RangeQueryTime: "
                    << std::chrono::duration_cast<std::chrono::nanoseconds>(
                           end_time - start_time)
                           .count()
                    << std::endl
                    << std::flush;
#endif  // PROFILE
        }
        // end measuring the time taken by the query
        range_query_end = std::chrono::high_resolution_clock::now();
        total_range_query_time_elapsed += range_query_end - range_query_start;
        counter++;
        fade_stats->range_queries_completed++;
        break;

      default:
        std::cerr << "ERROR: Case match NOT found !!" << std::endl;
        break;
    }
  }

  fade_stats->completion_status = true;

  // // Close DB in a way of detecting errors
  // // followed by deleting the database object when examined to determine if
  // there were any errors.
  // // Regardless of errors, it will release all resources and is irreversible.
  // // Flush the memtable before close
  // Status CloseDB(DB *&db, const FlushOptions &flush_op);

  // end measuring the time taken by the workload
  // and printing the results
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::nanoseconds elapsed_seconds = end - start;

  std::cout
      << "\n----------------------Workload Complete-----------------------"
      << std::endl;
  std::cout << "Total time taken by workload = " << elapsed_seconds.count()
            << " ns" << std::endl;
  std::cout << "Total time taken by inserts = "
            << total_insert_time_elapsed.count() << " ns" << std::endl;
  std::cout << "Total time taken by queries = "
            << total_query_time_elapsed.count() << " ns" << std::endl;
  std::cout << "Total time taken by updates = "
            << total_update_time_elapsed.count() << " ns" << std::endl;
  std::cout << "Total time taken by deletes = "
            << total_delete_time_elapsed.count() << " ns" << std::endl;
  std::cout << "Total time taken by range deletes = "
            << total_range_delete_time_elapsed.count() << " ns" << std::endl;
  std::cout << "Total time taken by range queries = "
            << total_range_query_time_elapsed.count() << " ns" << std::endl;

  workload_file.close();

  {
    std::vector<std::string> live_files;
    uint64_t manifest_size;
    db->GetLiveFiles(live_files, &manifest_size, true /*flush_memtable*/);
    WaitForCompactions(db);
  }
  s = db->Close();
  if (!s.ok()) std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  delete db;
  fade_stats->db_open = false;

  // std::ofstream flush_stats_file("flush_stats.csv");
  // flush_stats_file << "TimePoint,NumEntries,DataSize,IndexSize,FilterSize"
  //                  << std::endl;

  // for (int i = 0; i < flush_events_.size(); i++) {
  //   auto element = flush_events_[i];
  //   flush_stats_file << std::chrono::duration_cast<std::chrono::seconds>(
  //                           element.timestamp_ - start_time_point)
  //                           .count()
  //                    << "," << element.num_entries_ << "," << element.data_size_
  //                    << "," << element.index_size_ << ","
  //                    << element.filter_size_ << std::endl;
  // }

  // flush_stats_file.close();

  // std::ofstream inserts_stats_file("insert_stats.csv");
  // inserts_stats_file << "TimePoint,TimeTaken" << std::endl;

  // for (int i = 0; i < insert_events_.size(); i++) {
  //   auto element = insert_events_[i];
  //   inserts_stats_file << std::chrono::duration_cast<std::chrono::seconds>(
  //                             element.timestamp_ - start_time_point)
  //                             .count()
  //                      << "," << element.time_taken_.count() << std::endl;
  // }

  // inserts_stats_file.close();

  // std::ofstream pq_stats_file("pq_stats.csv");
  // pq_stats_file << "TimePoint,TimeTaken" << std::endl;

  // for (int i = 0; i < point_query_events_.size(); i++) {
  //   auto element = point_query_events_[i];
  //   pq_stats_file << std::chrono::duration_cast<std::chrono::seconds>(
  //                        element.timestamp_ - start_time_point)
  //                        .count()
  //                 << "," << element.time_taken_.count() << std::endl;
  // }

  // pq_stats_file.close();

  // std::ofstream compaction_stats_file("compaction_stats.csv");
  // compaction_stats_file
  //     << "TimePoint,NumInputFiles,NumOutputFile,NumEntriesInput,"
  //        "DataSizeInput,IndexSizeInput,FilterSizeInput,NumEntriesOutput,"
  //        "DataSizeOutput,IndexSizeOutput,FilterSizeOutput"
  //     << std::endl;

  // for (int i = 0; i < compaction_events_.size(); i++) {
  //   auto element = compaction_events_[i];
  //   compaction_stats_file << std::chrono::duration_cast<std::chrono::seconds>(
  //                                element.timestamp_ - start_time_point)
  //                                .count()
  //                         << "," << element.num_input_files_ << ","
  //                         << element.num_output_files_ << ","
  //                         << element.num_entries_inp_ << ","
  //                         << element.data_size_inp_ << ","
  //                         << element.index_size_inp_ << ","
  //                         << element.filter_size_inp_ << ","
  //                         << element.num_entries_out_ << ","
  //                         << element.data_size_out_ << ","
  //                         << element.index_size_out_ << ","
  //                         << element.filter_size_out_ << std::endl;
  // }

  // compaction_stats_file.close();

  // !YBS-sep09-XX!
  if (my_clock_get_time(&end_time) == -1)
    std::cerr << "Failed to get experiment end time" << std::endl;

  fade_stats->exp_runtime = getclock_diff_ns(start_time, end_time);
  // !END

  if (env->IsPerfIOStatEnabled()) {  // !YBS-feb15-XXI!
    // sleep(5);
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    std::cout << "RocksDB Perf Context : " << std::endl;
    std::cout << rocksdb::get_perf_context()->ToString() << std::endl;
    std::cout << "RocksDB Iostats Context : " << std::endl;
    std::cout << rocksdb::get_iostats_context()->ToString() << std::endl;
    // END ROCKS PROFILE
    // Print Full RocksDB stats
    std::cout << "RocksDB Statistics : " << std::endl;
    std::cout << options.statistics->ToString() << std::endl;
    std::cout << "----------------------------------------" << std::endl;
  }
  // !END
}
