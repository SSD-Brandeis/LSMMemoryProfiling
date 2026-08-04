#include <iostream>

#include "args.hxx"
#include "db_env.h"

int parse_arguments(int argc, char *argv[], std::unique_ptr<DBEnv> &env) {
  args::ArgumentParser parser("RocksDB_parser.", "");
  args::Group group1(parser, "This group is all exclusive:",
                     args::Group::Validators::DontCare);

  args::ValueFlag<int> destroy_database_cmd(
      group1, "d", "Destroy and recreate the database [def: 1]",
      {'d', "destroy"});
  args::ValueFlag<int> clear_system_cache_cmd(
      group1, "cc", "Clear system cache [def: 1]", {"cc"});

  args::ValueFlag<int> size_ratio_cmd(
      group1, "T",
      "The number of unique inserts to issue in the experiment [def: 10]",
      {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(
      group1, "P",
      "The number of unique inserts to issue in the experiment [def: 512]",
      {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(
      group1, "B",
      "The number of unique inserts to issue in the experiment [def: 4]",
      {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(
      group1, "E",
      "The number of unique inserts to issue in the experiment [def: 1024 B]",
      {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(
      group1, "M",
      "The number of unique inserts to issue in the experiment [def: 16 MB]",
      {'M', "memory_size"});
  args::ValueFlag<int> file_to_memtable_size_ratio_cmd(
      group1, "file_to_memtable_size_ratio",
      "The number of unique inserts to issue in the experiment [def: 1]",
      {'f', "file_to_memtable_size_ratio"});
  args::ValueFlag<long> file_size_cmd(
      group1, "file_size",
      "The number of unique inserts to issue in the experiment [def: 256 KB]",
      {'F', "file_size"});
  args::ValueFlag<int> compaction_pri_cmd(
      group1, "compaction_pri",
      "[Compaction priority: 1 for kMinOverlappingRatio, 2 for "
      "kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for "
      "kOldestSmallestSeqFirst; def: 1]",
      {'c', "compaction_pri"});
  args::ValueFlag<int> compaction_style_cmd(
      group1, "compaction_style",
      "[Compaction priority: 1 for kCompactionStyleLevel, 2 for "
      "kCompactionStyleUniversal, 3 for kCompactionStyleFIFO, 4 for "
      "kCompactionStyleNone; def: 1]",
      {'C', "compaction_style"});
  args::ValueFlag<int> bits_per_key_cmd(
      group1, "bits_per_key",
      "The number of bits per key assigned to Bloom filter [def: 10]",
      {'b', "bits_per_key"});
  args::ValueFlag<int> block_cache_cmd(
      group1, "bb", "Block cache size in MB [def: 8 MB]", {"bb"});
  args::ValueFlag<int> enable_perf_cmd(
      group1, "enable_perf_iostat",
      "Enable RocksDB's internal Perf and IOstat [def: 0]", {"perf"});
  args::ValueFlag<int> enable_iostat_cmd(
      group1, "enable_iostat", "Enable RocksDB's internal IOstat [def: 0]",
      {"iostat"});
  args::ValueFlag<int> enable_rocksdb_stats_cmd(
      group1, "enable_rocksdb_stats",
      "Enable RocksDB's internal RocksDB stats [def: 0]", {"stat"});
  args::ValueFlag<int> show_progress_cmd(
      group1, "show_progress_bar", "Shows progress bar [def: 0]", {"progress"});

  args::ValueFlag<long> num_inserts_cmd(
      group1, "inserts",
      "The number of unique inserts to issue in the experiment [def: 1]",
      {'I', "inserts"});
  args::ValueFlag<long> num_updates_cmd(
      group1, "updates",
      "The number of unique updates to issue in the experiment [def: 0]",
      {'U', "updates"});
  args::ValueFlag<long> num_range_queries_cmd(
      group1, "range_queries",
      "The number of unique range queries to issue in the experiment [def: 0]",
      {'S', "range_queries"});
  args::ValueFlag<float> range_query_selectivity_cmd(
      group1, "Y", "Range query selectivity [def: 0]",
      {'Y', "range_query_selectivity"});

  args::ValueFlag<int> memtable_factory_cmd(
      group1, "memtable_factory",
      "[Memtable Factory: 1 for Skiplist, 2 for Vector, 3 for Hash Skiplist, 4 "
      "for Hash Linkedlist; 5 for Unsorted Vector; 6 for Always Sorted Vector; "
      "7 for LinkList; 8 for Simple Skiplist; 11 for ART Synchronized Tree; "
      "12 for TLX B+Tree; def: 1]",
      {'m', "memtable_factory"});
  args::ValueFlag<int> prefix_length_cmd(
      group1, "prefix_length",
      "[Prefix Length: Number of bytes of the key forming the prefix; def: 0]",
      {'X', "prefix_length"});
  args::ValueFlag<int> common_prefix_len_cmd(
      group1, "common_prefix_len",
      "[Common Prefix Length: Number of leading bytes shared between RQ start "
      "and synthesised end key; when equal to prefix_length, total_order_seek "
      "is disabled; def: 0]",
      {"common_prefix_len"});
  args::ValueFlag<long> bucket_count_cmd(
      group1, "bucket_count",
      "[Bucket Count: Number of buckets for the hash table in HashSkipList & "
      "HashLinkList Memtables; def: 50000]",
      {'H', "bucket_count"});
  args::ValueFlag<long> threshold_use_skiplist_cmd(
      group1, "threshold_use_skiplist",
      "[Threshold Use SkipList: Threshold based on which the conversion will "
      "happen from HashLinkList to HashSkipList; def: 256]",
      {"threshold_use_skiplist", "threshold_use_skiplist"});
  args::ValueFlag<long> vector_pre_allocation_size_cmd(
      group1, "preallocation_vector_size",
      "[Preallocation Vector Size: Size to preallocation to vector memtable; "
      "def: 0]",
      {'A', "preallocation_size"});
  args::ValueFlag<int> low_pri_cmd(
      group1, "low_pri",
      "Set the priority of write requests (0 means compactions aren't "
      "prioritized) [def: 1]",
      {"lowpri"});
  args::ValueFlag<int> wal_cmd(
      group1, "wal",
      "Enable Write-Ahead Log (1 = enabled / disableWAL=false, 0 = disabled "
      "/ disableWAL=true) [def: 0]",
      {"wal"});
  args::ValueFlag<int> num_threads_cmd(
      group1, "threads",
      "Number of concurrent client threads issuing operations against a "
      "shared DB handle (working_version_mt / runWorkloadMultithread only) "
      "[def: 1]",
      {"threads"});
  args::ValueFlag<double> duration_seconds_cmd(
      group1, "duration_seconds",
      "If > 0, each shard thread wraps back to the start of its shard file "
      "and keeps replaying it until this many wall-clock seconds have "
      "elapsed, instead of stopping at end-of-file (working_version_mt / "
      "runWorkloadMultithread only). 0 disables this -- fixed-op-count "
      "behavior, unchanged from before this flag existed [def: 0]",
      {"duration_seconds"});
  args::ValueFlag<int> wait_for_compact_cmd(
      group1, "wait_for_compact",
      "If 1, call DB::WaitForCompact() (flushing the active memtable first) "
      "before stopping the TOTAL_TIMER, so total_seconds/ops_per_sec "
      "reflect the time for the LSM tree to fully stabilize (flush + "
      "compaction fully drained), not just for inserts to be issued. "
      "0 disables this -- timer stops immediately after shard threads "
      "join, unchanged from before this flag existed (working_version_mt "
      "/ runWorkloadMultithread only) [def: 0]",
      {"wait_for_compact"});
  args::ValueFlag<int> max_background_jobs_cmd(
      group1, "max_background_jobs",
      "Maximum concurrent background flush/compaction jobs [def: 1]",
      {"bg_jobs"});
  args::ValueFlag<int> max_background_flushes_cmd(
      group1, "max_background_flushes",
      "Explicit flush thread pool size (rocksdb::Options::max_background_"
      "flushes). -1 = auto (max(1,bg_jobs/4)). If set, max_background_"
      "compactions should also be set explicitly, otherwise it silently "
      "collapses to 1 [def: -1]",
      {"max_background_flushes"});
  args::ValueFlag<int> max_background_compactions_cmd(
      group1, "max_background_compactions",
      "Explicit compaction thread pool size (rocksdb::Options::max_"
      "background_compactions). -1 = auto (max(1,bg_jobs-flushes)). If "
      "set, max_background_flushes should also be set explicitly, "
      "otherwise it silently collapses to 1 [def: -1]",
      {"max_background_compactions"});
  args::ValueFlag<int> max_write_buffer_number_cmd(
      group1, "max_write_buffer_number",
      "Maximum number of memtables (active + immutable pending flush) "
      "before writers stall (rocksdb::Options::max_write_buffer_number) "
      "[def: 2]",
      {"max_write_buffer_number"});
  args::ValueFlag<int> max_subcompactions_cmd(
      group1, "max_subcompactions",
      "Maximum number of threads RocksDB may use WITHIN one compaction "
      "job, splitting its key range into concurrent sub-ranges "
      "(rocksdb::Options::max_subcompactions) [def: 1]",
      {"max_subcompactions"});
  args::ValueFlag<int> concurrent_memtable_write_cmd(
      group1, "concurrent_memtable_write",
      "Allow multiple writer threads to update the memtable in parallel "
      "(rocksdb::Options::allow_concurrent_memtable_write) [def: 1]",
      {"concurrent_memtable_write"});
  args::ValueFlag<int> auto_concurrent_memtable_write_cmd(
      group1, "auto_concurrent_memtable_write",
      "Silently force concurrent_memtable_write off when memtable_factory "
      "doesn't support it, instead of DB::Open rejecting the combination "
      "[def: 0]",
      {"auto_concurrent_memtable_write"});
  args::ValueFlag<int> unordered_write_cmd(
      group1, "unordered_write",
      "rocksdb::Options::unordered_write -- trades snapshot immutability "
      "for higher write throughput; requires concurrent_memtable_write=1 "
      "[def: 0]",
      {"unordered_write"});
  args::ValueFlag<int> enable_pipelined_write_cmd(
      group1, "enable_pipelined_write",
      "rocksdb::Options::enable_pipelined_write -- separates the write "
      "thread queue into memtable-insertion and WAL-write stages so they "
      "can overlap across writers [def: 0]",
      {"enable_pipelined_write"});
  args::ValueFlag<std::string> load_file_cmd(
      group1, "load_file",
      "If set, replayed single-threaded immediately after DB::Open(), "
      "before the --threads shard threads are spawned -- lets a "
      "load-then-query experiment run in one process/DB (working_version_mt "
      "only). This phase is timed (see workload.log's [load phase] block) "
      "but excluded from throughput.csv/ops_per_sec [def: none]",
      {"load_file"});
  try {
    parser.ParseCLI(argc, argv);
  } catch (args::Help &) {
    std::cout << parser;
    exit(0);
  } catch (args::ParseError &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  } catch (args::ValidationError &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  }

  env->SetDestroyDatabase(destroy_database_cmd
                              ? args::get(destroy_database_cmd)
                              : env->IsDestroyDatabaseEnabled());
  env->clear_system_cache = clear_system_cache_cmd
                                ? args::get(clear_system_cache_cmd)
                                : env->clear_system_cache;
  env->size_ratio =
      size_ratio_cmd ? args::get(size_ratio_cmd) : env->size_ratio;
  //   env->level0_slowdown_writes_trigger = env->size_ratio - 1;
  //   env->level0_stop_writes_trigger = env->size_ratio;
  env->level0_file_num_compaction_trigger = env->size_ratio;
  env->buffer_size_in_pages = buffer_size_in_pages_cmd
                                  ? args::get(buffer_size_in_pages_cmd)
                                  : env->buffer_size_in_pages;
  env->entries_per_page = entries_per_page_cmd ? args::get(entries_per_page_cmd)
                                               : env->entries_per_page;
  env->entry_size =
      entry_size_cmd ? args::get(entry_size_cmd) : env->entry_size;
  env->SetBufferSize(buffer_size_cmd ? args::get(buffer_size_cmd) : 0);
  env->file_to_memtable_size_ratio =
      file_to_memtable_size_ratio_cmd
          ? args::get(file_to_memtable_size_ratio_cmd)
          : env->file_to_memtable_size_ratio;
  env->compaction_pri =
      compaction_pri_cmd ? args::get(compaction_pri_cmd) : env->compaction_pri;
  env->compaction_style = compaction_style_cmd ? args::get(compaction_style_cmd)
                                               : env->compaction_style;
  env->bits_per_key =
      bits_per_key_cmd ? args::get(bits_per_key_cmd) : env->bits_per_key;
  env->block_cache =
      block_cache_cmd ? args::get(block_cache_cmd) : env->block_cache;
  env->SetPerfStat(enable_perf_cmd ? args::get(enable_perf_cmd)
                                   : env->IsPerfStatEnabled());
  env->SetIOStat(enable_iostat_cmd ? args::get(enable_iostat_cmd)
                                   : env->IsIOStatEnabled());
  env->SetRocksDBStat(enable_rocksdb_stats_cmd
                          ? args::get(enable_rocksdb_stats_cmd)
                          : env->IsRocksDBStatEnabled());
  env->SetShowProgress(show_progress_cmd ? args::get(show_progress_cmd)
                                         : env->IsShowProgressEnabled());

  // LSM options
  env->num_inserts =
      num_inserts_cmd ? args::get(num_inserts_cmd) : env->num_inserts;
  env->num_updates =
      num_updates_cmd ? args::get(num_updates_cmd) : env->num_updates;
  env->num_range_queries = num_range_queries_cmd
                               ? args::get(num_range_queries_cmd)
                               : env->num_range_queries;

  env->memtable_factory = memtable_factory_cmd ? args::get(memtable_factory_cmd)
                                               : env->memtable_factory;
  env->prefix_length =
      prefix_length_cmd ? args::get(prefix_length_cmd) : env->prefix_length;
  env->common_prefix_len = common_prefix_len_cmd
                               ? args::get(common_prefix_len_cmd)
                               : env->common_prefix_len;
  env->bucket_count =
      bucket_count_cmd ? args::get(bucket_count_cmd) : env->bucket_count;
  env->linklist_threshold_use_skiplist =
      threshold_use_skiplist_cmd ? args::get(threshold_use_skiplist_cmd)
                                 : env->linklist_threshold_use_skiplist;
  // This size is computed in number of entries that can fit in buffer
  // theoretically
  env->vector_preallocation_size_in_bytes =
      vector_pre_allocation_size_cmd
          ? args::get(vector_pre_allocation_size_cmd)
          : env->entries_per_page * env->buffer_size_in_pages;
  env->low_pri = low_pri_cmd ? args::get(low_pri_cmd) : env->low_pri;
  // --wal 1 enables WAL (disableWAL=false); --wal 0 disables it (disableWAL=true)
  if (wal_cmd)
    env->disableWAL = !args::get(wal_cmd);
  env->num_client_threads = num_threads_cmd ? args::get(num_threads_cmd)
                                            : env->num_client_threads;
  env->duration_seconds = duration_seconds_cmd ? args::get(duration_seconds_cmd)
                                               : env->duration_seconds;
  env->wait_for_compact_before_stop =
      wait_for_compact_cmd
          ? static_cast<bool>(args::get(wait_for_compact_cmd))
          : env->wait_for_compact_before_stop;
  env->max_background_jobs = max_background_jobs_cmd
                                 ? args::get(max_background_jobs_cmd)
                                 : env->max_background_jobs;
  env->max_background_flushes = max_background_flushes_cmd
                                     ? args::get(max_background_flushes_cmd)
                                     : env->max_background_flushes;
  env->max_background_compactions =
      max_background_compactions_cmd
          ? args::get(max_background_compactions_cmd)
          : env->max_background_compactions;
  env->max_write_buffer_number = max_write_buffer_number_cmd
                                     ? args::get(max_write_buffer_number_cmd)
                                     : env->max_write_buffer_number;
  env->max_subcompactions = max_subcompactions_cmd
                                ? args::get(max_subcompactions_cmd)
                                : env->max_subcompactions;
  env->allow_concurrent_memtable_write =
      concurrent_memtable_write_cmd
          ? static_cast<bool>(args::get(concurrent_memtable_write_cmd))
          : env->allow_concurrent_memtable_write;
  env->auto_concurrent_memtable_write =
      auto_concurrent_memtable_write_cmd
          ? static_cast<bool>(args::get(auto_concurrent_memtable_write_cmd))
          : env->auto_concurrent_memtable_write;
  env->unordered_write = unordered_write_cmd
                             ? static_cast<bool>(args::get(unordered_write_cmd))
                             : env->unordered_write;
  env->enable_pipelined_write =
      enable_pipelined_write_cmd
          ? static_cast<bool>(args::get(enable_pipelined_write_cmd))
          : env->enable_pipelined_write;
  env->load_file = load_file_cmd ? args::get(load_file_cmd) : env->load_file;

  return 0;
}
