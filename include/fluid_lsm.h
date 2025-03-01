#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <rocksdb/rocksdb_namespace.h>

#include <iostream>
#include <mutex>
#include <set>
#include <vector>

namespace ROCKSDB_NAMESPACE {

class FluidLSM;

/**
 * Encapsulate comapction arguments required to call CompactFiles
 */
struct CompactionTask {
  CompactionTask(DB* db, FluidLSM* compactor, const std::string& cf_name,
                 const std::vector<std::string>& input_file_names,
                 const int output_lvl, const CompactionOptions& compact_options,
                 bool retry_on_fail, bool debug_mode)
      : db_(db),
        compactor_(compactor),
        cf_name_(cf_name),
        input_file_names_(input_file_names),
        output_lvl_(output_lvl),
        compact_options_(compact_options),
        retry_on_fail_(retry_on_fail),
        debug_mode_(debug_mode) {}
  DB* db_;
  FluidLSM* compactor_;
  const std::string& cf_name_;
  std::vector<std::string> input_file_names_;
  int output_lvl_;
  CompactionOptions compact_options_;
  bool retry_on_fail_;
  bool debug_mode_;
};

/**
 * Run represent one tier on a level
 * we can have several files in one run
 */
struct Run {
  Run(int RocksDB_level) : files_(), RocksDB_level_(RocksDB_level) {}
  void AddFile(SstFileMetaData file);
  std::vector<SstFileMetaData> files_;
  std::set<std::string> file_names_;
  int RocksDB_level_;
};

/**
 * One lazy level encapsulate multiple runs (tiers)
 * in one level, this can be just one run (for largest level)
 * or couple of runs (for smaller level) upto size_ratio of tree
 */
struct LazyLevel {
  LazyLevel() {}
  int NumLiveRuns();
  long SizeInBytes();
  void AddRun(Run run) { runs.push_back(run); }
  void Clear() { runs.clear(); }
  std::vector<Run> runs;
};

/**
 * Formation of Fluid LSM-tree is achieved by storing T sorted
 * runs of a level into multiple levels. For example: with size
 * ratio 4 Level i of Fluid LSM stores 4 runs of size level i-1
 * but instead of storing them in one level we store them in T
 * level and hence look like below
 * Assume file size is 1
 *
 *          L0 : <L0 can have multiple files as per RocksDB>
 *          L1 : 1
 *          L2 : 1
 *          L3 : 1
 *          L4 : 1
 *          L5 : 4 (4 files of size 1 i.e. L1 + L2 + L3 + L4)
 *          L6 : 4
 *          ......
 *          L9 : 16
 *          ......
 *          L13 : 64
 * so on.
 */
class FluidLSM : public EventListener {
 public:
  FluidLSM(int size_ratio, int smaller_lvl_runs_count /* K */,
           int larger_lvl_runs_count /* Z */, long file_size,
           const Options options);

  /**
   * Build structure of FluidLSM out of RocksDB levels.
   * Reads all level meta data and create a view for FluidLSM
   */
  void BuildStructure(DB* db);

  int GetSizeRatio() { return size_ratio_; }
  int GetSmallerLevelRunsCount() { return smaller_lvl_runs_count_; }
  int GetLargerLevelRunsCount() { return larger_lvl_runs_count_; }
  int GetLargestOccupiedLevel() const;
  void SetDebugMode(bool mode) { debug_mode_ = mode; }

  /**
   * Prints current state of FluidLSM
   */
  void PrintFluidLSM(DB* db);

  /**
   * Captures the Flush completion event
   */
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override;

  static void CompactFiles(void* args);

 protected:
  void AddFilesToCompaction(DB* db, int lvl,
                            std::vector<SstFileMetaData*>& input_file_names);
  long GetCompactionSize(
      std::vector<SstFileMetaData*> const& input_file_names) const;

  /**
   * Prepare a compaction and schedule it to run
   */
  void PickCompaction(DB* db, const std::string& cf_name);

  /**
   * Creates new run at `lvl` in FluidLSM using `file_names`
   * the files comes from RocksDB_lvl and mapped to lvl in FluidLSM
   */
  void CreateRun(DB* db, std::vector<SstFileMetaData> const& file_names,
                 int lvl, int RocksDB_lvl);
  void ScheduleCompaction(DB* db, const std::string& cf_name, int origin_lvl,
                          int target_lvl,
                          std::vector<SstFileMetaData*> input_files);

  /**
   * Computes the target level for compaction
   */
  int GetCompactionTargetLevel(
      int origin_lvl, std::vector<SstFileMetaData*> const& input_files) const;

  std::mutex lazy_levels_mutex_;
  std::mutex parallel_compactions_mutex_;
  std::vector<LazyLevel> lazy_levels_;
  int size_ratio_;              // T
  int smaller_lvl_runs_count_;  // K
  int larger_lvl_runs_count_;   // Z
  long file_size_;
  Options options_;
  CompactionOptions compact_options_;
  int parallel_compactions_allowed_;
  int parallel_compactions_running_;
  bool debug_mode_;
};
}  // namespace ROCKSDB_NAMESPACE