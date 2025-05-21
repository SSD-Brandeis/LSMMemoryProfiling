#include <fluid_lsm.h>

#include <cmath>

using namespace std;

namespace ROCKSDB_NAMESPACE {

void Run::AddFile(SstFileMetaData file) {
  files_.push_back(file);
  file_names_.insert(file.name);
}

int LazyLevel::NumLiveRuns() {
  int num_live_runs = 0;

  for (auto& run : runs) {
    bool being_compacted = false;

    for (auto& file : run.files_) {
      being_compacted |= file.being_compacted;
    }

    num_live_runs += !being_compacted && !run.files_.empty();
  }

  return num_live_runs;
}

long LazyLevel::SizeInBytes() {
  long size_in_bytes = 0;

  for (auto& run : runs) {
    for (auto& file : run.files_) {
      size_in_bytes += file.size;
    }
  }

  return size_in_bytes;
}

FluidLSM::FluidLSM(int size_ratio, int smaller_lvl_runs_count /* K */,
                   int larger_lvl_runs_count /* Z */, long file_size,
                   const Options options)
    : size_ratio_(size_ratio),
      smaller_lvl_runs_count_(smaller_lvl_runs_count),
      larger_lvl_runs_count_(larger_lvl_runs_count),
      file_size_(file_size),
      options_(options),
      compact_options_(),
      parallel_compactions_allowed_(1),
      parallel_compactions_running_(0) {
  compact_options_.compression = options_.compression;
  compact_options_.output_file_size_limit = options_.target_file_size_base;
  lazy_levels_.resize(options_.num_levels);
}

void FluidLSM::CreateRun(DB* db, std::vector<SstFileMetaData> const& file_names,
                         int lvl, int RocksDB_lvl) {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  Run run(RocksDB_lvl);

  for (SstFileMetaData file : file_names) {
    run.AddFile(file);
  }

  lazy_levels_[lvl].AddRun(run);
}

void FluidLSM::BuildStructure(DB* db) {
  lazy_levels_mutex_.lock();
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);

  for (int lvl = 0; lvl < lazy_levels_.size(); lvl++) {
    lazy_levels_[lvl].Clear();
  }

  unsigned long num_runs_in_lvl1 =
      cf_meta.levels[0].files.size() + !cf_meta.levels[1].files.empty();

  std::vector<SstFileMetaData> file_names;
  for (unsigned long i = num_runs_in_lvl1; i < smaller_lvl_runs_count_; i++) {
    CreateRun(db, file_names, 0, 0);
  }

  for (auto file : cf_meta.levels[0].files) {
    file_names.push_back(file);
    CreateRun(db, file_names, 0, 0);
    file_names.clear();
  }

  for (auto file : cf_meta.levels[1].files) {
    file_names.push_back(file);
  }
  CreateRun(db, file_names, 0, 1);
  file_names.clear();

  for (int lvl = 2; lvl < cf_meta.levels.size(); lvl++) {
    LevelMetaData level = cf_meta.levels[lvl];
    int fluid_lvl = ceil(((double)level.level - 1) /
                         ((double)smaller_lvl_runs_count_ + 1.0));

    file_names.clear();
    for (auto file : level.files) {
      file_names.push_back(file);
    }
    CreateRun(db, file_names, fluid_lvl, level.level);
  }
  lazy_levels_mutex_.unlock();
}

int FluidLSM::GetLargestOccupiedLevel() const {
  int largest = 0;
  for (int lvl = 0; lvl < lazy_levels_.size(); lvl++) {
    for (auto& run : lazy_levels_[lvl].runs) {
      if (run.files_.size() > 0) {
        largest = lvl;
      }
    }
  }
  return largest;
}

void FluidLSM::PrintFluidLSM(DB* db) {
  ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);
  int num_levels = GetLargestOccupiedLevel();

  for (int i = 0; i < num_levels + 1; i++) {
    cerr << "lvl: " << i << endl;

    for (int j = 0; j < lazy_levels_[i].runs.size(); j++) {
      if (lazy_levels_[i].runs[j].files_.empty()) {
        continue;
      }
      cerr << "\t run: " << j
           << "   (RocksDBLvl: " << lazy_levels_[i].runs[j].RocksDB_level_
           << ")" << endl;

      for (auto file : lazy_levels_[i].runs[j].files_) {
        cerr << "\t\t " << file.size;
        cerr << "\t " << file.smallestkey << "-" << file.largestkey;
        cerr << " \t " << file.name << " \t "
             << (file.being_compacted ? "being compacted" : "") << endl;
      }
    }
  }
  cerr << endl;
}

void FluidLSM::AddFilesToCompaction(
    DB* db, int lvl, std::vector<SstFileMetaData*>& input_file_names) {
  ColumnFamilyMetaData cf_name;
  db->GetColumnFamilyMetaData(&cf_name);

  for (auto& run : lazy_levels_[lvl].runs) {
    for (auto& file : run.files_) {
      if (file.being_compacted == false) {
        input_file_names.push_back(&file);
      }
    }
  }
}

long FluidLSM::GetCompactionSize(
    std::vector<SstFileMetaData*> const& input_file_names) const {
  long size_in_bytes = 0;

  for (auto file : input_file_names) {
    size_in_bytes += file->size;
  }

  return size_in_bytes;
}

int FluidLSM::GetCompactionTargetLevel(
    int origin_lvl, std::vector<SstFileMetaData*> const& input_files) const {
  long projected_output_size = GetCompactionSize(input_files);
  int target_lvl = origin_lvl;

  long origin_lvl_capacity = options_.write_buffer_size *
                             pow(size_ratio_, origin_lvl + 1) *
                             (size_ratio_ - 1) / size_ratio_;
  if (projected_output_size > origin_lvl_capacity) {
    target_lvl = origin_lvl + 1;
  }

  return target_lvl;
}

void FluidLSM::CompactFiles(void* args) {
  std::unique_ptr<CompactionTask> task(reinterpret_cast<CompactionTask*>(args));
  assert(task && task->db_);
  std::vector<std::string>* output_file_names = new std::vector<std::string>();
  Status s =
      task->db_->CompactFiles(task->compact_options_, task->input_file_names_,
                              task->output_lvl_, -1, output_file_names);

  if (task->debug_mode_) {
    for (auto name : *output_file_names) {
      cerr << " ---- new file created:    " << name << endl;
    }

    cerr << "CompactFiles() finished with status " << s.ToString() << endl;
    for (auto name : task->input_file_names_) {
      cerr << "     " << name << endl;
    }
    cerr << "   -> level " << task->output_lvl_ << endl;
  }
  FluidLSM* tree = reinterpret_cast<FluidLSM*>(task->compactor_);

  if (!s.IsIOError()) {
    tree->PickCompaction(task->db_, task->cf_name_);
  }

  tree->parallel_compactions_mutex_.lock();
  tree->parallel_compactions_running_--;
  tree->parallel_compactions_mutex_.unlock();
}

void FluidLSM::ScheduleCompaction(DB* db, const std::string& cf_name,
                                  int origin_lvl, int target_lvl,
                                  std::vector<SstFileMetaData*> input_files) {
  if (parallel_compactions_allowed_ > parallel_compactions_running_) {
    int RocksDB_lvl = 1 + target_lvl * (smaller_lvl_runs_count_ + 1);
    int slot = smaller_lvl_runs_count_;

    for (int lvl = 0; lvl <= smaller_lvl_runs_count_ && target_lvl > origin_lvl;
         lvl++) {
      if (lazy_levels_[target_lvl]
              .runs[smaller_lvl_runs_count_ - lvl]
              .files_.empty()) {
        RocksDB_lvl = 1 + target_lvl * (smaller_lvl_runs_count_ + 1) - lvl;
        slot = smaller_lvl_runs_count_ - lvl;
        break;
      }
    }
    target_lvl = std::min(options_.num_levels - 1, target_lvl);
    int largest_lvl = GetLargestOccupiedLevel();

    std::vector<std::string> input_file_names;
    for (auto file : input_files) {
      input_file_names.push_back(file->name);
      file->being_compacted = true;
    }
    parallel_compactions_mutex_.lock();
    parallel_compactions_running_++;
    parallel_compactions_mutex_.unlock();

    CompactionTask* task =
        new CompactionTask(db, this, cf_name, input_file_names, RocksDB_lvl,
                           compact_options_, false, debug_mode_);
    task->compact_options_.output_file_size_limit = file_size_;
    options_.env->Schedule(&FluidLSM::CompactFiles, task);
  } else if (debug_mode_) {
    cerr << "skip compaction.  origin lvl: " << origin_lvl
         << "   num files: " << input_files.size();
    cerr << "   ongoing compactions: " << parallel_compactions_running_ << endl;
    int runs = lazy_levels_[origin_lvl].runs.size();
    int live_runs = lazy_levels_[origin_lvl].NumLiveRuns();
    cerr << "runs " << runs
         << "    live runs:  " << lazy_levels_[origin_lvl].NumLiveRuns()
         << endl;
  }
}

void FluidLSM::PickCompaction(DB* db, const std::string& cf_name) {
  BuildStructure(db);

  if (debug_mode_) {
    PrintFluidLSM(db);
  }

  std::vector<SstFileMetaData*> input_files;
  int largest_lvl = GetLargestOccupiedLevel();
  int origin_lvl = 0;

  for (int lvl = largest_lvl; lvl >= 0; lvl--) {
    int num_live_runs = lazy_levels_[lvl].NumLiveRuns();
    origin_lvl = lvl;

    if ((lvl < largest_lvl && num_live_runs > smaller_lvl_runs_count_) ||
        (lvl == largest_lvl && num_live_runs > larger_lvl_runs_count_)) {
      AddFilesToCompaction(db, lvl, input_files);
      int target_lvl = GetCompactionTargetLevel(origin_lvl, input_files);
      ScheduleCompaction(db, cf_name, origin_lvl, target_lvl, input_files);
      input_files.clear();
    }
  }
}

void FluidLSM::OnFlushCompleted(DB* db, const FlushJobInfo& info) {
  PickCompaction(db, info.cf_name);
}

}  // namespace ROCKSDB_NAMESPACE