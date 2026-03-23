#ifndef UTILS_H_
#define UTILS_H_

void PrintExperimentalSetup(std::unique_ptr<DBEnv> &env,
                            std::shared_ptr<Buffer> &buffer);
void PrintRocksDBPerfStats(std::unique_ptr<DBEnv> &env,
                           std::shared_ptr<Buffer> &buffer, Options options);
void UpdateProgressBar(std::unique_ptr<DBEnv> &env, size_t current,
                       size_t total, size_t update_interval = 1000,
                       size_t bar_width = 50);

#ifdef PROFILE
void LogTreeState(rocksdb::DB *db, std::shared_ptr<Buffer> &buffer, std::unique_ptr<DBEnv> &env);
// void LogRocksDBStatistics(rocksdb::DB *db, const rocksdb::Options &options,
//                           std::shared_ptr<Buffer> &buffer);
#endif // PROFILE
#endif // UTILS_H_