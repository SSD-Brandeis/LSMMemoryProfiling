#include "db_env.h"

std::unique_ptr<DBEnv> DBEnv::instance_ = nullptr;
std::mutex DBEnv::mutex_;
std::string DBEnv::kDBPath = []() -> std::string {
    const char* p = std::getenv("ROCKSDB_DB_PATH");
    return p ? p : "./db";
}();