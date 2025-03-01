#include "db_env.h"

std::unique_ptr<DBEnv> DBEnv::instance_ = nullptr;
std::mutex DBEnv::mutex_;
std::string DBEnv::kDBPath = "./db";