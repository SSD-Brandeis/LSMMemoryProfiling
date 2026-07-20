#ifndef RUN_WORKLOAD_MULTITHREAD_H_
#define RUN_WORKLOAD_MULTITHREAD_H_

#include <memory>

#include "db_env.h"

// Multithreaded counterpart to runWorkload() (run_workload.h/.cc). Opens a
// single shared DB and replays env->num_client_threads independent workload
// shards concurrently against it, each issuing real rocksdb::DB API calls
// (Put/Get/Delete/Iterator/...) on its own thread. Shards are files named
// "shard_<i>.txt" (i in [0, num_client_threads)) expected in the current
// working directory, matching the "cd into a per-run directory, cp
// workload.txt ." convention used by run_workload.cc and runexp.sh.
//
// Writes a one-line "threads,total_ops,seconds,ops_per_sec" summary to
// throughput.csv in the current working directory.
int runWorkloadMultithread(std::unique_ptr<DBEnv> &env);

#endif // RUN_WORKLOAD_MULTITHREAD_H_
