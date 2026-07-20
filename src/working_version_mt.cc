/*
 *  Multithreaded counterpart to working_version.cc. Identical argument
 *  parsing; dispatches to runWorkloadMultithread() instead of runWorkload()
 *  so that env->num_client_threads (--threads) spawns concurrent client
 *  threads against a single shared DB handle.
 */
#include <memory>

#include <db_env.h>
#include <parse_arguments.h>
#include <run_workload_multithread.h>

int main(int argc, char *argv[]) {
  std::unique_ptr<DBEnv> env = DBEnv::GetInstance();

  if (parse_arguments(argc, argv, env)) {
    std::cerr << "Failed to parse arguments. Exiting." << std::endl;
    return 1;
  }

  return runWorkloadMultithread(env);
}
