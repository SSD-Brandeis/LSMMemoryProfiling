/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */
#include <memory>

#include <db_env.h>
#include <parse_arguments.h>
#include <run_workload.h>

int main(int argc, char *argv[]) {
  std::unique_ptr<DBEnv> env = DBEnv::GetInstance();

  if (parse_arguments(argc, argv, env)) {
    std::cerr << "Failed to parse arguments. Exiting." << std::endl;
    return 1;
  }

  return runWorkload(env);
}