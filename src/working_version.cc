/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#include <parse_arguments.h>
#include <run_workload.h>
#include <db_env.h>

int main(int argc, char *argv[]) {
  // check db_env.h for the contents of DBEnv and also 
  // the definitions of the singleton experimental environment
  DBEnv *env = DBEnv::GetInstance();

  // parse the command line arguments
  if (parse_arguments(argc, argv, env)) {
    exit(1);
  }

  int s = runWorkload(env);
  return 0;
}
