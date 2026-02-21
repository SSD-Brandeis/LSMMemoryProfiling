#!/usr/bin/env bash

ROOT_DIR="$HOME/LSMMemoryProfiling/.result"
find "$ROOT_DIR" -type f -name 'workload.txt' -print -exec rm -f {} \;
# find "$ROOT_DIR" -type f -name 'stats.log' -print -exec rm -f {} \;
# find "$ROOT_DIR" -type f -name 'temp.log' -print -exec rm -f {} \;
# find "$ROOT_DIR" -type f -name 'LOG' -print -exec rm -f {} \;
echo "All workload.txt files have been removed."
