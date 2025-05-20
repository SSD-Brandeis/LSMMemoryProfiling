#!/usr/bin/env bash

ROOT_DIR="$HOME/LSMMemoryProfiling/.result"
find "$ROOT_DIR" -type f -name 'workload.txt' -print -exec rm -f {} \;

echo "All workload.txt files have been removed."
