#!/bin/bash
set -e

PROJECT_ROOT="/home/cc/LSMMemoryProfiling"

cd "$PROJECT_ROOT"

git submodule update --init --recursive --remote

mkdir -p build

mkdir -p build
cd build
cmake ..
make -j"$(nproc)"
cd ..

# clear

echo "build complete!"