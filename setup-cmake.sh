#!/bin/bash

set -e

rm -rf cmake-build-debug
rm -rf cmake-build-release

mkdir cmake-build-debug
cd cmake-build-debug
cmake .. -DCMAKE_BUILD_TYPE=Debug
cd ..

mkdir cmake-build-release
cd cmake-build-release
cmake .. -DCMAKE_BUILD_TYPE=Release
cd ..
