#!/bin/bash
set -e

echo "Installing system dependencies"
sudo apt-get update -y
sudo apt-get install -y build-essential cmake libgflags-dev

echo "Installing Rust"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
rustup default nightly

echo "Updating git submodules"
git submodule update --init --recursive

mkdir -p build

echo "Reloading CMake and building LSMBUFFER"
cd build && cmake ..
make -j"$(nproc)"

echo "Setup complete"