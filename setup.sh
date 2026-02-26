#!/bin/bash
set -e

OS="$(uname)"

echo "Installing system dependencies"

if [[ "$OS" == "Linux" ]]; then
  sudo apt-get update -y
  sudo apt-get install -y build-essential cmake libgflags-dev
  NPROC="$(nproc)"
elif [[ "$OS" == "Darwin" ]]; then
  if ! command -v brew >/dev/null 2>&1; then
    echo "Homebrew not found. Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  fi
  brew update
  brew install cmake gflags
  NPROC="$(sysctl -n hw.ncpu)"
else
  echo "Unsupported OS: $OS"
  exit 1
fi

echo "Installing Rust"
if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi

source "$HOME/.cargo/env"
rustup default nightly

echo "Updating git submodules"
git submodule update --init --recursive

mkdir -p build

echo "Reloading CMake and building LSMMemoryProfiling"
cd build
cmake ..
make -j"$NPROC"

echo "Setup complete"