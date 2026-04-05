#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$PROJECT_ROOT"

OS="$(uname)"

if [[ "$OS" == "Linux" ]]; then
  NPROC="$(nproc)"
elif [[ "$OS" == "Darwin" ]]; then
  if ! command -v brew >/dev/null 2>&1; then
    echo "Homebrew not found. Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  fi
  NPROC="$(sysctl -n hw.ncpu)"
else
  echo "Unsupported OS: $OS"
  exit 1
fi

git submodule update --init --recursive

[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"

mkdir -p build
cd build
cmake ..
make -j"$NPROC"

clear

echo "build complete!"