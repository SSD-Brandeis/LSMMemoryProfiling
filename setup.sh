# This script is used to setup the environment for the project. 
# It will install all the required packages and dependencies.

echo "\n"
echo "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
echo "%                                      %"
echo "%           RocksDB Wrapper            %"
echo "%                                      %"
echo "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"
echo "\n"
echo "Setting up the environment for the project..."

get_linux_cores() {
    echo $(nproc)
}

get_macos_cores() {
    echo $(sysctl -n hw.physicalcpu)
}

get_windows_cores() {
    echo $(grep -c ^processor /proc/cpuinfo)
}

CORES=1

if [ "$(uname)" == "Linux" ]; then
    CORES=$(get_linux_cores)
elif [ "$(uname)" == "Darwin" ]; then
    CORES=$(get_macos_cores)
elif [ "$(expr substr $(uname -s) 1 5)" == "MINGW" ] || [ "$(expr substr $(uname -s) 1 4)" == "CYGWIN" ]; then
    CORES=$(get_windows_cores)
fi

SCRIPT_PATH="$(realpath "$0")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"

cd "$SCRIPT_DIR" || exit
echo "\nCurrent directory: $(pwd)"

# clone the submodules for the project if not already present
echo "\nCloning submodules..."
git submodule update --init --recursive

# make the rocksdb library
echo "\nMaking RocksDB..."
cd RocksDB-SSD
make clean
make -j$CORES static_lib
cd ..

# make load_generator
echo "\nMaking Load Generator..."
cd KV-WorkloadGenerator
make clean
make -j$CORES load_gen
cd ..

# make the rocksdb wrapper
echo "\nMaking RocksDB Wrapper..."
cd Wrapper
make clean
make -j$CORES working_version
cd ..

echo "Environment setup complete!"

