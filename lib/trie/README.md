## Usage

### Testing

```bash
sudo apt install libgtest-dev
```


### Fuzz testing

Setup (already done)

```bash
mkdir vendor
cd vendor

sudo apt-get update
sudo apt-get install protobuf-compiler libprotobuf-dev binutils cmake \
  ninja-build liblzma-dev libz-dev pkg-config autoconf libtool
git clone https://github.com/google/libprotobuf-mutator.git
cd libprotobuf-mutator
git checkout af3bb187
mkdir build
cd build
cmake .. -GNinja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug -DLIB_PROTO_MUTATOR_DOWNLOAD_PROTOBUF=ON
ninja

cd ../../.. # to lib/trie
```

```bash
cd fuzz
make trie_fuzzer
./trie_fuzzer

# once you have a crash run this to catch it
./trie_fuzzer crash-...
```
