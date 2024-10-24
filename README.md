# RocksDB-Wrapper

This repository provides a simple wrapper module for **RocksDB**, designed to facilitate database operations, workload generation, and performance testing. The wrapper leverages [RocksDB-SSD](https://github.com/SSD-Brandeis/RocksDB-SSD) for database storage and [KV-WorkloadGenerator](https://github.com/SSD-Brandeis/KV-WorkloadGenerator) for generating customizable key-value workloads.

## Prerequisites

Before using this module, ensure you have installed the following dependencies:

- Git
- CMake
- A C++ compiler (e.g., GCC, Clang)

## Cloning the Repository and Submodules

To clone the repository along with its submodules in one step, use the following command:

```bash
git clone --recurse-submodules https://github.com/SSD-Brandeis/RocksDB-Wrapper
```

This command will clone the repository and automatically initialize and update the submodules, which include RocksDB-SSD and KV-WorkloadGenerator. If you've already cloned the repository without initializing the submodules, you can run:

```bash
git submodule update --init --recursive
```

This will initialize and update the submodules in your existing clone.

## Setup Instructions

### 1. **Generate Workload**

The first step is to generate a workload using the `load_gen` tool from the `KV-WorkloadGenerator` submodule. You will find an executable named `load_gen` inside the `bin` folder. This tool generates a `workload.txt` file, which will be used in experiments.

For detailed instructions on how to use the `load_gen` tool, refer to the [KV-WorkloadGenerator repository](https://github.com/SSD-Brandeis/KV-WorkloadGenerator).

### 2. **Run RocksDB-Wrapper**

Once you have the `workload.txt` file in the project root directory, you're ready to run experiments. Use the `./bin/working_version <ARGS>` executable with the desired options.

### Example Command:

```bash
./bin/working_version --file_size 512
```

This example runs the experiment and sets the SST file size to 512 KB.

## Available Options

Below are the supported options for running experiments with the RocksDB wrapper:

```
    -d, --destroy                      Destroy and recreate the database [default: 1]
    --cc                               Clear system cache [default: 1]
    -T, --size_ratio                   The size ratio for the LSM [default: 10]
    -P, --buffer_size_in_pages         The number of pages in memory buffer [default: 4096]
    -B, --entries_per_page             The number of entries in one page [default: 4]
    -E, --entry_size                   The size of one entry you have in workload.txt [default: 1024 B]
    -M, --memory_size                  The memory buffer size in bytes [default: 16 MB]
    -f, --file_to_memtable_size_ratio  The ratio between files and memtable [default: 1]
    -F, --file_size                    The size of one SST file [default: 256 KB]
    -c, --compaction_pri               Compaction priority [1: kMinOverlappingRatio, 2: kByCompensatedSize, 3: kOldestLargestSeqFirst, 4: kOldestSmallestSeqFirst;default: 1]
    -C, --compaction_style             Compaction style [1: kCompactionStyleLevel, 2: kCompactionStyleUniversal, 3: kCompactionStyleFIFO, 4: kCompactionStyleNone; default: 1]
    -b, --bits_per_key                 The number of bits per key assigned to Bloom filter [default: 10]
    --bb                               Block cache size in MB [default: 8 MB]
    --stat                             Enable RocksDB's internal Perf and IOstat monitoring [default: 0]
```

