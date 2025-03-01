# RocksDB-Wrapper

This repository provides a simple wrapper module for **RocksDB**, designed to facilitate database operations, workload generation, and performance testing. The wrapper leverages [RocksDB-SSD](https://github.com/SSD-Brandeis/RocksDB-SSD) for database storage and [KV-WorkloadGenerator](https://github.com/SSD-Brandeis/KV-WorkloadGenerator) for generating customizable key-value workloads.

## Prerequisites

Before using this module, ensure you have installed the following dependencies:

- Git
- CMake
- A C++ compiler (e.g., GCC, Clang)

## Forking the Repository and Adding Submodules

This repository no longer includes the submodules (RocksDB-SSD and KV-WorkloadGenerator) by default. Instead, we expect you to fork the repository and add the submodules manually. This allows you to modify the wrapper and submodule code as needed for your specific project.

### Steps to Get Started

1. **Fork the Repository:**  
   Go to the GitHub page for [RocksDB-Wrapper](https://github.com/SSD-Brandeis/RocksDB-Wrapper) and click the **Fork** button in the top-right corner. This creates your own copy of the repository under your account.

2. **Clone Your Fork Locally**  

3. **Add the Submodules:**  
   Since the submodules are not part of the initial clone, add them manually using the following commands (replace `<branch>` with the branch you wish to track if needed):

   - **Add RocksDB-SSD Submodule:**
     ```bash
     git submodule add -b <branch> https://github.com/SSD-Brandeis/RocksDB-SSD.git lib/rocksdb
     ```

   - **Add KV-WorkloadGenerator Submodule:**
     ```bash
     git submodule add -b <branch> https://github.com/SSD-Brandeis/KV-WorkloadGenerator.git lib/KV-WorkloadGenerator
     ```

4. **Initialize and Update Submodules:**  
   Once you've added the submodules, run:
   ```bash
   git submodule update --init --recursive
   ```
   This command ensures that all submodules are properly initialized and checked out to the specified branch.

## Build the Project

Before running any experiments or setup instructions, you must build the project. You can configure and build using CMake. For example:

```bash
mkdir build && cd build
cmake ..
cmake --build . --parallel <number-of-cores>
```

Replace `<number-of-cores>` with the number of parallel jobs you wish to run (e.g., `8` or `14`).

## Setup Instructions

### 1. **Generate Workload**

After building the project, the first step is to generate a workload using the `load_gen` tool from the `KV-WorkloadGenerator` submodule. You will find an executable named `load_gen` inside the `bin` folder. This tool generates a `workload.txt` file, which will be used in experiments.

For detailed instructions on how to use the `load_gen` tool, refer to the [KV-WorkloadGenerator repository](https://github.com/SSD-Brandeis/KV-WorkloadGenerator).

### 2. **Run RocksDB-Wrapper**

Once you have the `workload.txt` file in the project root directory, you're ready to run experiments. Use the `./bin/working_version <ARGS>` executable with the desired options.

#### Example Command:

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
    --progress                         Shows progress bar [def: 0]
```