# RocksDB-Wrapper

A simple wrapper module for RocksDB, designed to facilitate database operations and workload generation for performance testing and experimental analysis. This wrapper utilizes [RocksDB-SSD](https://github.com/SSD-Brandeis/RocksDB-SSD) for storage and [KV-WorkloadGenerator](https://github.com/SSD-Brandeis/KV-WorkloadGenerator) for generating customizable key-value workloads.

## Setup Instructions

**Run the Setup Script**

Navigate to the repository directory and execute the `setup.sh` script. This script will automatically update the submodules and set up RocksDB for you.

```bash
./setup.sh
```

## Post-Setup Steps

After setting up the wrapper, you'll need to generate a workload for your experiments. Here's how to proceed:

1. **Generate Workload**

   Use the `load_gen` tool inside the KV-WorkloadGenerator submodule to create a `workload.txt` file. You can either generate this file within the KV-WorkloadGenerator directory and then move it to the RocksDB-Wrapper directory, or directly create it in the RocksDB-Wrapper directory using the relative path to `load_gen`. Look at the Readme in the KV-WorkloadGenerator repository for more information on how to use the tool.

2. **Run RocksDB-Wrapper**

   With the `workload.txt` file in your RocksDB-Wrapper directory, you're now ready to run experiments. Use the `./working_version` executable with desired options for your test run.

### Options

The wrapper supports below options:

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
    -V, --verbosity                    The verbosity level of execution [0,1,2; default: 0]
    -c, --compaction_pri               Compaction priority [1 for kMinOverlappingRatio, 2 for kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for kOldestSmallestSeqFirst; default: 1]
    -C, --compaction_style             Compaction style [1 for kCompactionStyleLevel, 2 for kCompactionStyleUniversal, 3 for kCompactionStyleFIFO, 4 for kCompactionStyleNone; default: 1]
    -b, --bits_per_key                 The number of bits per key assigned to Bloom filter [default: 10]
    --bb                               Block cache size in MB [default: 8 MB]
    -s, --sp                           Show progress [default: 0]
    -t, --dpth                         Delete persistence threshold [default: -1]
    --stat                             Enable RocksDB's internal Perf and IOstat monitoring [default: 0]
    -i, --inserts                      The number of unique inserts to issue in the experiment [default: 1]
```

For example, to run with a custom number of inserts and a specific file size, you might use:

```bash
./working_version --inserts 1000000 --file_size 512
```