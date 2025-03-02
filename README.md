# Anatomy of the LSM Memory Buffer: Insights & Implications

This repository contains benchmarking for different write buffer implementations in LSM-based systems. We use RocksDB source as base to profile four type of write buffers (i) _skip-list_, (ii) _vector_ (iii) _hash skip-list_ (iv) _hash linked-list_. All four implemenations are already implemented in RocksDB.

We also use a `KV-WorkloadGenerator` as a submodule for generating different type of workloads with different compositions (inserts, updates, point queries and range queries etc).

## Pre-requisites
The step 1 is to clone this respository in your local machine. You can do this by running the following command:
```bash
git clone https://github.com/SSD-Brandeis/LSMMemoryProfiling
```

You might also need `cmake` and `make`. Other than this you may need to install `libgflags-dev` for RocksDB. You can install this by running the following command:
```bash
sudo apt-get install libgflags-dev
```

After installing the dependencies, you can run the following command to install all the submodules and build the project. Make sure you are in the root directory of the project:
```bash
git submodule update --init --recursive
mkdir build
cd build
cmake ..
make -j66  # 66 is the number of cores, change if required
```

The above command will do the following:
1. Install the `KV-WorkloadGenerator` submodule.
2. Build the RocksDB source code.
3. Build the `KV-WorkloadGenerator` source code.
4. Build the `working_version` source code. (This you can find in `./MemoryProfiling/examples/__working_branch` directory)

## Running the benchmarks

### Step 1: Generating the workload
To run any benchmark, you have to first generate the workload. You can do this by going to the `KV-WorkloadGenerator` directory and running the following command:
```bash
./load_gen -I 100 -U 50 -Q 50 -S 100 -Y 0.1

# -I: Inserts
# -U: Updates
# -Q: Point Queries
# -S: Range Queries
# -Y: Range Query Selectivity

# You can also read the README file of KV-WorkloadGenerator for more details.
```
**Note**: The above command will generate a `workload.txt` file in the same directory.

### Step 2: Running the benchmark
After generating the workload, you may want to copy paste the `workload.txt` to the `./MemoryProfiling/examples/__working_branch` directory.

Great! Now you can run the benchmark by running the following command:
```bash
cd MemoryProfiling/examples/__working_branch

./working_version 
```

The above command will run the benchmark with default parameters. You can also pass different parameters to the benchmark. For example, if you want to run the benchmark with `vector` write buffer, you can run the following command:
```bash
./working_version --memtable_factory=2
```

### Step 3: Analyzing the results
After running the benchmark, you can find the results in the `./MemoryProfiling/examples/__working_branch` directory. The execution will generate a workload.log file which contains the execution details including the time taken for each operation. You can also find the `db_working_home` directory which contains the RocksDB database files.



---
> The `working_verion` takes few arguments as input. Here is the list of arguments:
```bash
RocksDB_parser.

  OPTIONS:

      This group is all exclusive:
        -d[d], --destroy=[d]              Destroy and recreate the database
                                          [def: 1]
        --cc=[cc]                         Clear system cache [def: 1]
        -T[T], --size_ratio=[T]           The size ratio for the LSM [def: 10]
        -P[P], --buffer_size_in_pages=[P] The number of pages in memory buffer
                                          [def: 4096]
        -B[B], --entries_per_page=[B]     The number of entries in one page
                                          [def: 4]
        -E[E], --entry_size=[E]           The size of one entry you have in
                                          workload.txt [def: 1024 B]
        -M[M], --memory_size=[M]          The memory buffer size in bytes [def:
                                          16 MB]
        -f[file_to_memtable_size_ratio],
        --file_to_memtable_size_ratio=[file_to_memtable_size_ratio]
                                          The ratio between files and memtable
                                          [def: 1]
        -F[file_size],
        --file_size=[file_size]           The size of one SST file [def: 256 KB]
        -V[verbosity],
        --verbosity=[verbosity]           The verbosity level of execution
                                          [0,1,2; def: 0]
        -c[compaction_pri],
        --compaction_pri=[compaction_pri] [Compaction priority: 1 for
                                          kMinOverlappingRatio, 2 for
                                          kByCompensatedSize, 3 for
                                          kOldestLargestSeqFirst, 4 for
                                          kOldestSmallestSeqFirst; def: 1]
        -C[compaction_style],
        --compaction_style=[compaction_style]
                                          [Compaction priority: 1 for
                                          kCompactionStyleLevel, 2 for
                                          kCompactionStyleUniversal, 3 for
                                          kCompactionStyleFIFO, 4 for
                                          kCompactionStyleNone; def: 1]
        -b[bits_per_key],
        --bits_per_key=[bits_per_key]     The number of bits per key assigned to
                                          Bloom filter [def: 10]
        --bb=[bb]                         Block cache size in MB [def: 8 MB]
        -s[show_progress],
        --sp=[show_progress]              Show progress [def: 0]
        -t[del_per_th],
        --dpth=[del_per_th]               Delete persistence threshold [def: -1]
        --stat=[enable_rocksdb_perf_iostat]
                                          Enable RocksDBs internal Perf and
                                          IOstat [def: 0]
        -i[inserts], --inserts=[inserts]  The number of unique inserts to issue
                                          in the experiment [def: 1]
        -m[memtable_factory],
        --memtable_factory=[memtable_factory]
                                          [Memtable Factory: 1 for Skiplist, 2
                                          for Vector, 3 for Hash Skiplist, 4 for
                                          Hash Linkedlist; def: 1]
        -X[prefix_length],
        --prefix_length=[prefix_length]   [Prefix Length: Number of bytes of the
                                          key forming the prefix; def: 0]
        -H[bucket_count],
        --bucket_count=[bucket_count]     [Bucket Count: Number of buckets for
                                          the hash table in HashSkipList &
                                          HashLinkList Memtables; def: 50000]
        --threshold_use_skiplist=[threshold_use_skiplist]
                                          [Threshold Use SkipList: Threshold
                                          based on which the conversion will
                                          happen from HashLinkList to
                                          HashSkipList; def: 256]
        -A[preallocation_vector_size],
        --preallocation_size=[preallocation_vector_size]
                                          [Preallocation Vector Size: Size to
                                          preallocation to vector memtable; def:
                                          0]
