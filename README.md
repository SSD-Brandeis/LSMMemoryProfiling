# Mind the Buffer: Dissecting the LSM-Buffer Design Space

This repository contains benchmarking for different write buffer implementations in LSM-based systems. We use RocksDB source as base to profile four type of write buffers (i) V-Qsort, (ii) V-Qscan (iii) V-Sorted (iv) Link-L (v) Skip-L (vi)InSkip-L (vii)Hash-SL (viii)Hash-LL (iX)Hash-V. All 9 implemenations are already implemented in RocksDB.

We also use a `KV-WorkloadGenerator` and `Tectonic` as a submodule for generating different type of workloads with different compositions (inserts, updates, point queries and range queries etc) and different distributions (uniform, Beta, etc).

## Pre-requisites
The first step is to clone this repository to your local machine. Run:
```bash
git clone https://github.com/SSD-Brandeis/LSMMemoryProfiling
cd LSMMemoryProfiling
```

This repository now includes `setup.sh` and `scripts/rebuild.sh`.

### Install dependencies and build
On macOS or Linux, run:
```bash
bash ./setup.sh
```

Then build the project and its submodules with:
```bash
bash ./scripts/rebuild.sh
```

`./scripts/rebuild.sh` will:
1. initialize and update git submodules, including `lib/Tectonic` and `lib/KV-WorkloadGenerator`
2. build RocksDB and the `working_version` benchmark binary
3. build the Tectonic CLI and copy it to `./bin/tectonic-cli`

After the build completes, you should have:
- `./bin/working_version`
- `./bin/tectonic-cli`

## Running the benchmarks

### Generating the workload
To run any benchmark, you have to first generate the workload. You can do this by going to the `Tectonic` directory and running the following command: 
```bash
./tectonic-cli generate -w workload.spec.json -o workload_output_path
```
Generate takes in a path to a json workload specification file and an output file path as well. Without an output path, it will default to spec.txt. Generate will output a special text file that you can then feed into the execute command to execute the workload on a database. Use --help with the generate command to see additional flags that can be used with generate


Alternatively, you can do this by going to the `KV-WorkloadGenerator` directory and running the following command:
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

### Running the benchmark
After generating the workload, you may want to copy paste the `workload.txt` to the `./LSMMemoryProfiling/examples/__working_branch` directory.

Great! Now you can run the benchmark by running the following command:
```bash
cd LSMMemoryProfiling/examples/__working_branch

./working_version 
```

The above command will run the benchmark with default parameters. You can also pass different parameters to the benchmark. For example, if you want to run the benchmark with `vector` write buffer, you can run the following command:
```bash
./working_version --memtable_factory=2
```



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
