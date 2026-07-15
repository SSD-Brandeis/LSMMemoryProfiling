#!/bin/bash
set -e

bash ./scripts/rebuild.sh

TAG="correctness_check1"
ENTRY_SIZE=1024
PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

# 128MB buffer size: 32768 pages * 4096 bytes/page = 134,217,728 bytes (128MB)
PAGES_PER_FILE=32768
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRIES_PER_PAGE : $ENTRIES_PER_PAGE"
echo "PAGES_PER_FILE   : $PAGES_PER_FILE"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"

BASE_DIR=".vstats/${TAG}"
mkdir -p "$BASE_DIR/inmemory"
mkdir -p "$BASE_DIR/disk"

# Copy spec files to output directory
cp lib/Tectonic/specs/in_memory.specs.json "$BASE_DIR/inmemory/workload.specs.json"
cp lib/Tectonic/specs/to_disk.specs.json "$BASE_DIR/disk/workload.specs.json"

# Generate workloads
if [ ! -f "$BASE_DIR/inmemory/workload.txt" ]; then
    echo "Generating in-memory workload..."
    ./bin/tectonic-cli generate -w "$BASE_DIR/inmemory/workload.specs.json" -o "$BASE_DIR/inmemory/workload.txt"
fi

if [ ! -f "$BASE_DIR/disk/workload.txt" ]; then
    echo "Generating to-disk workload..."
    ./bin/tectonic-cli generate -w "$BASE_DIR/disk/workload.specs.json" -o "$BASE_DIR/disk/workload.txt"
fi

run_exp() {
    local factory_id=$1
    local name=$2
    local mode=$3 # "inmemory" or "disk"
    
    local work_dir="$BASE_DIR/$mode/$name"
    mkdir -p "$work_dir"
    cd "$work_dir"
    
    echo "Running $mode workload for $name..."
    cp ../workload.txt workload.txt
    ../../../../bin/working_version \
        --memtable_factory="$factory_id" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG || true
    rm -rf db workload.txt
    
    cd ../../../..
    echo -e "\n"
    sleep 2
}

# Run experiments for In-Memory scenario
run_exp 2 "vector-preallocated" "inmemory"
run_exp 11 "art" "inmemory"
run_exp 12 "btree" "inmemory"

# Run experiments for To-Disk scenario
run_exp 2 "vector-preallocated" "disk"
run_exp 11 "art" "disk"
run_exp 12 "btree" "disk"

echo "Done."
echo "Correctness experiments finished."
