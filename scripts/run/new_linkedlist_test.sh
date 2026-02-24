#!/bin/bash
set -e
bash ./scripts/rebuild.sh
#!/bin/bash
# rm -rf build
# mkdir build
# cd build
# cmake -DCMAKE_BUILD_TYPE=Release ..
# make -j$(nproc)

# -------------------------------
# Config
# -------------------------------
TAG="lsmbuffer-concurrent-write-off-WAL-0-compression-disabled-feb21_test"

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="Vector"
#   [3]="hash_skip_list"
#   [4]="hash_linked_list"
#   [5]="UnsortedVector"
#   [6]="AlwayssortedVector"
  [7]="linkedlist"
  [8]="simple_skiplist"
)

ENTRY_SIZE=128
LAMBDA=0.125
INSERTS=100000
UPDATES=0
POINT_QUERIES=10
POINT_DELETES=0
RANGE_QUERIES=1
SELECTIVITY=0.1
RANGE_DELETES=0
RANGE_DELETES_SEL=0

SIZE_RATIO=5
PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
PAGES_PER_FILE=131072
LOW_PRI=1

WORKLOAD_TXT="workload.txt"
BASE_EXP_DIR=".vstats/sanitycheck-${TAG}-${INSERTS}"

PROJECT_ROOT=$(pwd)
GEN_SCRIPT="${PROJECT_ROOT}/scripts/generate_specs.py"
TECTONIC="${PROJECT_ROOT}/bin/tectonic-cli"
ROCKSDB_EXE="${PROJECT_ROOT}/bin/rocksdb_experiment"
WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"



mkdir -p "$BASE_EXP_DIR"
cd "$BASE_EXP_DIR" || exit

# -------------------------------
# Generate inserts-only workload
# -------------------------------
echo "Generating inserts-only workload..."
python3 "$GEN_SCRIPT" \
    -I ${INSERTS} -U ${UPDATES} -Q ${POINT_QUERIES} -D ${POINT_DELETES} \
    -S ${RANGE_QUERIES} -Y ${SELECTIVITY} -R ${RANGE_DELETES} \
    -y ${RANGE_DELETES_SEL} -E ${ENTRY_SIZE} -L ${LAMBDA}

"$TECTONIC" generate -w "workload.specs.json"

# -------------------------------
# Execution Loop
# -------------------------------
# Iterate over the IDs defined in the array
for mem in "${!BUFFER_IMPLEMENTATIONS[@]}"; do 
    mem_name="${BUFFER_IMPLEMENTATIONS[$mem]}"
    DIR_NAME="buffer-${mem}-${mem_name}"
    
    mkdir -p "$DIR_NAME"
    echo "------------------------------------------------"
    echo "Running experiment: $mem_name (ID: $mem)"
    echo "Directory: $DIR_NAME"
    echo "------------------------------------------------"
    
    cd "$DIR_NAME"

    # Copy workload into current dir or Option B wrapper will fail
    cp "../$WORKLOAD_TXT" ./workload.txt
    
    LOG_FILE="rocksdb_stats.log"

    # --- OPTION A: Standard RocksDB ---
    # "$ROCKSDB_EXE" \
    #     --db db \
    #     --workload "./$WORKLOAD_TXT" \
    #     --disableWAL=1 \
    #     --sync=0 \
    #     --no_slowdown=0 \
    #     --low_pri=0 \
    #     --memtable="$mem" | tee "$LOG_FILE"

    # --- OPTION B: Working Version 
    "$WORKING_VERSION" --memtable_factory="$mem" \
        -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
        -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
        -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "$LOG_FILE"

    # Preserve logs
    if [ -f "db/LOG" ]; then
        mv db/LOG LOG_rocksdb
    fi
    if [ -f "workload.log" ]; then
        mv workload.log workload_run.log
    fi

    rm -rf db
    cd ..
done

echo "All experiments finished."