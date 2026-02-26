#!/bin/bash
set -e


# ==============================================================================
TAG="lsmbuffer-concurrent-write-off-WAL-0-compression-disabled-feb24_unsortedvectest"
RUN_PREALLOCATED=0

declare -A BUFFER_IMPLEMENTATIONS=(
#   [1]="skiplist"
#   [2]="Vector"
#   [3]="hash_skip_list"
#   [4]="hash_linked_list"
  [5]="UnsortedVector"
#   [6]="AlwayssortedVector"
#   [7]="linkedlist"
#   [8]="simple_skiplist"
#   [9]="hash_vector"
)

ENTRY_SIZE=1024
LAMBDA=0.125
INSERTS=45000
UPDATES=0
POINT_QUERIES=1
POINT_DELETES=0
RANGE_QUERIES=0
SELECTIVITY=0
RANGE_DELETES=0
RANGE_DELETES_SEL=0

SIZE_RATIO=5
PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
#512mb
PAGES_PER_FILE=131072
LOW_PRI=0

# Hash/Bucket settings
BUCKET_COUNTS=(1 5 10 100000) 
PREFIX_LENGTH=6
THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

# Paths
PROJECT_ROOT=$(pwd)
GEN_SCRIPT="${PROJECT_ROOT}/scripts/generate_specs.py"
TECTONIC="${PROJECT_ROOT}/bin/tectonic-cli"
LOAD_GEN="${PROJECT_ROOT}/bin/load_gen"
WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"

WORKLOAD_TXT="workload.txt"
BASE_EXP_DIR=".results/sanitycheck-${TAG}-${INSERTS}-${POINT_QUERIES}-${RANGE_QUERIES}-${SELECTIVITY}-${LOW_PRI}"


bash ./scripts/rebuild.sh

mkdir -p "$BASE_EXP_DIR"

# ==============================================================================
# 3. WORKLOAD GENERATION (Toggle between New and Old here)
# ==============================================================================

# --- METHOD A: Python + Tectonic (NEW SCRIPT) ---
echo "Generating workload using Method A (Python + Tectonic)..."
python3 "$GEN_SCRIPT" \
    -I ${INSERTS} -U ${UPDATES} -Q ${POINT_QUERIES} -D ${POINT_DELETES} \
    -S ${RANGE_QUERIES} -Y ${SELECTIVITY} -R ${RANGE_DELETES} \
    -y ${RANGE_DELETES_SEL} -E ${ENTRY_SIZE} -L ${LAMBDA}
"$TECTONIC" generate -w "workload.specs.json"

# --- METHOD B: load_gen (OLD SCRIPT) ---
# echo "Generating workload using Method B (load_gen)..."
# "${LOAD_GEN}" -I "${INSERTS}" -U "${UPDATES}" -Q "${POINT_QUERIES}" \
#              -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" \
#              -L "${LAMBDA}"


mv "$WORKLOAD_TXT" "$BASE_EXP_DIR/"
cd "$BASE_EXP_DIR"

# ==============================================================================
# 4. EXECUTION LOOP
# ==============================================================================
for mem in "${!BUFFER_IMPLEMENTATIONS[@]}"; do 
    mem_name="${BUFFER_IMPLEMENTATIONS[$mem]}"
    
    # --- BRANCH 1: Hash-based (bucket loops) ---
    if [[ "$mem_name" == *"hash"* ]]; then
        for BUCKET_COUNT in "${BUCKET_COUNTS[@]}"; do
            DIR_NAME="buffer-${mem}-${mem_name}-H${BUCKET_COUNT}"
            mkdir -p "$DIR_NAME"
            cp "$WORKLOAD_TXT" "./$DIR_NAME/workload.txt"
            
            echo "------------------------------------------------"
            echo "Running: $mem_name (ID: $mem) | Buckets: $BUCKET_COUNT"
            cd "$DIR_NAME"
            
            "$WORKING_VERSION" --memtable_factory="$mem" \
                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 \
                --bucket_count="${BUCKET_COUNT}" --prefix_length="${PREFIX_LENGTH}" \
                --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" > "rocksdb_stats.log"

            [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
            [ -f "workload.log" ] && mv workload.log workload_run.log
            rm -rf db && cd ..
        done

    # --- BRANCH 2: Vector Types (Dynamic & Optional Preallocated) ---
    elif [[ "$mem_name" == "Vector" || "$mem_name" == "UnsortedVector" || "$mem_name" == "AlwayssortedVector" ]]; then
        
        # 1. DYNAMIC (Uses -A 0)
        DIR_NAME="buffer-${mem}-${mem_name}-dynamic"
        mkdir -p "$DIR_NAME"
        cp "$WORKLOAD_TXT" "./$DIR_NAME/workload.txt"
        echo "Running: $mem_name (ID: $mem) - DYNAMIC"
        cd "$DIR_NAME"
        "$WORKING_VERSION" --memtable_factory="$mem" \
            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
            -E "${ENTRY_SIZE}" -A 0 --lowpri "${LOW_PRI}" --stat 1 > "rocksdb_stats.log"
        [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
        [ -f "workload.log" ] && mv workload.log workload_run.log
        rm -rf db && cd ..

        # 2. PREALLOCATED (Conditional based on RUN_PREALLOCATED flag)
        if [ "$RUN_PREALLOCATED" -eq 1 ]; then
            DIR_NAME="buffer-${mem}-${mem_name}-preallocated"
            mkdir -p "$DIR_NAME"
            cp "$WORKLOAD_TXT" "./$DIR_NAME/workload.txt"
            echo "Running: $mem_name (ID: $mem) - PREALLOCATED"
            cd "$DIR_NAME"
            "$WORKING_VERSION" --memtable_factory="$mem" \
                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "rocksdb_stats.log"
            [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
            [ -f "workload.log" ] && mv workload.log workload_run.log
            rm -rf db && cd ..
        fi

    # --- BRANCH 3: Standard (SkipList, LinkedList, etc.) ---
    else
        DIR_NAME="buffer-${mem}-${mem_name}"
        mkdir -p "$DIR_NAME"
        cp "$WORKLOAD_TXT" "./$DIR_NAME/workload.txt"
        echo "Running: $mem_name (ID: $mem)"
        cd "$DIR_NAME"
        "$WORKING_VERSION" --memtable_factory="$mem" \
            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
            -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "rocksdb_stats.log"
        [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
        [ -f "workload.log" ] && mv workload.log workload_run.log
        rm -rf db && cd ..
    fi
done


echo "------------------------------------------------"
echo "All experiments finished. Cleaning up workload files..."

# Remove the master workload file in the current directory (BASE_EXP_DIR)
rm -f "$WORKLOAD_TXT"

# Remove all secondary copies inside the implementation subdirectories
find . -maxdepth 2 -name "$WORKLOAD_TXT" -delete

echo "Cleanup complete."