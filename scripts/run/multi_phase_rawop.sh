#!/bin/bash
set -e


# ==============================================================================
# TAG="lsmbuffer-concurrent-write-off-WAL-0-compression-disabled-feb24_unsortedvectest"
TAG="multiphase"
RUN_PREALLOCATED=1

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [5]="unsortedvector"
  [6]="alwayssortedVector"
  [7]="linkedlist"
  [8]="simple_skiplist"
  [9]="hash_vector"
  # [10]="inplaceupdatesortedvector"
)

ENTRY_SIZE=128
LAMBDA=0.0625
INSERTS=740000
UPDATES=0
POINT_QUERIES=50000
POINT_DELETES=0
RANGE_QUERIES=1000
SELECTIVITY=0.1
RANGE_DELETES=0
RANGE_DELETES_SEL=0

SIZE_RATIO=10
PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
#512mb
# PAGES_PER_FILE=131072
#128MB
PAGES_PER_FILE=32768 
LOW_PRI=0

THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}


PROJECT_ROOT=$(pwd)
GEN_SCRIPT="${PROJECT_ROOT}/scripts/generate_specs.py"
TECTONIC="${PROJECT_ROOT}/bin/tectonic-cli"
LOAD_GEN="${PROJECT_ROOT}/bin/load_gen"
WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"

WORKLOAD_TXT="workload.txt"
BASE_EXP_DIR=".results/sanitycheck-${TAG}-I-${INSERTS}-U-${UPDATES}-PQ-${POINT_QUERIES}-RQ-${RANGE_QUERIES}-S-${SELECTIVITY}-Low_Pri${LOW_PRI}"


bash ./scripts/rebuild.sh

mkdir -p "$BASE_EXP_DIR"


# cd "$BASE_EXP_DIR"
# --- METHOD A: tectonic ---
echo "Generating workload using Method A (Python + Tectonic)..."
# python3 "$GEN_SCRIPT" \
#     -I ${INSERTS} -U ${UPDATES} -Q ${POINT_QUERIES} -D ${POINT_DELETES} \
#     -S ${RANGE_QUERIES} -Y ${SELECTIVITY} -R ${RANGE_DELETES} \
#     -y ${RANGE_DELETES_SEL} -E ${ENTRY_SIZE} -L ${LAMBDA}

# Check for spec file in current dir or results dir to allow toggling Python gen
if [ -f "workload.specs.json" ]; then
    SPEC_PATH="workload.specs.json"
elif [ -f "$BASE_EXP_DIR/workload.specs.json" ]; then
    SPEC_PATH="$BASE_EXP_DIR/workload.specs.json"
else
    echo "Error: workload.specs.json not found. Run it again."
    exit 1
fi

"$TECTONIC" generate -w "$SPEC_PATH"

# --- METHOD B: load_gen  ---
# echo "Generating workload using Method B (load_gen)..."
# "${LOAD_GEN}" -I "${INSERTS}" -U "${UPDATES}" -Q "${POINT_QUERIES}" \
#              -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" \
#              -L "${LAMBDA}"


# Move files to results dir if they were freshly generated in the root
[ -f "$WORKLOAD_TXT" ] && mv "$WORKLOAD_TXT" "$BASE_EXP_DIR/"
[ -f "workload.specs.json" ] && mv "workload.specs.json" "$BASE_EXP_DIR/"

# Source of truth for all buffer runs
MASTER_WORKLOAD="$PROJECT_ROOT/$BASE_EXP_DIR/$WORKLOAD_TXT"

cd "$BASE_EXP_DIR"


for mem in "${!BUFFER_IMPLEMENTATIONS[@]}"; do 
    mem_name="${BUFFER_IMPLEMENTATIONS[$mem]}"
    
    # --- BRANCH 1: Hash-based (bucket loops) ---
    if [[ "$mem_name" == *"hash"* ]]; then
        
        HASH_SETTINGS=("2:1000" "6:100000")

        for setting in "${HASH_SETTINGS[@]}"; do
            PREFIX_X="${setting%%:*}"
            BUCKET_H="${setting##*:}"
            
            DIR_NAME="buffer-${mem}-${mem_name}-X${PREFIX_X}-H${BUCKET_H}"
            mkdir -p "$DIR_NAME"
            cp "$MASTER_WORKLOAD" "./$DIR_NAME/workload.txt"
            
            echo "------------------------------------------------"
            echo "Running: $mem_name (ID: $mem) | X: $PREFIX_X | H: $BUCKET_H"
            cd "$DIR_NAME"
            
            "$WORKING_VERSION" --memtable_factory="$mem" \
                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 \
                --bucket_count="${BUCKET_H}" --prefix_length="${PREFIX_X}" \
                --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" > "rocksdb_stats.log"

            [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
            [ -f "workload.log" ] && mv workload.log workload_run.log
            rm -rf db && cd ..
        done

    # --- BRANCH 2: Vector Types (Dynamic & Preallocated) ---
    elif [[ "$mem_name" == "vector" || "$mem_name" == "unsortedvector" || "$mem_name" == "alwayssortedVector" ]]; then
        
        # 1. DYNAMIC (Uses -A 0)
        DIR_NAME="buffer-${mem}-${mem_name}-dynamic"
        mkdir -p "$DIR_NAME"
        cp "$MASTER_WORKLOAD" "./$DIR_NAME/workload.txt"
        
        echo "------------------------------------------------"
        echo "Running: $mem_name (ID: $mem) - DYNAMIC"
        cd "$DIR_NAME"
        
        "$WORKING_VERSION" --memtable_factory="$mem" \
            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
            -E "${ENTRY_SIZE}" -A 0 --lowpri "${LOW_PRI}" --stat 1 > "rocksdb_stats.log"
        [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
        [ -f "workload.log" ] && mv workload.log workload_run.log
        rm -rf db
        cd ..

        # 2. PREALLOCATED
        DIR_NAME="buffer-${mem}-${mem_name}-preallocated"
        mkdir -p "$DIR_NAME"
        cp "$MASTER_WORKLOAD" "./$DIR_NAME/workload.txt"
        
        echo "------------------------------------------------"
        echo "Running: $mem_name (ID: $mem) - PREALLOCATED"
        cd "$DIR_NAME"
        
        "$WORKING_VERSION" --memtable_factory="$mem" \
            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
            -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "rocksdb_stats.log"
        
        [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
        [ -f "workload.log" ] && mv workload.log workload_run.log
        rm -rf db && cd ..

    # --- BRANCH 3: Standard (SkipList, LinkedList, etc.) ---
    else
        DIR_NAME="buffer-${mem}-${mem_name}"
        mkdir -p "$DIR_NAME"
        cp "$MASTER_WORKLOAD" "./$DIR_NAME/workload.txt"
        
        echo "------------------------------------------------"
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

find . -maxdepth 2 -name "$WORKLOAD_TXT" -delete

echo "Cleanup complete."