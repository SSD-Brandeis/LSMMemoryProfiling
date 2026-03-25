#!/bin/bash
set -e


# ==============================================================================
# TAG="lsmbuffer-concurrent-write-off-WAL-0-compression-disabled-feb24_unsortedvectest"
TAG="multiphase_ondisk_setup2_t2_rq"
RUN_DYNAMIC=0

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
#   [2]="vector"
#   [3]="hash_skip_list"
#   [4]="hash_linked_list"
#   [5]="unsortedvector"
#   [6]="alwayssortedVector"
#   [7]="linkedlist"
#   [8]="simple_skiplist"
#   [9]="hash_vector"

)

#setup1_t10_32mb
# ENTRY_SIZE=32
# LAMBDA=0.25
# INSERTS=100000000
# UPDATES=0
# POINT_QUERIES=10000
# POINT_DELETES=0
# RANGE_QUERIES=0
# SELECTIVITY=0
# RANGE_DELETES=0
# RANGE_DELETES_SEL=0

# SIZE_RATIO=10
# PAGE_SIZE=4096
# ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
# #512mb
# # PAGES_PER_FILE=131072
# #128MB
# # PAGES_PER_FILE=32768 
# #32MB
# PAGES_PER_FILE=8192
# LOW_PRI=1

# THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

#setup2_t2_1mb
ENTRY_SIZE=32
LAMBDA=0.25
INSERTS=1000000
UPDATES=0
POINT_QUERIES=0
POINT_DELETES=0
RANGE_QUERIES=1000
SELECTIVITY=0.0001
RANGE_DELETES=0
RANGE_DELETES_SEL=0

SIZE_RATIO=2
PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
#512mb
# PAGES_PER_FILE=131072
#128MB
# PAGES_PER_FILE=32768 
# 32MB
# PAGES_PER_FILE=8192
#1MB
PAGES_PER_FILE=256
LOW_PRI=0

THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}


PROJECT_ROOT=$(pwd)
GEN_SCRIPT="${PROJECT_ROOT}/scripts/generate_specs.py"
TECTONIC="${PROJECT_ROOT}/bin/tectonic-cli"
LOAD_GEN="${PROJECT_ROOT}/bin/load_gen"
WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"

WORKLOAD_TXT="workload.txt"
BASE_EXP_DIR=".results/-${TAG}-I-${INSERTS}-U-${UPDATES}-PQ-${POINT_QUERIES}-RQ-${RANGE_QUERIES}-S-${SELECTIVITY}-Low_Pri${LOW_PRI}-Size_Ratio${SIZE_RATIO}"


bash ./scripts/rebuild.sh

mkdir -p "$BASE_EXP_DIR"


# cd "$BASE_EXP_DIR"
# --- METHOD A: tectonic ---
echo "Generating workload using Method A (Python + Tectonic)..."
# Generating the spec file based on the variables defined above
# python3 "$GEN_SCRIPT" \
#     -I ${INSERTS} -U ${UPDATES} -Q ${POINT_QUERIES} -D ${POINT_DELETES} \
#     -S ${RANGE_QUERIES} -Y ${SELECTIVITY} -R ${RANGE_DELETES} \
#     -y ${RANGE_DELETES_SEL} -E ${ENTRY_SIZE} -L ${LAMBDA}

# Strict check for spec file in the .results directory only
if [ -f "$BASE_EXP_DIR/workload.specs.json" ]; then
    SPEC_PATH="$BASE_EXP_DIR/workload.specs.json"
    echo "Found spec file at: $SPEC_PATH"
else
    echo "Error: workload.specs.json not found in $BASE_EXP_DIR."
    exit 1
fi

# Run tectonic to generate workload.txt in the current directory
# "$TECTONIC" generate -w "$SPEC_PATH"

# Logic to handle existing workload.txt in .results
MASTER_WORKLOAD="$PROJECT_ROOT/$BASE_EXP_DIR/$WORKLOAD_TXT"

if [ -f "$WORKLOAD_TXT" ]; then
    echo "Moving $WORKLOAD_TXT from root to $BASE_EXP_DIR"
    mv "$WORKLOAD_TXT" "$BASE_EXP_DIR/"
    [ -f "workload.specs.json" ] && mv "workload.specs.json" "$BASE_EXP_DIR/"
elif [ -f "$MASTER_WORKLOAD" ]; then
    echo "Using existing workload found in $BASE_EXP_DIR"
else
    echo "Error: $WORKLOAD_TXT not found in root or $BASE_EXP_DIR."
    exit 1
fi

cd "$BASE_EXP_DIR"


for mem in "${!BUFFER_IMPLEMENTATIONS[@]}"; do 
    mem_name="${BUFFER_IMPLEMENTATIONS[$mem]}"
    
    # --- BRANCH 1: Hash-based (bucket loops) ---
    if [[ "$mem_name" == *"hash"* ]]; then
        
        HASH_SETTINGS=("6:100000")

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
        
        # 1. PREALLOCATED (Always Runs)
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

        # 2. DYNAMIC (Uses -A 0) - Only runs if RUN_DYNAMIC=1
        if [ "$RUN_DYNAMIC" -eq 1 ]; then
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
        fi

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
echo "All experiments finished. Cleaning up workload copies..."


find . -mindepth 2 -name "$WORKLOAD_TXT" -delete

echo "Cleanup complete."