#!/bin/bash
set -e

# ==============================================================================
# TAG="lsmbuffer-concurrent-write-off-WAL-0-compression-disabled-feb24_unsortedvectest"
TAG_BASE="multiphase_ondisk_3phase_varybuffer_t6"
RUN_DYNAMIC=0

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [5]="unsortedvector"
  [6]="alwayssortedVector"
#   [7]="linkedlist"
  [8]="simple_skiplist"
  [9]="hash_vector"
)

#t6_128mb
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

SIZE_RATIO=6
PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

# --- Added Loop for Varying Buffer Sizes ---
BUFFER_SIZES=(2 4 8 16 32 64 128)

for MB in "${BUFFER_SIZES[@]}"; do
    # Dynamic computation: 1MB = 256 pages
    PAGES_PER_FILE=$((MB * 256))

    # Updating Tag for this specific run
    TAG="${TAG_BASE}_${MB}MB"

    #512mb
    # PAGES_PER_FILE=131072
    #128MB
    # PAGES_PER_FILE=32768 
    # 32MB
    # PAGES_PER_FILE=8192
    #2mb 
    # PAGES_PER_FILE=512

    #1MB
    # PAGES_PER_FILE=256
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

    # --- WORKLOAD REUSE LOGIC ---
    echo "Searching for existing workload.txt in .results..."
    # Find the first workload.txt inside the .results directory
    EXISTING_WORKLOAD=$(find .results -name "workload.txt" -print -quit)

    if [ -n "$EXISTING_WORKLOAD" ]; then
        echo "Found existing workload at: $EXISTING_WORKLOAD"
        cp "$EXISTING_WORKLOAD" "$BASE_EXP_DIR/workload.txt"
    else
        echo "Error: No workload.txt found in the .results folder. Cannot proceed."
        exit 1
    fi

    # --- METHOD A: tectonic (Commented out to reuse existing workload) ---
    # Generating the spec file based on the variables defined above
    # python3 "$GEN_SCRIPT" \
    #     -I ${INSERTS} -U ${UPDATES} -Q ${POINT_QUERIES} -D ${POINT_DELETES} \
    #     -S ${RANGE_QUERIES} -Y ${SELECTIVITY} -R ${RANGE_DELETES} \
    #     -y ${RANGE_DELETES_SEL} -E ${ENTRY_SIZE} -L ${LAMBDA}

    # Removed strict check for .json file as per request.
    "$TECTONIC" generate -w "$SPEC_PATH"
    cd "$BASE_EXP_DIR"

    for mem in "${!BUFFER_IMPLEMENTATIONS[@]}"; do 
        mem_name="${BUFFER_IMPLEMENTATIONS[$mem]}"
        
        # --- BRANCH 1: Hash-based (bucket loops) ---
        if [[ "$mem_name" == *"hash"* ]]; then
            
            HASH_SETTINGS=("6:100000")

            for setting in "${HASH_SETTINGS[@]}"; do
                PREFIX_X="${setting%%:*}"
                BUCKET_H="${setting##*:}"
                
                DIR_NAME="buffer-${MB}MB-${mem}-${mem_name}-X${PREFIX_X}-H${BUCKET_H}"
                mkdir -p "$DIR_NAME"
                cp "workload.txt" "./$DIR_NAME/workload.txt"
                
                echo "------------------------------------------------"
                echo "Running: $mem_name (ID: $mem) | Size: ${MB}MB | X: $PREFIX_X | H: $BUCKET_H"
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
            DIR_NAME="buffer-${MB}MB-${mem}-${mem_name}-preallocated"
            mkdir -p "$DIR_NAME"
            cp "workload.txt" "./$DIR_NAME/workload.txt"
            
            echo "------------------------------------------------"
            echo "Running: $mem_name (ID: $mem) | Size: ${MB}MB - PREALLOCATED"
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
                DIR_NAME="buffer-${MB}MB-${mem}-${mem_name}-dynamic"
                mkdir -p "$DIR_NAME"
                cp "workload.txt" "./$DIR_NAME/workload.txt"
                
                echo "------------------------------------------------"
                echo "Running: $mem_name (ID: $mem) | Size: ${MB}MB - DYNAMIC"
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
            DIR_NAME="buffer-${MB}MB-${mem}-${mem_name}"
            mkdir -p "$DIR_NAME"
            cp "workload.txt" "./$DIR_NAME/workload.txt"
            
            echo "------------------------------------------------"
            echo "Running: $mem_name (ID: $mem) | Size: ${MB}MB"
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

    echo "Finished buffer size: ${MB}MB"
    echo "------------------------------------------------"

    # Return to project root before next buffer size iteration
    cd "$PROJECT_ROOT"

done

echo "------------------------------------------------"
echo "All experiments finished. Cleaning up workload copies..."

find .results -mindepth 3 -name "$WORKLOAD_TXT" -delete

echo "Cleanup complete."