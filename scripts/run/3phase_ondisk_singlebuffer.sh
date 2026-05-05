#!/bin/bash
set -e

# ==============================================================================
TAG_BASE="multiphase_ondisk_3phase_1mb_t6_newwl"
RUN_DYNAMIC=0

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [5]="unsortedvector"
  [6]="alwayssortedVector"
  [8]="simple_skiplist"
  [9]="hash_vector"
)

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

BUFFER_SIZES=(1)

for MB in "${BUFFER_SIZES[@]}"; do
    PAGES_PER_FILE=$((MB * 256))
    TAG="${TAG_BASE}_${MB}MB"
    LOW_PRI=0
    THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

    PROJECT_ROOT=$(pwd)
    WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"
    TECTONIC="${PROJECT_ROOT}/bin/tectonic-cli"
    
    BASE_EXP_DIR=".results/-${TAG}-I-${INSERTS}-U-${UPDATES}-PQ-${POINT_QUERIES}-RQ-${RANGE_QUERIES}-S-${SELECTIVITY}-Low_Pri${LOW_PRI}-Size_Ratio${SIZE_RATIO}"

    bash ./scripts/rebuild.sh
    mkdir -p "$BASE_EXP_DIR"

    # --- WORKLOAD GENERATION/SYMLINK LOGIC ---
    echo "Checking for workload in .results..."
    
    WORKLOAD_FILE=".results/workload.txt"
    SPECS_FILE=".results/workload.specs.json"

    # If workload.txt doesn't exist, generate it from specs
    if [ ! -f "$WORKLOAD_FILE" ]; then
        if [ -f "$SPECS_FILE" ]; then
            echo "workload.txt not found. Generating from $SPECS_FILE using tectonic-cli..."
            
            # Using -w based on your previous script usage
            "$TECTONIC" generate -w "$SPECS_FILE"
            
            # Handle naming inconsistency: check if it generated workload.specs.txt or workload.txt
            if [ ! -f "$WORKLOAD_FILE" ]; then
                if [ -f ".results/workload.specs.txt" ]; then
                    mv ".results/workload.specs.txt" "$WORKLOAD_FILE"
                elif [ -f "workload.txt" ]; then
                    mv "workload.txt" "$WORKLOAD_FILE"
                fi
            fi
        else
            echo "Error: Neither workload.txt nor workload.specs.json found in .results."
            exit 1
        fi
    fi

    # Final check to ensure generation succeeded
    if [ ! -f "$WORKLOAD_FILE" ]; then
        echo "Error: Workload generation failed. $WORKLOAD_FILE not found."
        exit 1
    fi

    WORKLOAD_SOURCE_ABS=$(realpath "$WORKLOAD_FILE")
    echo "Using workload source at: $WORKLOAD_SOURCE_ABS"
    
    # Symlink workload into the base experiment directory
    if [ "$WORKLOAD_SOURCE_ABS" != "$(realpath "$BASE_EXP_DIR/workload.txt" 2>/dev/null)" ]; then
        ln -sf "$WORKLOAD_SOURCE_ABS" "$BASE_EXP_DIR/workload.txt"
    fi

    cd "$BASE_EXP_DIR"

    for mem in "${!BUFFER_IMPLEMENTATIONS[@]}"; do 
        mem_name="${BUFFER_IMPLEMENTATIONS[$mem]}"
        
        run_experiment() {
            local dir=$1
            local extra_args=$2
            
            mkdir -p "$dir"
            ln -sf "$WORKLOAD_SOURCE_ABS" "./$dir/workload.txt"
            
            echo "------------------------------------------------"
            echo "Running: $mem_name | Dir: $dir"
            cd "$dir"
            
            "$WORKING_VERSION" --memtable_factory="$mem" \
                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 \
                $extra_args > "rocksdb_stats.log"

            [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
            [ -f "workload.log" ] && mv workload.log workload_run.log
            
            # Cleanup symlink and database
            rm -f workload.txt
            rm -rf db
            cd ..
        }

        if [[ "$mem_name" == *"hash"* ]]; then
            HASH_SETTINGS=("6:100000")
            for setting in "${HASH_SETTINGS[@]}"; do
                PREFIX_X="${setting%%:*}"
                BUCKET_H="${setting##*:}"
                DIR_NAME="buffer-${MB}MB-${mem}-${mem_name}-X${PREFIX_X}-H${BUCKET_H}"
                run_experiment "$DIR_NAME" "--bucket_count=${BUCKET_H} --prefix_length=${PREFIX_X} --threshold_use_skiplist=${THRESHOLD_TO_CONVERT_TO_SKIPLIST}"
            done

        elif [[ "$mem_name" == "vector" || "$mem_name" == "unsortedvector" || "$mem_name" == "alwayssortedVector" ]]; then
            # 1. PREALLOCATED
            run_experiment "buffer-${MB}MB-${mem}-${mem_name}-preallocated" ""
            
            # 2. DYNAMIC
            if [ "$RUN_DYNAMIC" -eq 1 ]; then
                run_experiment "buffer-${MB}MB-${mem}-${mem_name}-dynamic" "-A 0"
            fi
        else
            run_experiment "buffer-${MB}MB-${mem}-${mem_name}" ""
        fi
    done

    echo "Finished buffer size: ${MB}MB"
    cd "$PROJECT_ROOT"
done

echo "Experiments finished. The master workload remains at: $WORKLOAD_SOURCE_ABS"