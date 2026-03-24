#!/bin/bash
set -e

# Rebuild the project before starting
bash ./scripts/rebuild.sh

RESULTS_DIR=".result"

# ================= CONFIGURATION =================
RUN_PREALLOCATED=0 
# =================================================

# MOVED THESE UP so TAG can use them
INSERTS=740000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=500

# Now TAG will correctly expand the numbers
TAG="hashhybrid_newsetting_fig14_316-I${INSERTS}-U${UPDATES}-PQ${POINT_QUERIES}-RQ${RANGE_QUERIES}-S${SELECTIVITY}"
SETTINGS="lowpri_true"
LOW_PRI=0

SIZE_RATIO=10
ENTRY_SIZES=(128)
LAMBDA=0.0625
PAGE_SIZES=(4096)

BUCKET_COUNTS=(1 2 5 10 100000)
PREFIX_LENGTH=6
THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [9]="hash_vector"
)

# --- FIXED PATH LOGIC ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"
LOAD_GEN="${PROJECT_ROOT}/bin/load_gen"
WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"

if [ ! -f "$LOAD_GEN" ]; then
    echo "Error: load_gen not found at $LOAD_GEN"
    exit 1
fi

reorder_workload() {
  echo "No Ordering Done"
}

mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
    if   [ "$PAGE_SIZE" -eq 2048 ];  then PAGES_PER_FILE_LIST=(4096)
    elif [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE_LIST=(32768)
    else PAGES_PER_FILE_LIST=(1024); fi

    for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
        ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

        for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
            EXP_DIR="${TAG}-${SETTINGS}-I${INSERTS}-P${PAGES_PER_FILE}"
            mkdir -p "$EXP_DIR"
            cd "$EXP_DIR"

            echo "Generating workload..."
            "${LOAD_GEN}" -I "${INSERTS}" -U "${UPDATES}" -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"
            reorder_workload "workload.txt"

            for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
                BUFFER_IMPL="${BUFFER_IMPLEMENTATIONS[$impl]}"

                # hash_vector enters here because it contains "hash"
                if [[ "$BUFFER_IMPL" == *"hash"* ]]; then
                    for BUCKET_COUNT in "${BUCKET_COUNTS[@]}"; do
                        DIR_NAME="${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
                        mkdir -p "$DIR_NAME"
                        cd "$DIR_NAME"
                        cp ../workload.txt ./workload.txt
                        
                        echo "Running ${BUFFER_IMPL} | H=${BUCKET_COUNT}"
                        CMD_ARGS=(
                            --memtable_factory="$impl"
                            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}"
                            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}"
                            -E "${ENTRY_SIZE}" --bucket_count="${BUCKET_COUNT}" 
                            --prefix_length="${PREFIX_LENGTH}" --lowpri "${LOW_PRI}" --stat 1
                        )
                        # Correctly excludes hash_vector from threshold flag
                        if [[ "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                            CMD_ARGS+=(--threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}")
                        fi
                        "${WORKING_VERSION}" "${CMD_ARGS[@]}" > "run1.log"
                        [ -f db/LOG ] && mv db/LOG "LOG1"
                        rm -rf db workload.txt && cd ..
                    done

                elif [[ "$BUFFER_IMPL" == "Vector" || "$BUFFER_IMPL" == "UnsortedVector" ]]; then
                    # hash_vector will NEVER reach here, which is what you want
                    modes=("dynamic")
                    [[ "$RUN_PREALLOCATED" -eq 1 ]] && modes+=("preallocated")
                    for mode in "${modes[@]}"; do
                        mkdir -p "${BUFFER_IMPL}-${mode}"
                        cd "${BUFFER_IMPL}-${mode}"
                        cp ../workload.txt .
                        EXTRA_ARGS=""
                        [[ "$mode" == "dynamic" ]] && EXTRA_ARGS="-A 0"
                        "${WORKING_VERSION}" --memtable_factory="$impl" \
                            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                            -E "${ENTRY_SIZE}" ${EXTRA_ARGS} --lowpri "${LOW_PRI}" --stat 1 \
                            > "run1.log"
                        rm -rf db workload.txt && cd ..
                    done
                else
                    mkdir -p "${BUFFER_IMPL}"
                    cd "${BUFFER_IMPL}"
                    cp ../workload.txt .
                    "${WORKING_VERSION}" --memtable_factory="$impl" \
                        -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                        -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                        -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "run1.log"
                    rm -rf db workload.txt && cd ..
                fi
            done
      # REMOVED: rm -f workload.txt 
            # This allows the master workload.txt to persist in the EXP_DIR
            cd ..
        done
    done
done
echo "Experiments finished."