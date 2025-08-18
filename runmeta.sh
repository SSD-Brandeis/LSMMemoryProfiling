#!/bin/bash

RESULTS_DIR=".result"

TAG=metadata
SETTINGS="lowpri_false"
LOW_PRI=0

INSERTS=1000000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=0

SIZE_RATIO=10

ENTRY_SIZES=(8 16 32 64 128 256 512 1024 2048 4096)
LAMBDA=0.5
# PAGE_SIZES=(2048 4096 8192 16384)
PAGE_SIZES=(8192)
BUCKET_COUNT=100000
PREFIX_LENGTH=4
THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

SHOW_PROGRESS=1
SANITY_CHECK=0

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="Vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [5]="UnsortedVector"
  [6]="AlwayssortedVector"
)

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION="${PROJECT_DIR}/bin/working_version"

mkdir -p "$RESULTS_DIR"

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
    if   [ "$PAGE_SIZE" -eq 2048 ];  then PAGES_PER_FILE_LIST=(4096)
    elif [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE_LIST=(2048)
    elif [ "$PAGE_SIZE" -eq 8192 ];  then PAGES_PER_FILE_LIST=(1024)
    elif [ "$PAGE_SIZE" -eq 16384 ];  then PAGES_PER_FILE_LIST=(512)
    else
        echo "Unsupported PAGE_SIZE: $PAGE_SIZE"
        exit 1
    fi

    for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
        ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

        for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
            EXP_DIR="${TAG}-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE_LIST}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
            FULL_EXP="$RESULTS_DIR/$EXP_DIR"

            mkdir -p "$FULL_EXP"
            pushd "$FULL_EXP" >/dev/null

            echo
            echo "SETTINGS: ${SETTINGS}"
            echo "Generating workload ... (ENTRY_SIZE=${ENTRY_SIZE}, LAMBDA=${LAMBDA})"
            echo

            "${LOAD_GEN}" \
                -I "${INSERTS}" -U "${UPDATES}" \
                -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -E "${ENTRY_SIZE}" -L "${LAMBDA}"

            echo

            for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
                BUFFER_IMPL="${BUFFER_IMPLEMENTATIONS[$impl]}"
       
                if [[ "$BUFFER_IMPL" == "Vector" || "$BUFFER_IMPL" == "UnsortedVector" || "$BUFFER_IMPL" == "AlwayssortedVector" ]]; then
                    IMPL_DIR="${BUFFER_IMPL}-dynamic"
                elif [[ "$BUFFER_IMPL" == "hash_skip_list" || "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                    IMPL_DIR="${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
                else
                    IMPL_DIR="${BUFFER_IMPL}"
                fi

                mkdir -p "$IMPL_DIR"
                pushd "$IMPL_DIR" >/dev/null

      
                cp "../workload.txt" .

                for run in 1 2 3; do
                    echo "Run ${BUFFER_IMPL} trial #${run}"
                    cmd=( "${WORKING_VERSION}" --memtable_factory="${impl}"
                          -I "${INSERTS}" -U "${UPDATES}" \
                          -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                          -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" \
                          -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                          --lowpri "${LOW_PRI}" --stat 1 )

                    if [[ "$BUFFER_IMPL" == "hash_skip_list" ]]; then
                        cmd+=( --bucket_count="${BUCKET_COUNT}" --prefix_length="${PREFIX_LENGTH}" )
                    elif [[ "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                        cmd+=( --bucket_count="${BUCKET_COUNT}" --prefix_length="${PREFIX_LENGTH}" \
                               --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" )
                    fi

                    "${cmd[@]}" > "run${run}.log"
                    mv db/LOG "LOG${run}"      2>/dev/null || true
                    mv workload.log "workload${run}.log" 2>/dev/null || true
                    rm -rf db
                done

                popd >/dev/null
            done

            rm -f workload.txt
            popd >/dev/null
        done
    done
done

echo
echo "Finished experiments"
