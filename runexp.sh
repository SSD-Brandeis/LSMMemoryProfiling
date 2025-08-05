#!/bin/bash
RESULTS_DIR=".result"

TAG=debugcheck
SETTINGS="lowpri_false"
LOW_PRI=0

INSERTS=10000
UPDATES=0
RANGE_QUERIES=10
SELECTIVITY=0.1
POINT_QUERIES=100

SIZE_RATIO=10

ENTRY_SIZES=(128)
LAMBDA=0.5
PAGE_SIZES=(4096)

BUCKET_COUNT=100000
PREFIX_LENGTH=6
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

echo 3 | sudo tee /proc/sys/vm/drop_caches
sudo sysctl kernel.perf_event_paranoid=-1

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION="${PROJECT_DIR}/bin/working_version"

mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
    if   [ "$PAGE_SIZE" -eq 2048 ];  then PAGES_PER_FILE_LIST=(4096)
    elif [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE_LIST=(16384)
    elif [ "$PAGE_SIZE" -eq 8192 ];  then PAGES_PER_FILE_LIST=(1024)
    elif [ "$PAGE_SIZE" -eq 16384 ]; then PAGES_PER_FILE_LIST=(512)
    elif [ "$PAGE_SIZE" -eq 32768 ]; then PAGES_PER_FILE_LIST=(256)
    elif [ "$PAGE_SIZE" -eq 65536 ]; then PAGES_PER_FILE_LIST=(128)
    else
        echo "Unknown PAGE_SIZE: $PAGE_SIZE"
        exit 1
    fi

    for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
        ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

        for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
            EXP_DIR="${TAG}-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
            mkdir -p "$EXP_DIR"
            cd "$EXP_DIR"

            "${LOAD_GEN}" -I "${INSERTS}" -U "${UPDATES}" -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"

            for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
                BUFFER_IMPL="${BUFFER_IMPLEMENTATIONS[$impl]}"

                if [[ "$BUFFER_IMPL" == "Vector" || "$BUFFER_IMPL" == "UnsortedVector" || "$BUFFER_IMPL" == "AlwayssortedVector" ]]; then
                    mkdir -p "${BUFFER_IMPL}-dynamic"
                    cd "${BUFFER_IMPL}-dynamic"
                    cp ../workload.txt ./workload.txt
                    for run in 1 2 3; do
                        echo "Run ${BUFFER_IMPL}-dynamic trial #${run}"
                        "${WORKING_VERSION}" --memtable_factory="$impl" \
                            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                            -E "${ENTRY_SIZE}" -A 0 --lowpri "${LOW_PRI}" --stat 1 \
                            > "run${run}.log"
                        mv db/LOG        "LOG${run}"
                        mv workload.log  "workload${run}.log"
                        rm -rf db
                    done
                    rm -f workload.txt
                    cd ..

                    mkdir -p "${BUFFER_IMPL}-preallocated"
                    cd "${BUFFER_IMPL}-preallocated"
                    cp ../workload.txt ./workload.txt
                    for run in 1 2 3; do
                        echo "Run ${BUFFER_IMPL}-preallocated trial #${run}"
                        "${WORKING_VERSION}" --memtable_factory="$impl" \
                            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                            -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 \
                            > "run${run}.log"
                        mv db/LOG        "LOG${run}"
                        mv workload.log  "workload${run}.log"
                        rm -rf db
                    done
                    rm -f workload.txt
                    cd ..

                elif [[ "$BUFFER_IMPL" == "hash_skip_list" || "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                    mkdir -p "${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
                    cd "${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
                    cp ../workload.txt ./workload.txt
                    for run in 1 2 3; do
                        echo "Run ${BUFFER_IMPL} trial #${run}"
                        if [[ "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                            "${WORKING_VERSION}" --memtable_factory="$impl" \
                                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                                -E "${ENTRY_SIZE}" --bucket_count="${BUCKET_COUNT}" --prefix_length="${PREFIX_LENGTH}" \
                                --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                        else
                            "${WORKING_VERSION}" --memtable_factory="$impl" \
                                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                                -E "${ENTRY_SIZE}" --bucket_count="${BUCKET_COUNT}" --prefix_length="${PREFIX_LENGTH}" \
                                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                        fi
                        mv db/LOG        "LOG${run}"
                        mv workload.log  "workload${run}.log"
                        rm -rf db
                    done
                    rm -f workload.txt
                    cd ..

                else
                    mkdir -p "${BUFFER_IMPL}"
                    cd "${BUFFER_IMPL}"
                    cp ../workload.txt ./workload.txt
                    for run in 1 2 3; do
                        echo "Run ${BUFFER_IMPL} trial #${run}"
                        "${WORKING_VERSION}" --memtable_factory="$impl" \
                            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                            -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                        mv db/LOG        "LOG${run}"
                        mv workload.log  "workload${run}.log"
                        rm -rf db
                    done
                    rm -f workload.txt
                    cd ..
                fi
            done
            cd ..
        done
        rm -f workload.txt
    done
done
echo
echo "Finished experiments"
