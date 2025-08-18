#!/bin/bash

RESULTS_DIR=".result"

TAG=variedlambdametadata
SETTINGS="lowpri_false"
LOW_PRI=0

INSERTS=500000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=0

SIZE_RATIO=10

ENTRY_SIZES=(1024)
PAGE_SIZES=(4096)

KEY_SIZES=(8 16 32 64 128 256 512)

BUCKET_COUNT=100000
PREFIX_LENGTH=6
THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

SHOW_PROGRESS=1
SANITY_CHECK=0
SORT_WORKLOAD=1

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
cd "$RESULTS_DIR"

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
    if   [ "$PAGE_SIZE" -eq 2048 ];  then PAGES_PER_FILE_LIST=(4096)
        #64mb
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
          for KEY_SIZE in "${KEY_SIZES[@]}"; do
            if [ "$KEY_SIZE" -gt "$ENTRY_SIZE" ]; then
              echo "Skipping: KEY_SIZE (${KEY_SIZE}) > ENTRY_SIZE (${ENTRY_SIZE})"
              continue
            fi

            LAMBDA=$(awk -v k="$KEY_SIZE" -v e="$ENTRY_SIZE" 'BEGIN { printf "%.8f", k/e }')
            LAMBDA_TAG=$(echo "$LAMBDA" | sed -e 's/0\+$//' -e 's/\.$//')

            RUN_TAG="${TAG}-K${KEY_SIZE}-L${LAMBDA_TAG}"

            EXP_DIR="${RUN_TAG}-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
            mkdir -p "$EXP_DIR"
            cd "$EXP_DIR"

            echo
            echo "SETTINGS: ${SETTINGS}"
            echo "Debug: I=${INSERTS}, U=${UPDATES}, Q=${POINT_QUERIES}, S=${RANGE_QUERIES}, Y=${SELECTIVITY}"
            echo "Generating workload ... (E=${ENTRY_SIZE}, K=${KEY_SIZE}, L=${LAMBDA_TAG}, PAGE_SIZE=${PAGE_SIZE}, PAGES_PER_FILE=${PAGES_PER_FILE})"
            echo

            "${LOAD_GEN}" \
              -I "${INSERTS}" \
              -U "${UPDATES}" \
              -Q "${POINT_QUERIES}" \
              -S "${RANGE_QUERIES}" \
              -Y "${SELECTIVITY}" \
              -E "${ENTRY_SIZE}" \
              -L "${LAMBDA_TAG}"

            if [[ "$SORT_WORKLOAD" -eq 1 ]]; then
              WORKLOAD_FILE="workload.txt"
              INSERTS_FILE=$(mktemp)
              POINT_QUERIES_FILE=$(mktemp)
              RANGE_QUERIES_FILE=$(mktemp)

              grep '^I ' "$WORKLOAD_FILE" > "$INSERTS_FILE"
              grep '^Q ' "$WORKLOAD_FILE" > "$POINT_QUERIES_FILE"
              grep '^S ' "$WORKLOAD_FILE" > "$RANGE_QUERIES_FILE"

              cat "$INSERTS_FILE" "$POINT_QUERIES_FILE" "$RANGE_QUERIES_FILE" > "$WORKLOAD_FILE"

              rm -f "$INSERTS_FILE" "$POINT_QUERIES_FILE" "$RANGE_QUERIES_FILE"
            fi

            for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
              BUFFER_IMPL="${BUFFER_IMPLEMENTATIONS[${impl}]}"

              if [[ "$BUFFER_IMPL" == "Vector" || "$BUFFER_IMPL" == "UnsortedVector" || "$BUFFER_IMPL" == "AlwayssortedVector" ]]; then
                mkdir -p "${BUFFER_IMPL}-dynamic"
                cd "${BUFFER_IMPL}-dynamic"
                cp ../workload.txt ./workload.txt
                for run in 1 2 3; do
                  echo "Run ${BUFFER_IMPL}-dynamic trial #${run}"
                  "${WORKING_VERSION}" --memtable_factory="${impl}" \
                    -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                    -A 0 --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                  mv db/LOG "./LOG${run}"
                  mv workload.log "./workload${run}.log"
                  rm -rf "db"
                done
                rm -rf "workload.txt"
                cd ..

                mkdir -p "${BUFFER_IMPL}-preallocated"
                cd "${BUFFER_IMPL}-preallocated"
                cp ../workload.txt ./workload.txt
                for run in 1 2 3; do
                  echo "Run ${BUFFER_IMPL}-preallocated trial #${run}"
                  "${WORKING_VERSION}" --memtable_factory="${impl}" \
                    -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                    --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                  mv db/LOG "./LOG${run}"
                  mv workload.log "./workload${run}.log"
                  rm -rf "db"
                done
                rm -rf "workload.txt"

              elif [[ "$BUFFER_IMPL" == "hash_skip_list" || "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                mkdir -p "${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
                cd "${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
                cp ../workload.txt ./workload.txt
                for run in 1 2 3; do
                  echo "Run ${BUFFER_IMPL} trial #${run}"
                  if [[ "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                    "${WORKING_VERSION}" --memtable_factory="${impl}" \
                      -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                      -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                      --bucket_count="${BUCKET_COUNT}" --prefix_length="${PREFIX_LENGTH}" \
                      --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                      --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                  else
                    "${WORKING_VERSION}" --memtable_factory="${impl}" \
                      -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                      -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                      --bucket_count="${BUCKET_COUNT}" --prefix_length="${PREFIX_LENGTH}" \
                      --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                  fi
                  mv db/LOG "./LOG${run}"
                  mv workload.log "./workload${run}.log"
                  rm -rf "db"
                done
                rm -rf "workload.txt"

              else
                mkdir -p "${BUFFER_IMPL}"
                cd "${BUFFER_IMPL}"
                cp ../workload.txt ./workload.txt
                for run in 1 2 3; do
                  echo "Run ${BUFFER_IMPL} trial #${run}"
                  "${WORKING_VERSION}" --memtable_factory="${impl}" \
                    -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                    --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                  mv db/LOG "./LOG${run}"
                  mv workload.log "./workload${run}.log}"
                  rm -rf "db"
                done
                rm -rf "workload.txt"
              fi

              cd ..
            done

            cd ..
          done
        done
        rm -rf "workload.txt"
    done
done

echo
echo "Finished experiments"
