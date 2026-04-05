#!/bin/bash
set -e

RESULTS_DIR=".result"
# Prefix length updated to 6
TAG="common_prefix_"
SETTINGS="lowpri_false"
LOW_PRI=0

INSERTS=740000
UPDATES=0
POINT_QUERIES=10000 
RANGE_QUERIES=1000 
SELECTIVITY=0.1
SIZE_RATIO=10
ENTRY_SIZES=(128)
LAMBDA=0.0625
PAGE_SIZES=(4096)

THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}
SORT_WORKLOAD=1

declare -A BUFFER_IMPLEMENTATIONS=(
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [9]="hash_vector"
)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"

LOAD_GEN="${PROJECT_ROOT}/bin/load_gen"
WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"

if [ ! -f "$LOAD_GEN" ] || [ ! -f "$WORKING_VERSION" ]; then
    echo "Error: Binaries not found in ${PROJECT_ROOT}/bin/"
    exit 1
fi

mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
  # if   [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE=131072
  #128mb
  if   [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE=32768 
  elif [ "$PAGE_SIZE" -eq 2048 ];  then PAGES_PER_FILE=4096
  else PAGES_PER_FILE=1024; fi

  for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
    ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

    # Updated directory naming convention: Insert, PQ, RQ, and S (Selectivity)
    EXP_DIR="${TAG}-varC-Insert${INSERTS}-PQ${POINT_QUERIES}-RQ${RANGE_QUERIES}-S${SELECTIVITY}"
    mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    echo "Generating workload..."
    "${LOAD_GEN}" -I "${INSERTS}" -U 0 -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"
    
    if [[ "$SORT_WORKLOAD" -eq 1 ]]; then
      grep '^I ' workload.txt > ifile.tmp || true
      grep '^Q ' workload.txt > qfile.tmp || true
      grep '^S ' workload.txt > sfile.tmp || true
      cat ifile.tmp qfile.tmp sfile.tmp > workload.txt
      rm -f *.tmp
    fi

    for C in {0..6}; do
      for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
        NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
        # Directory name reflects X6 prefix length
        OUT_DIR="${NAME}-X6-H1M-C${C}"
        mkdir -p "$OUT_DIR"; cp workload.txt "$OUT_DIR/"
        cd "$OUT_DIR"

        echo "Running ${NAME} | C=${C}"
        
        # prefix_length set to 6, 100k bucket count
        EXTRA_FLAGS="--bucket_count=100000 --prefix_length=6"
        if [[ "$NAME" == "hash_linked_list" ]]; then
          EXTRA_FLAGS="$EXTRA_FLAGS --threshold_use_skiplist=${THRESHOLD_TO_CONVERT_TO_SKIPLIST}"
        fi

        COMMON_PREFIX_C="${C}" "${WORKING_VERSION}" --memtable_factory="${impl}" \
          -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
          -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
          ${EXTRA_FLAGS} --lowpri "${LOW_PRI}" --stat 1 > "run1.log"

        [ -f "db/LOG" ] && mv db/LOG "./LOG1"
        [ -f "workload.log" ] && mv workload.log "./workload1.log"
        rm -rf db workload.txt
        cd ..
      done
    done
    
    # rm -f workload.txt  
    cd ..
  done
done

echo "Finished experiments."