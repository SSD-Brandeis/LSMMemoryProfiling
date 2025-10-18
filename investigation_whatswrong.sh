#!/bin/bash
set -e

RESULTS_DIR=".result"

TAG="exp_diff_inserts_entry_per_entrysize"
SETTINGS="lowpri_false"
LOW_PRI=0

# --- CHANGE: Static insert/query counts are removed from here ---
UPDATES=0
POINT_QUERIES=0
RANGE_QUERIES=100
SELECTIVITY=0.00001

SIZE_RATIO=10

ENTRY_SIZES=(32 64 128 256 512 1024)
PAGE_SIZES=(4096)

# --- NEW: Define the number of inserts for each entry size ---
# --- EDIT THESE VALUES AS NEEDED ---
declare -A INSERTS_MAP=(
  [32]=6000000
  [64]=4000000
  [128]=2000000
  [256]=1400000
  [512]=780000
  [1024]=440000
)

# declare -A INSERTS_MAP=(
#   [32]=6138571
#   [64]=4063566
#   [128]=2464873
#   [256]=1406655
#   [512]=785266
#   [1024]=447227
# )

SORT_WORKLOAD=1

declare -A BUFFER_IMPLEMENTATIONS=(
  [3]="hash_skip_list"
  [4]="hash_linked_list"
)

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION="${PROJECT_DIR}/bin/working_version"

mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
  if   [ "$PAGE_SIZE" -eq 2048 ];  then PAGES_PER_FILE=4096
  elif [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE=131072
  elif [ "$PAGE_SIZE" -eq 8192 ];  then PAGES_PER_FILE=1024
  elif [ "$PAGE_SIZE" -eq 16384 ]; then PAGES_PER_FILE=512
  elif [ "$PAGE_SIZE" -eq 32768 ]; then PAGES_PER_FILE=256
  elif [ "$PAGE_SIZE" -eq 65536 ]; then PAGES_PER_FILE=128
  else echo "Unknown PAGE_SIZE: $PAGE_SIZE"; exit 1; fi

  for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
    # --- CHANGE: Get the INSERTS count from the map for the current entry size ---
    INSERTS=${INSERTS_MAP[$ENTRY_SIZE]}

    # Check if a value was found in the map
    if [ -z "$INSERTS" ]; then
      echo "Error: Insert count for ENTRY_SIZE=${ENTRY_SIZE} is not defined in INSERTS_MAP."
      exit 1
    fi

    # Set the threshold based on the dynamic insert count
    THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

    LAMBDA=$(echo "scale=10; 8 / ${ENTRY_SIZE}" | bc)
    echo "--- Running with Entry Size: ${ENTRY_SIZE}, Inserts: ${INSERTS}, Lambda: ${LAMBDA} ---"

    ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

    EXP_DIR="${TAG}-insertPQRS_X8H1M-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
    mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    "${LOAD_GEN}" -I "${INSERTS}" -U 0 -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"
    if [[ "$SORT_WORKLOAD" -eq 1 ]]; then
      IFILE=$(mktemp); QFILE=$(mktemp); SFILE=$(mktemp)
      grep '^I ' workload.txt > "$IFILE" || true
      grep '^Q ' workload.txt > "$QFILE" || true
      grep '^S ' workload.txt > "$SFILE" || true
      cat "$IFILE" "$QFILE" "$SFILE" > workload.txt
      rm -f "$IFILE" "$QFILE" "$SFILE"
    fi

    for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
      NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
      mkdir -p "${NAME}"; cd "${NAME}"
      
      cp ../workload.txt ./workload.txt

      for run in 1 2 3; do
        echo "Run ${NAME} #${run} for Entry Size ${ENTRY_SIZE}"
        
        if [[ "$NAME" == "hash_skip_list" ]]; then
          "${WORKING_VERSION}" --memtable_factory="${impl}" \
            -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
            --bucket_count=1000000 --prefix_length=8 \
            --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
        else
          "${WORKING_VERSION}" --memtable_factory="${impl}" \
            -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
            --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
        fi
        
        mv db/LOG "./LOG${run}"
        mv workload.log "./workload${run}.log"
        rm -rf db
      done
      cd ..
    done
    
    rm -f workload.txt
    cd ..
  done
done

echo
echo "Finished experiments"