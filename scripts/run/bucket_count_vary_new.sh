#!/bin/bash
set -e

# Rebuild before starting
bash ./scripts/rebuild.sh

RESULTS_DIR=".result"

# --- Parameters ---
INSERTS=800000 
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=0

TAG="fig18_partc_single-I${INSERTS}"
SETTINGS="lowpri_false"
LOW_PRI=0

SIZE_RATIO=10
BUFFER_SIZES_MB=(128)
ENTRY_SIZES=(128)
LAMBDA=0.0625
PAGE_SIZES=(4096)

BUCKET_COUNTS=(1 200000 400000 600000 800000 1000000)
PREFIX_LENGTH=6
THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="Vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [9]="hash_vector"
)

# --- FIXED PATH LOGIC ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../" && pwd)"

LOAD_GEN="${PROJECT_ROOT}/bin/load_gen"
WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"

# Safety Check
if [ ! -f "$LOAD_GEN" ]; then
    echo "Error: load_gen not found at $LOAD_GEN"
    exit 1
fi

mkdir -p "${RESULTS_DIR}"
cd "${RESULTS_DIR}"

for BUFFER_MB in "${BUFFER_SIZES_MB[@]}"; do
  BUFFER_LABEL="${BUFFER_MB}M"
  for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
    PAGES_PER_FILE=$(( (BUFFER_MB * 1024 * 1024) / PAGE_SIZE ))
    for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
      ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))

      EXP_DIR="${TAG}-${SETTINGS}-${BUFFER_LABEL}-P${PAGES_PER_FILE}-E${ENTRY_SIZE}"
      mkdir -p "${EXP_DIR}"
      cd "${EXP_DIR}"

      echo "-------------------------------------------------------"
      echo "Generating workload: I=${INSERTS}"
      "${LOAD_GEN}" -I "${INSERTS}" -U "${UPDATES}" -Q "${POINT_QUERIES}" \
                    -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -E "${ENTRY_SIZE}" -L "${LAMBDA}"

      for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
        BUFFER_IMPL="${BUFFER_IMPLEMENTATIONS[${impl}]}"

        # CASE 1: Hash Hybrids (Including hash_vector)
        if [[ "$BUFFER_IMPL" == *"hash"* ]]; then
          for BUCKET_COUNT in "${BUCKET_COUNTS[@]}"; do
            OUT_DIR="${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
            mkdir -p "${OUT_DIR}"; cp workload.txt "${OUT_DIR}/"; cd "${OUT_DIR}"

            echo "Running Hash Hybrid: ${BUFFER_IMPL} | H=${BUCKET_COUNT}"
            CMD_ARGS=(
              --memtable_factory="${impl}"
              -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}"
              -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}"
              -E "${ENTRY_SIZE}" --bucket_count="${BUCKET_COUNT}"
              --prefix_length="${PREFIX_LENGTH}" --lowpri "${LOW_PRI}" --stat 1
            )
            [[ "$BUFFER_IMPL" == "hash_linked_list" ]] && CMD_ARGS+=( --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" )

            "${WORKING_VERSION}" "${CMD_ARGS[@]}" > "run1.log"
            [ -f db/LOG ] && mv db/LOG "LOG1"
            rm -rf db workload.txt && cd ..
          done

        # CASE 2: Vector Types
        elif [[ "$BUFFER_IMPL" == *"Vector"* ]]; then
          for mode in dynamic preallocated; do
            OUT_DIR="${BUFFER_IMPL}-${mode}"
            mkdir -p "${OUT_DIR}"; cp workload.txt "${OUT_DIR}/"; cd "${OUT_DIR}"
            echo "Running Vector: ${BUFFER_IMPL} | Mode=${mode}"
            EXTRA_ARGS=""; [[ "$mode" == "dynamic" ]] && EXTRA_ARGS="-A 0"
            "${WORKING_VERSION}" --memtable_factory="${impl}" \
              -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
              -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
              -E "${ENTRY_SIZE}" ${EXTRA_ARGS} --lowpri "${LOW_PRI}" --stat 1 > "run1.log"
            [ -f db/LOG ] && mv db/LOG "LOG1"
            rm -rf db workload.txt && cd ..
          done

        # CASE 3: Standard
        else
          OUT_DIR="${BUFFER_IMPL}"
          mkdir -p "${OUT_DIR}"; cp workload.txt "${OUT_DIR}/"; cd "${OUT_DIR}"
          "${WORKING_VERSION}" --memtable_factory="${impl}" \
            -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
            -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
            -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "run1.log"
          [ -f db/LOG ] && mv db/LOG "LOG1"
          rm -rf db workload.txt && cd ..
        fi
      done  
      cd ..
      rm -f workload.txt
    done   
  done     
done