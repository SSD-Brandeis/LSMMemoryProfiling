RESULTS_DIR=".result"

TAG=wastagebuffer
SETTINGS="lowpri_false"
LOW_PRI=0

# INSERTS=100000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=0

SIZE_RATIO=10

# ★ buffer sizes to sweep (MiB)
BUFFER_SIZES_MB=(8 16 32 64 128 256 512 1024)

ENTRY_SIZES=(1024)
LAMBDA=0.125
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

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION="${PROJECT_DIR}/bin/working_version"

mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

# ★ OUTER LOOP over desired buffer sizes
for BUFFER_MB in "${BUFFER_SIZES_MB[@]}"; do
 INSERTS=$(( BUFFER_MB * 1024 * 11 * 2 / 10 ))
  BUFFER_LABEL="${BUFFER_MB}M"

  for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
    # ★ compute pages-per-file from buffer_MB and page_size
    PAGES_PER_FILE=$(( (BUFFER_MB * 1024 * 1024) / PAGE_SIZE ))
    PAGES_PER_FILE_LIST=("${PAGES_PER_FILE}")   # keeps inner loop unchanged

    for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
      ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

      for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
        EXP_DIR="${TAG}-${SETTINGS}-${BUFFER_LABEL}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"

        mkdir -p "$EXP_DIR"
        cd "$EXP_DIR"

        echo
        echo "SETTINGS: ${SETTINGS}"
        echo "Debug: BUFFER=${BUFFER_MB}MB  PAGE_SIZE=${PAGE_SIZE}  PAGES/FILE=${PAGES_PER_FILE}"
        echo "Generating workload ... (ENTRY_SIZE=${ENTRY_SIZE}, LAMBDA=${LAMBDA})"
        echo

        "${LOAD_GEN}" -I "${INSERTS}" -U "${UPDATES}" -Q "${POINT_QUERIES}" \
                      -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}"              \
                      -E "${ENTRY_SIZE}" -L "${LAMBDA}"

        echo; echo

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
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                  -E "${ENTRY_SIZE}" -A 0 --lowpri "${LOW_PRI}" --stat 1 \
                  > "run${run}.log"
              mv db/LOG "./LOG${run}"
              mv workload.log "./workload${run}.log"
              rm -rf db
            done
            rm -rf workload.txt
            cd ..

            mkdir -p "${BUFFER_IMPL}-preallocated"
            cd "${BUFFER_IMPL}-preallocated"
            cp ../workload.txt ./workload.txt
            for run in 1 2 3; do
              echo "Run ${BUFFER_IMPL}-preallocated trial #${run}"
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                  -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                  -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 \
                  > "run${run}.log"
              mv db/LOG "./LOG${run}"
              mv workload.log "./workload${run}.log"
              rm -rf db
            done
            rm -rf workload.txt
            cd ..

          elif [[ "$BUFFER_IMPL" == "hash_skip_list" || "$BUFFER_IMPL" == "hash_linked_list" ]]; then
            mkdir -p "${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
            cd "${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${BUCKET_COUNT}"
            cp ../workload.txt ./workload.txt
            for run in 1 2 3; do
              echo "Run ${BUFFER_IMPL} trial #${run}"
              if [[ "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                "${WORKING_VERSION}" --memtable_factory="${impl}" \
                    -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                    -E "${ENTRY_SIZE}" --bucket_count="${BUCKET_COUNT}" \
                    --prefix_length="${PREFIX_LENGTH}" \
                    --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                    --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
              else
                "${WORKING_VERSION}" --memtable_factory="${impl}" \
                    -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                    -E "${ENTRY_SIZE}" --bucket_count="${BUCKET_COUNT}" \
                    --prefix_length="${PREFIX_LENGTH}" \
                    --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
              fi
              mv db/LOG "./LOG${run}"
              mv workload.log "./workload${run}.log"
              rm -rf db
            done
            rm -rf workload.txt
            cd ..

          else
            mkdir -p "${BUFFER_IMPL}"
            cd "${BUFFER_IMPL}"
            cp ../workload.txt ./workload.txt
            for run in 1 2 3; do
              echo "Run ${BUFFER_IMPL} trial #${run}"
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                  -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                  -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 \
                  > "run${run}.log"
              mv db/LOG "./LOG${run}"
              mv workload.log "./workload${run}.log"
              rm -rf db
            done
            rm -rf workload.txt
            cd ..
          fi
        done

        cd ..
      done
      rm -rf workload.txt
    done
  done
done

echo
echo "Finished experiments"
