

RESULTS_DIR=".result"

TAG=vary1to4prefixnew
SETTINGS="lowpri_false"
LOW_PRI=0

INSERTS=500000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=0

SIZE_RATIO=10


BUFFER_SIZES_MB=(15)

ENTRY_SIZES=(1024)
LAMBDA=0.125
PAGE_SIZES=(4096)


# prefix_length_list=(2 4 6 8 10)
prefix_length_list=(1 2 3 4)

fixed_bucket_count=100000

THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="Vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  # [5]="UnsortedVector"
  # [6]="AlwayssortedVector"
)

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION="${PROJECT_DIR}/bin/working_version"

mkdir -p "${RESULTS_DIR}"
cd "${RESULTS_DIR}"

for BUFFER_MB in "${BUFFER_SIZES_MB[@]}"; do
  BUFFER_LABEL="${BUFFER_MB}M"

  for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
    PAGES_PER_FILE=$(( (BUFFER_MB * 1024 * 1024) / PAGE_SIZE ))
    PAGES_PER_FILE_LIST=("${PAGES_PER_FILE}")

    for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
      ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))

      for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
        EXP_DIR="${TAG}-${SETTINGS}-${BUFFER_LABEL}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
        mkdir -p "${EXP_DIR}"
        cd "${EXP_DIR}"

        "${LOAD_GEN}" \
          -I "${INSERTS}" -U "${UPDATES}" -Q "${POINT_QUERIES}" \
          -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
          -E "${ENTRY_SIZE}" -L "${LAMBDA}"

        for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
          BUFFER_IMPL="${BUFFER_IMPLEMENTATIONS[${impl}]}"

          if [[ "$BUFFER_IMPL" == "Vector" || "$BUFFER_IMPL" == "UnsortedVector" || "$BUFFER_IMPL" == "AlwayssortedVector" ]]; then
            for mode in dynamic preallocated; do
              OUT_DIR="${BUFFER_IMPL}-${mode}"
              mkdir -p "${OUT_DIR}"
              cd "${OUT_DIR}"; cp ../workload.txt .

              for run in 1 2 3; do
                "${WORKING_VERSION}" --memtable_factory="${impl}" \
                  -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                  -E "${ENTRY_SIZE}" ${mode:+-A 0} --lowpri "${LOW_PRI}" --stat 1 \
                  > "run${run}.log"
                mv db/LOG "LOG${run}"; mv workload.log "workload${run}.log"; rm -rf db
              done
              rm -f workload.txt; cd ..
            done

          elif [[ "$BUFFER_IMPL" == "hash_skip_list" || "$BUFFER_IMPL" == "hash_linked_list" ]]; then
            for PREFIX_LENGTH in "${prefix_length_list[@]}"; do
              OUT_DIR="${BUFFER_IMPL}-X${PREFIX_LENGTH}-H${fixed_bucket_count}"
              mkdir -p "${OUT_DIR}"
              cd "${OUT_DIR}"; cp ../workload.txt .

              for run in 1 2 3; do
                COMMON_ARGS=(
                  --memtable_factory="${impl}"
                  -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}"
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}"
                  -E "${ENTRY_SIZE}" --bucket_count="${fixed_bucket_count}"
                  --prefix_length="${PREFIX_LENGTH}" --lowpri "${LOW_PRI}" --stat 1
                )
                if [[ "$BUFFER_IMPL" == "hash_linked_list" ]]; then
                  COMMON_ARGS+=( --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" )
                fi
                "${WORKING_VERSION}" "${COMMON_ARGS[@]}" > "run${run}.log"
                mv db/LOG "LOG${run}"; mv workload.log "workload${run}.log"; rm -rf db
              done
              rm -f workload.txt; cd ..
            done

          else
            OUT_DIR="${BUFFER_IMPL}"
            mkdir -p "${OUT_DIR}"
            cd "${OUT_DIR}"; cp ../workload.txt .

            for run in 1 2 3; do
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 \
                > "run${run}.log"
              mv db/LOG "LOG${run}"; mv workload.log "workload${run}.log"; rm -rf db
            done
            rm -f workload.txt; cd ..
          fi
        done  # BUFFER_IMPLEMENTATIONS

        cd ..
        rm -f workload.txt
      done   
    done    
  done       
done        

echo
echo "Finished experiments"
