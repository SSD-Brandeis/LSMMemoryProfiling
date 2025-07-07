entry_sizes_list=(128)
page_sizes=(4096)

INSERTS=10000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=0
LAMBDA=0.5
SIZE_RATIO=10
SHOW_PROGRESS=1
SANITY_CHECK=0

BUCKET_COUNT=100000
PREFIX_LENGTH=6
LINKLIST_THRESHOLD_USE_SKIPLIST=${INSERTS}

top_level_names=()
for PAGE_SIZE in "${page_sizes[@]}"; do
    name="${INSERTS}_inserts_${POINT_QUERIES}_PQ_${RANGE_QUERIES}_RQ_S_${SELECTIVITY}"
    top_level_names+=("${name}")
done

for i in "${!page_sizes[@]}"; do
    PAGE_SIZE=${page_sizes[$i]}
    TOP_LEVEL_DIR_NAME=${top_level_names[$i]}

    PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    RESULT_PARENT_DIR="${PROJECT_DIR}/.result/6_39_rawop_low_pri_true_default_refill_I_PQ/${TOP_LEVEL_DIR_NAME}"
    mkdir -p "${RESULT_PARENT_DIR}"

    for ENTRY_SIZE in "${entry_sizes_list[@]}"; do
        ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

 
        if   [ "${PAGE_SIZE}" -eq 2048 ];  then PAGE_TAG="2kb_page"
        elif [ "${PAGE_SIZE}" -eq 4096 ];  then PAGE_TAG="4kb_page"
        elif [ "${PAGE_SIZE}" -eq 8192 ];  then PAGE_TAG="8kb_page"
        elif [ "${PAGE_SIZE}" -eq 16384 ]; then PAGE_TAG="16kb_page"
        elif [ "${PAGE_SIZE}" -eq 32768 ]; then PAGE_TAG="32kb_page"
        elif [ "${PAGE_SIZE}" -eq 65536 ]; then PAGE_TAG="64kb_page"
        else echo "Unknown PAGE_SIZE: ${PAGE_SIZE}"; exit 1; fi

        TAG="${PAGE_TAG}_entry_${ENTRY_SIZE}b_buffer_64mb"

  
        if   [ "${PAGE_SIZE}" -eq 2048 ];  then PAGES_PER_FILE_LIST=(4096)
        elif [ "${PAGE_SIZE}" -eq 4096 ];  then PAGES_PER_FILE_LIST=(16384)
        elif [ "${PAGE_SIZE}" -eq 8192 ];  then PAGES_PER_FILE_LIST=(1024)
        elif [ "${PAGE_SIZE}" -eq 16384 ]; then PAGES_PER_FILE_LIST=(512)
        elif [ "${PAGE_SIZE}" -eq 32768 ]; then PAGES_PER_FILE_LIST=(256)
        elif [ "${PAGE_SIZE}" -eq 65536 ]; then PAGES_PER_FILE_LIST=(128)
        else echo "if you are using new PAGE_SIZE, you need to change this script: ${PAGE_SIZE}" >&2; exit 1; fi

        LOAD_GEN_PATH="${PROJECT_DIR}/bin/load_gen"
        WORKING_VERSION_PATH="${PROJECT_DIR}/bin/working_version"

        log_info()  { echo "[INFO] $*"; }
        log_error() { echo "[ERROR] $*"; }

        declare -A BUFFER_IMPLEMENTATIONS=(
          [1]="skiplist"
          [2]="Vector"
          [3]="hash_skip_list"
          [4]="hash_linked_list"
          [5]="UnsortedVector"
          [6]="AlwayssortedVector"
        )

        for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
          log_info "Generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"
          pushd "${PROJECT_DIR}" >/dev/null || exit
          if ! "${LOAD_GEN_PATH}" \
               -I "${INSERTS}" \
               -Q "${POINT_QUERIES}" \
               -U "${UPDATES}" \
               -S "${RANGE_QUERIES}" \
               -Y "${SELECTIVITY}" \
               -E "${ENTRY_SIZE}" \
               -L "${LAMBDA}"; then
            log_error "Workload generation failed"
            popd >/dev/null
            continue
          fi
          WORKLOAD_FILE="${PROJECT_DIR}/workload.txt"
          [ -f "${WORKLOAD_FILE}" ] || { log_error "workload.txt missing"; popd >/dev/null; continue; }
          # sort I, Q, S
          tmpI=$(mktemp); tmpQ=$(mktemp); tmpR=$(mktemp)
          grep '^I ' "${WORKLOAD_FILE}" > "${tmpI}"
          grep '^Q ' "${WORKLOAD_FILE}" > "${tmpQ}"
          grep '^S ' "${WORKLOAD_FILE}" > "${tmpR}"
          cat "${tmpI}" "${tmpQ}" "${tmpR}" > "${WORKLOAD_FILE}"
          rm -f "${tmpI}" "${tmpQ}" "${tmpR}"
          popd >/dev/null

          for IMPL_NUM in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
            IMPL_NAME="${BUFFER_IMPLEMENTATIONS[${IMPL_NUM}]}"

            # vector variants has prealloca and dynamic 
            if [[ "${IMPL_NUM}" -eq 2 || "${IMPL_NUM}" -eq 5 || "${IMPL_NUM}" -eq 6 ]]; then
              for mode in dynamic prealloc; do
                if [ "${mode}" == "prealloc" ]; then
                  A_FLAG="-A 0"
                  BUF_DIR="preallocated ${IMPL_NAME}"
                else
                  A_FLAG=""
                  BUF_DIR="${IMPL_NAME}"
                fi

                log_info "Run: ${IMPL_NAME} (${mode}), pages=${PAGES_PER_FILE}"
                EXP_DIR="${IMPL_NAME}-${TAG}-I${INSERTS}-Q${POINT_QUERIES}-U${UPDATES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}"
                OUTDIR="${RESULT_PARENT_DIR}/${BUF_DIR}/${EXP_DIR}/P_${PAGES_PER_FILE}"
                mkdir -p "${OUTDIR}"

                cp "${WORKLOAD_FILE}" "${OUTDIR}/"
                pushd "${OUTDIR}" >/dev/null || exit
             

                extra=""
                if   [ "${IMPL_NUM}" -eq 3 ]; then
                  extra="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
                elif [ "${IMPL_NUM}" -eq 4 ]; then
                  extra="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH} --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
                fi

                if ! "${WORKING_VERSION_PATH}" \
                      --memtable_factory="${IMPL_NUM}" ${extra} \
                      -I "${INSERTS}" -U "${UPDATES}" \
                      -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                      -E "${ENTRY_SIZE}" -B "${ENTRIES_PER_PAGE}" \
                      -P "${PAGES_PER_FILE}" -T "${SIZE_RATIO}" \
                      ${A_FLAG} --stat 1 > temp.log; then
                  log_error "Error: ${IMPL_NAME} (${mode})"
                fi

                mv db/LOG .    2>/dev/null || true
                mv db/workload.log . 2>/dev/null || true
                rm -rf db
                popd >/dev/null
              done

            else
    
              A_FLAG="-A 0"
              BUF_DIR="${IMPL_NAME}"
              log_info "Run: ${IMPL_NAME} (single), pages=${PAGES_PER_FILE}"
              EXP_DIR="${IMPL_NAME}-${TAG}-I${INSERTS}-Q${POINT_QUERIES}-U${UPDATES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}"
                OUTDIR="${RESULT_PARENT_DIR}/${BUF_DIR}/${EXP_DIR}/P_${PAGES_PER_FILE}"
              mkdir -p "${OUTDIR}"

              cp "${WORKLOAD_FILE}" "${OUTDIR}/"
              pushd "${OUTDIR}" >/dev/null || exit
          

              extra=""
              if   [ "${IMPL_NUM}" -eq 3 ]; then
                extra="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
              elif [ "${IMPL_NUM}" -eq 4 ]; then
                extra="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH} --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
              fi

              if ! "${WORKING_VERSION_PATH}" \
                    --memtable_factory="${IMPL_NUM}" ${extra} \
                    -I "${INSERTS}" -U "${UPDATES}" \
                    -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -E "${ENTRY_SIZE}" -B "${ENTRIES_PER_PAGE}" \
                    -P "${PAGES_PER_FILE}" -T "${SIZE_RATIO}" \
                    ${A_FLAG} --stat 1 > temp.log; then
                log_error "Error: ${IMPL_NAME}"
              fi

              mv db/LOG .    2>/dev/null || true
              mv db/workload.log . 2>/dev/null || true
              rm -rf db
              popd >/dev/null
            fi

          done  
        done  
    done  
done  

log_info "All workloads completed!"