#!/usr/bin/env bash

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

# preallocated = ${INSERTS}
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
    RESULT_PARENT_DIR="${PROJECT_DIR}/.result/6_7_insert_only_rawop_low_pri_true_dynamic_vec_memtable_profile/${TOP_LEVEL_DIR_NAME}"

    if [ -d "${RESULT_PARENT_DIR}" ]; then
        echo "[INFO] Top-level directory ${RESULT_PARENT_DIR} already exists. Using the existing directory."
    else
        mkdir -p "${RESULT_PARENT_DIR}"
    fi

    for ENTRY_SIZE in "${entry_sizes_list[@]}"; do
        ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

        if [ "${PAGE_SIZE}" -eq 2048 ]; then
            PAGE_TAG="2kb_page"
        elif [ "${PAGE_SIZE}" -eq 4096 ]; then
            PAGE_TAG="4kb_page"
        elif [ "${PAGE_SIZE}" -eq 8192 ]; then
            PAGE_TAG="8kb_page"
        elif [ "${PAGE_SIZE}" -eq 16384 ]; then
            PAGE_TAG="16kb_page"
        elif [ "${PAGE_SIZE}" -eq 32768 ]; then
            PAGE_TAG="32kb_page"
        elif [ "${PAGE_SIZE}" -eq 65536 ]; then
            PAGE_TAG="64kb_page"
        else
            echo "Unknown PAGE_SIZE: ${PAGE_SIZE}"
            exit 1
        fi

        TAG="${PAGE_TAG}_entry_${ENTRY_SIZE}b_buffer_64mb"

        if [ "${PAGE_SIZE}" -eq 2048 ]; then
            PAGES_PER_FILE_LIST=(4096)
        elif [ "${PAGE_SIZE}" -eq 4096 ]; then
            PAGES_PER_FILE_LIST=(16384)
        elif [ "${PAGE_SIZE}" -eq 8192 ]; then
            PAGES_PER_FILE_LIST=(1024)
        elif [ "${PAGE_SIZE}" -eq 16384 ]; then
            PAGES_PER_FILE_LIST=(512)
        elif [ "${PAGE_SIZE}" -eq 32768 ]; then
            PAGES_PER_FILE_LIST=(256)
        elif [ "${PAGE_SIZE}" -eq 65536 ]; then
            PAGES_PER_FILE_LIST=(128)
        else
            echo "Unsupported PAGE_SIZE: ${PAGE_SIZE}" >&2
            exit 1
        fi

        LOAD_GEN_PATH="${PROJECT_DIR}/bin/load_gen"
        WORKING_VERSION_PATH="${PROJECT_DIR}/bin/working_version"

        log_info() {
          echo "[INFO] $*"
        }
        log_error() {
          echo "[ERROR] $*"
        }

        declare -A BUFFER_IMPLEMENTATIONS=(
        #   [1]="skiplist"
          [2]="pre_vector"
        #   [3]="hash_skip_list"
        #   [4]="hash_linked_list"
        #   [5]="UnsortedVector"
        #   [6]="AlwayssortedVector"
        )

        echo "current Parameters:"
        echo "  TAG=${TAG}"
        echo "  ENTRY_SIZE=${ENTRY_SIZE}, ENTRIES_PER_PAGE=${ENTRIES_PER_PAGE}"
        echo "  INSERTS=${INSERTS}, Point_Queries=${POINT_QUERIES}, UPDATES=${UPDATES}, RANGE_QUERIES=${RANGE_QUERIES}, SELECTIVITY=${SELECTIVITY}"
        echo "  LAMBDA=${LAMBDA}, SIZE_RATIO=${SIZE_RATIO}"
        echo "  SHOW_PROGRESS=${SHOW_PROGRESS}, SANITY_CHECK=${SANITY_CHECK}"
        echo
        echo "Experiment with different PAGES_PER_FILE in: ${PAGES_PER_FILE_LIST[*]}"
        echo "Buffer size (bytes) = $((ENTRY_SIZE * ENTRIES_PER_PAGE)) * PAGES_PER_FILE"

        for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
          buffer_size_bytes=$((ENTRY_SIZE * ENTRIES_PER_PAGE * PAGES_PER_FILE))
          log_info "Generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"
          echo " => buffer_size_bytes = ${buffer_size_bytes} (E*B*P)"

          pushd "${PROJECT_DIR}" >/dev/null || exit
          if ! "${LOAD_GEN_PATH}" \
               -I "${INSERTS}" \
               -Q "${POINT_QUERIES}" \
               -U "${UPDATES}" \
               -S "${RANGE_QUERIES}" \
               -Y "${SELECTIVITY}" \
               -E "${ENTRY_SIZE}" \
               -L "${LAMBDA}"; then
            log_error "Something went wrong generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"
            continue
          fi

          WORKLOAD_FILE="${PROJECT_DIR}/workload.txt"
          if [ ! -f "${WORKLOAD_FILE}" ]; then
            log_error "workload.txt not found for PAGES_PER_FILE=${PAGES_PER_FILE}"
            continue
          fi

          # Sort workload in the sequence of I, Q, RQ
          INSERTS_FILE=$(mktemp)  
          POINT_QUERIES_FILE=$(mktemp)  
          RANGE_QUERIES_FILE=$(mktemp)  

          grep '^I ' "${WORKLOAD_FILE}" > "${INSERTS_FILE}" 
          grep '^Q ' "${WORKLOAD_FILE}" > "${POINT_QUERIES_FILE}"  
          grep '^S ' "${WORKLOAD_FILE}" > "${RANGE_QUERIES_FILE}" 

          cat "${INSERTS_FILE}" "${POINT_QUERIES_FILE}" "${RANGE_QUERIES_FILE}" > "${WORKLOAD_FILE}"  

          rm -f "${INSERTS_FILE}" "${POINT_QUERIES_FILE}" "${RANGE_QUERIES_FILE}"  

          for IMPL_NUM in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
            IMPL_NAME="${BUFFER_IMPLEMENTATIONS[${IMPL_NUM}]}"
            log_info "Running working_version with PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME}"

            EXP_DIR="${IMPL_NAME}-${TAG}-I${INSERTS}-Q${POINT_QUERIES}-U${UPDATES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}"
            WORKLOAD_RESULT_DIR="${RESULT_PARENT_DIR}/${IMPL_NAME}/${EXP_DIR}/P_${PAGES_PER_FILE}"
            mkdir -p "${WORKLOAD_RESULT_DIR}"

            cp "${WORKLOAD_FILE}" "${WORKLOAD_RESULT_DIR}/"

            pushd "${WORKLOAD_RESULT_DIR}" >/dev/null || exit
            delete_db_folder "db"

            valid_arg=""
            case "${IMPL_NUM}" in
              1) ;;
              2) ;;
              3)
                valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
                ;;
              4)
                valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH} --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
                ;;
            esac

            if ! "${WORKING_VERSION_PATH}" \
                  --memtable_factory="${IMPL_NUM}" \
                  ${valid_arg} \
                  -I "${INSERTS}" \
                  -U "${UPDATES}" \
                  -S "${RANGE_QUERIES}" \
                  -Y "${SELECTIVITY}" \
                  -E "${ENTRY_SIZE}" \
                  -B "${ENTRIES_PER_PAGE}" \
                  -P "${PAGES_PER_FILE}" \
                  -T "${SIZE_RATIO}" \
                  --stat 1 > temp.log; then
              log_error "Something is wrong with working_version (PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME})"
            fi

            mv db/LOG "." 2>/dev/null || true
            mv db/workload.log "." 2>/dev/null || true
            rm -rf db
            popd >/dev/null || exit
          done

          rm -f "${WORKLOAD_FILE}"
          popd >/dev/null || exit
        done
    done
done

log_info "All workloads completed!"
                  # --progress "${SHOW_PROGRESS}" \

                                  #   -A 0 \