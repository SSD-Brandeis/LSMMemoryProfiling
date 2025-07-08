#!/usr/bin/env bash

entry_sizes_list=(8 16 32 64 128 256 512 1024)
page_sizes=(4096)
top_level_names=("new_metadata_overhead")

# Workload parameters
INSERTS=1000000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=0

LAMBDA=0.5
SIZE_RATIO=10
SHOW_PROGRESS=1
SANITY_CHECK=0

BUCKET_COUNT=1000000
PREFIX_LENGTH=4
LINKLIST_THRESHOLD_USE_SKIPLIST=${INSERTS}

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN_PATH="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION_PATH="${PROJECT_DIR}/bin/working_version"

log_info()  { echo "[INFO] $*"; }
log_error() { echo "[ERROR] $*"; }
delete_db_folder() { rm -rf "$1"; }

extract_db_logs () {
  local run="$1"
  mv db/LOG          "LOG_run${run}"        2>/dev/null || true
  mv db/workload.log "workload_run${run}"   2>/dev/null || true
}

declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [5]="unsorted_vector"
  [6]="always_sorted_vector"
)

for i in "${!page_sizes[@]}"; do
    PAGE_SIZE=${page_sizes[$i]}
    TOP_LEVEL_DIR_NAME=${top_level_names[$i]}
    RESULT_PARENT_DIR="${PROJECT_DIR}/.result/${TOP_LEVEL_DIR_NAME}"
    mkdir -p "${RESULT_PARENT_DIR}"

    for ENTRY_SIZE in "${entry_sizes_list[@]}"; do
        ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
        PAGE_TAG="${PAGE_SIZE}B_page"
        TAG="entry_${ENTRY_SIZE}b"
        #mainly 64mb, partialy set 4kb page to 8mb buffer
        if [ "${PAGE_SIZE}" -eq 2048 ]; then
            PAGES_PER_FILE_LIST=(4096)
        elif [ "${PAGE_SIZE}" -eq 4096 ]; then
            PAGES_PER_FILE_LIST=(2048)
            #64mb
            # PAGES_PER_FILE_LIST=(16384)
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

        pushd "${PROJECT_DIR}" >/dev/null || exit
        log_info "Generating workload: ENTRY_SIZE=${ENTRY_SIZE}, PAGE_SIZE=${PAGE_SIZE}"
        if ! "${LOAD_GEN_PATH}" -I "${INSERTS}" -Q "${POINT_QUERIES}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"; then
            log_error "Workload generation failed."
            popd >/dev/null || exit
            continue
        fi
        WORKLOAD_FILE="${PROJECT_DIR}/workload.txt"
        if [ ! -f "${WORKLOAD_FILE}" ]; then
            log_error "workload.txt not found."
            popd >/dev/null || exit
            continue
        fi
        popd >/dev/null || exit

        for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
            buffer_size_bytes=$((ENTRY_SIZE * ENTRIES_PER_PAGE * PAGES_PER_FILE))
            log_info "Buffer size: ${buffer_size_bytes} bytes (E*B*P)"

            for IMPL_NUM in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
                IMPL_NAME="${BUFFER_IMPLEMENTATIONS[${IMPL_NUM}]}"

                EXP_DIR="P${PAGE_SIZE}_E${ENTRY_SIZE}_I${INSERTS}_Q${POINT_QUERIES}_U${UPDATES}_S${RANGE_QUERIES}_Y${SELECTIVITY}_L${LAMBDA}_T${SIZE_RATIO}"
                WORKLOAD_RESULT_DIR="${RESULT_PARENT_DIR}/${IMPL_NAME}/${EXP_DIR}/P_${PAGES_PER_FILE}"
                mkdir -p "${WORKLOAD_RESULT_DIR}"

                cp "${WORKLOAD_FILE}" "${WORKLOAD_RESULT_DIR}/"

                pushd "${WORKLOAD_RESULT_DIR}" >/dev/null || exit
                delete_db_folder "db"

                valid_arg=""
                case "${IMPL_NUM}" in
                  3)
                    valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
                    ;;
                  4)
                    valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH} --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
                    ;;
                esac

                for run in 1 2 3; do
                    log_info "Running trial #${run} for ${IMPL_NAME} at PAGES_PER_FILE=${PAGES_PER_FILE}"
                    delete_db_folder "db"
                    if ! "${WORKING_VERSION_PATH}" \
                          --memtable_factory="${IMPL_NUM}" \
                          ${valid_arg} \
                          -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                          -E "${ENTRY_SIZE}" -B "${ENTRIES_PER_PAGE}" -P "${PAGES_PER_FILE}" -T "${SIZE_RATIO}" \
                          --progress "${SHOW_PROGRESS}" --stat 1 > "run${run}.log"; then
                        log_error "working_version failed on run ${run} (PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME})"
                        continue
                    fi
                    extract_db_logs "${run}"
                    delete_db_folder "db"
                done

                popd >/dev/null || exit
            done
        done
    done
done

log_info "All workloads completed!"
