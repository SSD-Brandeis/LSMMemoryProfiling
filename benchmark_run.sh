#!/usr/bin/env bash
# set -euo pipefail

ENTRY_SIZE=128
ENTRIES_PER_PAGE=32

INSERTS=100000
UPDATES=10
RANGE_QUERIES=10
SELECTIVITY=0.1

LAMBDA=0.125
SIZE_RATIO=6
SHOW_PROGRESS=1
SANITY_CHECK=0

# hash hybrid parameters
BUCKET_COUNT=(10000 20000)
PREFIX_LENGTH=8
#in byte
VECTOR_PREALLOCATION_SIZE=1048576
LINKLIST_THRESHOLD_USE_SKIPLIST=4
SKIPLIST_HEIGHT=4
#64kb minimum buffer size, if file size is 4kb, then minimum 16 pages required to get minimum buffer size
PAGES_PER_FILE_LIST=(32)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN_PATH="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION_PATH="${PROJECT_DIR}/bin/working_version"

log_info() {
  echo "[INFO] $*"
}
log_error() {
  echo "[ERROR] $*"
}

declare -A BUFFER_IMPLEMENTATIONS=(
  # [1]="skiplist"
  [2]="vector"
  # [3]="hash_skip_list"
  # [4]="hash_linked_list"
  # [5]="unsorted_vector"
  # [6]="always_sorted_vector"
  # [7]="linklist"
)
# TODO: change the result structure such that all experiments
# are stored in a single folder, add a tag to name 
# the experiments
RESULT_DIR="${PROJECT_DIR}/vector_result"
# RESULT_DIR="${PROJECT_DIR}/.result"
mkdir -p "${RESULT_DIR}"

delete_db_folder() {
  local db_folder="$1"
  if [ -d "${db_folder}" ]; then
    rm -rf "${db_folder}"
    log_info "Deleted db folder at ${db_folder}"
  fi
}

remove_trailing_newline() {
  local file_path="$1"
  if [ ! -s "${file_path}" ]; then
    return
  fi
  local last_char
  last_char="$(tail -c 1 "${file_path}")" || true
  if [ -z "${last_char}" ]; then
    truncate -s -1 "${file_path}"
    log_info "Removed trailing newline from ${file_path}"
  fi
}

echo "current Parameters:"
echo "  ENTRY_SIZE=${ENTRY_SIZE}, ENTRIES_PER_PAGE=${ENTRIES_PER_PAGE}"
echo "  INSERTS=${INSERTS}, UPDATES=${UPDATES}, RANGE_QUERIES=${RANGE_QUERIES}, SELECTIVITY=${SELECTIVITY}"
echo "  LAMBDA=${LAMBDA}, SIZE_RATIO=${SIZE_RATIO}"
echo "  SHOW_PROGRESS=${SHOW_PROGRESS}, SANITY_CHECK=${SANITY_CHECK}"
echo
echo "Experiment with different PAGES_PER_FILE in: ${PAGES_PER_FILE_LIST[*]}"
echo "Buffer size (bytes) = $((ENTRY_SIZE * ENTRIES_PER_PAGE)) * PAGES_PER_FILE"

# load gen command
for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
  buffer_size_bytes=$((ENTRY_SIZE * ENTRIES_PER_PAGE * PAGES_PER_FILE))
  log_info "Generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"
  echo " => buffer_size_bytes = ${buffer_size_bytes} (E*B*P)"

  pushd "${PROJECT_DIR}" >/dev/null || exit
  if ! "${LOAD_GEN_PATH}" \
       -I "${INSERTS}" \
       -U "${UPDATES}" \
       -S "${RANGE_QUERIES}" \
       -Y "${SELECTIVITY}" \
       -E "${ENTRY_SIZE}"
  then
    log_error "Something went wrong generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"

    continue
  fi

  WORKLOAD_FILE="${PROJECT_DIR}/workload.txt"
  if [ ! -f "${WORKLOAD_FILE}" ]; then
    log_error "workload.txt not found for PAGES_PER_FILE=${PAGES_PER_FILE}"

    continue
  fi

  remove_trailing_newline "${WORKLOAD_FILE}"


  # workingversion command
  for IMPL_NUM in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
    IMPL_NAME="${BUFFER_IMPLEMENTATIONS[${IMPL_NUM}]}"
    log_info "Running working_version with PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME}"

    WORKLOAD_RESULT_DIR="${RESULT_DIR}/P_${PAGES_PER_FILE}/${IMPL_NAME}"
    mkdir -p "${WORKLOAD_RESULT_DIR}"
    cp "${WORKLOAD_FILE}" "${WORKLOAD_RESULT_DIR}/"

    pushd "${WORKLOAD_RESULT_DIR}" >/dev/null || exit
    delete_db_folder "db"

    # LOG_FILE="stats.log"
    log_info "Executing working_version for memtable_factory=${IMPL_NUM}..."

    # memtable specific parameters that are valid in working version
    valid_arg=""
    case "${IMPL_NUM}" in
      1) # skiplist is just default setting 
        ;;
      2) # vector 
        valid_arg="--preallocation_size=${VECTOR_PREALLOCATION_SIZE}"
        ;;
      3) # hash_skip_list 
        valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
        ;;
      4) # hash_linked_list
        valid_arg="--bucket_count=${BUCKET_COUNT} \
                --prefix_length=${PREFIX_LENGTH} \
                --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
        ;;
      5) # unsorted_vector 
        valid_arg="--preallocation_size=${VECTOR_PREALLOCATION_SIZE}"
        ;;
      6) # always_sorted_vector 
        valid_arg="--preallocation_size=${VECTOR_PREALLOCATION_SIZE}"
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
          --progress "${SHOW_PROGRESS}" \
          --stat 1 \
          # > "${LOG_FILE}"
    then
      log_error "something is wrong with working version (PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME})"
    fi

    mv db/LOG "."
    rm -rf db

  done

  rm -f "${WORKLOAD_FILE}"
done

log_info "All workloads completed! for PAGES_PER_FILE in ${PAGES_PER_FILE_LIST[*]}!"
