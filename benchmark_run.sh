#!/usr/bin/env bash
# set -euo pipefail

ENTRY_SIZE=128
ENTRIES_PER_PAGE=32

INSERTS=100000
UPDATES=10
RANGE_QUERIES=10
SELECTIVITY=0.1

#prefix length, bucket size (pass in as a parameter) 
#dynamic/static vector  (static preallocate buffer size) 
LAMBDA=0.125
SIZE_RATIO=6   
SHOW_PROGRESS=1
SANITY_CHECK=0

# buffersize 2MB, 4MB, 8MB
# PAGES_PER_FILE_LIST=(512 1024 2048)
# buffersize 0.25MB, 0.5MB, 1MB
# PAGES_PER_FILE_LIST=(128 256 512 )
# PAGES_PER_FILE_LIST=(2 4 8 )
PAGES_PER_FILE_LIST=(16 )
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN_PATH="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION_PATH="${PROJECT_DIR}/bin/working_version"

log_info() {
  echo "[INFO] $*"
}
log_error() {
  echo "[ERROR] $*" 
}
# for update config.h, once accept the parameter, it should be accepted by the working_version
# check if rocksdb actually 
declare -A BUFFER_IMPLEMENTATIONS=(
  [1]="skiplist"
  [2]="vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
  [5]="unsorted_vector"
  [6]="always_sorted_vector"
)

RESULT_DIR="${PROJECT_DIR}/.result"
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
echo "Experiment with differen PAGES_PER_FILE in: ${PAGES_PER_FILE_LIST[*]}"
echo "Buffer size (bytes) = ENTRY_SIZE * ENTRIES_PER_PAGE * PAGES_PER_FILE"

for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
  buffer_size_bytes=$((ENTRY_SIZE * ENTRIES_PER_PAGE * PAGES_PER_FILE))

  log_info "Generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"
  echo " => buffer_size_bytes = ${buffer_size_bytes} (E*B*P)"

  pushd "${PROJECT_DIR}" >/dev/null || exit

# load_gen para
  if ! "${LOAD_GEN_PATH}" \
       -I "${INSERTS}" \
       -U "${UPDATES}" \
       -S "${RANGE_QUERIES}" \
       -Y "${SELECTIVITY}" \
       -E "${ENTRY_SIZE}" \
      #  -L "${LAMBDA}"
  then
    log_error "Something went wrong generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"
    popd >/dev/null || exit
    continue
  fi

  WORKLOAD_FILE="${PROJECT_DIR}/workload.txt"
  if [ ! -f "${WORKLOAD_FILE}" ]; then
    log_error "workload.txt not found for PAGES_PER_FILE=${PAGES_PER_FILE}"
    popd >/dev/null || exit
    continue
  fi

  remove_trailing_newline "${WORKLOAD_FILE}"
  popd >/dev/null || exit

  # run working_version with -B, -P, -T 
  for IMPL_NUM in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
    IMPL_NAME="${BUFFER_IMPLEMENTATIONS[${IMPL_NUM}]}"
    log_info "Running working_version with PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME}"

    WORKLOAD_RESULT_DIR="${RESULT_DIR}/P_${PAGES_PER_FILE}/${IMPL_NAME}"
    mkdir -p "${WORKLOAD_RESULT_DIR}"

    cp "${WORKLOAD_FILE}" "${WORKLOAD_RESULT_DIR}/"
    pushd "${WORKLOAD_RESULT_DIR}" >/dev/null || exit

    delete_db_folder "db"

    LOG_FILE="stats.log"
    log_info "Executing working_version..."

    if ! "${WORKING_VERSION_PATH}" \
          --memtable_factory="${IMPL_NUM}" \
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
          > "${LOG_FILE}"
    then
      log_error "working_version failed (PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME})"
    fi



    # delete_db_folder "db"
    popd >/dev/null || exit
  done

  rm -f "${WORKLOAD_FILE}"
done

log_info "All workloads completed for PAGES_PER_FILE in ${PAGES_PER_FILE_LIST[*]}!"log_info "All workloads completed for PAGES_PER_FILE in ${PAGES_PER_FILE_LIST[*]}!"
