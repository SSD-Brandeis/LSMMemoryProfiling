entry_sizes_list=(128)
page_sizes=(4096)

INSERTS=30000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0.1
POINT_QUERIES=10000
LAMBDA=0.5
SIZE_RATIO=10
SHOW_PROGRESS=1
SANITY_CHECK=0

BUCKET_COUNT=100000
PREFIX_LENGTH=6
LINKLIST_THRESHOLD_USE_SKIPLIST=${INSERTS}

log_info() { echo "[INFO] $*"; }
log_error() { echo "[ERROR] $*"; }

delete_db_folder() { rm -rf "$1"; }

extract_db_logs() {
  local run="$1"
  mv db/LOG "LOG_run${run}" 2>/dev/null || true
  mv db/workload.log "workload_run${run}" 2>/dev/null || true
}

reorder_workload() {
  local f="$1"
  tmpI=$(mktemp)
  tmpQ=$(mktemp)
  tmpS=$(mktemp)
  grep '^I ' "$f" >"$tmpI" || true
  grep '^Q ' "$f" >"$tmpQ" || true
  grep '^S ' "$f" >"$tmpS" || true
  cat "$tmpI" "$tmpQ" "$tmpS" >"$f"
  rm -f "$tmpI" "$tmpQ" "$tmpS"
}

declare -A BUFFER_IMPLEMENTATIONS=(
  # [1]="skiplist"
  [2]="Vector"
  # [3]="hash_skip_list"
  # [4]="hash_linked_list"
  # [5]="UnsortedVector"
  # [6]="AlwayssortedVector"
)

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_PARENT_DIR_BASE="${PROJECT_DIR}/.result/dddebug_7_5_rawop_low_pri_true_default_refill"

LOAD_GEN_PATH="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION_PATH="${PROJECT_DIR}/bin/working_version"

top_level_names=()
for PAGE_SIZE in "${page_sizes[@]}"; do
  top_level_names+=("${INSERTS}_inserts_${POINT_QUERIES}_PQ_${RANGE_QUERIES}_RQ_S_${SELECTIVITY}")
done

for i in "${!page_sizes[@]}"; do
  PAGE_SIZE=${page_sizes[$i]}
  TOP_LEVEL_DIR_NAME=${top_level_names[$i]}
  RESULT_PARENT_DIR="${RESULT_PARENT_DIR_BASE}/${TOP_LEVEL_DIR_NAME}"
  mkdir -p "${RESULT_PARENT_DIR}"

  if [ "${PAGE_SIZE}" -eq 2048 ]; then
    PAGE_TAG="2kb_page"
    PAGES_PER_FILE_LIST=(4096)
  elif [ "${PAGE_SIZE}" -eq 4096 ]; then
    PAGE_TAG="4kb_page"
    PAGES_PER_FILE_LIST=(16384)
  elif [ "${PAGE_SIZE}" -eq 8192 ]; then
    PAGE_TAG="8kb_page"
    PAGES_PER_FILE_LIST=(1024)
  elif [ "${PAGE_SIZE}" -eq 16384 ]; then
    PAGE_TAG="16kb_page"
    PAGES_PER_FILE_LIST=(512)
  elif [ "${PAGE_SIZE}" -eq 32768 ]; then
    PAGE_TAG="32kb_page"
    PAGES_PER_FILE_LIST=(256)
  elif [ "${PAGE_SIZE}" -eq 65536 ]; then
    PAGE_TAG="64kb_page"
    PAGES_PER_FILE_LIST=(128)
  else
    echo "Unknown PAGE_SIZE: ${PAGE_SIZE}"
    exit 1
  fi

  for ENTRY_SIZE in "${entry_sizes_list[@]}"; do
    ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
    TAG="${PAGE_TAG}_entry_${ENTRY_SIZE}b_buffer_64mb"

    log_info "Generating workload (E=${ENTRY_SIZE}, PAGE=${PAGE_SIZE})"
    pushd "${PROJECT_DIR}" >/dev/null || exit
    "${LOAD_GEN_PATH}" \
      -I "${INSERTS}" -Q "${POINT_QUERIES}" -U "${UPDATES}" \
      -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
      -E "${ENTRY_SIZE}" -L "${LAMBDA}"
    WORKLOAD_FILE="${PROJECT_DIR}/workload.txt"
    [ -f "${WORKLOAD_FILE}" ] || {
      log_error "workload.txt missing"
      popd >/dev/null
      continue
    }
    reorder_workload "${WORKLOAD_FILE}"
    popd >/dev/null

    for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
      log_info "PAGES_PER_FILE=${PAGES_PER_FILE}"

      for IMPL_NUM in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
        IMPL_NAME="${BUFFER_IMPLEMENTATIONS[${IMPL_NUM}]}"

        # Vectorvariants two flag, prealloc/dynamic. no flag means prealloc
        if [[ "${IMPL_NUM}" -eq 2 || "${IMPL_NUM}" -eq 5 || "${IMPL_NUM}" -eq 6 ]]; then
          modes=(dynamic prealloc)
        else
          modes=(single)
        fi

        for mode in "${modes[@]}"; do

          if [[ "${IMPL_NUM}" -eq 2 || "${IMPL_NUM}" -eq 5 || "${IMPL_NUM}" -eq 6 ]]; then
            # Vector / UnsortedVector / AlwayssortedVector
            if [ "${mode}" = "dynamic" ]; then
              A_FLAG="-A 0" # dynamic
              BUF_DIR="${IMPL_NAME}"
            else
              A_FLAG="" # pre-allocated
              BUF_DIR="preallocated ${IMPL_NAME}"
            fi
          else
            # skiplist & hash_*  → never pass -A
            A_FLAG=""
            BUF_DIR="${IMPL_NAME}"
          fi

          EXP_DIR="${IMPL_NAME}-${TAG}-I${INSERTS}-Q${POINT_QUERIES}-U${UPDATES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}"
          OUTDIR="${RESULT_PARENT_DIR}/${BUF_DIR}/${EXP_DIR}/P_${PAGES_PER_FILE}"
          mkdir -p "${OUTDIR}"
          cp "${WORKLOAD_FILE}" "${OUTDIR}/"

          pushd "${OUTDIR}" >/dev/null || exit

          extra=""
          if [ "${IMPL_NUM}" -eq 3 ]; then
            extra="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
          elif [ "${IMPL_NUM}" -eq 4 ]; then
            extra="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH} --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
          fi

          for run in 1 2 3; do
            log_info "Run ${IMPL_NAME}/${mode} trial #${run}"
            delete_db_folder "db"
            "${WORKING_VERSION_PATH}" \
              --memtable_factory="${IMPL_NUM}" ${extra} \
              -I "${INSERTS}" -U "${UPDATES}" \
              -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
              -E "${ENTRY_SIZE}" -B "${ENTRIES_PER_PAGE}" \
              -P "${PAGES_PER_FILE}" -T "${SIZE_RATIO}" \
              ${A_FLAG} --stat 1 >"run${run}.log"
            extract_db_logs "${run}"
            delete_db_folder "db"
          done

          popd >/dev/null
        done
      done # impl
    done   # pages/file
  done     # entry-size
done       # page-size
log_info "All workloads completed!"
