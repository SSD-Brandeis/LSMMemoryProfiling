set -e

RESULTS_DIR=".result"

# TAG="rqcommonprefix_selectivity"
TAG="fig19ABCrerun_sequential"
SETTINGS="lowpri_false"
LOW_PRI=0

INSERTS=480000
UPDATES=0
POINT_QUERIES=10000 # 10000
RANGE_QUERIES=1000 # 000
SELECTIVITY=0.1

SIZE_RATIO=10

ENTRY_SIZES=(128)
LAMBDA=0.0625
PAGE_SIZES=(4096)

THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}
SORT_WORKLOAD=1

declare -A BUFFER_IMPLEMENTATIONS=(
# [1]="skiplist"
# [2]="Vector"
  [3]="hash_skip_list"
  [4]="hash_linked_list"
# [5]="UnsortedVector"
# [6]="AlwayssortedVector"
  [9]="hash_vector"
)

# FIXED PATH RESOLUTION: Added /../../ to point to the root project directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../" && pwd)"
LOAD_GEN="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION="${PROJECT_DIR}/bin/working_version"

mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
  if   [ "$PAGE_SIZE" -eq 2048 ];  then PAGES_PER_FILE=4096
  #128MB
  elif [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE=32768 
  # elif [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE=131072
  elif [ "$PAGE_SIZE" -eq 8192 ];  then PAGES_PER_FILE=1024
  elif [ "$PAGE_SIZE" -eq 16384 ]; then PAGES_PER_FILE=512
  elif [ "$PAGE_SIZE" -eq 32768 ]; then PAGES_PER_FILE=256
  elif [ "$PAGE_SIZE" -eq 65536 ]; then PAGES_PER_FILE=128
  else echo "Unknown PAGE_SIZE: $PAGE_SIZE"; exit 1; fi

  for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
    ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

    # --- WORKLOAD GENERATION (Once per Entry Size) ---
    # This creates the master workload in the root of .result
    "${LOAD_GEN}" -I "${INSERTS}" -U 0 -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"
    if [[ "$SORT_WORKLOAD" -eq 1 ]]; then
      IFILE=$(mktemp); QFILE=$(mktemp); SFILE=$(mktemp)
      grep '^I ' workload.txt > "$IFILE" || true
      grep '^Q ' workload.txt > "$QFILE" || true
      grep '^S ' workload.txt > "$SFILE" || true
      cat "$IFILE" "$QFILE" "$SFILE" > workload_sorted.txt
      rm -f "$IFILE" "$QFILE" "$SFILE" workload.txt
      mv workload_sorted.txt workload.txt
    fi
    # workload.txt now persists in .result

    # Sweep 1: vary H at X=6 (I+Q+S together)
    EXP_DIR="IPQRS_X6_varH-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
    mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    for H in 100 1000 10000 100000 250000 500000 1000000 2000000 4000000; do
      for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
        NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
        mkdir -p "${NAME}-X6-H${H}"; cd "${NAME}-X6-H${H}"
        cp ../../workload.txt ./workload.txt
        for run in 1; do
          echo "Run ${NAME} #${run} Sweep 1: X=6 H=${H}"
          if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" || "$NAME" == "hash_vector" ]]; then
            if [[ "$NAME" == "hash_linked_list" ]]; then
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count="${H}" --prefix_length=6 \
                --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
            else
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count="${H}" --prefix_length=6 \
                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
            fi
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
        rm -f workload.txt
        cd ..
      done
    done
    cd ..

    # Sweep 2: PQ focus, X=6 vary H (still I+Q+S)
    EXP_DIR="insertPQRS_X6_varH_PQ-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
    mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    for H in 100 1000 10000 100000 250000 500000 1000000 2000000 4000000; do
      for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
        NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
        mkdir -p "${NAME}-X6-H${H}"; cd "${NAME}-X6-H${H}"
        cp ../../workload.txt ./workload.txt
        for run in 1; do
          echo "Run ${NAME} #${run} Sweep 2: X=6 H=${H}"
          if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" || "$NAME" == "hash_vector" ]]; then
            if [[ "$NAME" == "hash_linked_list" ]]; then
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count="${H}" --prefix_length=6 \
                --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
            else
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count="${H}" --prefix_length=6 \
                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
            fi
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
        rm -f workload.txt
        cd ..
      done
    done
    cd ..

    # Sweep 3: PQ focus, H=100k vary X=1..6 (still I+Q+S)
    EXP_DIR="insertPQRS_H100k_varX-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
    mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    for X in 1 2 3 4 5 6; do
      for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
        NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
        mkdir -p "${NAME}-X${X}-H100000"; cd "${NAME}-X${X}-H100000"
        cp ../../workload.txt ./workload.txt
        for run in 1; do
          echo "Run ${NAME} #${run} Sweep 3: X=${X} H=100000"
          if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" || "$NAME" == "hash_vector" ]]; then
            if [[ "$NAME" == "hash_linked_list" ]]; then
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count=100000 --prefix_length="${X}" \
                --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
            else
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count=100000 --prefix_length="${X}" \
                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
            fi
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
        rm -f workload.txt
        cd ..
      done
    done
    cd ..
  done
done

echo
echo "Finished experiments. Master workload preserved in ${RESULTS_DIR}/workload.txt"