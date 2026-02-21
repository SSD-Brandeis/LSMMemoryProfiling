
set -e

RESULTS_DIR=".result"

TAG="rqcommonprefix_1GB"
SETTINGS="lowpri_false"
LOW_PRI=0

INSERTS=960000
UPDATES=0
POINT_QUERIES=10000
RANGE_QUERIES=1000
SELECTIVITY=0.1

SIZE_RATIO=10

ENTRY_SIZES=(1024)
LAMBDA=0.125
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
)

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOAD_GEN="${PROJECT_DIR}/bin/load_gen"
WORKING_VERSION="${PROJECT_DIR}/bin/working_version"

mkdir -p "$RESULTS_DIR"
cd "$RESULTS_DIR"

for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
  if   [ "$PAGE_SIZE" -eq 2048 ];  then PAGES_PER_FILE=4096
  #change it to 1gb now 
  elif [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE=262144
  elif [ "$PAGE_SIZE" -eq 8192 ];  then PAGES_PER_FILE=1024
  elif [ "$PAGE_SIZE" -eq 16384 ]; then PAGES_PER_FILE=512
  elif [ "$PAGE_SIZE" -eq 32768 ]; then PAGES_PER_FILE=256
  elif [ "$PAGE_SIZE" -eq 65536 ]; then PAGES_PER_FILE=128
  else echo "Unknown PAGE_SIZE: $PAGE_SIZE"; exit 1; fi

  for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
    ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

    # Sweep 1: vary H at X=8 (I+Q+S together)
    EXP_DIR="IPQRS_X8_varH-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
    mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    "${LOAD_GEN}" -I "${INSERTS}" -U 0 -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"
    if [[ "$SORT_WORKLOAD" -eq 1 ]]; then
      IFILE=$(mktemp); QFILE=$(mktemp); SFILE=$(mktemp)
      grep '^I ' workload.txt > "$IFILE" || true
      grep '^Q ' workload.txt > "$QFILE" || true
      grep '^S ' workload.txt > "$SFILE" || true
      cat "$IFILE" "$QFILE" "$SFILE" > workload.txt
      rm -f "$IFILE" "$QFILE" "$SFILE"
    fi

    for H in 100 1000 10000 100000 250000 500000 1000000 2000000 4000000; do
      for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
        NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
        mkdir -p "${NAME}-X8-H${H}"; cd "${NAME}-X8-H${H}"
        cp ../workload.txt ./workload.txt
        for run in 1 2 3; do
          echo "Run ${NAME} #${run} I+Q+S X=8 H=${H}"
          if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
            if [[ "$NAME" == "hash_linked_list" ]]; then
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count="${H}" --prefix_length=8 \
                --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
            else
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count="${H}" --prefix_length=8 \
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
    rm -f workload.txt
    cd ..

    # Sweep 2: PQ focus, X=8 vary H (still I+Q+S)
    EXP_DIR="insertPQRS_X8_varH_PQ-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
    mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    "${LOAD_GEN}" -I "${INSERTS}" -U 0 -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"
    if [[ "$SORT_WORKLOAD" -eq 1 ]]; then
      IFILE=$(mktemp); QFILE=$(mktemp); SFILE=$(mktemp)
      grep '^I ' workload.txt > "$IFILE" || true
      grep '^Q ' workload.txt > "$QFILE" || true
      grep '^S ' workload.txt > "$SFILE" || true
      cat "$IFILE" "$QFILE" "$SFILE" > workload.txt
      rm -f "$IFILE" "$QFILE" "$SFILE"
    fi

    for H in 100 1000 10000 100000 250000 500000 1000000 2000000 4000000; do
      for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
        NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
        mkdir -p "${NAME}-X8-H${H}"; cd "${NAME}-X8-H${H}"
        cp ../workload.txt ./workload.txt
        for run in 1 2 3; do
          echo "Run ${NAME} #${run} I+Q+S X=8 H=${H}"
          if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
            if [[ "$NAME" == "hash_linked_list" ]]; then
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count="${H}" --prefix_length=8 \
                --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
            else
              "${WORKING_VERSION}" --memtable_factory="${impl}" \
                -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                --bucket_count="${H}" --prefix_length=8 \
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
    rm -f workload.txt
    cd ..

    # Sweep 3: PQ focus, H=1M vary X=1..8 (still I+Q+S)
    # EXP_DIR="insertPQRS_H1M_varX-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
    # mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    # "${LOAD_GEN}" -I "${INSERTS}" -U 0 -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"
    # if [[ "$SORT_WORKLOAD" -eq 1 ]]; then
    #   IFILE=$(mktemp); QFILE=$(mktemp); SFILE=$(mktemp)
    #   grep '^I ' workload.txt > "$IFILE" || true
    #   grep '^Q ' workload.txt > "$QFILE" || true
    #   grep '^S ' workload.txt > "$SFILE" || true
    #   cat "$IFILE" "$QFILE" "$SFILE" > workload.txt
    #   rm -f "$IFILE" "$QFILE" "$SFILE"
    # fi

    # for X in 1 2 3 4 5 6 7 8; do
    #   for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
    #     NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
    #     mkdir -p "${NAME}-X${X}-H1000000"; cd "${NAME}-X${X}-H1000000"
    #     cp ../workload.txt ./workload.txt
    #     for run in 1 2 3; do
    #       echo "Run ${NAME} #${run} I+Q+S X=${X} H=1000000"
    #       if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
    #         if [[ "$NAME" == "hash_linked_list" ]]; then
    #           "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #             -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #             -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #             --bucket_count=1000000 --prefix_length="${X}" \
    #             --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
    #             --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
    #         else
    #           "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #             -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #             -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #             --bucket_count=1000000 --prefix_length="${X}" \
    #             --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
    #         fi
    #       else
    #         "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #           -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #           -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #           --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
    #       fi
    #       mv db/LOG "./LOG${run}"
    #       mv workload.log "./workload${run}.log"
    #       rm -rf db
    #     done
    #     rm -f workload.txt
    #     cd ..
    #   done
    # done
    # rm -f workload.txt
    # cd ..

    # Sweep 4: RQ common prefix C = x−y with X=8, H=1M (still I+Q+S)
    # EXP_DIR="insertPQRS_X8H1M_varC-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
    # mkdir -p "$EXP_DIR"; cd "$EXP_DIR"

    # "${LOAD_GEN}" -I "${INSERTS}" -U 0 -Q "${POINT_QUERIES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" -E "${ENTRY_SIZE}" -L "${LAMBDA}"
    # if [[ "$SORT_WORKLOAD" -eq 1 ]]; then
    #   IFILE=$(mktemp); QFILE=$(mktemp); SFILE=$(mktemp)
    #   grep '^I ' workload.txt > "$IFILE" || true
    #   grep '^Q ' workload.txt > "$QFILE" || true
    #   grep '^S ' workload.txt > "$SFILE" || true
    #   cat "$IFILE" "$QFILE" "$SFILE" > workload.txt
    #   rm -f "$IFILE" "$QFILE" "$SFILE"
    # fi

    # for C in 0 1 2 3 4 5 6 7 8; do
    #   PATCHED="workload_c${C}.txt"
    #   awk -v c="$C" 'BEGIN{OFS=" "}
    #     /^S[[:space:]]+/ {
    #       if (NF>=3) {
    #         s=$2; e=$3;
    #         pre=substr(s,1,c);
    #         if (length(e)>=c) e=pre substr(e,c+1); else e=pre;
    #         print $1, s, e;
    #         next
    #       }
    #     }
    #     {print}
    #   ' "workload.txt" > "${PATCHED}"

    #   for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
    #     NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
    #     mkdir -p "${NAME}-X8-H1000000-C${C}"; cd "${NAME}-X8-H1000000-C${C}"
    #     cp ../"${PATCHED}" ./workload.txt
    #     for run in 1 2 3; do
    #       echo "Run ${NAME} #${run} I+Q+S X=8 H=1000000 C=${C}"
    #       if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
    #         if [[ "$NAME" == "hash_linked_list" ]]; then
    #           "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #             -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #             -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #             --bucket_count=1000000 --prefix_length=8 \
    #             --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
    #             --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
    #         else
    #           "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #             -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #             -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #             --bucket_count=1000000 --prefix_length=8 \
    #             --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
    #         fi
    #       else
    #         "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #           -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #           -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #           --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
    #       fi
    #       mv db/LOG "./LOG${run}"
    #       mv workload.log "./workload${run}.log"
    #       rm -rf db
    #     done
    #     rm -f workload.txt
    #     cd ..
    #   done

    #   rm -f "${PATCHED}"
    # done
    # rm -f workload.txt
    # cd ..
  done
done

echo
echo "Finished experiments"
