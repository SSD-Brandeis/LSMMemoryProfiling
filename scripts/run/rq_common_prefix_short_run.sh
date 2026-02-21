#!/usr/bin/env bash
set -e

RESULTS_DIR=".result"

TAG="rqcommonprefix"
SETTINGS="lowpri_false"
LOW_PRI=0

INSERTS=480000
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
  elif [ "$PAGE_SIZE" -eq 4096 ];  then PAGES_PER_FILE=131072
  elif [ "$PAGE_SIZE" -eq 8192 ];  then PAGES_PER_FILE=1024
  elif [ "$PAGE_SIZE" -eq 16384 ]; then PAGES_PER_FILE=512
  elif [ "$PAGE_SIZE" -eq 32768 ]; then PAGES_PER_FILE=256
  elif [ "$PAGE_SIZE" -eq 65536 ]; then PAGES_PER_FILE=128
  else echo "Unknown PAGE_SIZE: $PAGE_SIZE"; exit 1; fi

  for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
    ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

    ########################################################################
    # DISABLED: Sweep 1 — vary H at X=8 (I+Q+S together)
    ########################################################################
    # EXP_DIR="IPQRS_X8_varH-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
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
    # for H in 100 1000 10000 100000 250000 500000 1000000; do
    #   for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
    #     NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
    #     mkdir -p "${NAME}-X8-H${H}"; cd "${NAME}-X8-H${H}"
    #     cp ../workload.txt ./workload.txt
    #     for run in 1 2 3; do
    #       echo "Run ${NAME} #${run} I+Q+S X=8 H=${H}"
    #       if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
    #         if [[ "$NAME" == "hash_linked_list" ]]; then
    #           "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #             -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #             -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #             --bucket_count="${H}" --prefix_length=8 \
    #             --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
    #             --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
    #         else
    #           "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #             -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #             -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #             --bucket_count="${H}" --prefix_length=8 \
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

    ########################################################################
    # DISABLED: Sweep 2 — PQ focus, X=8 vary H (I+Q+S)
    ########################################################################
    # EXP_DIR="insertPQRS_X8_varH_PQ-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
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
    # for H in 100 1000 10000 100000 250000 500000 1000000; do
    #   for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
    #     NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
    #     mkdir -p "${NAME}-X8-H${H}"; cd "${NAME}-X8-H${H}"
    #     cp ../workload.txt ./workload.txt
    #     for run in 1 2 3; do
    #       echo "Run ${NAME} #${run} I+Q+S X=8 H=${H}"
    #       if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
    #         if [[ "$NAME" == "hash_linked_list" ]]; then
    #           "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #             -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #             -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #             --bucket_count="${H}" --prefix_length=8 \
    #             --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
    #             --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
    #         else
    #           "${WORKING_VERSION}" --memtable_factory="${impl}" \
    #             -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
    #             -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
    #             --bucket_count="${H}" --prefix_length=8 \
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

    ########################################################################
    # DISABLED: Sweep 3 — PQ focus, H=1M vary X=1..8 (I+Q+S)
    ########################################################################
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

    ########################################################################
    # DISABLED: Sweep 4 — RQ common prefix C with X=8, H=1M (I+Q+S)
    ########################################################################
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

    #####################################################
    # Sweep 5: NEW — PQ focus, LOW H in {100,1000,10000}, vary X=1..8
    #####################################################
    for HLOW in 100 1000 10000; do
      EXP_DIR="insertPQRS_H${HLOW}_varX-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
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

      for X in 1 2 3 4 5 6 7 8; do
        for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
          NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
          mkdir -p "${NAME}-X${X}-H${HLOW}"; cd "${NAME}-X${X}-H${HLOW}"
          cp ../workload.txt ./workload.txt
          for run in 1 2 3; do
            echo "Run ${NAME} #${run} I+Q+S X=${X} H=${HLOW}"
            if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
              if [[ "$NAME" == "hash_linked_list" ]]; then
                "${WORKING_VERSION}" --memtable_factory="${impl}" \
                  -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                  --bucket_count="${HLOW}" --prefix_length="${X}" \
                  --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                  --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
              else
                "${WORKING_VERSION}" --memtable_factory="${impl}" \
                  -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                  --bucket_count="${HLOW}" --prefix_length="${X}" \
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
    done

    #####################################################
    # Sweep 6: NEW — PQ focus, SHORT X in {1,2,3}, vary H across full set
    #####################################################
    for XSHORT in 1 2 3; do
      EXP_DIR="insertPQRS_X${XSHORT}_varH-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
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

      for H in 100 1000 10000 100000 250000 500000 1000000; do
        for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
          NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
          mkdir -p "${NAME}-X${XSHORT}-H${H}"; cd "${NAME}-X${XSHORT}-H${H}"
          cp ../workload.txt ./workload.txt
          for run in 1 2 3; do
            echo "Run ${NAME} #${run} I+Q+S X=${XSHORT} H=${H}"
            if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
              if [[ "$NAME" == "hash_linked_list" ]]; then
                "${WORKING_VERSION}" --memtable_factory="${impl}" \
                  -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                  --bucket_count="${H}" --prefix_length="${XSHORT}" \
                  --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                  --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
              else
                "${WORKING_VERSION}" --memtable_factory="${impl}" \
                  -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                  -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                  --bucket_count="${H}" --prefix_length="${XSHORT}" \
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
    done

    #####################################################
    # Sweep 7: NEW — RQ with SHORT X in {2,3} and LOW H in {100,1000}, vary C=0..X
    #####################################################
    for Xs in 2 3; do
      for Hs in 100 1000; do
        EXP_DIR="insertPQRS_X${Xs}H${Hs}_varC-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
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

        for C in $(seq 0 "${Xs}"); do
          PATCHED="workload_c${C}.txt"
          awk -v c="$C" 'BEGIN{OFS=" "}
            /^S[[:space:]]+/ {
              if (NF>=3) {
                s=$2; e=$3;
                pre=substr(s,1,c);
                if (length(e)>=c) e=pre substr(e,c+1); else e=pre;
                print $1, s, e;
                next
              }
            }
            {print}
          ' "workload.txt" > "${PATCHED}"

          for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
            NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
            mkdir -p "${NAME}-X${Xs}-H${Hs}-C${C}"; cd "${NAME}-X${Xs}-H${Hs}-C${C}"
            cp ../"${PATCHED}" ./workload.txt
            for run in 1 2 3; do
              echo "Run ${NAME} #${run} I+Q+S X=${Xs} H=${Hs} C=${C}"
              if [[ "$NAME" == "hash_linked_list" || "$NAME" == "hash_skip_list" ]]; then
                if [[ "$NAME" == "hash_linked_list" ]]; then
                  "${WORKING_VERSION}" --memtable_factory="${impl}" \
                    -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                    --bucket_count="${Hs}" --prefix_length="${Xs}" \
                    --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" \
                    --lowpri "${LOW_PRI}" --stat 1 > "run${run}.log"
                else
                  "${WORKING_VERSION}" --memtable_factory="${impl}" \
                    -I "${INSERTS}" -U 0 -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" -E "${ENTRY_SIZE}" \
                    --bucket_count="${Hs}" --prefix_length="${Xs}" \
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
          rm -f "${PATCHED}"
        done
        rm -f workload.txt
        cd ..
      done
    done

    #####################################################
    # Sweep 8: NEW — RQ, X=8, vary H over full set, C=0..8
    #####################################################
    EXP_DIR="insertPQRS_X8_varC_varH-${SETTINGS}-I${INSERTS}-U${UPDATES}-Q${POINT_QUERIES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-P${PAGES_PER_FILE}-B${ENTRIES_PER_PAGE}-E${ENTRY_SIZE}"
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

    for H in 100 1000 10000 100000 250000 500000 1000000; do
      for C in 0 1 2 3 4 5 6 7 8; do
        PATCHED="workload_c${C}.txt"
        awk -v c="$C" 'BEGIN{OFS=" "}
          /^S[[:space:]]+/ {
            if (NF>=3) {
              s=$2; e=$3;
              pre=substr(s,1,c);
              if (length(e)>=c) e=pre substr(e,c+1); else e=pre;
              print $1, s, e;
              next
            }
          }
          {print}
        ' "workload.txt" > "${PATCHED}"

        for impl in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
          NAME="${BUFFER_IMPLEMENTATIONS[$impl]}"
          mkdir -p "${NAME}-X8-H${H}-C${C}"; cd "${NAME}-X8-H${H}-C${C}"
          cp ../"${PATCHED}" ./workload.txt
          for run in 1 2 3; do
            echo "Run ${NAME} #${run} I+Q+S X=8 H=${H} C=${C}"
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
        rm -f "${PATCHED}"
      done
    done
    rm -f workload.txt
    cd ..

  done
done

echo
echo "Finished experiments"
