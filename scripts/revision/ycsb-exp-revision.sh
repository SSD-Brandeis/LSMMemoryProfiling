#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=ycsb-exp
# YCSB entries: ~33B key + 1024B value ≈ 1057B; use 1024 as the nearest round
# power-of-two approximation so -E -B -P remain consistent with other scripts.
ENTRY_SIZE=1024
PAGE_SIZE=4096
BUFFER_SIZE_MB=1
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 256 pages
ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))                    # 4 entries/page
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGES_PER_FILE * ENTRIES_PER_PAGE ))  # 1024 entries
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

BUCKET_COUNT=100000
PREFIX_LENGTH=6

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"
YCSB_SPECS_DIR="$REPO_ROOT/lib/Tectonic/example-specs/ycsb"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (approx; YCSB actual ~1057B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE  ENTRIES_PER_PAGE=$ENTRIES_PER_PAGE)"
echo "THRESHOLD        : $THRESHOLD_TO_CONVERT_TO_SKIPLIST  (hashlinkedlist conversion)"
echo "BUCKET_COUNT     : $BUCKET_COUNT   PREFIX_LENGTH: $PREFIX_LENGTH"
echo "LOW_PRI          : $LOW_PRI  (fixed)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo "YCSB WORKLOADS   : a b c d e 0.1xf"
echo -e "========================================\n"


########################################
# Helper: run all memtables for a given workload output dir
# Usage: run_all_memtables <workload_src> <outdir> <phase_boundaries> <phase_names>
#
# phase_boundaries/phase_names come from compute_phase_boundaries.py (see
# below) and are forwarded to $BIN as --phase_boundaries/--phase_names, so
# run_workload.cc flushes and resets its stats at each group boundary
# instead of only printing one cumulative summary at the very end.
########################################
run_all_memtables() {
    local WORKLOAD_SRC="$1"
    local OUTDIR="$2"
    local PHASE_BOUNDARIES="$3"
    local PHASE_NAMES="$4"

    mkdir -p \
        "$OUTDIR/art" \
        "$OUTDIR/btree"
        # "$OUTDIR/skiplist" \
        # "$OUTDIR/simpleskiplist" \
        # "$OUTDIR/vector-preallocated" \
        # "$OUTDIR/unsortedvector-preallocated" \
        # "$OUTDIR/sortedvector-preallocated" \
        # "$OUTDIR/hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}" \
        # "$OUTDIR/hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}" \
        # "$OUTDIR/hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"

    ########################################
    # echo "  Running skiplist..."
    # cd "$OUTDIR/skiplist"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=1 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    # echo "  Running simpleskiplist..."
    # cd "$OUTDIR/simpleskiplist"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=8 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running art..."
    cd "$OUTDIR/art"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=11 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --phase_boundaries "$PHASE_BOUNDARIES" --phase_names "$PHASE_NAMES" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running btree..."
    cd "$OUTDIR/btree"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=12 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --phase_boundaries "$PHASE_BOUNDARIES" --phase_names "$PHASE_NAMES" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    # echo "  Running vector-preallocated..."
    # cd "$OUTDIR/vector-preallocated"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=2 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    # echo "  Running unsortedvector-preallocated..."
    # cd "$OUTDIR/unsortedvector-preallocated"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=5 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    # echo "  Running sortedvector-preallocated..."
    # cd "$OUTDIR/sortedvector-preallocated"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=6 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    # echo "  Running hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    # cd "$OUTDIR/hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=3 \
    #     --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    # echo "  Running hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    # cd "$OUTDIR/hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=9 \
    #     --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    # echo "  Running hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    # cd "$OUTDIR/hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=4 \
    #     --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
    #     --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
    # mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5
}


########################################
# Sweep YCSB workloads a-e, plus the 10x-scaled-down f (0.1xf) in place of
# the full-size f (too slow to run in full here).
########################################
for WORKLOAD in a b c d e 0.1xf; do

    SPEC="$YCSB_SPECS_DIR/${WORKLOAD}.spec.json"
    WDIR="$BASE_DIR/ycsb-${WORKLOAD}"
    mkdir -p "$WDIR"

    echo "========================================"
    echo "YCSB Workload: $WORKLOAD"
    echo "========================================"

    # Generate workload from the YCSB spec file
    pushd "$WDIR" > /dev/null
    "$TECTONIC_CLI" generate -w "$SPEC" -o "workload.txt"
    popd > /dev/null

    # Derive per-group op-count boundaries/names from the spec itself, so
    # run_workload.cc can flush+reset stats at each group boundary (see
    # run_all_memtables above) instead of only at the very end.
    PHASE_INFO=$(python3 "$REPO_ROOT/scripts/revision/compute_phase_boundaries.py" "$SPEC")
    PHASE_BOUNDARIES=$(sed -n '1p' <<< "$PHASE_INFO")
    PHASE_NAMES=$(sed -n '2p' <<< "$PHASE_INFO")
    echo "  Phase boundaries : $PHASE_BOUNDARIES"
    echo "  Phase names      : $PHASE_NAMES"

    run_all_memtables "$WDIR/workload.txt" "$WDIR" "$PHASE_BOUNDARIES" "$PHASE_NAMES"

    echo "  Done with ycsb-${WORKLOAD}"
    echo ""
done


cd "$REPO_ROOT"
echo "Done."
echo "All YCSB experiments finished."


# shellcheck source=.env
source .env

HOSTNAME=$(hostname)

MESSAGE="YCSB Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" "${SLACK_WEBHOOK_URL}"
