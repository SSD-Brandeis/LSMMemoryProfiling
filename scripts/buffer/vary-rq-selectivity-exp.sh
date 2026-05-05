#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=vary-rq-selectivity-exp
ENTRY_SIZE=128
LAMBDA=0.0625
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

BUCKET_COUNT=100000

# Hash skiplist overhead is 29% of total buffer capacity (observed).
HASHSKIPLIST_OVERHEAD_PCT=29
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - HASHSKIPLIST_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))
INSERTS_WARMUP=$(( INSERTS / 10 ))
INSERTS_MAIN=$(( INSERTS - INSERTS_WARMUP ))

RQ=1000

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE ))

# RQ selectivities to sweep
SELECTIVITIES=(0.1 0.01 0.001 0.0001 0.00001 0.000001 0.0000001)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "INSERTS          : $INSERTS  (hashskiplist overhead=${HASHSKIPLIST_OVERHEAD_PCT}%)"
echo "  warmup group   : $INSERTS_WARMUP"
echo "  mixed group    : $INSERTS_MAIN"
echo "RQ               : $RQ  (start_end format)"
echo "SELECTIVITIES    : ${SELECTIVITIES[*]}"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "BUCKET_COUNT     : $BUCKET_COUNT   PREFIX sweep: 2, 6"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Helper: run all memtables for a given workload + output subdir
# Usage: run_all_memtables <workload_file> <output_dir>
########################################
run_all_memtables() {
    local WORKLOAD_SRC="$1"
    local SUBDIR="$2"

    mkdir -p \
        "$SUBDIR/skiplist" \
        "$SUBDIR/simpleskiplist" \
        "$SUBDIR/vector-preallocated" \
        "$SUBDIR/unsortedvector-preallocated" \
        "$SUBDIR/sortedvector-preallocated" \
        "$SUBDIR/hashskiplist-H${BUCKET_COUNT}-X2" \
        "$SUBDIR/hashskiplist-H${BUCKET_COUNT}-X6" \
        "$SUBDIR/hashvector-H${BUCKET_COUNT}-X2" \
        "$SUBDIR/hashvector-H${BUCKET_COUNT}-X6" \
        "$SUBDIR/hashlinkedlist-H${BUCKET_COUNT}-X2" \
        "$SUBDIR/hashlinkedlist-H${BUCKET_COUNT}-X6"

    ########################################
    echo "  Running skiplist..."
    cd "$SUBDIR/skiplist"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=1 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running simpleskiplist..."
    cd "$SUBDIR/simpleskiplist"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=8 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running vector-preallocated..."
    cd "$SUBDIR/vector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=2 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running unsortedvector-preallocated..."
    cd "$SUBDIR/unsortedvector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=5 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running sortedvector-preallocated..."
    cd "$SUBDIR/sortedvector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    # Hash hybrids — run twice, once per prefix length
    for PREFIX_LENGTH in 2 6; do

        echo "  Running hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
        cd "$SUBDIR/hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
        cp "$WORKLOAD_SRC" workload.txt
        "$BIN" \
            --memtable_factory=3 \
            --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
            -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

        echo "  Running hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
        cd "$SUBDIR/hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
        cp "$WORKLOAD_SRC" workload.txt
        "$BIN" \
            --memtable_factory=9 \
            --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
            -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

        echo "  Running hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
        cd "$SUBDIR/hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
        cp "$WORKLOAD_SRC" workload.txt
        "$BIN" \
            --memtable_factory=4 \
            --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
            -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
            --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
        mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    done
}


########################################
# Sweep selectivities
########################################
for SELECTIVITY in "${SELECTIVITIES[@]}"; do

    SEL_DIR="$BASE_DIR/sel-${SELECTIVITY}"
    mkdir -p "$SEL_DIR"

    echo "========================================"
    echo "Selectivity: $SELECTIVITY"
    echo "========================================"

    # Generate a fresh workload for this selectivity.
    # Group 1: 10% inserts (warmup — ensures non-empty keys exist before RQs fire).
    # Group 2: 90% inserts + RQs interleaved (mixed).
    # range_format StartEnd: tectonic generates a start key and an end key
    # whose span covers selectivity × inserted keyspace.
    python3 - <<EOF
import json
spec = {
  "sections": [{
    "groups": [
      {
        "inserts": {
          "op_count": $INSERTS_WARMUP,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        }
      },
      {
        "inserts": {
          "op_count": $INSERTS_MAIN,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        },
        "range_queries": {
          "op_count": $RQ,
          "selectivity": $SELECTIVITY,
          "range_format": "StartEnd",
          "selection": {"uniform": {"min": 0, "max": 1}}
        }
      }
    ]
  }]
}
with open("$SEL_DIR/workload.specs.json", "w") as f:
    json.dump(spec, f, indent=2)
print("Wrote workload.specs.json  (selectivity=$SELECTIVITY)")
EOF

    pushd "$SEL_DIR" > /dev/null
    "$TECTONIC_CLI" generate -w workload.specs.json
    popd > /dev/null

    run_all_memtables "$SEL_DIR/workload.txt" "$SEL_DIR"

    echo "  Done with selectivity $SELECTIVITY"
    echo ""
done


cd "$REPO_ROOT"
echo "Done."
echo "All vary-rq-selectivity experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="vary-rq-selectivity Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
