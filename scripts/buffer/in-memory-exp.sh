#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=inmemory-mixed-ops-exp
ENTRY_SIZE=128
LAMBDA=0.25
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

BUCKET_COUNT=100000
PREFIX_LENGTH=6


# Hash skiplist overhead is measured as a fraction of total buffer capacity.
# Observed: 749,369 entries × 128B = 71% of 128MB → overhead = 29% of buffer.
#   INSERTS = buffer_bytes * (100 - overhead_pct) / (entry_size * 100)
#   = 128MB * 71 / (128B * 100) = 744,488  (safely under 749,369 → exactly 1 flush)
HASHSKIPLIST_OVERHEAD_PCT=29
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - HASHSKIPLIST_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

EMPTY_PQ=50000        # queries on non-existing keys
NONEMPTY_PQ=50000     # queries on existing keys
RQ=1000
SELECTIVITY=0.1       # fraction of the inserted keyspace

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE ))

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=$KEY_LEN val=$VAL_LEN)"
echo "INSERTS          : $INSERTS  (fills 1 hashskiplist buffer at ${HASHSKIPLIST_OVERHEAD_PCT}% overhead)"
echo "EMPTY_PQ         : $EMPTY_PQ"
echo "NONEMPTY_PQ      : $NONEMPTY_PQ"
echo "RQ               : $RQ  (selectivity $SELECTIVITY)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "BUCKET_COUNT     : $BUCKET_COUNT   PREFIX_LENGTH: $PREFIX_LENGTH"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Generate workload specs
########################################

INSERTS_WARMUP=$(( INSERTS / 10 ))
INSERTS_MAIN=$(( INSERTS - INSERTS_WARMUP ))

# Sequential: each op type in its own group → tectonic emits them one after another
python3 - <<EOF
import json
spec = {
  "sections": [{
    "groups": [
      {
        "inserts": {
          "op_count": $INSERTS,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        }
      },
      { "empty_point_queries": { "op_count": $EMPTY_PQ, "key": {"uniform": {"len": $KEY_LEN}} } },
      { "point_queries":       { "op_count": $NONEMPTY_PQ, "selection": {"uniform": {"min": 0, "max": 1}} } },
      { "range_queries":       { "op_count": $RQ, "selectivity": $SELECTIVITY, "selection": {"uniform": {"min": 0, "max": 1}} } }
    ]
  }]
}
with open("$BASE_DIR/workload-sequential.specs.json", "w") as f:
    json.dump(spec, f, indent=2)
print("Wrote workload-sequential.specs.json")
EOF

# Mixed: 10% inserts first (warmup so non-empty PQs have keys to hit),
# then 90% inserts interleaved with all queries in a second group.
# Groups share the keyset within the section, so PQs/RQs in group 2
# draw from keys inserted in group 1.
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
        "empty_point_queries": { "op_count": $EMPTY_PQ, "key": {"uniform": {"len": $KEY_LEN}} },
        "point_queries":       { "op_count": $NONEMPTY_PQ, "selection": {"uniform": {"min": 0, "max": 1}} },
        "range_queries":       { "op_count": $RQ, "selectivity": $SELECTIVITY, "selection": {"uniform": {"min": 0, "max": 1}} }
      }
    ]
  }]
}
with open("$BASE_DIR/workload-mixed.specs.json", "w") as f:
    json.dump(spec, f, indent=2)
print("Wrote workload-mixed.specs.json")
EOF

pushd "$BASE_DIR" > /dev/null
echo "Generating sequential workload..."
"$TECTONIC_CLI" generate -w workload-sequential.specs.json -o workload-sequential.txt
echo "Generating mixed workload..."
"$TECTONIC_CLI" generate -w workload-mixed.specs.json -o workload-mixed.txt
popd > /dev/null


########################################
# Helper: run all memtables for a given workload
# Usage: run_all_memtables <workload_file> <output_subdir>
########################################
run_all_memtables() {
    local WORKLOAD_SRC="$1"
    local SUBDIR="$BASE_DIR/$2"

    mkdir -p \
        "$SUBDIR/skiplist" \
        "$SUBDIR/simpleskiplist" \
        "$SUBDIR/vector-preallocated" \
        "$SUBDIR/unsortedvector-preallocated" \
        "$SUBDIR/sortedvector-preallocated" \
        "$SUBDIR/linkedlist" \
        "$SUBDIR/hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}" \
        "$SUBDIR/hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}" \
        "$SUBDIR/hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"

    ########################################
    echo "  [$2] Running skiplist..."
    cd "$SUBDIR/skiplist"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=1 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [$2] Running simpleskiplist..."
    cd "$SUBDIR/simpleskiplist"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=8 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [$2] Running vector-preallocated..."
    cd "$SUBDIR/vector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=2 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [$2] Running unsortedvector-preallocated..."
    cd "$SUBDIR/unsortedvector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=5 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [$2] Running sortedvector-preallocated..."
    cd "$SUBDIR/sortedvector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    # ########################################
    # echo "  [$2] Running linkedlist..."
    # cd "$SUBDIR/linkedlist"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=7 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd "$REPO_ROOT"
    # echo -e "\n"
    # sleep 5

    ########################################
    echo "  [$2] Running hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$SUBDIR/hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=3 \
        --bucket_count="$BUCKET_COUNT" \
        --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [$2] Running hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$SUBDIR/hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=9 \
        --bucket_count="$BUCKET_COUNT" \
        --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [$2] Running hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$SUBDIR/hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=4 \
        --bucket_count="$BUCKET_COUNT" \
        --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5
}


########################################
echo "========================================"
echo "Running SEQUENTIAL workload (insert → empty-PQ → PQ → RQ)"
echo "========================================"
run_all_memtables "$BASE_DIR/workload-sequential.txt" "sequential"

########################################
echo "========================================"
echo "Running MIXED workload (all ops interleaved)"
echo "========================================"
run_all_memtables "$BASE_DIR/workload-mixed.txt" "mixed"


cd "$REPO_ROOT"
echo "Done."
echo "All in-memory mixed-ops experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="inmemory-mixed-ops Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
