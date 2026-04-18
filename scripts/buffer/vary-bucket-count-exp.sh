#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=vary-bucket-count-exp
ENTRY_SIZE=128
LAMBDA=0.0625          # key = 8B, val = 120B
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

PREFIX_LENGTH=6        # fixed across this sweep

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 120

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE ))

# Hash skiplist overhead is 29% of total buffer capacity (observed).
HASHSKIPLIST_OVERHEAD_PCT=29
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - HASHSKIPLIST_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

POINT_QUERIES=10000

# Bucket counts to sweep; prefix length fixed at 6
BUCKET_COUNTS=(100 1000 10000 100000 250000 500000 1000000)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "INSERTS          : $INSERTS  (hashskiplist overhead=${HASHSKIPLIST_OVERHEAD_PCT}%)"
echo "POINT_QUERIES    : $POINT_QUERIES  (sequential, after inserts)"
echo "PREFIX_LENGTH    : $PREFIX_LENGTH  (fixed)"
echo "BUCKET_COUNTS    : ${BUCKET_COUNTS[*]}"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Generate workload once — two sequential groups:
#   Group 1: all inserts
#   Group 2: 10K non-empty PQs (draws from keys inserted in group 1)
########################################
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
      {
        "point_queries": {
          "op_count": $POINT_QUERIES,
          "selection": {"uniform": {"min": 0, "max": 1}}
        }
      }
    ]
  }]
}
with open("$BASE_DIR/workload.specs.json", "w") as f:
    json.dump(spec, f, indent=2)
print("Wrote workload.specs.json")
EOF

pushd "$BASE_DIR" > /dev/null
"$TECTONIC_CLI" generate -w workload.specs.json
popd > /dev/null


########################################
# Sweep bucket counts
########################################
for BUCKET_COUNT in "${BUCKET_COUNTS[@]}"; do

    BDIR="$BASE_DIR/B${BUCKET_COUNT}-X${PREFIX_LENGTH}"

    mkdir -p \
        "$BDIR/hashskiplist" \
        "$BDIR/hashvector" \
        "$BDIR/hashlinkedlist"

    echo "========================================"
    echo "BUCKET_COUNT=$BUCKET_COUNT  PREFIX_LENGTH=$PREFIX_LENGTH"
    echo "========================================"

    ########################################
    echo "  [B${BUCKET_COUNT}-X${PREFIX_LENGTH}] Running hashskiplist..."
    cd "$BDIR/hashskiplist"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=3 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  [B${BUCKET_COUNT}-X${PREFIX_LENGTH}] Running hashvector..."
    cd "$BDIR/hashvector"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=9 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  [B${BUCKET_COUNT}-X${PREFIX_LENGTH}] Running hashlinkedlist..."
    cd "$BDIR/hashlinkedlist"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=4 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    echo "  Done with B${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    echo ""
done


cd "$REPO_ROOT"
echo "Done."
echo "All vary-bucket-count experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="vary-bucket-count Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
