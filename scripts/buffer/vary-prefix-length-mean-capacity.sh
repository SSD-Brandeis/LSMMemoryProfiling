#!/bin/bash
set -e

bash ./scripts/rebuild.sh


TAG=vary-prefix-length-mean-capacity
ENTRY_SIZE=128
LAMBDA=0.0625          # key = 8B, val = 120B
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

BUCKET_COUNT=100000    # fixed across this sweep

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 120

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE ))

# Vector overhead is 14% of total buffer capacity (observed).
VECTOR_OVERHEAD_PCT=14
NUM_FLUSHES=10
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - VECTOR_OVERHEAD_PCT) * NUM_FLUSHES / (ENTRY_SIZE * 100) ))

# Prefix lengths to sweep; bucket count fixed at 100K.
# Key size is 8B so prefix_length up to 6 stays within the key.
PREFIX_LENGTHS=(1 2 3 4 5 6)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "NUM_FLUSHES      : $NUM_FLUSHES"
echo "INSERTS          : $INSERTS  (vector overhead=${VECTOR_OVERHEAD_PCT}%)"
echo "BUCKET_COUNT     : $BUCKET_COUNT  (fixed)"
echo "PREFIX_LENGTHS   : ${PREFIX_LENGTHS[*]}"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Generate workload once — insert-only, single group.
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
# Non-hash baselines — run once (independent of prefix length)
########################################
mkdir -p \
    "$BASE_DIR/skiplist" \
    "$BASE_DIR/simpleskiplist" \
    "$BASE_DIR/vector"

echo "Running skiplist..."
cd "$BASE_DIR/skiplist"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=1 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

echo "Running simpleskiplist..."
cd "$BASE_DIR/simpleskiplist"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=8 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

echo "Running vector..."
cd "$BASE_DIR/vector"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=2 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5


########################################
# Sweep prefix lengths
########################################
for PREFIX_LENGTH in "${PREFIX_LENGTHS[@]}"; do

    PDIR="$BASE_DIR/B${BUCKET_COUNT}-X${PREFIX_LENGTH}"

    mkdir -p \
        "$PDIR/hashskiplist" \
        "$PDIR/hashvector" \
        "$PDIR/hashlinkedlist"

    echo "========================================"
    echo "BUCKET_COUNT=$BUCKET_COUNT  PREFIX_LENGTH=$PREFIX_LENGTH"
    echo "========================================"

    ########################################
    echo "  [B${BUCKET_COUNT}-X${PREFIX_LENGTH}] Running hashskiplist..."
    cd "$PDIR/hashskiplist"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=3 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  [B${BUCKET_COUNT}-X${PREFIX_LENGTH}] Running hashvector..."
    cd "$PDIR/hashvector"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=9 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  [B${BUCKET_COUNT}-X${PREFIX_LENGTH}] Running hashlinkedlist..."
    cd "$PDIR/hashlinkedlist"
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
echo "All vary-prefix-length-small experiments finished."


# shellcheck source=.env
source .env

HOSTNAME=$(hostname)

MESSAGE="vary-prefix-length-small Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" "${SLACK_WEBHOOK_URL}"
