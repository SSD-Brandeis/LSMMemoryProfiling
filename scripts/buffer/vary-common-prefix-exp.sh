#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=vary-common-prefix-exp
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
BUCKET_COUNT=100000    # fixed across this sweep

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 120

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE ))

# Hash skiplist overhead is 29% of total buffer capacity (observed).
HASHSKIPLIST_OVERHEAD_PCT=29
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - HASHSKIPLIST_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

RQ_COUNT=1000
RQ_SELECTIVITY=0.1

# Common prefix lengths to sweep; PREFIX_LENGTH and BUCKET_COUNT are fixed.
# common_prefix_len=0  → no prefix synthesis, total_order_seek=true (baseline)
# common_prefix_len=6  → fully prefix-bounded (==PREFIX_LENGTH), total_order_seek=false
COMMON_PREFIX_LENS=(0 1 2 3 4 5 6 7 8)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# This experiment uses run_workload_prefix which synthesises end_key with a
# controlled common prefix.  Build target: working_version.
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "INSERTS          : $INSERTS  (hashskiplist overhead=${HASHSKIPLIST_OVERHEAD_PCT}%)"
echo "RQ               : $RQ_COUNT  (selectivity=$RQ_SELECTIVITY, start_end format)"
echo "PREFIX_LENGTH    : $PREFIX_LENGTH  (fixed)"
echo "BUCKET_COUNT     : $BUCKET_COUNT  (fixed)"
echo "COMMON_PREFIX    : ${COMMON_PREFIX_LENS[*]}"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Generate workload once — two sequential groups:
#   Group 1: all inserts
#   Group 2: 1K RQs with selectivity 0.1 (draws start/end keys from inserted set)
# The binary synthesises the end key at runtime based on common_prefix_len,
# so a single workload.txt is shared across all CPL values.
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
        "range_queries": {
          "op_count": $RQ_COUNT,
          "selectivity": $RQ_SELECTIVITY,
          "range_format": "StartEnd",
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
# Sweep common prefix lengths
########################################
for COMMON_PREFIX_LEN in "${COMMON_PREFIX_LENS[@]}"; do

    CDIR="$BASE_DIR/CPL${COMMON_PREFIX_LEN}-X${PREFIX_LENGTH}-B${BUCKET_COUNT}"

    mkdir -p \
        "$CDIR/hashskiplist" \
        "$CDIR/hashvector" \
        "$CDIR/hashlinkedlist"

    echo "========================================"
    echo "COMMON_PREFIX_LEN=$COMMON_PREFIX_LEN  PREFIX_LENGTH=$PREFIX_LENGTH  BUCKET_COUNT=$BUCKET_COUNT"
    echo "========================================"

    ########################################
    echo "  [CPL${COMMON_PREFIX_LEN}] Running hashskiplist..."
    cd "$CDIR/hashskiplist"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=3 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        --common_prefix_len="$COMMON_PREFIX_LEN" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  [CPL${COMMON_PREFIX_LEN}] Running hashvector..."
    cd "$CDIR/hashvector"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=9 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        --common_prefix_len="$COMMON_PREFIX_LEN" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  [CPL${COMMON_PREFIX_LEN}] Running hashlinkedlist..."
    cd "$CDIR/hashlinkedlist"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=4 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        --common_prefix_len="$COMMON_PREFIX_LEN" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    echo "  Done with CPL${COMMON_PREFIX_LEN}"
    echo ""
done


cd "$REPO_ROOT"
echo "Done."
echo "All vary-common-prefix experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="vary-common-prefix Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
