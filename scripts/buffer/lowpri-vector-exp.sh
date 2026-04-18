#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=lowpri-vector-exp
ENTRY_SIZE=128
LAMBDA=0.0625          # key = 8B, val = 120B
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
ROCKSDB_STATS=1
SHOW_PROGRESS=1

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 120

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))

# Vector overhead is 21% of total buffer capacity:
#   INSERTS = buffer_bytes * (100 - overhead_pct) / (entry_size * 100)
VECTOR_OVERHEAD_PCT=21
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - VECTOR_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "INSERTS          : $INSERTS  (vector overhead=${VECTOR_OVERHEAD_PCT}%)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo "LOW_PRI sweep    : 0 (writes prioritized)  →  1 (compaction prioritized)"
echo -e "========================================\n"


########################################
# Generate one workload shared across all runs
########################################
python3 - <<EOF
import json
spec = {
  "sections": [{
    "groups": [{
      "inserts": {
        "op_count": $INSERTS,
        "key": {"uniform": {"len": $KEY_LEN}},
        "val": {"uniform": {"len": $VAL_LEN}}
      }
    }]
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
# Sweep LOW_PRI: 0 = writes prioritized, 1 = compaction prioritized
########################################
for LOW_PRI in 0; do # 1; do

    LPDIR="$BASE_DIR/lowpri-${LOW_PRI}"
    mkdir -p \
        "$LPDIR/vector-preallocated" \
        "$LPDIR/vector-dynamic"

    echo "========================================"
    echo "LOW_PRI=$LOW_PRI"
    echo "========================================"

    ########################################
    echo "  [lowpri-${LOW_PRI}] Running vector-preallocated..."
    cd "$LPDIR/vector-preallocated"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=2 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    # sleep 5

    # ########################################
    # echo "  [lowpri-${LOW_PRI}] Running vector-dynamic..."
    # cd "$LPDIR/vector-dynamic"
    # cp "$BASE_DIR/workload.txt" .
    # "$BIN" \
    #     --memtable_factory=2 \
    #     -A 0 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd "$REPO_ROOT"
    # echo -e "\n"
    # sleep 5

    echo "  Done with lowpri-${LOW_PRI}"
    echo ""
done


cd "$REPO_ROOT"
echo "Done."
echo "All lowpri vector experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="lowpri-vector Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
