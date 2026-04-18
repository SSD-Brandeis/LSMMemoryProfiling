#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=snapshot-compare-exp
ENTRY_SIZE=128
LAMBDA=0.0625          # key = 8B, val = 120B
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 120

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))

# Overhead is measured as fraction of total buffer capacity:
#   INSERTS = buffer_bytes * (100 - overhead_pct) / (entry_size * 100)
# Using skiplist's overhead for both implementations so they share one workload.
# TODO: update once you confirm the exact skiplist fill count
SKIPLIST_OVERHEAD_PCT=23
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - SKIPLIST_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

POINT_QUERIES=10000    # issued after buffer is filled; snapshot_ns printed per PQ in vector

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR/vector-preallocated"
mkdir -p "$BASE_DIR/skiplist"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "INSERTS          : $INSERTS  (skiplist overhead=${SKIPLIST_OVERHEAD_PCT}%)"
echo "POINT_QUERIES    : $POINT_QUERIES  (after fill)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Workload: fill buffer with inserts (group 1), then 10K non-empty PQs (group 2).
# Separate groups → sequential execution; shared section keyset → PQs hit real keys.
# snapshot_ns is printed to stdout (→ rocksdb_stats.log) by VectorRep::Get
# on every PQ issued against a mutable vector.
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
echo "Running vector-preallocated..."
cd "$BASE_DIR/vector-preallocated"
cp "$BASE_DIR/workload.txt" .
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
echo "Running skiplist..."
cd "$BASE_DIR/skiplist"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=1 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG
rm -rf db workload.txt
cd "$REPO_ROOT"
echo -e "\n"


cd "$REPO_ROOT"
echo "Done."
echo "snapshot_ns lines → .vstats/${TAG}/vector-preallocated/rocksdb_stats.log"
echo "Snapshot compare experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="snapshot-compare Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
