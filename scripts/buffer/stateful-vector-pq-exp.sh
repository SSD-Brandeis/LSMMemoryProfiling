#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=stateful-vector-pq-exp
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

VECTOR_OVERHEAD_PCT=15
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - VECTOR_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

# YCSB B-style run phase: 95% point queries, 5% updates.
TOTAL_RUN_OPS=10000
POINT_QUERIES=$(( TOTAL_RUN_OPS * 95 / 100 ))   # 9500
UPDATES=$(( TOTAL_RUN_OPS - POINT_QUERIES ))     # 500

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR/vector-preallocated"
mkdir -p "$BASE_DIR/statefulvector-preallocated"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "INSERTS          : $INSERTS  (vector overhead=${VECTOR_OVERHEAD_PCT}%)"
echo "RUN PHASE        : ${POINT_QUERIES} PQs + ${UPDATES} updates (YCSB B 95/5)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Workload: fill buffer with inserts (group 1), then YCSB-B run phase (group 2):
#   95% point queries (uniform selection) + 5% updates (uniform selection).
# Separate groups → sequential execution; shared section keyset → ops hit real keys.
# Updates go through Insert() in the memtable, invalidating the stateful snapshot.
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
        },
        "updates": {
          "op_count": $UPDATES,
          "val": {"uniform": {"len": $VAL_LEN}},
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
"$TECTONIC_CLI" generate -w workload.specs.json -o workload.txt
popd > /dev/null

########################################
echo "Running vector-preallocated (factory=2, copy-per-PQ)..."
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
echo "Running statefulvector-preallocated (factory=11, cached snapshot)..."
cd "$BASE_DIR/statefulvector-preallocated"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=11 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG
rm -rf db workload.txt
cd "$REPO_ROOT"
echo -e "\n"


cd "$REPO_ROOT"
echo "Done."
echo "Results in .vstats/${TAG}/"
echo "  vector-preallocated/rocksdb_stats.log        — copy-per-PQ"
echo "  statefulvector-preallocated/rocksdb_stats.log — cached snapshot"
echo "Stateful vector PQ experiment finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="stateful-vector-pq Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
