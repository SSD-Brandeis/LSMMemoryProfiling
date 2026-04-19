#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=vector-get-timer-exp
ENTRY_SIZE=128
LAMBDA=0.0625
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

# Vector overhead is 15% of buffer capacity.
#   INSERTS = buffer_bytes * (100 - overhead_pct) / (entry_size * 100)
VECTOR_OVERHEAD_PCT=15
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - VECTOR_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

EMPTY_PQ=50000     # queries on non-existing keys
NONEMPTY_PQ=50000  # queries on existing keys

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=$KEY_LEN val=$VAL_LEN)"
echo "INSERTS          : $INSERTS  (fills 1 vector buffer at ${VECTOR_OVERHEAD_PCT}% overhead)"
echo "  WARMUP         : $(( INSERTS / 10 ))  (10% of inserts, group 1)"
echo "  MAIN           : $(( INSERTS - INSERTS / 10 ))  (90% of inserts, mixed with PQs, group 2)"
echo "EMPTY_PQ         : $EMPTY_PQ"
echo "NONEMPTY_PQ      : $NONEMPTY_PQ"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Generate workload: 10% inserts warmup, then 90% inserts mixed with PQs
########################################
INSERTS_WARMUP=$(( INSERTS / 10 ))
INSERTS_MAIN=$(( INSERTS - INSERTS_WARMUP ))

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
        "point_queries":       { "op_count": $NONEMPTY_PQ, "selection": {"uniform": {"min": 0, "max": 1}} }
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
mkdir -p "$BASE_DIR/vector-preallocated"
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


cd "$REPO_ROOT"
echo "Done."
echo "vector-get-timer experiment finished."


# shellcheck source=.env
source .env

HOSTNAME=$(hostname)

MESSAGE="vector-get-timer Experiment Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" "${SLACK_WEBHOOK_URL}"
