
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source scripts/buffer/lib_run_all_memtables.sh

TAG="io-cpu-exp2-readmostly-inmemory"

ENTRY_SIZE=128
KEY_LEN=8
VAL_LEN=$((ENTRY_SIZE - KEY_LEN))

PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$((BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE))
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1
BUCKET_COUNT=100000
PREFIX_LENGTH=6
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$((PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE))


HASHSKIPLIST_OVERHEAD_PCT=29
INSERTS_CAP=$((BUFFER_SIZE_MB * 1024 * 1024 * (100 - HASHSKIPLIST_OVERHEAD_PCT) / (ENTRY_SIZE * 100)))

INSERTS_PRELOAD=300000
INSERTS_MAIN=500
PQ_MAIN=10000
TOTAL_INSERTED=$((INSERTS_PRELOAD + INSERTS_MAIN))

if [ "$TOTAL_INSERTED" -gt "$INSERTS_CAP" ]; then
    echo "ERROR: TOTAL_INSERTED ($TOTAL_INSERTED) exceeds INSERTS_CAP ($INSERTS_CAP)." \
         "Increase BUFFER_SIZE_MB or lower INSERTS_PRELOAD/INSERTS_MAIN." >&2
    exit 1
fi

BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"
BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : ${ENTRY_SIZE}B (key=$KEY_LEN val=$VAL_LEN)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB (PAGES_PER_FILE=$PAGES_PER_FILE), INSERTS_CAP=$INSERTS_CAP"
echo "INSERTS_PRELOAD  : $INSERTS_PRELOAD"
echo "MAIN             : insert=$INSERTS_MAIN, PQ=$PQ_MAIN (uniform)"
echo "TOTAL_INSERTED   : $TOTAL_INSERTED  (must stay <= INSERTS_CAP so nothing ever flushes)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


python3 - <<EOF
import json
spec = {
  "sections": [{
    "groups": [
      {
        "name": "Preload",
        "inserts": {
          "op_count": $INSERTS_PRELOAD,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        }
      },
      {
        "name": "Main",
        "inserts": {
          "op_count": $INSERTS_MAIN,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        },
        "point_queries": {
          "op_count": $PQ_MAIN,
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

if [ ! -f "$BASE_DIR/workload.txt" ]; then
    echo "Generating workload.txt..."
    "$TECTONIC_CLI" generate -w "$BASE_DIR/workload.specs.json" -o "$BASE_DIR/workload.txt"
else
    echo "workload.txt already exists, reusing it."
fi

PHASE_INFO=$(python3 "$REPO_ROOT/scripts/revision/compute_phase_boundaries.py" "$BASE_DIR/workload.specs.json")
PHASE_BOUNDARIES=$(sed -n '1p' <<< "$PHASE_INFO")
PHASE_NAMES=$(sed -n '2p' <<< "$PHASE_INFO")
echo "Phase boundaries : $PHASE_BOUNDARIES"
echo "Phase names      : $PHASE_NAMES"

run_all_memtables "$BASE_DIR/workload.txt" "runs" "$PHASE_BOUNDARIES" "$PHASE_NAMES"

cd "$REPO_ROOT"
echo "EXP2 finished. Per-phase stats: grep -A20 'Phase: ' \$BASE_DIR/runs/<memtable>/workload.log"
