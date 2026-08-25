
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
source scripts/buffer/lib_run_all_memtables.sh

TAG="io-cpu-exp3-update-delete-heavy"

ENTRY_SIZE=128
KEY_LEN=8
VAL_LEN=$((ENTRY_SIZE - KEY_LEN))

# Small on-disk buffer (~1MB) -> frequent flush/compaction, matches
# diskbased-exp.sh's geometry.
PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
BUFFER_SIZE_MB=1
PAGES_PER_FILE=$((BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE))
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1
BUCKET_COUNT=100000
PREFIX_LENGTH=6
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$((PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE))

INSERTS_PRELOAD=1000000

# Main phase: insert 40K / delete 30K / update 30K, interleaved.
INSERTS_MAIN=40000
DELETES_MAIN=30000
UPDATES_MAIN=30000

BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"
BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : ${ENTRY_SIZE}B (key=$KEY_LEN val=$VAL_LEN)"
echo "INSERTS_PRELOAD  : $INSERTS_PRELOAD"
echo "MAIN             : insert=$INSERTS_MAIN, delete=$DELETES_MAIN, update=$UPDATES_MAIN"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"

########################################
# Generate workload spec: one section, two named groups sharing one keyset.
#   Group 1: Preload -- bulk inserts, builds the on-disk dataset
#   Group 2: Main     -- insert / delete / update, interleaved
########################################
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
        "point_deletes": {
          "op_count": $DELETES_MAIN,
          "selection": {"uniform": {"min": 0, "max": 1}}
        },
        "updates": {
          "op_count": $UPDATES_MAIN,
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
echo "EXP3 finished. Per-phase stats: grep -A20 'Phase: ' \$BASE_DIR/runs/<memtable>/workload.log"
