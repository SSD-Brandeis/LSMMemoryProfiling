#!/bin/bash
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

bash ./scripts/rebuild.sh

TAG=diskbased-exp
ENTRY_SIZE=32
PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
PAGES_PER_FILE=256
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=0
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$((PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE))

LAMBDA=0.25
KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")   # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")  # 24

BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"
WORKLOAD_DIR="$REPO_ROOT/.vstats/${TAG}"
WORKLOAD_SRC="$WORKLOAD_DIR/workload.txt"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRIES_PER_PAGE : $ENTRIES_PER_PAGE"
echo "PAGES_PER_FILE   : $PAGES_PER_FILE"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo -e "========================================\n"

if [ ! -f "$WORKLOAD_SRC" ]; then
    echo "Generating workload..."
    mkdir -p "$WORKLOAD_DIR"
    # Three groups matching diskbased-exp convention:
    #   Group 1: 80M bulk inserts
    #   Group 2: 10M inserts + 10K point queries (interleaved)
    #   Group 3: 10M inserts + 1K range queries  (interleaved)
    python3 - <<EOF
import json
spec = {
  "sections": [{
    "groups": [
      {
        "inserts": {
          "op_count": 80000000,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        }
      },
      {
        "inserts": {
          "op_count": 10000000,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        },
        "point_queries": {
          "op_count": 10000,
          "selection": {"uniform": {"min": 0, "max": 1}}
        }
      },
      {
        "inserts": {
          "op_count": 10000000,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        },
        "range_queries": {
          "op_count": 1000,
          "selection": {"uniform": {"min": 0, "max": 1}},
          "selectivity": 0.0000001,
          "range_format": "StartEnd"
        }
      }
    ]
  }]
}
with open("$WORKLOAD_DIR/workload.specs.json", "w") as f:
    json.dump(spec, f, indent=2)
print("Wrote workload.specs.json  (80M + 10M + 10M inserts, 10K PQ, 1K RQ)")
EOF
    pushd "$WORKLOAD_DIR" > /dev/null
    "$TECTONIC_CLI" generate -w workload.specs.json -o workload.txt
    popd > /dev/null
    echo "Workload generated: $WORKLOAD_SRC"
fi

# $1 = absolute path where RocksDB db/ will be written (on the target device)
# remaining args = extra flags for the binary
# must be called from within the memtable result directory
run_one_memtable() {
    local DB_TARGET="$1"
    shift
    local EXTRA_ARGS="$*"

    cp "$WORKLOAD_SRC" workload.txt

    # Clean target dir on device; ROCKSDB_DB_PATH tells the binary where to write
    rm -rf "$DB_TARGET"
    mkdir -p "$DB_TARGET"

    ROCKSDB_DB_PATH="$DB_TARGET" $BIN $EXTRA_ARGS \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        > rocksdb_stats.log

    mv "$DB_TARGET/LOG" LOG 2>/dev/null || true
    rm -f workload.txt
    rm -rf "$DB_TARGET"          # clean up device storage
}

run_device() {
    local DEVICE_TAG="$1"
    local DB_MOUNT="$2"          # root of the target device mount

    local BASE_DIR="$REPO_ROOT/.vstats/${TAG}-${DEVICE_TAG}"
    local DB_BASE="${DB_MOUNT}/rocksdb_exp_${DEVICE_TAG}"

    echo -e "\n########################################"
    echo "DEVICE: ${DEVICE_TAG}  (db writes → ${DB_BASE})"
    echo "Results: ${BASE_DIR}"
    echo -e "########################################\n"

    mkdir -p "$BASE_DIR"
    mkdir -p "${BASE_DIR}/"{skiplist,simpleskiplist,vector-preallocated,unsortedvector-preallocated,sortedvector-preallocated,linkedlist,hashskiplist-H100000-X6,hashvector-H100000-X6,hashlinkedlist-H100000-X6}

    cd "${BASE_DIR}/skiplist"
    run_one_memtable "${DB_BASE}/skiplist" \
        --memtable_factory=1
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "${BASE_DIR}/simpleskiplist"
    run_one_memtable "${DB_BASE}/simpleskiplist" \
        --memtable_factory=8
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "${BASE_DIR}/vector-preallocated"
    run_one_memtable "${DB_BASE}/vector-preallocated" \
        --memtable_factory=2
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "${BASE_DIR}/unsortedvector-preallocated"
    run_one_memtable "${DB_BASE}/unsortedvector-preallocated" \
        --memtable_factory=5
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "${BASE_DIR}/sortedvector-preallocated"
    run_one_memtable "${DB_BASE}/sortedvector-preallocated" \
        --memtable_factory=6
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "${BASE_DIR}/linkedlist"
    run_one_memtable "${DB_BASE}/linkedlist" \
        --memtable_factory=7
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "${BASE_DIR}/hashskiplist-H100000-X6"
    run_one_memtable "${DB_BASE}/hashskiplist-H100000-X6" \
        --memtable_factory=3 --bucket_count=100000 --prefix_length=6
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "${BASE_DIR}/hashvector-H100000-X6"
    run_one_memtable "${DB_BASE}/hashvector-H100000-X6" \
        --memtable_factory=9 --bucket_count=100000 --prefix_length=6
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "${BASE_DIR}/hashlinkedlist-H100000-X6"
    run_one_memtable "${DB_BASE}/hashlinkedlist-H100000-X6" \
        --memtable_factory=4 --bucket_count=100000 --prefix_length=6 \
        --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST"
    cd "$BASE_DIR"; echo ""; sleep 5

    cd "$REPO_ROOT"
    echo "Done: ${DEVICE_TAG}"
}

# HDD: project lives on /dev/sdc3 (HDD), use a local sub-dir
run_device "hdd"  "$REPO_ROOT/.vstats"

# SSD: /dev/sde mounted at /mnt/ssd (btrfs)
run_device "ssd"  "/mnt/ssd"

# NVMe: /dev/nvme1n1 mounted at /mnt/nvme (ext4)
run_device "nvme" "/mnt/nvme"

echo -e "\nAll three device experiments finished."
echo "Results in:"
echo "  $REPO_ROOT/.vstats/${TAG}-hdd/"
echo "  $REPO_ROOT/.vstats/${TAG}-ssd/"
echo "  $REPO_ROOT/.vstats/${TAG}-nvme/"
