#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=vary-buffersize-diskbased-exp
ENTRY_SIZE=32
LAMBDA=0.25            # key = 8B, val = 24B
PAGE_SIZE=4096
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

BUCKET_COUNT=100000
PREFIX_LENGTH=6

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 24

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))

# Buffer sizes to sweep in MB
BUFFER_SIZES_MB=(1 2 4 8 16 32 64 128)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "LOW_PRI          : $LOW_PRI  (fixed)"
echo "BUCKET_COUNT     : $BUCKET_COUNT   PREFIX_LENGTH: $PREFIX_LENGTH"
echo "BUFFER_SIZES_MB  : ${BUFFER_SIZES_MB[*]}"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Generate workload once — three sequential groups:
#   Group 1: 80M inserts (bulk load)
#   Group 2: 10M inserts + 10K non-empty PQs (interleaved)
#   Group 3: 10M inserts + 1K RQs selectivity=0.0000001 (interleaved)
########################################
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
with open("$BASE_DIR/workload.specs.json", "w") as f:
    json.dump(spec, f, indent=2)
print("Wrote workload.specs.json")
EOF

pushd "$BASE_DIR" > /dev/null
"$TECTONIC_CLI" generate -w workload.specs.json
popd > /dev/null


########################################
# Helper: run all memtables for a given output dir
# Usage: run_all_memtables <workload_src> <outdir> <pages_per_file> <threshold>
########################################
run_all_memtables() {
    local WORKLOAD_SRC="$1"
    local OUTDIR="$2"
    local PAGES_PER_FILE="$3"
    local THRESHOLD="$4"

    mkdir -p \
        "$OUTDIR/skiplist" \
        "$OUTDIR/simpleskiplist" \
        "$OUTDIR/vector-preallocated" \
        "$OUTDIR/unsortedvector-preallocated" \
        "$OUTDIR/sortedvector-preallocated" \
        "$OUTDIR/hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}" \
        "$OUTDIR/hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}" \
        "$OUTDIR/hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"

    ########################################
    echo "  Running skiplist..."
    cd "$OUTDIR/skiplist"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=1 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running simpleskiplist..."
    cd "$OUTDIR/simpleskiplist"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=8 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running vector-preallocated..."
    cd "$OUTDIR/vector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=2 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running unsortedvector-preallocated..."
    cd "$OUTDIR/unsortedvector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=5 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running sortedvector-preallocated..."
    cd "$OUTDIR/sortedvector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$OUTDIR/hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=3 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$OUTDIR/hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=9 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$OUTDIR/hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=4 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --threshold_use_skiplist "$THRESHOLD" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5
}


########################################
# Sweep buffer sizes
########################################
for BUFFER_SIZE_MB in "${BUFFER_SIZES_MB[@]}"; do

    PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))
    THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE ))

    BDIR="$BASE_DIR/BUF${BUFFER_SIZE_MB}MB"
    mkdir -p "$BDIR"

    echo "========================================"
    echo "BUFFER_SIZE=${BUFFER_SIZE_MB}MB  PAGES_PER_FILE=$PAGES_PER_FILE"
    echo "========================================"

    run_all_memtables \
        "$BASE_DIR/workload.txt" \
        "$BDIR" \
        "$PAGES_PER_FILE" \
        "$THRESHOLD_TO_CONVERT_TO_SKIPLIST"

    echo "  Done with BUF${BUFFER_SIZE_MB}MB"
    echo ""
done


cd "$REPO_ROOT"
echo "Done."
echo "All vary-buffersize-diskbased experiments finished."


# shellcheck source=.env
source .env

HOSTNAME=$(hostname)

MESSAGE="vary-buffersize-diskbased Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" "${SLACK_WEBHOOK_URL}"
