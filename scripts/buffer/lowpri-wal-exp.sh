#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=lowpri-wal-exp
ENTRY_SIZE=32
LAMBDA=0.25            # key = 8B, val = 24B
PAGE_SIZE=4096
BUFFER_SIZE_MB=1       # 1MB buffer → 256 pages (matches diskbased-exp)
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 256 pages
SIZE_RATIO=6
ROCKSDB_STATS=1
SHOW_PROGRESS=1

BUCKET_COUNT=100000
PREFIX_LENGTH=6

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 24

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE ))

# low_pri x WAL knob sweep: (LP=0,WAL=0) (LP=0,WAL=1) (LP=1,WAL=0) (LP=1,WAL=1)
LOW_PRI_VALUES=(0 1)
WAL_VALUES=(0 1)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "BUCKET_COUNT     : $BUCKET_COUNT   PREFIX_LENGTH: $PREFIX_LENGTH"
echo "KNOB SWEEP       : low_pri x WAL → 00 01 10 11"
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
# Usage: run_all_memtables <workload_src> <outdir> <low_pri> <wal>
########################################
run_all_memtables() {
    local WORKLOAD_SRC="$1"
    local OUTDIR="$2"
    local LP="$3"
    local WAL="$4"

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
        --lowpri "$LP" --wal "$WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running simpleskiplist..."
    cd "$OUTDIR/simpleskiplist"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=8 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LP" --wal "$WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running vector-preallocated..."
    cd "$OUTDIR/vector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=2 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LP" --wal "$WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running unsortedvector-preallocated..."
    cd "$OUTDIR/unsortedvector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=5 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LP" --wal "$WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running sortedvector-preallocated..."
    cd "$OUTDIR/sortedvector-preallocated"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LP" --wal "$WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$OUTDIR/hashskiplist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=3 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LP" --wal "$WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$OUTDIR/hashvector-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=9 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LP" --wal "$WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5

    ########################################
    echo "  Running hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}..."
    cd "$OUTDIR/hashlinkedlist-H${BUCKET_COUNT}-X${PREFIX_LENGTH}"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=4 \
        --bucket_count="$BUCKET_COUNT" --prefix_length="$PREFIX_LENGTH" \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LP" --wal "$WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5
}


########################################
# Sweep low_pri x WAL
########################################
for LP in "${LOW_PRI_VALUES[@]}"; do
    for WAL in "${WAL_VALUES[@]}"; do

        KNOB_DIR="$BASE_DIR/LP${LP}-WAL${WAL}"
        mkdir -p "$KNOB_DIR"

        echo "========================================"
        echo "LOW_PRI=$LP  WAL=$WAL"
        echo "========================================"

        run_all_memtables "$BASE_DIR/workload.txt" "$KNOB_DIR" "$LP" "$WAL"

        echo "  Done with LP${LP}-WAL${WAL}"
        echo ""
    done
done


cd "$REPO_ROOT"
echo "Done."
echo "All lowpri-wal experiments finished."


# shellcheck source=.env
source .env

HOSTNAME=$(hostname)

MESSAGE="lowpri-wal Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" "${SLACK_WEBHOOK_URL}"
