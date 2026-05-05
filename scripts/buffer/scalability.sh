#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=scalability-exp
ENTRY_SIZE=32
LAMBDA=0.25            # key = 8B, val = 24B
PAGE_SIZE=4096
BUFFER_SIZE_MB=1       # fixed buffer size
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

BUCKET_COUNT=100000
PREFIX_LENGTH=6

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 24

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))
THRESHOLD_TO_CONVERT_TO_SKIPLIST=$(( PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE ))

# Scale factors: label, numerator, denominator (scale = num/den)
# Scales: 1/4, 1/2, 1, 2, 4, 8  relative to base workload (100M inserts, 10K PQs, 1K RQs)
SCALE_ENTRIES=("4x:4:1" ) # "1x:1:1" "2x:2:1" "4x:4:1" "8x:8:1")

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "LOW_PRI          : $LOW_PRI  (fixed)"
echo "BUCKET_COUNT     : $BUCKET_COUNT   PREFIX_LENGTH: $PREFIX_LENGTH"
echo "SCALE_FACTORS    : 4x"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Helper: run all memtables for a given output dir
# Usage: run_all_memtables <workload_src> <outdir>
########################################
run_all_memtables() {
    local WORKLOAD_SRC="$1"
    local OUTDIR="$2"

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
        --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
    mv db/LOG LOG ; rm -rf db workload.txt ; cd "$REPO_ROOT" ; sleep 5
}


########################################
# Sweep scale factors
########################################
for SCALE_ENTRY in "${SCALE_ENTRIES[@]}"; do
    SCALE_LABEL=$(echo "$SCALE_ENTRY" | cut -d: -f1)
    SCALE_NUM=$(echo "$SCALE_ENTRY"   | cut -d: -f2)
    SCALE_DEN=$(echo "$SCALE_ENTRY"   | cut -d: -f3)

    SDIR="$BASE_DIR/SCALE${SCALE_LABEL}"
    mkdir -p "$SDIR"

    echo "========================================"
    echo "SCALE=${SCALE_LABEL}  (${SCALE_NUM}/${SCALE_DEN} of base workload)"
    echo "========================================"

    # Generate a scaled workload for this scale factor
    python3 - <<EOF
import json, math
scale = $SCALE_NUM / $SCALE_DEN
# Base: 80M bulk inserts | 10M inserts + 10K PQs | 10M inserts + 1K RQs
g1_ins = round(80_000_000 * scale)
g2_ins = round(10_000_000 * scale)
g2_pq  = round(10_000     * scale)
g3_ins = round(10_000_000 * scale)
g3_rq  = round(1_000      * scale)
spec = {
  "sections": [{
    "groups": [
      {
        "inserts": {
          "op_count": g1_ins,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        }
      },
      {
        "inserts": {
          "op_count": g2_ins,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        },
        "point_queries": {
          "op_count": g2_pq,
          "selection": {"uniform": {"min": 0, "max": 1}}
        }
      },
      {
        "inserts": {
          "op_count": g3_ins,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        },
        "range_queries": {
          "op_count": g3_rq,
          "selection": {"uniform": {"min": 0, "max": 1}},
          "selectivity": 0.0000001,
          "range_format": "StartEnd"
        }
      }
    ]
  }]
}
with open("$SDIR/workload.specs.json", "w") as f:
    json.dump(spec, f, indent=2)
print(f"  Wrote workload.specs.json  (ins={g1_ins+g2_ins+g3_ins:,}  PQ={g2_pq:,}  RQ={g3_rq:,})")
EOF

    pushd "$SDIR" > /dev/null
    "$TECTONIC_CLI" generate -w workload.specs.json
    popd > /dev/null

    run_all_memtables "$SDIR/workload.txt" "$SDIR"

    echo "  Done with SCALE${SCALE_LABEL}"
    echo ""
done


cd "$REPO_ROOT"
echo "Done."
echo "All scalability experiments finished."


# shellcheck source=.env
source .env

HOSTNAME=$(hostname)

MESSAGE="scalability Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" "${SLACK_WEBHOOK_URL}"
