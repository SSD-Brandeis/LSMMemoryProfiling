#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=vector-breakdown
ENTRY_SIZE=128
LAMBDA=0.0625
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

# Vector overhead is 21% of total buffer capacity:
#   INSERTS = buffer_bytes * (100 - overhead_pct) / (entry_size * 100)
VECTOR_OVERHEAD_PCT=15
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - VECTOR_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

PQ=5000        # queries on non-existing keys

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
echo "INSERTS          : $INSERTS  (fills 1 hashskiplist buffer at ${HASHSKIPLIST_OVERHEAD_PCT}% overhead)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Generate workload specs
########################################

INSERTS_WARMUP=$(( INSERTS / 80 ))
INSERTS_MAIN=$(( INSERTS - INSERTS_WARMUP ))

# Mixed: 80% inserts first (warmup so non-empty PQs have keys to hit),
# then 20% inserts interleaved with all queries in a second group.
# Groups share the keyset within the section, so PQs
# draw from keys inserted in group 1.
# python3 - <<EOF
# import json
# spec = {
#   "sections": [{
#     "groups": [
#       {
#         "inserts": {
#           "op_count": $INSERTS_WARMUP,
#           "key": {"uniform": {"len": $KEY_LEN}},
#           "val": {"uniform": {"len": $VAL_LEN}}
#         }
#       },
#       {
#         "point_queries":       { "op_count": $PQ, "selection": {"uniform": {"min": 0, "max": 1}} },
#       },
#       {
#         "inserts": {
#           "op_count": $INSERTS_MAIN,
#           "key": {"uniform": {"len": $KEY_LEN}},
#           "val": {"uniform": {"len": $VAL_LEN}}
#         },
#         "point_queries":       { "op_count": $PQ, "selection": {"uniform": {"min": 0, "max": 1}} },
#       }
#     ]
#   }]
# }
# with open("$BASE_DIR/workload-mixed.specs.json", "w") as f:
#     json.dump(spec, f, indent=2)
# print("Wrote workload-mixed.specs.json")
# EOF

# pushd "$BASE_DIR" > /dev/null
# echo "Generating mixed workload..."
# "$TECTONIC_CLI" generate -w workload-mixed.specs.json -o workload-mixed.txt
# popd > /dev/null


########################################
# Helper: run all memtables for a given workload
# Usage: run_all_memtables <workload_file> <output_subdir>
########################################
run_all_memtables() {
    local WORKLOAD_SRC="$1"
    local SUBDIR="$BASE_DIR/$2"

    mkdir -p \
        "$SUBDIR/vector-preallocated" \
        "$SUBDIR/vector-optimized"

    # ########################################
    # echo "  [$2] Running vector-preallocated..."
    # cd "$SUBDIR/vector-preallocated"
    # cp "$WORKLOAD_SRC" workload.txt
    # "$BIN" \
    #     --memtable_factory=2 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd "$REPO_ROOT"
    # echo -e "\n"
    # sleep 5

    ########################################
    echo "  [$2] Running vector-optimized..."
    cd "$SUBDIR/vector-optimized"
    cp "$WORKLOAD_SRC" workload.txt
    "$BIN" \
        --memtable_factory=11 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

}

########################################
echo "========================================"
echo "Running SEQUENTIAL workload (all ops interleaved)"
echo "========================================"
run_all_memtables "$BASE_DIR/workload-sequential.txt" "mixed"


cd "$REPO_ROOT"
echo "Done."
echo "All in-memory mixed-ops experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="vector optimized Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
