#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=vary-buffersize-overhead-exp
ENTRY_SIZE=128
LAMBDA=0.0625
PAGE_SIZE=4096
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

# Each buffer size gets exactly this many flushes worth of inserts:
#   INSERTS = (buffer_size_bytes / ENTRY_SIZE) * NUM_FLUSHES
NUM_FLUSHES=10

# Buffer sizes to sweep (KB): covers 2^6 to 2^20
BUFFER_SIZES_KB=(64 256 1024 4096 16384 65536 262144 1048576)

# Pre-computed op counts (BUFFER_SIZE_KB * 1024 / ENTRY_SIZE * NUM_FLUSHES):
#   64KB  →      5120   256KB →     20480   1024KB →     81920
#   4096KB→    327680  16384KB →  1310720  65536KB →  5242880
#   262144KB→20971520  1048576KB→83886080

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"
GENSPECS="$REPO_ROOT/scripts/generate_specs.py"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG             : $TAG"
echo "ENTRY_SIZE      : $ENTRY_SIZE B"
echo "NUM_FLUSHES     : $NUM_FLUSHES"
echo "LAMBDA          : $LAMBDA"
echo "SIZE_RATIO      : $SIZE_RATIO"
echo "BUFFER_SIZES_KB : ${BUFFER_SIZES_KB[*]}"
echo -e "========================================\n"


for BUFFER_SIZE_KB in "${BUFFER_SIZES_KB[@]}"; do
    PAGES_PER_FILE=$(( BUFFER_SIZE_KB * 1024 / PAGE_SIZE ))
    THRESHOLD_TO_CONVERT_TO_SKIPLIST=$((PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE))

    # entries that fit in one buffer × desired flush count
    INSERTS=$(( BUFFER_SIZE_KB * 1024 * NUM_FLUSHES / ENTRY_SIZE ))

    # Generate a fresh workload for this buffer size (op count differs per buffer)
    python3 "$GENSPECS" \
        -I "$INSERTS" \
        -Q 0 \
        -E "$ENTRY_SIZE" \
        -L "$LAMBDA" \
        -o "$BASE_DIR/workload.specs.json"

    pushd "$BASE_DIR" > /dev/null
    "$TECTONIC_CLI" generate -w workload.specs.json
    popd > /dev/null

    BUF_DIR="$BASE_DIR/${BUFFER_SIZE_KB}KB"

    mkdir -p \
        "$BUF_DIR/vector-preallocated" \
        "$BUF_DIR/skiplist" \
        "$BUF_DIR/simpleskiplist" \
        "$BUF_DIR/unsortedvector-preallocated" \
        "$BUF_DIR/sortedvector-preallocated" \
        "$BUF_DIR/hashskiplist-H100000-X6" \
        "$BUF_DIR/hashvector-H100000-X6" \
        "$BUF_DIR/hashlinkedlist-H100000-X6" \
        "$BUF_DIR/linkedlist"

    echo "========================================"
    echo "Buffer: ${BUFFER_SIZE_KB}KB   PAGES_PER_FILE: $PAGES_PER_FILE"
    echo "========================================"


    ########################################
    echo "  [${BUFFER_SIZE_KB}KB] Running skiplist..."
    cd "$BUF_DIR/skiplist"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=1 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [${BUFFER_SIZE_KB}KB] Running simpleskiplist..."
    cd "$BUF_DIR/simpleskiplist"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=8 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [${BUFFER_SIZE_KB}KB] Running vector-preallocated..."
    cd "$BUF_DIR/vector-preallocated"
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
    echo "  [${BUFFER_SIZE_KB}KB] Running unsortedvector-preallocated..."
    cd "$BUF_DIR/unsortedvector-preallocated"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=5 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [${BUFFER_SIZE_KB}KB] Running sortedvector-preallocated..."
    cd "$BUF_DIR/sortedvector-preallocated"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    # ########################################
    # echo "  [${BUFFER_SIZE_KB}KB] Running linkedlist..."
    # cd "$BUF_DIR/linkedlist"
    # cp "$BASE_DIR/workload.txt" .
    # "$BIN" \
    #     --memtable_factory=7 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd "$REPO_ROOT"
    # echo -e "\n"
    # sleep 5

    ########################################
    echo "  [${BUFFER_SIZE_KB}KB] Running hashskiplist-H100000-X6..."
    cd "$BUF_DIR/hashskiplist-H100000-X6"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=3 \
        --bucket_count=100000 \
        --prefix_length=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [${BUFFER_SIZE_KB}KB] Running hashvector-H100000-X6..."
    cd "$BUF_DIR/hashvector-H100000-X6"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=9 \
        --bucket_count=100000 \
        --prefix_length=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [${BUFFER_SIZE_KB}KB] Running hashlinkedlist-H100000-X6..."
    cd "$BUF_DIR/hashlinkedlist-H100000-X6"
    cp "$BASE_DIR/workload.txt" .
    "$BIN" \
        --memtable_factory=4 \
        --bucket_count=100000 \
        --prefix_length=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    echo "  Done with ${BUFFER_SIZE_KB}KB"
    echo ""
done

cd "$REPO_ROOT"
echo "Done."
echo "All vary-buffersize experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="vary-buffersize Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
