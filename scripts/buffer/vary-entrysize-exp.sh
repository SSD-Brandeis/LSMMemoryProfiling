#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=vary-entrysize-exp-32-64
LAMBDA=0.5
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

# Each entry size gets exactly this many flushes worth of inserts:
#   INSERTS = (buffer_size_bytes / ENTRY_SIZE) * NUM_FLUSHES
NUM_FLUSHES=10

# Entry sizes to sweep (bytes)
ENTRY_SIZES=(32 64) # 128 256 512 1024 2048) # 8 16 

# Pre-computed op counts (128MB / ENTRY_SIZE * NUM_FLUSHES):
#   8B→167772160  16B→83886080  32B→41943040   64B→20971520
#  128B→10485760 256B→ 5242880 512B→ 2621440 1024B→ 1310720
# 2048B→  655360

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"
GENSPECS="$REPO_ROOT/scripts/generate_specs.py"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR"

echo -e "\n========================================"
echo "TAG            : $TAG"
echo "LAMBDA         : $LAMBDA"
echo "PAGE_SIZE      : $PAGE_SIZE"
echo "PAGES_PER_FILE : $PAGES_PER_FILE"
echo "SIZE_RATIO     : $SIZE_RATIO"
echo "ENTRY_SIZES    : ${ENTRY_SIZES[*]}"
echo -e "========================================\n"


for ENTRY_SIZE in "${ENTRY_SIZES[@]}"; do
    INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * NUM_FLUSHES / ENTRY_SIZE ))
    ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
    THRESHOLD_TO_CONVERT_TO_SKIPLIST=$((PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE))

    ENTRY_DIR="$BASE_DIR/${ENTRY_SIZE}B"

    mkdir -p \
        "$ENTRY_DIR/vector-preallocated" \
        "$ENTRY_DIR/skiplist" \
        "$ENTRY_DIR/simpleskiplist" \
        "$ENTRY_DIR/unsortedvector-preallocated" \
        "$ENTRY_DIR/sortedvector-preallocated" \
        "$ENTRY_DIR/hashskiplist-H100000-X6" \
        "$ENTRY_DIR/hashvector-H100000-X6" \
        "$ENTRY_DIR/hashlinkedlist-H100000-X6"
        # "$ENTRY_DIR/linkedlist"

    echo "========================================"
    echo "Entry size: ${ENTRY_SIZE}B   inserts: $INSERTS   entries/page: $ENTRIES_PER_PAGE"
    echo "========================================"

    # Generate workload.specs.json for this entry size via generate_specs.py,
    # then use tectonic-cli to produce workload.txt
    python3 "$GENSPECS" \
        -I "$INSERTS" \
        -Q 0 \
        -E "$ENTRY_SIZE" \
        -L "$LAMBDA" \
        -o "$ENTRY_DIR/workload.specs.json"

    pushd "$ENTRY_DIR" > /dev/null
    "$TECTONIC_CLI" generate -w workload.specs.json
    popd > /dev/null


    ########################################
    echo "  [${ENTRY_SIZE}B] Running skiplist..."
    cd "$ENTRY_DIR/skiplist"
    cp ../workload.txt .
    "$BIN" \
        --memtable_factory=1 \
        -E "$ENTRY_SIZE" \
        -B "$ENTRIES_PER_PAGE" \
        -P "$PAGES_PER_FILE" \
        -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [${ENTRY_SIZE}B] Running simpleskiplist..."
    cd "$ENTRY_DIR/simpleskiplist"
    cp ../workload.txt .
    "$BIN" \
        --memtable_factory=8 \
        -E "$ENTRY_SIZE" \
        -B "$ENTRIES_PER_PAGE" \
        -P "$PAGES_PER_FILE" \
        -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd "$REPO_ROOT"
    echo -e "\n"
    sleep 5

    ########################################
    echo "  [${ENTRY_SIZE}B] Running vector-preallocated..."
    cd "$ENTRY_DIR/vector-preallocated"
    cp ../workload.txt .
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
    echo "  [${ENTRY_SIZE}B] Running unsortedvector-preallocated..."
    cd "$ENTRY_DIR/unsortedvector-preallocated"
    cp ../workload.txt .
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
    echo "  [${ENTRY_SIZE}B] Running sortedvector-preallocated..."
    cd "$ENTRY_DIR/sortedvector-preallocated"
    cp ../workload.txt .
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
    # echo "  [${ENTRY_SIZE}B] Running linkedlist..."
    # cd "$ENTRY_DIR/linkedlist"
    # cp ../workload.txt .
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
    echo "  [${ENTRY_SIZE}B] Running hashskiplist-H100000-X6..."
    cd "$ENTRY_DIR/hashskiplist-H100000-X6"
    cp ../workload.txt .
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
    echo "  [${ENTRY_SIZE}B] Running hashvector-H100000-X6..."
    cd "$ENTRY_DIR/hashvector-H100000-X6"
    cp ../workload.txt .
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
    echo "  [${ENTRY_SIZE}B] Running hashlinkedlist-H100000-X6..."
    cd "$ENTRY_DIR/hashlinkedlist-H100000-X6"
    cp ../workload.txt .
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

    echo "  Done with ${ENTRY_SIZE}B"
    echo ""
done

cd "$REPO_ROOT"
echo "Done."
echo "All vary-entrysize experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="vary-entrysize Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
