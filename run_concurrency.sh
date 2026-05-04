#!/bin/bash
set -e

bash ./scripts/rebuild.sh

TAG=concurrency
ENTRY_SIZE=32
LAMBDA=0.25

INSERTS=1000000
UPDATES=0
POINT_QUERIES=0
POINT_DELETES=0
RANGE_QUERIES=0
SELECTIVITY=0
RANGE_DELETES=0
RANGE_DELETES_SEL=0

PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
PAGES_PER_FILE=256
# PAGES_PER_FILE=16384
SIZE_RATIO=6

LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

THRESHOLD_TO_CONVERT_TO_SKIPLIST=$INSERTS

# Define the thread counts to test
THREAD_COUNTS=(16)


echo -e "\n========================================"
echo "Starting Concurrency Experiment"
echo "TAG              : $TAG"
echo "INSERTS          : $INSERTS"
echo "POINT_QUERIES    : $POINT_QUERIES"
echo "RANGE_QUERIES    : $RANGE_QUERIES"
echo -e "========================================\n"

# Generate workload ONCE for all thread counts to ensure consistency
BASE_EXP_DIR="experiments-${TAG}-I${INSERTS}-PQ${POINT_QUERIES}-RQ${RANGE_QUERIES}"
mkdir -p .vstats
cd .vstats || exit
mkdir -p "$BASE_EXP_DIR"
cd "$BASE_EXP_DIR" || exit

echo "Generating shared workload..."
python3 ../../scripts/generate_specs.py \
    -I ${INSERTS} -U ${UPDATES} -Q ${POINT_QUERIES} \
    -D ${POINT_DELETES} -S ${RANGE_QUERIES} -Y ${SELECTIVITY} \
    -R ${RANGE_DELETES} -y ${RANGE_DELETES_SEL} \
    -E ${ENTRY_SIZE} -L ${LAMBDA}

../../bin/tectonic-cli generate -w workload.specs.json

# Loop through each thread count
for T in "${THREAD_COUNTS[@]}"; do
    echo -e "\n========================================"
    echo "Running Experiment with $T THREADS"
    echo -e "========================================\n"

    THREAD_DIR="threads_${T}"
    mkdir -p "$THREAD_DIR"
    
    # FIXED: Placed active directories first, commented ones last to prevent Bash \ errors
    mkdir -p \
        "$THREAD_DIR/skiplist" \
        "$THREAD_DIR/vector-preallocated" \
        "$THREAD_DIR/unsortedvector-preallocated"
        # "$THREAD_DIR/sortedvector-preallocated"
    #   "$THREAD_DIR/linkedlist"
    #   "$THREAD_DIR/simpleskiplist"

    ########################################
    echo "[$T Threads] Running skiplist ... "
    cd "$THREAD_DIR/skiplist"
    cp ../../workload.txt .
    ../../../../bin/working_version \
        --threads "$T" \
        --memtable_factory=1 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ../..
    echo -e "\n"

    ########################################
    # echo "[$T Threads] Running simpleskiplist ... "
    # cd "$THREAD_DIR/simpleskiplist"
    # cp ../../workload.txt .
    # ../../../../bin/working_version \
    #     --threads "$T" \
    #     --memtable_factory=8 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd ../..
    # echo -e "\n"

    ########################################
    echo "[$T Threads] Running vector-preallocated ... "
    cd "$THREAD_DIR/vector-preallocated"
    cp ../../workload.txt .
    ../../../../bin/working_version \
        --threads "$T" \
        --memtable_factory=2 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ../..
    echo -e "\n"

    ########################################
    echo "[$T Threads] Running unsortedvector-preallocated ... "
    cd "$THREAD_DIR/unsortedvector-preallocated"
    cp ../../workload.txt .
    ../../../../bin/working_version \
        --threads "$T" \
        --memtable_factory=5 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ../..
    echo -e "\n"

    ########################################
    # echo "[$T Threads] Running sortedvector-preallocated ... "
    # cd "$THREAD_DIR/sortedvector-preallocated"
    # cp ../../workload.txt .
    # ../../../../bin/working_version \
    #     --threads "$T" \
    #     --memtable_factory=6 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd ../..
    # echo -e "\n"

    ########################################
    # echo "[$T Threads] Running linkedlist ... "
    # cd "$THREAD_DIR/linkedlist"
    # cp ../../workload.txt .
    # ../../../../bin/working_version \
    #     --threads "$T" \
    #     --memtable_factory=7 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd ../..
    # echo -e "\n"

done

cd ../..
echo "All concurrency experiments done."