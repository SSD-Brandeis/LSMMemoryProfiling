#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=testing2-diskbased-1mb-buffer-vary-t
ENTRY_SIZE=32
LAMBDA=0.25

PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

# 1MB buffer → 256 pages
PAGES_PER_FILE=256

# SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

THRESHOLD_TO_CONVERT_TO_SKIPLIST=$((PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE))

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRIES_PER_PAGE : $ENTRIES_PER_PAGE"
echo "PAGES_PER_FILE   : $PAGES_PER_FILE"
echo -e "========================================\n"


BASE_DIR=".results/${TAG}"
mkdir -p "$BASE_DIR"
cd "$BASE_DIR" || exit


WORKLOAD_FILE="workload.txt"
SPECS_FILE="workload.specs.json"

if [ ! -f "$WORKLOAD_FILE" ]; then
    if [ -f "$SPECS_FILE" ]; then
        echo "Generating workload..."
        ../../bin/tectonic-cli generate -w "$SPECS_FILE" -o "workload.txt"
    else
        echo "Error: workload not found"
        exit 1
    fi
fi

# Define the ratios for the experiment
SIZE_RATIOS=(2 4 6 8 10)

for SIZE_RATIO in "${SIZE_RATIOS[@]}"; do
    echo "########################################"
    echo "STARTING EXPERIMENT WITH SIZE_RATIO=$SIZE_RATIO"
    echo "########################################"

    # Fixed mkdir: separated active directories from commented ones to prevent syntax errors
    mkdir -p \
    vector-preallocated-T${SIZE_RATIO} \
    skiplist-T${SIZE_RATIO} \
    simpleskiplist-T${SIZE_RATIO} \
    unsortedvector-preallocated-T${SIZE_RATIO} \
    hashskiplist-H100000-X6-T${SIZE_RATIO} \
    hashvector-H100000-X6-T${SIZE_RATIO} \
    hashlinkedlist-H100000-X6-T${SIZE_RATIO} \
    sortedvector-preallocated-T${SIZE_RATIO}
    # linkedlist
    # vector-dynamic \
    # unsortedvector-dynamic \
    # sortedvector-dynamic \
    # hashskiplist-H1000-X2 \
    # hashvector-H1000-X2 \
    # hashlinkedlist-H1000-X2 \

    # ########################################
    echo "Running skiplist (T=$SIZE_RATIO) ... "
    cd skiplist-T${SIZE_RATIO}
    cp ../workload.txt .
    ../../../bin/working_version \
        --memtable_factory=1 \
        -E "$ENTRY_SIZE" \
        -B "$ENTRIES_PER_PAGE" \
        -P "$PAGES_PER_FILE" \
        -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" \
        --stat "$ROCKSDB_STATS" \
        --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ..
    echo -e "\n"
    sleep 5


    echo "Running simpleskiplist (T=$SIZE_RATIO) ... "
    cd simpleskiplist-T${SIZE_RATIO}
    cp ../workload.txt .
    ../../../bin/working_version \
        --memtable_factory=8 \
        -E "$ENTRY_SIZE" \
        -B "$ENTRIES_PER_PAGE" \
        -P "$PAGES_PER_FILE" \
        -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" \
        --stat "$ROCKSDB_STATS" \
        --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ..
    echo -e "\n"
    sleep 5


    # ########################################
    # echo "Running vector-dynamic ... "
    # cd vector-dynamic
    # cp ../workload.txt .
    # ../../../bin/working_version \
    #     --memtable_factory=2 \
    #     -A 0 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd ..
    # echo -e "\n"

    echo "Running vector-preallocated (T=$SIZE_RATIO) ... "
    cd vector-preallocated-T${SIZE_RATIO}
    cp ../workload.txt .
    ../../../bin/working_version \
        --memtable_factory=2 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ..
    echo -e "\n"
    sleep 5

    # ########################################
    # echo "Running unsortedvector-dynamic ... "
    # cd unsortedvector-dynamic
    # cp ../workload.txt .
    # ../../../bin/working_version \
    #     --memtable_factory=5 \
    #     -A 0 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd ..
    # echo -e "\n"

    echo "Running unsortedvector-preallocated (T=$SIZE_RATIO) ... "
    cd unsortedvector-preallocated-T${SIZE_RATIO}
    cp ../workload.txt .
    ../../../bin/working_version \
        --memtable_factory=5 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ..
    echo -e "\n"
    sleep 5

    # ########################################
    # echo "Running sortedvector-dynamic ... "
    # cd sortedvector-dynamic
    # cp ../workload.txt .
    # ../../../bin/working_version \
    #     --memtable_factory=6 \
    #     -A 0 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd ..
    # echo -e "\n"

    echo "Running sortedvector-preallocated (T=$SIZE_RATIO) ... "
    cd sortedvector-preallocated-T${SIZE_RATIO}
    cp ../workload.txt .
    ../../../bin/working_version \
        --memtable_factory=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ..
    echo -e "\n"
    sleep 5

    # ########################################
    # echo "Running linkedlist ... "
    # cd linkedlist
    # cp ../workload.txt .
    # ../../../bin/working_version \
    #     --memtable_factory=7 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd ..
    # echo -e "\n"
    # sleep 5

    # ########################################
    echo "Running hashskiplist-H100000-X6 (T=$SIZE_RATIO) ... "
    cd hashskiplist-H100000-X6-T${SIZE_RATIO}
    cp ../workload.txt .
    ../../../bin/working_version \
        --memtable_factory=3 \
        --bucket_count=100000 \
        --prefix_length=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ..
    echo -e "\n"
    sleep 5

    # echo "Running hashskiplist-H1000-X2 ... "
    # cd hashskiplist-H1000-X2
    # cp ../workload.txt .
    # ../../../bin/working_version \
    #     --memtable_factory=3 \
    #     --bucket_count=1000 \
    #     --prefix_length=2 \
    #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    #     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    # mv db/LOG LOG
    # rm -rf db workload.txt
    # cd ..
    # echo -e "\n"

    # ########################################
    echo "Running hashvector-H100000-X6 (T=$SIZE_RATIO) ... "
    cd hashvector-H100000-X6-T${SIZE_RATIO}
    cp ../workload.txt .
    ../../../bin/working_version \
        --memtable_factory=9 \
        --bucket_count=100000 \
        --prefix_length=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ..
    echo -e "\n"
    sleep 5




    # ########################################
    echo "Running hashlinkedlist-H100000-X6 (T=$SIZE_RATIO) ... "
    cd hashlinkedlist-H100000-X6-T${SIZE_RATIO}
    cp ../workload.txt .
    ../../../bin/working_version \
        --memtable_factory=4 \
        --bucket_count=100000 \
        --prefix_length=6 \
        -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
    mv db/LOG LOG
    rm -rf db workload.txt
    cd ..
    echo -e "\n"
    sleep 5
done


# ########################################
cd ../..
echo "Done."
echo "Experiments finished."