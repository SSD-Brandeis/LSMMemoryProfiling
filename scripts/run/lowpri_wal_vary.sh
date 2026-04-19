#!/bin/bash
set -e

bash ./scripts/rebuild.sh

TAG=vary_lowpri_wal
ENTRY_SIZE=32
LAMBDA=0.25

PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

# 1MB buffer → 256 pages
PAGES_PER_FILE=256

SIZE_RATIO=6
ROCKSDB_STATS=1
SHOW_PROGRESS=1

THRESHOLD_TO_CONVERT_TO_SKIPLIST=$((PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE))

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRIES_PER_PAGE : $ENTRIES_PER_PAGE"
echo "PAGES_PER_FILE   : $PAGES_PER_FILE"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"

BASE_DIR=".vstats/${TAG}"
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

# 4 Combinations: WAL (0/1) x LOW_PRI (0/1)
for WAL in 0 1; do
    for LOW_PRI in 0 1; do

        # Map WAL (0 or 1) to disableWAL logic
        if [ "$WAL" -eq 1 ]; then
            DISABLE_WAL=0
        else
            DISABLE_WAL=1
        fi

        COMBO_DIR="wal_${WAL}_lowpri_${LOW_PRI}"
        echo -e "\n========================================"
        echo "Running Combination: WAL=$WAL, LOW_PRI=$LOW_PRI"
        echo "========================================"

        mkdir -p "$COMBO_DIR"
        cd "$COMBO_DIR" || exit

        mkdir -p \
        vector-preallocated \
        skiplist \
        simpleskiplist \
        unsortedvector-preallocated \
        hashskiplist-H100000-X6 \
        hashvector-H100000-X6 \
        hashlinkedlist-H100000-X6

        # mkdir -p \
        # sortedvector-preallocated \
        # linkedlist \
        # vector-dynamic \
        # unsortedvector-dynamic \
        # sortedvector-dynamic \
        # hashskiplist-H1000-X2 \
        # hashvector-H1000-X2 \
        # hashlinkedlist-H1000-X2

        # ########################################
        echo "Running skiplist ... "
        cd skiplist
        ln -sf ../../workload.txt .
        ../../../../bin/working_version \
            --memtable_factory=1 \
            -E "$ENTRY_SIZE" \
            -B "$ENTRIES_PER_PAGE" \
            -P "$PAGES_PER_FILE" \
            -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" \
            --disableWAL "$DISABLE_WAL" \
            --stat "$ROCKSDB_STATS" \
            --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        mv db/LOG LOG
        rm -rf db workload.txt
        cd ..
        echo -e "\n"
        sleep 5

        echo "Running simpleskiplist ... "
        cd simpleskiplist
        ln -sf ../../workload.txt .
        ../../../../bin/working_version \
            --memtable_factory=8 \
            -E "$ENTRY_SIZE" \
            -B "$ENTRIES_PER_PAGE" \
            -P "$PAGES_PER_FILE" \
            -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" \
            --disableWAL "$DISABLE_WAL" \
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
        # ln -sf ../../workload.txt .
        # ../../../../bin/working_version \
        #     --memtable_factory=2 \
        #     -A 0 \
        #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        #     --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        # mv db/LOG LOG
        # rm -rf db workload.txt
        # cd ..
        # echo -e "\n"

        echo "Running vector-preallocated ... "
        cd vector-preallocated
        ln -sf ../../workload.txt .
        ../../../../bin/working_version \
            --memtable_factory=2 \
            -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        mv db/LOG LOG
        rm -rf db workload.txt
        cd ..
        echo -e "\n"
        sleep 5

        # ########################################
        # echo "Running unsortedvector-dynamic ... "
        # cd unsortedvector-dynamic
        # ln -sf ../../workload.txt .
        # ../../../../bin/working_version \
        #     --memtable_factory=5 \
        #     -A 0 \
        #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        #     --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        # mv db/LOG LOG
        # rm -rf db workload.txt
        # cd ..
        # echo -e "\n"

        echo "Running unsortedvector-preallocated ... "
        cd unsortedvector-preallocated
        ln -sf ../../workload.txt .
        ../../../../bin/working_version \
            --memtable_factory=5 \
            -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        mv db/LOG LOG
        rm -rf db workload.txt
        cd ..
        echo -e "\n"
        sleep 5

        # ########################################
        # echo "Running sortedvector-dynamic ... "
        # cd sortedvector-dynamic
        # ln -sf ../../workload.txt .
        # ../../../../bin/working_version \
        #     --memtable_factory=6 \
        #     -A 0 \
        #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        #     --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        # mv db/LOG LOG
        # rm -rf db workload.txt
        # cd ..
        # echo -e "\n"

        # echo "Running sortedvector-preallocated ... "
        # cd sortedvector-preallocated
        # ln -sf ../../workload.txt .
        # ../../../../bin/working_version \
        #     --memtable_factory=6 \
        #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        #     --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        # mv db/LOG LOG
        # rm -rf db workload.txt
        # cd ..
        # echo -e "\n"
        # sleep 5

        # ########################################
        # echo "Running linkedlist ... "
        # cd linkedlist
        # ln -sf ../../workload.txt .
        # ../../../../bin/working_version \
        #     --memtable_factory=7 \
        #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        #     --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        # mv db/LOG LOG
        # rm -rf db workload.txt
        # cd ..
        # echo -e "\n"
        # sleep 5

        # ########################################
        echo "Running hashskiplist-H100000-X6 ... "
        cd hashskiplist-H100000-X6
        ln -sf ../../workload.txt .
        ../../../../bin/working_version \
            --memtable_factory=3 \
            --bucket_count=100000 \
            --prefix_length=6 \
            -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        mv db/LOG LOG
        rm -rf db workload.txt
        cd ..
        echo -e "\n"
        sleep 5

        # echo "Running hashskiplist-H1000-X2 ... "
        # cd hashskiplist-H1000-X2
        # ln -sf ../../workload.txt .
        # ../../../../bin/working_version \
            # --memtable_factory=3 \
            # --bucket_count=1000 \
            # --prefix_length=2 \
            # -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            # --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        # mv db/LOG LOG
        # rm -rf db workload.txt
        # cd ..
        # echo -e "\n"

        # ########################################
        echo "Running hashvector-H100000-X6 ... "
        cd hashvector-H100000-X6
        ln -sf ../../workload.txt .
        ../../../../bin/working_version \
            --memtable_factory=9 \
            --bucket_count=100000 \
            --prefix_length=6 \
            -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        mv db/LOG LOG
        rm -rf db workload.txt
        cd ..
        echo -e "\n"
        sleep 5

        # echo "Running hashvector-H1000-X2 ... "
        # cd hashvector-H1000-X2
        # ln -sf ../../workload.txt .
        # ../../../../bin/working_version \
        #     --memtable_factory=9 \
        #     --bucket_count=1000 \
        #     --prefix_length=2 \
        #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        #     --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
        # mv db/LOG LOG
        # rm -rf db workload.txt
        # cd ..
        # echo -e "\n"

        # ########################################
        echo "Running hashlinkedlist-H100000-X6 ... "
        cd hashlinkedlist-H100000-X6
        ln -sf ../../workload.txt .
        ../../../../bin/working_version \
            --memtable_factory=4 \
            --bucket_count=100000 \
            --prefix_length=6 \
            -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
            --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
            --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
        mv db/LOG LOG
        rm -rf db workload.txt
        cd ..
        echo -e "\n"
        sleep 5

        # echo "Running hashlinkedlist-H1000-X2 ... "
        # cd hashlinkedlist-H1000-X2
        # ln -sf ../../workload.txt .
        # ../../../../bin/working_version \
        #     --memtable_factory=4 \
        #     --bucket_count=1000 \
        #     --prefix_length=2 \
        #     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
        #     --lowpri "$LOW_PRI" --disableWAL "$DISABLE_WAL" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
        #     --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
        # mv db/LOG LOG
        # rm -rf db workload.txt
        # cd ..
        # echo -e "\n"

        # Step back out of the combo directory
        cd ..
    done
done

cd ..
echo "Done."
echo "Experiments finished."