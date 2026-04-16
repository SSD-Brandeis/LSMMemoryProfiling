#!/bin/bash
set -e

bash ./scripts/rebuild.sh

TAG=multiphase
ENTRY_SIZE=128
LAMBDA=0.0625

TAG=multiphase-diskbased-1mb-buffer-t6
ENTRY_SIZE=32
LAMBDA=0.25

PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
PAGES_PER_FILE=32768
SIZE_RATIO=6

LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

THRESHOLD_TO_CONVERT_TO_SKIPLIST=$INSERTS

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE"
echo "LAMBDA           : $LAMBDA"
echo "INSERTS          : $INSERTS"
echo "POINT_QUERIES    : $POINT_QUERIES"
echo "RANGE_QUERIES    : $RANGE_QUERIES"
echo "SELECTIVITY      : $SELECTIVITY"
echo "ENTRIES_PER_PAGE : $ENTRIES_PER_PAGE"
echo "PAGES_PER_FILE   : $PAGES_PER_FILE"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"

EXP_DIR="experiments-${TAG}-I${INSERTS}-PQ${POINT_QUERIES}-RQ${RANGE_QUERIES}"

mkdir -p .vstats
cd .vstats || exit
mkdir -p "$EXP_DIR"
cd "$EXP_DIR" || exit

python3 ../../scripts/generate_specs.py \
    -I ${INSERTS} \
    -U ${UPDATES} \
    -Q ${POINT_QUERIES} \
    -D ${POINT_DELETES} \
    -S ${RANGE_QUERIES} \
    -Y ${SELECTIVITY} \
    -R ${RANGE_DELETES} \
    -y ${RANGE_DELETES_SEL} \
    -E ${ENTRY_SIZE} \
    -L ${LAMBDA}

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


mkdir -p \
vector-preallocated \
skiplist \
simpleskiplist \
unsortedvector-preallocated \
sortedvector-preallocated \
hashskiplist-H100000-X6 \
hashvector-H100000-X6 \
hashlinkedlist-H100000-X6 \
# linkedlist

# ########################################
echo "Running skiplist ... "
cd skiplist
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


echo "Running simpleskiplist ... "
cd simpleskiplist
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

echo "Running vector-preallocated ... "
cd vector-preallocated
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

echo "Running unsortedvector-preallocated ... "
cd unsortedvector-preallocated
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

echo "Running sortedvector-preallocated ... "
cd sortedvector-preallocated
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

# ########################################
echo "Running hashskiplist-H100000-X6 ... "
cd hashskiplist-H100000-X6
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
echo "Running hashvector-H100000-X6 ... "
cd hashvector-H100000-X6
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


# echo "Running hashvector-H1000-X2 ... "
# cd hashvector-H1000-X2
# cp ../workload.txt .
# ../../../bin/working_version \
#     --memtable_factory=9 \
#     --bucket_count=1000 \
#     --prefix_length=2 \
#     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
#     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
# mv db/LOG LOG
# rm -rf db workload.txt
# cd ..
# echo -e "\n"

# ########################################
echo "Running hashlinkedlist-H100000-X6 ... "
cd hashlinkedlist-H100000-X6
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

# echo "Running hashlinkedlist-H1000-X2 ... "
# cd hashlinkedlist-H1000-X2
# cp ../workload.txt .
# ../../../bin/working_version \
#     --memtable_factory=4 \
#     --bucket_count=1000 \
#     --prefix_length=2 \
#     -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
#     --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" \
#     --threshold_use_skiplist "$THRESHOLD_TO_CONVERT_TO_SKIPLIST" > rocksdb_stats.log
# mv db/LOG LOG
# rm -rf db workload.txt
# cd ..
# echo -e "\n"

# ########################################
cd ../..
echo "Done."


