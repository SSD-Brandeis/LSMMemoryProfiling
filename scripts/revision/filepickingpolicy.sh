#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=filepickingpolicy-exp
ENTRY_SIZE=32
LAMBDA=0.25

PAGE_SIZE=4096
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

# 1MB buffer → 256 pages
PAGES_PER_FILE=256

SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

THRESHOLD_TO_CONVERT_TO_SKIPLIST=$((PAGE_SIZE * PAGES_PER_FILE / ENTRY_SIZE))

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRIES_PER_PAGE : $ENTRIES_PER_PAGE"
echo "PAGES_PER_FILE   : $PAGES_PER_FILE"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


REPO_ROOT="$(pwd)"
BIN="${REPO_ROOT}/bin/working_version"

# Same workload used by .vstats/diskbased-exp -- already generated, reused as-is.
SOURCE_WORKLOAD="${REPO_ROOT}/.vstats/diskbased-exp/workload.txt"
if [ ! -f "$SOURCE_WORKLOAD" ]; then
    echo "Error: source workload not found at $SOURCE_WORKLOAD"
    exit 1
fi

BASE_DIR=".vstats/${TAG}"
mkdir -p "$BASE_DIR"
cd "$BASE_DIR" || exit


# Compaction priority a.k.a. file picking policy (db_env.h: compaction_pri, [c]).
#   3 = kOldestLargestSeqFirst
#   4 = kOldestSmallestSeqFirst
#   5 = kRoundRobin

POLICY_IDS=(5 3 4)
POLICY_NAMES=(kRoundRobin kOldestLargestSeqFirst kOldestSmallestSeqFirst)

# Memtables to sweep under each policy (same 9 folders uncommented in the
# mkdir list): art, btree, vector-preallocated, skiplist, simpleskiplist,
# unsortedvector-preallocated, hashskiplist-H100000-X6, hashvector-H100000-X6,
# hashlinkedlist-H100000-X6.
run_one () {
    local name="$1"
    local factory="$2"
    shift 2

    echo "Running ${name} [compaction_pri=${COMPACTION_PRI} (${POLICY_NAME})] ... "
    mkdir -p "$name"
    cd "$name"
    ln -sf "$SOURCE_WORKLOAD" workload.txt
    "$BIN" \
        --memtable_factory="$factory" \
        --compaction_pri="$COMPACTION_PRI" \
        "$@" \
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
}

for i in "${!POLICY_IDS[@]}"; do
    COMPACTION_PRI="${POLICY_IDS[$i]}"
    POLICY_NAME="${POLICY_NAMES[$i]}"

    echo -e "\n========================================"
    echo "Policy ${POLICY_NAME} (compaction_pri=${COMPACTION_PRI})"
    echo -e "========================================\n"

    mkdir -p "$POLICY_NAME"
    cd "$POLICY_NAME"

    run_one art 11
    run_one btree 12
    run_one vector-preallocated 2
    run_one skiplist 1
    run_one simpleskiplist 8
    run_one unsortedvector-preallocated 5
    run_one hashskiplist-H100000-X6 3 --bucket_count=100000 --prefix_length=6
    run_one hashvector-H100000-X6 9 --bucket_count=100000 --prefix_length=6
    run_one hashlinkedlist-H100000-X6 4 --bucket_count=100000 --prefix_length=6 \
        --threshold_use_skiplist="$THRESHOLD_TO_CONVERT_TO_SKIPLIST"

    cd ..
done


cd ../..
echo "Done."
echo "Experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}
