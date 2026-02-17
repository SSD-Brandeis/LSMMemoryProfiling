
#!/bin/bash
set -e

# -------------------------------
# Config
# -------------------------------
# TAG="lsmbuffer-concurrent-write-off-WAL-0-compression-disabled-feb16"
TAG="lsmbuffer-concurrent-write-off-WAL-0-compression-disabled-feb1_test"


ENTRY_SIZE=128
LAMBDA=0.125

INSERTS=1000000
UPDATES=0
POINT_QUERIES=0
POINT_DELETES=0
RANGE_QUERIES=0
SELECTIVITY=0
RANGE_DELETES=0
RANGE_DELETES_SEL=0

WORKLOAD_TXT="workload.txt"
BASE_EXP_DIR=".vstats/sanitycheck-${TAG}-${INSERTS}"

TECTONIC="../../bin/tectonic-cli"
ROCKSDB_EXE="../../../bin/rocksdb_experiment"

cmake -B defaultRDBbuild
cmake --build defaultRDBbuild -j$(nproc)

mkdir -p "$BASE_EXP_DIR"
cd "$BASE_EXP_DIR" || exit

# -------------------------------
# Generate inserts-only workload
# -------------------------------
echo "Generating inserts-only workload with ${NUM_INSERTS} entries..."
# python3 ../../scripts/generate_specs.py \
#     -I ${INSERTS} -U ${UPDATES} -Q ${POINT_QUERIES} -D ${POINT_DELETES} -S ${RANGE_QUERIES} -Y ${SELECTIVITY} -R ${RANGE_DELETES} -y ${RANGE_DELETES_SEL} -E ${ENTRY_SIZE} -L ${LAMBDA}

# $TECTONIC generate -w "workload.specs.json"

for mem in 1 2 3; do  # 4 <-- this is not finishing so
    mkdir -p "buffer-$mem"
    echo "Running experiment in $buffer-$mem"
    cd "buffer-$mem"

    # Run experiment and capture output (CSV-friendly) and full logs
    LOG_FILE="rocksdb_stats.log"
    CONFIG_OUTPUT=$($ROCKSDB_EXE \
        --db db \
        --workload "../$WORKLOAD_TXT" \
        --disableWAL=1 \
        --sync=0 \
        --no_slowdown=0 \
        --low_pri=0 \
        --memtable="$mem"| tee "$LOG_FILE")

    # Preserve RocksDB LOG file if present
    if [ -f "db/LOG" ]; then
        mv db/LOG LOG_rocksdb
    fi

    rm -rf db

    cd ..
done

# -------------------------------
# Slack notification (optional)
# -------------------------------
if [ -n "$SLACK_WEBHOOK_URL" ]; then
    HOSTNAME=$(hostname)
    MESSAGE="RocksDB write options experiments completed on ${HOSTNAME} (TAG=${TAG})"
    PAYLOAD="{\"text\": \"${MESSAGE}\"}"
    curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" "$SLACK_WEBHOOK_URL"
    echo "Slack notification sent."
fi

echo "All experiments finished. Results saved to $OUTPUT_CSV."