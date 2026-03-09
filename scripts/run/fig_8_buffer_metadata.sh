#!/bin/bash
set -e

# ==============================================================================
# 1. Define the list of BUFFER_SIZES (powers of 2 for KB)
# ==============================================================================
# Powers: 6 (64KB), 8 (256KB), 10 (1MB), 12 (4MB), 14 (16MB), 16 (64MB), 18 (256MB), 20 (1GB)
# BUFFER_SIZES_KB=(6 8 10 12 14 16 18 20)
BUFFER_SIZES_KB=(14 16 18 20 )
# BUFFER_SIZES_KB=( 18 20)
ENTRY_SIZE=128
PAGE_SIZE=4096
# B = 4096 / 128 = 32. This ensures B*E is always 4KB.
ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))

# TAG="lsmbuffer-concurrent-write-off-WAL-0-compression-disabled-feb24_unsortedvectest"
TAG="fig_8_metadata_varybuffer"
RUN_PREALLOCATED=0

declare -A BUFFER_IMPLEMENTATIONS=(
#   [1]="skiplist"
#   [2]="vector"
#   [3]="hash_skip_list"
#   [4]="hash_linked_list"
#   [5]="unsortedvector"
#   [6]="alwayssortedVector"
#   [7]="linkedlist"
#   [8]="simple_skiplist"
  [9]="hash_vector"
#   [10]="inplaceupdatesortedvector"
)

# Shared workload parameters
LAMBDA=0.5
# 8388608 fills a 1GB buffer exactly with 128B entries
INSERTS=500000
UPDATES=0
POINT_QUERIES=0
POINT_DELETES=0
RANGE_QUERIES=0
SELECTIVITY=0.0
RANGE_DELETES=0
RANGE_DELETES_SEL=0

SIZE_RATIO=5
LOW_PRI=0

BUCKET_COUNTS=(100000)
PREFIX_LENGTH=6
THRESHOLD_TO_CONVERT_TO_SKIPLIST=${INSERTS}

PROJECT_ROOT=$(pwd)
GEN_SCRIPT="${PROJECT_ROOT}/scripts/generate_specs.py"
TECTONIC="${PROJECT_ROOT}/bin/tectonic-cli"
WORKING_VERSION="${PROJECT_ROOT}/bin/working_version"
WORKLOAD_TXT="workload.txt"

bash ./scripts/rebuild.sh

# ==============================================================================
# 2. Outer Loop: Buffer Sizes
# ==============================================================================
for POWER in "${BUFFER_SIZES_KB[@]}"; do
    # Calculate Buffer Size
    BUF_KB=$(( 2**POWER ))
    BUFFER_SIZE_BYTES=$(( BUF_KB * 1024 ))
    
    # Calculate P: BufferSize / (B*E) -> BufferSize / 4096
    PAGES_PER_FILE=$(( BUFFER_SIZE_BYTES / PAGE_SIZE ))

    echo "=========================================================="
    echo "CONFIG: ${BUF_KB} KB Buffer"
    echo "P (Pages): ${PAGES_PER_FILE} | B (Entries/Page): ${ENTRIES_PER_PAGE} | E (Size): ${ENTRY_SIZE}"
    echo "Verification (P*B*E): $(( PAGES_PER_FILE * ENTRIES_PER_PAGE * ENTRY_SIZE )) bytes"
    echo "=========================================================="
    
    BASE_EXP_DIR=".results/sanitycheck-${TAG}-I${INSERTS}-L${LAMBDA}-B${BUF_KB}KB"
    mkdir -p "$BASE_EXP_DIR"

    python3 "$GEN_SCRIPT" \
        -I ${INSERTS} -U ${UPDATES} -Q ${POINT_QUERIES} -D ${POINT_DELETES} \
        -S ${RANGE_QUERIES} -Y ${SELECTIVITY} -R ${RANGE_DELETES} \
        -y ${RANGE_DELETES_SEL} -E ${ENTRY_SIZE} -L ${LAMBDA}
    
    "$TECTONIC" generate -w "workload.specs.json"
    mv "$WORKLOAD_TXT" "workload.specs.json" "$BASE_EXP_DIR/"

    cd "$BASE_EXP_DIR"

    for mem in "${!BUFFER_IMPLEMENTATIONS[@]}"; do 
        mem_name="${BUFFER_IMPLEMENTATIONS[$mem]}"
        
        # Determine DIR_NAME based on type
        if [[ "$mem_name" == *"hash"* ]]; then
            for BUCKET_COUNT in "${BUCKET_COUNTS[@]}"; do
                DIR_NAME="buffer-${mem}-${mem_name}-H${BUCKET_COUNT}-B${BUF_KB}KB"
                mkdir -p "$DIR_NAME"
                cp "workload.txt" "./$DIR_NAME/workload.txt"
                
                cd "$DIR_NAME"
                "$WORKING_VERSION" --memtable_factory="$mem" \
                    -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                    -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 \
                    --bucket_count="${BUCKET_COUNT}" --prefix_length="${PREFIX_LENGTH}" \
                    --threshold_use_skiplist="${THRESHOLD_TO_CONVERT_TO_SKIPLIST}" > "rocksdb_stats.log"
                
                [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
                [ -f "workload.log" ] && mv workload.log workload_run.log
                rm -rf db
                cd ..
            done

        elif [[ "$mem_name" == *"Vector"* || "$mem_name" == *"vector"* ]]; then
            DIR_NAME="buffer-${mem}-${mem_name}-dynamic-B${BUF_KB}KB"
            mkdir -p "$DIR_NAME"
            cp "workload.txt" "./$DIR_NAME/workload.txt"
            cd "$DIR_NAME"
            "$WORKING_VERSION" --memtable_factory="$mem" \
                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                -E "${ENTRY_SIZE}" -A 0 --lowpri "${LOW_PRI}" --stat 1 > "rocksdb_stats.log"
            
            [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
            [ -f "workload.log" ] && mv workload.log workload_run.log
            rm -rf db
            cd ..

            if [ "$RUN_PREALLOCATED" -eq 1 ]; then
                DIR_NAME="buffer-${mem}-${mem_name}-preallocated-B${BUF_KB}KB"
                mkdir -p "$DIR_NAME"
                cp "workload.txt" "./$DIR_NAME/workload.txt"
                cd "$DIR_NAME"
                "$WORKING_VERSION" --memtable_factory="$mem" \
                    -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                    -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                    -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "rocksdb_stats.log"
                [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
                [ -f "workload.log" ] && mv workload.log workload_run.log
                rm -rf db
                cd ..
            fi
        else
            DIR_NAME="buffer-${mem}-${mem_name}-B${BUF_KB}KB"
            mkdir -p "$DIR_NAME"
            cp "workload.txt" "./$DIR_NAME/workload.txt"
            cd "$DIR_NAME"
            "$WORKING_VERSION" --memtable_factory="$mem" \
                -I "${INSERTS}" -U "${UPDATES}" -S "${RANGE_QUERIES}" -Y "${SELECTIVITY}" \
                -T "${SIZE_RATIO}" -P "${PAGES_PER_FILE}" -B "${ENTRIES_PER_PAGE}" \
                -E "${ENTRY_SIZE}" --lowpri "${LOW_PRI}" --stat 1 > "rocksdb_stats.log"
            
            [ -f "db/LOG" ] && mv db/LOG LOG_rocksdb
            [ -f "workload.log" ] && mv workload.log workload_run.log
            rm -rf db
            cd ..
        fi
    done

    cd "$PROJECT_ROOT"
done

echo "everything complete."

echo "------------------------------------------------"
echo "All experiments finished. Cleaning up workload files..."

# Remove the master workload file in the current directory (BASE_EXP_DIR)
rm -f "$WORKLOAD_TXT"

find . -maxdepth 2 -name "$WORKLOAD_TXT" -delete

echo "Cleanup complete."