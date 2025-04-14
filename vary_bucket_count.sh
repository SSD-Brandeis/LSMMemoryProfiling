#!/bin/bash

# 4KB page
entry_sizes_list=(128)

# 4KB page
page_sizes=(4096)

# Base top-level directory name for experiments.
base_top_level_dir="varying_bucket_fix_prefixlength_4kb_page_15mb_buffer"

# List of bucket counts to sweep.
bucket_count_list=(1 200000 400000 600000 800000 1000000)

# Fixed prefix length for this experiment.
fixed_prefix_length=4

# Set the project directory once.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Loop over each page size configuration.
for i in "${!page_sizes[@]}"; do
    PAGE_SIZE=${page_sizes[$i]}
    
    # Loop over each entry size.
    for ENTRY_SIZE in "${entry_sizes_list[@]}"; do
        # Compute ENTRIES_PER_PAGE so that ENTRY_SIZE * ENTRIES_PER_PAGE = PAGE_SIZE.
        ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
        
        # Determine page tag for human-readable naming.
        if [ "${PAGE_SIZE}" -eq 2048 ]; then
            PAGE_TAG="2kb_page"
        elif [ "${PAGE_SIZE}" -eq 4096 ]; then
            PAGE_TAG="4kb_page"
        elif [ "${PAGE_SIZE}" -eq 8192 ]; then
            PAGE_TAG="8kb_page"
        elif [ "${PAGE_SIZE}" -eq 16384 ]; then
            PAGE_TAG="16kb_page"
        elif [ "${PAGE_SIZE}" -eq 32768 ]; then
            PAGE_TAG="32kb_page"
        elif [ "${PAGE_SIZE}" -eq 65536 ]; then
            PAGE_TAG="64kb_page"
        else
            echo "Unknown PAGE_SIZE: ${PAGE_SIZE}"
            exit 1
        fi
        
        TAG="${PAGE_TAG}_entry_${ENTRY_SIZE}b_buffer_15mb"
        
        # Set experiment parameters.
        INSERTS=1500000
        UPDATES=0
        RANGE_QUERIES=0
        SELECTIVITY=0
        POINT_QUERIES=0
        
        LAMBDA=0.5
        SIZE_RATIO=10
        SHOW_PROGRESS=1
        SANITY_CHECK=0
        
        # Fixed prefix length.
        PREFIX_LENGTH=${fixed_prefix_length}
        LINKLIST_THRESHOLD_USE_SKIPLIST=${INSERTS}
        
        # PAGES_PER_FILE_LIST based on PAGE_SIZE (15MB buffer for 4KB page using 3840 pages per file).
        if [ "${PAGE_SIZE}" -eq 2048 ]; then
            PAGES_PER_FILE_LIST=(4096)
        elif [ "${PAGE_SIZE}" -eq 4096 ]; then
            PAGES_PER_FILE_LIST=(3840)
        elif [ "${PAGE_SIZE}" -eq 8192 ]; then
            PAGES_PER_FILE_LIST=(1024)
        elif [ "${PAGE_SIZE}" -eq 16384 ]; then
            PAGES_PER_FILE_LIST=(512)
        elif [ "${PAGE_SIZE}" -eq 32768 ]; then
            PAGES_PER_FILE_LIST=(256)
        elif [ "${PAGE_SIZE}" -eq 65536 ]; then
            PAGES_PER_FILE_LIST=(128)
        else
            echo "Unsupported PAGE_SIZE: ${PAGE_SIZE}" >&2
            exit 1
        fi
        
        # Define paths to binaries.
        LOAD_GEN_PATH="${PROJECT_DIR}/bin/load_gen"
        WORKING_VERSION_PATH="${PROJECT_DIR}/bin/working_version"
        
        # Logging functions.
        log_info() {
          echo "[INFO] $*"
        }
        log_error() {
          echo "[ERROR] $*"
        }
        
        # Define available buffer implementations.
        declare -A BUFFER_IMPLEMENTATIONS=(
          [1]="skiplist"
          [2]="vector"
          [3]="hash_skip_list"
          [4]="hash_linked_list"
          [7]="linklist"
        )
        
        echo "Current Parameters:"
        echo "  TAG=${TAG}"
        echo "  ENTRY_SIZE=${ENTRY_SIZE}, ENTRIES_PER_PAGE=${ENTRIES_PER_PAGE}"
        echo "  INSERTS=${INSERTS}, POINT_QUERIES=${POINT_QUERIES}, UPDATES=${UPDATES}, RANGE_QUERIES=${RANGE_QUERIES}, SELECTIVITY=${SELECTIVITY}"
        echo "  LAMBDA=${LAMBDA}, SIZE_RATIO=${SIZE_RATIO}"
        echo "  SHOW_PROGRESS=${SHOW_PROGRESS}, SANITY_CHECK=${SANITY_CHECK}"
        echo
        echo "Experiment with different PAGES_PER_FILE in: ${PAGES_PER_FILE_LIST[*]}"
        
        # Outer loop: sweep over bucket counts.
        for BUCKET_COUNT in "${bucket_count_list[@]}"; do
            echo "[INFO] Running experiment with BUCKET_COUNT=${BUCKET_COUNT} and PREFIX_LENGTH=${PREFIX_LENGTH}"
            
            # Modify the top-level directory name to include the bucket count.
            CURRENT_TOP_LEVEL_DIR="${base_top_level_dir}_BC${BUCKET_COUNT}"
            RESULT_PARENT_DIR="${PROJECT_DIR}/.result/memory_footprint/${CURRENT_TOP_LEVEL_DIR}"
            
            if [ -d "${RESULT_PARENT_DIR}" ]; then
                echo "[INFO] Top-level directory ${RESULT_PARENT_DIR} already exists. Using the existing directory."
            else
                mkdir -p "${RESULT_PARENT_DIR}"
            fi
            
            # For each PAGES_PER_FILE setting.
            for PAGES_PER_FILE in "${PAGES_PER_FILE_LIST[@]}"; do
              buffer_size_bytes=$((ENTRY_SIZE * ENTRIES_PER_PAGE * PAGES_PER_FILE))
              log_info "Generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"
              echo " => buffer_size_bytes = ${buffer_size_bytes} (E*B*P)"
              
              pushd "${PROJECT_DIR}" >/dev/null || exit
              if ! "${LOAD_GEN_PATH}" \
                   -I "${INSERTS}" \
                   -Q "${POINT_QUERIES}" \
                   -U "${UPDATES}" \
                   -S "${RANGE_QUERIES}" \
                   -Y "${SELECTIVITY}" \
                   -E "${ENTRY_SIZE}" \
                   -L "${LAMBDA}"
              then
                log_error "Something went wrong generating workload for PAGES_PER_FILE=${PAGES_PER_FILE}"
                continue
              fi
              
              WORKLOAD_FILE="${PROJECT_DIR}/workload.txt"
              if [ ! -f "${WORKLOAD_FILE}" ]; then
                log_error "workload.txt not found for PAGES_PER_FILE=${PAGES_PER_FILE}"
                continue
              fi
              
              # Loop over available buffer implementations.
              for IMPL_NUM in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
                IMPL_NAME="${BUFFER_IMPLEMENTATIONS[${IMPL_NUM}]}"
                log_info "Running working_version with PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME}"
                
                # Build experiment directory name.
                EXP_DIR="${IMPL_NAME}-${TAG}-I${INSERTS}-Q${POINT_QUERIES}-U${UPDATES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}-BC${BUCKET_COUNT}-PL${PREFIX_LENGTH}"
                WORKLOAD_RESULT_DIR="${RESULT_PARENT_DIR}/${IMPL_NAME}/${EXP_DIR}/P_${PAGES_PER_FILE}"
                mkdir -p "${WORKLOAD_RESULT_DIR}"
                
                cp "${WORKLOAD_FILE}" "${WORKLOAD_RESULT_DIR}/"
                
                pushd "${WORKLOAD_RESULT_DIR}" >/dev/null || exit
                
                # Delete previous db folder if it exists.
                delete_db_folder() {
                  if [ -d "$1" ]; then
                      rm -rf "$1"
                  fi
                }
                delete_db_folder "db"
                
                # memtable specific parameters.
                valid_arg=""
                case "${IMPL_NUM}" in
                  1)
                    # skiplist has no extra parameters.
                    ;;
                  2)
                    # vector has no extra parameters.
                    ;;
                  3)
                    # hash_skip_list.
                    valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
                    ;;
                  4)
                    # hash_linked_list.
                    valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH} --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
                    ;;
                  7)
                    # linklist has no extra parameters.
                    ;;
                esac
                
                log_info "Executing working_version for memtable_factory=${IMPL_NUM}..."
                if ! "${WORKING_VERSION_PATH}" \
                      --memtable_factory="${IMPL_NUM}" \
                      ${valid_arg} \
                      -I "${INSERTS}" \
                      -U "${UPDATES}" \
                      -S "${RANGE_QUERIES}" \
                      -Y "${SELECTIVITY}" \
                      -E "${ENTRY_SIZE}" \
                      -B "${ENTRIES_PER_PAGE}" \
                      -P "${PAGES_PER_FILE}" \
                      -T "${SIZE_RATIO}" \
                      --progress "${SHOW_PROGRESS}" \
                      --stat 1 > temp.log
                then
                  log_error "Something is wrong with working_version (PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME})"
                fi
                
                mv db/LOG "." 2>/dev/null || true
                mv db/workload.log "." 2>/dev/null || true
                rm -rf db
                popd >/dev/null || exit
              done
              
              rm -f "${WORKLOAD_FILE}"
              popd >/dev/null || exit
            done
        done
    done
done

echo "[INFO] All workloads with varying bucket counts completed!"
