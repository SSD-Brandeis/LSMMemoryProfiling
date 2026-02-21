
entry_sizes_list=(128)

# For a 2kb page, PAGE_SIZE=2048; for a 4kb page, use 4096; for an 8kb page, use 8192.
# page_sizes=(2048 4096 8192)
# page_sizes=(2048 4096 8192)
# For a 16kb page, PAGE_SIZE=16384; for a 32kb page, use 32768; for an 64kb page, use 65536.
# page_sizes=(16384 32768 65536)
page_sizes=(4096)
# Define corresponding top-level directory names.
top_level_names=("inserts_10k_PQ_10k_prefix_4_bucket_100k")  

# Loop over each page size
for i in "${!page_sizes[@]}"; do
    PAGE_SIZE=${page_sizes[$i]}
    TOP_LEVEL_DIR_NAME=${top_level_names[$i]}
    
    # Set the top-level result dir 
    PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    RESULT_PARENT_DIR="${PROJECT_DIR}/.result/wl_gen/${TOP_LEVEL_DIR_NAME}"
    
    if [ -d "${RESULT_PARENT_DIR}" ]; then
        echo "[INFO] Top-level directory ${RESULT_PARENT_DIR} already exists. Using the existing directory."
    else
        mkdir -p "${RESULT_PARENT_DIR}"
    fi

    # Loop over each entry size.
    for ENTRY_SIZE in "${entry_sizes_list[@]}"; do
        # Compute ENTRIES_PER_PAGE so that ENTRY_SIZE * ENTRIES_PER_PAGE = PAGE_SIZE.
        ENTRIES_PER_PAGE=$((PAGE_SIZE / ENTRY_SIZE))
        

        # Example: for a 2kb page and ENTRY_SIZE=8, TAG becomes "2kb_page_entry_8b_buffer_8mb".
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

        TAG="${PAGE_TAG}_entry_${ENTRY_SIZE}b_buffer_64mb"



        INSERTS=100000
        UPDATES=0
        RANGE_QUERIES=1000
        SELECTIVITY=0.1
        POINT_QUERIES=100000
        
        LAMBDA=0.5
        SIZE_RATIO=10
        SHOW_PROGRESS=1
        SANITY_CHECK=0

        # hash hybrid parameters
        BUCKET_COUNT=100000
        PREFIX_LENGTH=4
        LINKLIST_THRESHOLD_USE_SKIPLIST=${INSERTS}

        # Choose PAGES_PER_FILE_LIST based on page size.
        # For 2kb pages, use 4096; for 4kb pages, use 2048; for 8kb pages, use 1024.
        if [ "${PAGE_SIZE}" -eq 2048 ]; then
            PAGES_PER_FILE_LIST=(4096)
            #64mb buffer, 16384 b
        elif [ "${PAGE_SIZE}" -eq 4096 ]; then
            PAGES_PER_FILE_LIST=(16384)
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


        LOAD_GEN_PATH="${PROJECT_DIR}/bin/load_gen"
        WORKING_VERSION_PATH="${PROJECT_DIR}/bin/working_version"


        log_info() {
          echo "[INFO] $*"
        }
        log_error() {
          echo "[ERROR] $*"
        }

        declare -A BUFFER_IMPLEMENTATIONS=(
          [1]="skiplist"
          # [2]="vector"
          # [3]="hash_skip_list"
          # [4]="hash_linked_list"
          # [7]="linklist"
        )

        echo "current Parameters:"
        echo "  TAG=${TAG}"
        echo "  ENTRY_SIZE=${ENTRY_SIZE}, ENTRIES_PER_PAGE=${ENTRIES_PER_PAGE}"
        echo "  INSERTS=${INSERTS}, Point_Queries=${POINT_QUERIES}, UPDATES=${UPDATES}, RANGE_QUERIES=${RANGE_QUERIES}, SELECTIVITY=${SELECTIVITY}"
        echo "  LAMBDA=${LAMBDA}, SIZE_RATIO=${SIZE_RATIO}"
        echo "  SHOW_PROGRESS=${SHOW_PROGRESS}, SANITY_CHECK=${SANITY_CHECK}"
        echo
        echo "Experiment with different PAGES_PER_FILE in: ${PAGES_PER_FILE_LIST[*]}"
        echo "Buffer size (bytes) = $((ENTRY_SIZE * ENTRIES_PER_PAGE)) * PAGES_PER_FILE"

        # load gen command
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
          # Sort workload in the sequence of I, Q, RQ
          # INSERTS_FILE=$(mktemp)  
          # POINT_QUERIES_FILE=$(mktemp)  
          # RANGE_QUERIES_FILE=$(mktemp)  

          # grep '^I ' "${WORKLOAD_FILE}" > "${INSERTS_FILE}" 
          # grep '^Q ' "${WORKLOAD_FILE}" > "${POINT_QUERIES_FILE}"  
          # grep '^S ' "${WORKLOAD_FILE}" > "${RANGE_QUERIES_FILE}" 

          # cat "${INSERTS_FILE}" "${POINT_QUERIES_FILE}" "${RANGE_QUERIES_FILE}" > "${WORKLOAD_FILE}"  

          # rm -f "${INSERTS_FILE}" "${POINT_QUERIES_FILE}" "${RANGE_QUERIES_FILE}"  


          # workingversion command
          for IMPL_NUM in "${!BUFFER_IMPLEMENTATIONS[@]}"; do
            IMPL_NAME="${BUFFER_IMPLEMENTATIONS[${IMPL_NUM}]}"
            log_info "Running working_version with PAGES_PER_FILE=${PAGES_PER_FILE}, impl=${IMPL_NAME}"

            # Build experiment directory name.
            EXP_DIR="${IMPL_NAME}-${TAG}-I${INSERTS}-Q${POINT_QUERIES}-U${UPDATES}-S${RANGE_QUERIES}-Y${SELECTIVITY}-T${SIZE_RATIO}"
            # Updated directory structure to group implementations together
            WORKLOAD_RESULT_DIR="${RESULT_PARENT_DIR}/${IMPL_NAME}/${EXP_DIR}/P_${PAGES_PER_FILE}"
            mkdir -p "${WORKLOAD_RESULT_DIR}"

            cp "${WORKLOAD_FILE}" "${WORKLOAD_RESULT_DIR}/"

            pushd "${WORKLOAD_RESULT_DIR}" >/dev/null || exit
            # Delete previous db folder if exists.
            delete_db_folder "db"

            # memtable specific parameters that are valid in working version.
            valid_arg=""
            case "${IMPL_NUM}" in
              1)
                ;;
              2)
                ;;
              3)
                valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
                ;;
              4)
                valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH} --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
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

log_info "All workloads completed!"
