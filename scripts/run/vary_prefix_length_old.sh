#!/bin/bash

entry_sizes_list=(128)

# Page size list (only 4KB as shown in the original).
page_sizes=(4096)

# We will sweep over these prefix lengths.
# prefix_length_list=(1 2 3 4)
prefix_length_list=(2 4 6 8 10)

# Fixed bucket count for this experiment. Adjust as desired.
fixed_bucket_count=100000

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

        TAG="${PAGE_TAG}_entry_${ENTRY_SIZE}b_buffer_16mb"

        # Set remaining experiment parameters.
        INSERTS=1500000
        UPDATES=0
        RANGE_QUERIES=0
        SELECTIVITY=0
        POINT_QUERIES=0

        LAMBDA=0.5
        SIZE_RATIO=10
        SHOW_PROGRESS=1
        SANITY_CHECK=0

        # Fixed bucket count.
        BUCKET_COUNT=${fixed_bucket_count}
        LINKLIST_THRESHOLD_USE_SKIPLIST=${INSERTS}

        # PAGES_PER_FILE_LIST depends on PAGE_SIZE. 16 MB
        if [ "${PAGE_SIZE}" -eq 2048 ]; then
            PAGES_PER_FILE_LIST=(4096)
        #buffer of 16mb
        elif [ "${PAGE_SIZE}" -eq 4096 ]; then
            PAGES_PER_FILE_LIST=(4096)
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

        # Outer loop: sweep over prefix lengths.
        for PREFIX_LENGTH in "${prefix_length_list[@]}"; do
            echo "[INFO] Running experiment with PREFIX_LENGTH=${PREFIX_LENGTH} and BUCKET_COUNT=${BUCKET_COUNT}"

            # Modify the top-level directory name to include the prefix length
            TOP_LEVEL_DIR_NAME="varying_prefix_4kb_page_15mb_buffer_PL${PREFIX_LENGTH}"
            RESULT_PARENT_DIR="${PROJECT_DIR}/.result/memory_footprint/${TOP_LEVEL_DIR_NAME}"

            if [ -d "${RESULT_PARENT_DIR}" ]; then
                echo "[INFO] Top-level directory ${RESULT_PARENT_DIR} already exists. Using the existing directory."
            else
                mkdir -p "${RESULT_PARENT_DIR}"
            fi

            # load_gen and working_version loops.
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

              # working_version command
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

                # memtable specific parameters
                valid_arg=""
                case "${IMPL_NUM}" in
                  1)
                    # skiplist
                    ;;
                  2)
                    # vector
                    ;;
                  3)
                    # hash_skip_list
                    valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH}"
                    ;;
                  4)
                    # hash_linked_list
                    valid_arg="--bucket_count=${BUCKET_COUNT} --prefix_length=${PREFIX_LENGTH} --threshold_use_skiplist=${LINKLIST_THRESHOLD_USE_SKIPLIST}"
                    ;;
                  7)
                    # linklist
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

echo "[INFO] All workloads with varying prefix lengths completed!"
