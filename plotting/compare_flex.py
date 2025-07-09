from pathlib import Path
from typing import Optional, Dict, List
from collections import defaultdict
import re, itertools
import matplotlib.pyplot as plt

RESULTS_DIR=".result"

TAG="experiment"
SETTINGS="lowpri_false"

INSERTS=10000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=100

SIZE_RATIO=10

ENTRY_SIZES=(128,)
LAMBDA=0.5
PAGE_SIZES=(4096,)

BUCKET_COUNT=100000
PREFIX_LENGTH=6
THRESHOLD_TO_CONVERT_TO_SKIPLIST=INSERTS

BUFFER_IMPLEMENTATIONS={
#   "1": "skiplist",
  "2": "Vector",
#   "3": "hash_skip_list",
#   "4": "hash_linked_list",
#   "5": "UnsortedVector",
#   "6": "AlwayssortedVector",
}

PROJ_DIR = Path(__file__).parent.parent
RESULT_DIR = PROJ_DIR / RESULTS_DIR

if not RESULT_DIR.is_dir():
    raise SystemExit(f"Missing experiment dir: {RESULT_DIR}")

# ==========================================================

TIME_TO_PLOT = [
    "VectorRep",
    "Lock",
    "MemTableRep",
    "MemTable",
    "PutCFImpl",
    "WriteBatchInternal",
    "DBImpl",
]


def process_log(file_path: Path) -> List[Dict[str, int]]:
    data = list()

    with file_path.open() as f:
        for line in f:
            metric = dict()
            if "," not in line or len(line.strip().split(",")) < 2:
                continue

            functions = line.strip().split(",")

            for func in functions:
                name, value = func.split(":")
                name = name.strip()
                value = int(value.strip())
                metric[name] = int(value)

            if metric:
                data.append(metric)
    return data


def plot_data(data, log_name, title: Optional[str] = None):
    # TODO (James) Use the Libertine format to generate pdf
    #              the one we used for tectonic
    fig, ax = plt.subplots(figsize=(8, 4))

    function_times = defaultdict(list)

    for line in data:
        for func in line:
            if func in TIME_TO_PLOT:
                function_times[func].append(line[func])

    for func, times in function_times.items():
        ax.plot(times, label=func)
    
    ax.set_yscale("log")
    ax.set_ylim(1e0, 1e6)
    ax.set_xlabel("operation")
    ax.set_ylabel("time (ns)")
    ax.set_title(title if title else "")
    ax.legend(loc="lower center", ncol=3, fontsize=7, frameon=False, borderaxespad=0, labelspacing=0, 
                borderpad=0, columnspacing=0.6)
    fig.tight_layout()
    fig.savefig(f"{log_name}.pdf")
    print(f"[SAVED] {log_name}.png")

def main():
    for page_size in PAGE_SIZES:
        if page_size == 2048:
            PAGES_PER_FILE_LIST = (4096,)
        elif page_size == 4096:
            PAGES_PER_FILE_LIST = (16384,)
        elif page_size == 8192:
            PAGES_PER_FILE_LIST = (1024,)
        elif page_size == 16384:
            PAGES_PER_FILE_LIST = (512,)
        elif page_size == 32768:
            PAGES_PER_FILE_LIST = (256,)
        elif page_size == 65536:
            PAGES_PER_FILE_LIST = (128,)
        else:
            raise SystemExit(f"Unknown PAGE_SIZE: {page_size}")
    
        for ENTRY_SIZE in ENTRY_SIZES:
            ENTRIES_PER_PAGE = page_size // ENTRY_SIZE

            for PAGES_PER_FILE in PAGES_PER_FILE_LIST:
                EXP_DIR = RESULT_DIR / f"{TAG}-{SETTINGS}-I{INSERTS}-U{UPDATES}-Q{POINT_QUERIES}-S{RANGE_QUERIES}-Y{SELECTIVITY}-T{SIZE_RATIO}-P{PAGES_PER_FILE}-B{ENTRIES_PER_PAGE}-E{ENTRY_SIZE}"

                if not EXP_DIR.is_dir():
                    raise SystemExit(f"Missing experiment dir: {EXP_DIR}")
                
                for _, buffer_name in BUFFER_IMPLEMENTATIONS.items():
                    if buffer_name in ("Vector", "UnsortedVector", "AlwayssortedVector"):
                        # TODO: (James) Read all three run file here
                        #               Pass it to some function and get 
                        #               the average result in the same format
                        log_file = EXP_DIR / f"{buffer_name}-dynamic" / f"run1.log"
                        data = process_log(log_file)
                        plot_data(data, log_file.absolute(), f"{buffer_name}-dynamic")

                        log_file = EXP_DIR / f"{buffer_name}-preallocated" / f"run1.log"
                        data = process_log(log_file)
                        plot_data(data, log_file.absolute(), f"{buffer_name}-preallocated")
                    elif buffer_name in ("hash_skip_list", "hash_linked_list"):
                        log_file = EXP_DIR / f"{buffer_name}-X{PREFIX_LENGTH}-H{BUCKET_COUNT}" / f"run1.log"
                        data = process_log(log_file)
                        plot_data(data, log_file.absolute(), buffer_name)
                    else:
                        log_file = EXP_DIR / f"{buffer_name}" / f"run1.log"
                        data = process_log(log_file)
                        plot_data(data, log_file.absolute(), buffer_name)

if __name__ == "__main__":
    main()