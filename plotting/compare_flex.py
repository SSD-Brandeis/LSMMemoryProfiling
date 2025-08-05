from pathlib import Path
from typing import Optional, Dict, List
from collections import defaultdict
import re, itertools
import matplotlib.pyplot as plt

RESULTS_DIR=".result"

TAG="perf_test"
SETTINGS="lowpri_false_debug"

INSERTS=10000
UPDATES=0
RANGE_QUERIES=0
SELECTIVITY=0
POINT_QUERIES=0

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



TIME_TO_PLOT = [
    # "Lock",
    "VectorRep",
    # "MemTableRep",
    # "MemTable",
    # "PutCFImpl",
    # "WriteBatchInternal",
    # "DBImpl",
]


def process_log(file_path: Path) -> List[Dict[str, int]]:
    data = list()

    with file_path.open() as f:
        for line in f:
            metric = dict()
            if "," not in line or len(line.strip().split(",")) < 3:
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

def process_cache_log(file_path: Path) -> Dict[int, Dict[str, int]]:
    data = dict()

    with file_path.open() as f:
        op_num = -1
        metric = dict()
        for line in f:
            if line.startswith("Op num:"):
                op_num = int(line.strip().split(":")[1].strip())
                continue
            if not line.startswith("---------"):
                name, value = line.strip().split(":")
                name = name.strip()
                value = int(value.strip())
                metric[name] = int(value)
            elif line.startswith("---------"):
                data[op_num] = metric.copy()
                metric.clear()
                op_num = -1
    return data

def average_runs(runs: List[List[Dict[str,int]]]) -> List[Dict[str,int]]:
    if not runs:
        return []
    n = len(runs)
    length = len(runs[0])
    avg = []
    for i in range(length):
       
        keys = set().union(*(run[i].keys() for run in runs))
        m: Dict[str,int] = {}
        for k in keys:
            total = sum(run[i].get(k, 0) for run in runs)
            m[k] = total // n
        avg.append(m)
    return avg

def plot_cache_data(data, log_name, title: Optional[str] = None):
    fig, ax = plt.subplots(figsize=(8, 4))
    cache_misses = [data[ith_op]["cache-misses"] if ith_op in data else 0 for ith_op in range(INSERTS)]
    cache_references = [data[ith_op]["cache-references"] if ith_op in data else 0 for ith_op in range(INSERTS)]
    dTLB_loads = [data[ith_op]["dTLB-loads"] if ith_op in data else 0 for ith_op in range(INSERTS)]
    dTLB_load_misses = [data[ith_op]["dTLB-load-misses"] if ith_op in data else 0 for ith_op in range(INSERTS)]
    branches = [data[ith_op]["branches"] if ith_op in data else 0 for ith_op in range(INSERTS)]
    branch_misses = [data[ith_op]["branch-misses"] if ith_op in data else 0 for ith_op in range(INSERTS)]
    page_faults = [data[ith_op]["page-faults"] if ith_op in data else 0 for ith_op in range(INSERTS)]
    context_switches = [data[ith_op]["context-switches"] if ith_op in data else 0 for ith_op in range(INSERTS)]

    # ax.plot(cache_misses, label="cache-misses")
    # ax.plot(cache_references, label="cache-references")
    # ax.plot(dTLB_loads, label="dTLB-loads")
    # ax.plot(dTLB_load_misses, label="dTLB-load-misses")
    # ax.plot(branches, label="branches")
    # ax.plot(branch_misses, label="branch-misses")
    ax.plot(page_faults, label="page-faults")
    ax.plot(context_switches, label="context-switches")
    
    # ax.set_yscale("log")
    # ax.set_ylim(1e0, 1e7)
    ax.set_xlabel("operation")
    ax.set_ylabel("count")
    ax.set_title(title if title else "")
    ax.legend(loc="upper center", ncol=3, frameon=False, borderaxespad=0, labelspacing=0, 
                borderpad=0, columnspacing=0.6)
    fig.tight_layout()
    fig.savefig(f"{log_name}.pdf")
    print(f"[SAVED] {log_name}.png")

def plot_data(data, log_name, title: Optional[str] = None):
    # TODO (James) Use the Libertine format to generate pdf
    #              the one we used for tectonic
    fig, ax = plt.subplots(figsize=(8, 4))

    function_times = defaultdict(list)

    for line in data:
        for func in line:
            if func in TIME_TO_PLOT:
                function_times[func].append(line[func])

    # for func, times in function_times.items():
    #     ax.plot(times, label=func)
    for func in TIME_TO_PLOT: 
        times = function_times.get(func)
        if times: 
            ax.plot(times, label = func)
    
    # ax.set_yscale("log")
    # ax.set_ylim(1e0, 1e6)
    ax.set_ylim(0, 150000)
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
                        for variant in ("dynamic", "preallocated"):
                            dir_path = EXP_DIR / f"{buffer_name}-{variant}"
                            runs = [ process_log(dir_path / f"run{i}.log") for i in (1,) ] # 2, 3) ]
                            avg_data = average_runs(runs)
                            plot_data(avg_data, dir_path / "run_avg", f"{buffer_name}-{variant}")

                            cache_log = process_cache_log(dir_path / f"cache.log")
                            plot_cache_data(cache_log, dir_path / f"cache.log", f"{buffer_name}-{variant}")

                    elif buffer_name in ("hash_skip_list", "hash_linked_list"):
                        dir_path = EXP_DIR / f"{buffer_name}-X{PREFIX_LENGTH}-H{BUCKET_COUNT}"
                        runs = [ process_log(dir_path / f"run{i}.log") for i in (1, 2, 3) ]
                        avg_data = average_runs(runs)
                        plot_data(avg_data, dir_path / "run_avg", buffer_name)

                    else:
                        dir_path = EXP_DIR / buffer_name
                        runs = [ process_log(dir_path / f"run{i}.log") for i in (1, 2, 3) ]
                        avg_data = average_runs(runs)
                        plot_data(avg_data, dir_path / "run_avg", buffer_name)


if __name__ == "__main__":
    main()