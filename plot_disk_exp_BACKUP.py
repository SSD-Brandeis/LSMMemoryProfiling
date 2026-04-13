import sys
import csv
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

class Logger(object):
    def __init__(self, filename="fig4_analysis_results.log"):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

from concurrent.futures import ProcessPoolExecutor

from plot.rocksdb_stats import parse_rocksdb_log
from plot.style import hatch_map, line_styles  # , hatch_map

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1] 
CURR_DIR = Path.cwd()

ROOT_DIR = PROJECT_ROOT / "data_new"
EXP_DIR = ROOT_DIR / "diskbased-1mbt6"

implementations = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "sortedvector-preallocated",
    "skiplist",
    "simpleskiplist",
    "hashlinkedlist-H100000-X6",
    "hashskiplist-H100000-X6",
    "hashvector-H100000-X6",
]

def dump_to_csv(filename, headers, rows):
    """Helper to dump plot data to CSV for paper usage."""
    filepath = CURR_DIR / filename
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"[DATA DUMP] Saved numeric values to {filepath}")

def normalize_name(name):
    name = name.lower()
    if "hashlinkedlist" in name:
        return "hashlinkedlist"
    if "hashskiplist" in name:
        return "hashskiplist"
    if "hashvector" in name:
        return "hashvector"
    if "simpleskiplist" in name:
        return "simpleskiplist"
    if name == "skiplist":
        return "skiplist"
    if "unsortedvector" in name:
        return "unsortedvector"
    if "sortedvector" in name:
        return "alwayssortedvector"
    if "vector" in name:
        return "vector"
    return None

def process_single_dir_for_PQs(args):
    impl, path = args
    log_file = os.path.join(path, "stats.log")
    cache_file = os.path.join(path, "insert_latencies_pqq.npy")

    if os.path.exists(cache_file):
        arr = np.load(cache_file, mmap_mode="r")
        print(f"[CACHE HIT] {impl}")
    else:
        print(f"[PARSING] {impl}")
        latencies = []
        with open(log_file, "r") as f:
            for _, line in enumerate(f):
                if not line.startswith("Q:"):
                    continue
                parts = line.split()
                latency_ms = float(parts[1]) / 1_000_000  # convert ns to ms
                latencies.append(latency_ms)
        arr = np.array(latencies, dtype=np.float64)
        np.save(cache_file, arr)

    stats = {
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "data": arr,
    }
    return impl, stats

def process_single_dir_for_RQs(args):
    impl, path = args
    log_file = os.path.join(path, "stats.log")
    cache_file = os.path.join(path, "insert_latencies_rq.npy")

    if os.path.exists(cache_file):
        arr = np.load(cache_file, mmap_mode="r")
        print(f"[CACHE HIT] {impl}")
    else:
        print(f"[PARSING] {impl}")
        latencies = []
        with open(log_file, "r") as f:
            for _, line in enumerate(f):
                if not line.startswith("S:"):
                    continue
                parts = line.split()
                latency_ms = float(parts[1]) / 1_000_000  # convert ns to ms
                latencies.append(latency_ms)
        arr = np.array(latencies, dtype=np.float64)
        np.save(cache_file, arr)

    stats = {
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "data": arr,
    }
    return impl, stats

def process_single_dir_for_Is(args):
    impl, path = args
    log_file = os.path.join(path, "stats.log")
    cache_file = os.path.join(path, "insert_latencies_all.npy")

    if os.path.exists(cache_file):
        arr = np.load(cache_file, mmap_mode="r")
        print(f"[CACHE HIT] {impl}")
    else:
        print(f"[PARSING] {impl}")
        latencies = []
        with open(log_file, "r") as f:
            for _, line in enumerate(f):
                if not line.startswith("I:"):
                    continue
                parts = line.split()
                latency_ms = float(parts[1]) / 1_000  # convert ns to μs
                latencies.append(latency_ms)
        arr = np.array(latencies, dtype=np.float64)
        np.save(cache_file, arr)

    stats = {
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "data": arr,
    }
    return impl, stats

def plot_PQ_latencies():
    tasks = [(impl, os.path.join(EXP_DIR, impl)) for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_single_dir_for_PQs, tasks):
            if stats is not None: results[impl] = stats

    labels = list(results.keys())
    data = [results[impl]["data"] for impl in labels]
    
    # Prep CSV data
    csv_rows = [[impl, results[impl]["mean"], results[impl]["median"]] for i, impl in enumerate(labels)]
    dump_to_csv("stats_pq_latencies.csv", ["Implementation", "Mean (ms)", "Median (ms)"], csv_rows)

    clipped_data = []
    for d in data:
        p0, p98 = np.percentile(d, [0, 98])
        clipped_data.append(d[(d >= p0) & (d <= p98)])

    _, ax = plt.subplots(figsize=(4, 3))
    vp = ax.violinplot(clipped_data, showmeans=True, showmedians=False, showextrema=False, widths=0.5)

    colors = []
    for i, impl in enumerate(labels):
        key = normalize_name(impl)
        color = line_styles[key]["color"]
        colors.append(color)
        body = vp["bodies"][i]
        body.set_facecolor(color)
        body.set_edgecolor(color)
        body.set_alpha(0.5)

    if "cmeans" in vp:
        vp["cmeans"].set_colors(colors)
        vp["cmeans"].set_linewidth(1.5)

    ax.set_xticks(range(1, len(labels) + 1), [""] * len(labels))
    ax.set_yticks([0, 10, 20], ["0", "10", "20"])
    ax.set_ylabel("PQ latency (ms)", labelpad=-1)
    ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    output_file = CURR_DIR / "disk-based-insert-latency-pq.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved plot to {output_file}")

def plot_RQ_latencies():
    tasks = [(impl, os.path.join(EXP_DIR, impl)) for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_single_dir_for_RQs, tasks):
            if stats is not None: results[impl] = stats

    labels = list(results.keys())
    data = [results[impl]["data"] for impl in labels]

    # Prep CSV data
    csv_rows = [[impl, results[impl]["mean"], results[impl]["median"]] for i, impl in enumerate(labels)]
    dump_to_csv("stats_rq_latencies.csv", ["Implementation", "Mean (ms)", "Median (ms)"], csv_rows)

    clipped_data = []
    for d in data:
        p0, p98 = np.percentile(d, [0, 98])
        clipped_data.append(d[(d >= p0) & (d <= p98)])

    _, ax = plt.subplots(figsize=(4, 3))
    vp = ax.violinplot(clipped_data, showmeans=True, showmedians=False, showextrema=False, widths=0.5)

    colors = []
    for i, impl in enumerate(labels):
        key = normalize_name(impl)
        color = line_styles[key]["color"]
        colors.append(color)
        body = vp["bodies"][i]
        body.set_facecolor(color)
        body.set_edgecolor(color)
        body.set_alpha(0.5)

    if "cmeans" in vp:
        vp["cmeans"].set_colors(colors)
        vp["cmeans"].set_linewidth(1.5)

    ax.set_xticks(range(1, len(labels) + 1), [""] * len(labels))
    ax.set_yticks([0, 10, 20], ["0", "10", "20"])
    ax.set_ylabel("RQ latency (ms)", labelpad=-1)
    ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    output_file = CURR_DIR / "disk-based-insert-latency-rq.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved plot to {output_file}")

def plot_insert_latencies_violin():
    tasks = [(impl, os.path.join(EXP_DIR, impl)) for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_single_dir_for_Is, tasks):
            if stats is not None: results[impl] = stats

    labels = list(results.keys())
    
    # Prep CSV data
    csv_rows = [[impl, results[impl]["mean"], results[impl]["median"]] for i, impl in enumerate(labels)]
    dump_to_csv("stats_insert_latencies.csv", ["Implementation", "Mean (us)", "Median (us)"], csv_rows)

    data = [results[impl]["data"] for impl in labels]
    clipped_data = []
    for d in data:
        p98 = np.percentile(d, 98)
        clipped_data.append(d[d <= p98])

    _, ax = plt.subplots(figsize=(4, 3))
    vp = ax.violinplot(clipped_data, showmeans=True, showmedians=False, showextrema=False, widths=0.5)

    colors = []
    for i, impl in enumerate(labels):
        key = normalize_name(impl)
        color = line_styles[key]["color"]
        colors.append(color)
        body = vp["bodies"][i]
        body.set_facecolor(color)
        body.set_edgecolor(color)
        body.set_alpha(0.5)

    if "cmeans" in vp:
        vp["cmeans"].set_colors(colors)
        vp["cmeans"].set_linewidth(1.5)

    ax.set_xticks(range(1, len(labels) + 1), [""] * len(labels))
    ax.set_yticks([0, 5, 10, 15], ["0", "5", "10", "15"])
    ax.set_ylabel("insert latency ($\\mu$s)", labelpad=-1)
    ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    output_file = CURR_DIR / "disk-based-insert-latency-violin.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved violin plot to {output_file}")

def process_impl(impl):
    path = os.path.join(EXP_DIR, impl)
    log_file = os.path.join(path, "workload.log")
    phases = parse_rocksdb_log(log_file)
    phasewise_wkl_time = [p["meta"].get("workload_time", 0) for p in phases]
    phasewise_flush_counts = sum([p["histograms"]["rocksdb.db.flush.micros"]["COUNT"] for p in phases])
    phasewise_compaction_counts = sum([p["histograms"]["rocksdb.compaction.times.micros"]["COUNT"] for p in phases])
    phasewise_wkl_ops = [80_000_000, 10_010_000, 10_010_000]

    def safe_tp(ops, time_us):
        return ops / (time_us / 1e9) if time_us != 0 else 0

    return (impl, [safe_tp(phasewise_wkl_ops[i], phasewise_wkl_time[i]) for i in range(len(phases))], phasewise_flush_counts, phasewise_compaction_counts)

def process_amplification_stats(impl):
    path = os.path.join(EXP_DIR, impl)
    log_file = os.path.join(path, "workload.log")
    wa_phases = []
    ra_phases = []
    stall_phases = []
    movement_phases = []
    entry_size = 128
    
    try:
        with open(log_file, "r") as f:
            content = f.read()
        
        # Split by phase stats occurrence
        phase_blocks = content.split("[Rocksdb Stats]")
        for block in phase_blocks[1:]: # Skip text before first stats block
            lines = block.split('\n')
            stats = {}
            for line in lines:
                if "COUNT:" in line and "rocksdb.db.write.stall" not in line:
                    parts = line.split("COUNT:")
                    key = parts[0].strip()
                    val = float(parts[1].strip())
                    stats[key] = val
                
                # Extract SUM for stall metric and convert micros to seconds
                if "rocksdb.db.write.stall" in line and "SUM :" in line:
                    parts = line.split("SUM :")
                    stall_sum_micros = float(parts[1].strip())
                    stats["rocksdb.db.write.stall.sum_s"] = stall_sum_micros / 1_000_000.0
            
            # WA calculation
            keys_w = stats.get("rocksdb.number.keys.written", 0)
            bytes_w = stats.get("rocksdb.bytes.written", 0)
            comp_w = stats.get("rocksdb.compact.write.bytes", 0)
            denom_w = keys_w * entry_size
            wa = (bytes_w + comp_w) / denom_w if denom_w > 0 else 0
            
            # RA calculation
            keys_r = stats.get("rocksdb.number.keys.read", 0)
            bytes_r = stats.get("rocksdb.bytes.read", 0)
            comp_r = stats.get("rocksdb.compact.read.bytes", 0)
            denom_r = keys_r * entry_size
            ra = (bytes_r + comp_r) / denom_r if denom_r > 0 else 0

            # Overall Data Movement (GB) = (Bytes read + Bytes written)
            total_bytes = (bytes_w + comp_w + bytes_r + comp_r)
            movement_gb = total_bytes / (1024**3)
            
            wa_phases.append(wa)
            ra_phases.append(ra)
            stall_phases.append(stats.get("rocksdb.db.write.stall.sum_s", 0))
            movement_phases.append(movement_gb)
            
    except Exception as e:
        print(f"[ERROR] Failed to parse {impl}: {e}")
        
    return impl, wa_phases, ra_phases, stall_phases, movement_phases

def plot_flush_and_compaction_counts():
    tasks = [impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, _, flush_counts, compaction_counts in executor.map(process_impl, tasks):
            results[impl] = (flush_counts, compaction_counts)

    labels = list(results.keys())
    
    # Prep CSV data
    csv_rows = [[impl, results[impl][0], results[impl][1]] for i, impl in enumerate(labels)]
    dump_to_csv("stats_flush_compaction.csv", ["Implementation", "Flush Count", "Compaction Count"], csv_rows)

    num_impls = len(labels)
    num_phases = 2
    _, ax = plt.subplots(figsize=(4, 3))
    x = np.arange(num_phases)
    bar_width = 0.8 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = results[impl]
        xpos = x + j * bar_width
        ax.bar(xpos, heights, bar_width, facecolor="none" if impl != "vector-preallocated" else color, edgecolor=color, hatch=hatch_map.get(key, None), label=style["label"])

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels(["flush", "compaction"])
    ax.set_yticks([0, 50_000, 100_000], ["0", "5", "10"])
    ax.set_ylabel("count", labelpad=-6)
    plt.savefig(CURR_DIR / "disk-based-flush-and-compaction-counts.pdf", bbox_inches="tight", pad_inches=0.02)

def plot_throughput():
    tasks = [impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats, _, _ in executor.map(process_impl, tasks):
            results[impl] = stats

    labels = list(results.keys())
    
    # Prep CSV data
    csv_rows = [[impl] + results[impl] for i, impl in enumerate(labels)]
    headers = ["Implementation"] + [f"Phase {i+1} TP" for i in range(len(next(iter(results.values()))))]
    dump_to_csv("stats_throughput.csv", headers, csv_rows)

    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))
    _, ax = plt.subplots(figsize=(4, 3))
    x = np.arange(num_phases)
    bar_width = 0.8 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = results[impl]
        xpos = x + j * bar_width
        ax.bar(xpos, heights, bar_width, facecolor="none" if impl != "vector-preallocated" else color, edgecolor=color, hatch=hatch_map.get(key, None), label=style["label"])

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels([f"{i+1}" for i in range(num_phases)])
    ax.set_yticks([0, 50_000, 100_000], ["0", "5", "10"])
    ax.set_ylabel("throughput (ops)", labelpad=-1, loc="top")
    ax.set_xlabel("phase", labelpad=-1)
    plt.savefig(CURR_DIR / "disk-based-throughput.pdf", bbox_inches="tight", pad_inches=0.02)

def plot_write_amplification():
    tasks = [impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, wa_phases, ra_phases, stall_phases, movement_phases in executor.map(process_amplification_stats, tasks):
            if wa_phases:
                results[impl] = wa_phases

    labels = list(results.keys())
    if not labels: return
    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))

    csv_rows = [[impl] + results[impl] for impl in labels]
    headers = ["Implementation"] + [f"Phase {i+1} WA" for i in range(num_phases)]
    dump_to_csv("stats_write_amplification.csv", headers, csv_rows)

    _, ax = plt.subplots(figsize=(4, 3))
    x = np.arange(num_phases)
    bar_width = 0.8 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = results[impl]
        xpos = x + j * bar_width
        ax.bar(xpos, heights, bar_width, facecolor="none" if impl != "vector-preallocated" else color, 
               edgecolor=color, hatch=hatch_map.get(key, None), label=style["label"])

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels([f"{i+1}" for i in range(num_phases)])
    ax.set_ylabel("write amplification", labelpad=12, y=0.42)
    ax.set_xlabel("phase", labelpad=-1)
    
    output_file = CURR_DIR / "disk-based-write-amplification.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.1)
    print(f"Saved plot to {output_file}")

def plot_read_amplification():
    tasks = [impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, wa_phases, ra_phases, stall_phases, movement_phases in executor.map(process_amplification_stats, tasks):
            if ra_phases:
                results[impl] = ra_phases

    labels = list(results.keys())
    if not labels: return
    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))

    csv_rows = [[impl] + results[impl] for impl in labels]
    headers = ["Implementation"] + [f"Phase {i+1} RA" for i in range(num_phases)]
    dump_to_csv("stats_read_amplification.csv", headers, csv_rows)

    _, ax = plt.subplots(figsize=(4, 3))
    x = np.arange(num_phases)
    bar_width = 0.8 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = results[impl]
        xpos = x + j * bar_width
        ax.bar(xpos, heights, bar_width, facecolor="none" if impl != "vector-preallocated" else color, 
               edgecolor=color, hatch=hatch_map.get(key, None), label=style["label"])

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels([f"{i+1}" for i in range(num_phases)])
    ax.set_ylabel("read amplification", labelpad=12, y=0.42)
    ax.set_xlabel("phase", labelpad=-1)
    
    output_file = CURR_DIR / "disk-based-read-amplification.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.1)
    print(f"Saved plot to {output_file}")

def plot_write_stall():
    tasks = [impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, wa_phases, ra_phases, stall_phases, movement_phases in executor.map(process_amplification_stats, tasks):
            if stall_phases:
                results[impl] = stall_phases

    labels = list(results.keys())
    if not labels: return
    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))

    csv_rows = [[impl] + results[impl] for impl in labels]
    headers = ["Implementation"] + [f"Phase {i+1} Stall Sum (s)" for i in range(num_phases)]
    dump_to_csv("stats_write_stall.csv", headers, csv_rows)

    _, ax = plt.subplots(figsize=(4, 3))
    x = np.arange(num_phases)
    bar_width = 0.8 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = results[impl]
        xpos = x + j * bar_width
        ax.bar(xpos, heights, bar_width, facecolor="none" if impl != "vector-preallocated" else color, 
               edgecolor=color, hatch=hatch_map.get(key, None), label=style["label"])

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels([f"{i+1}" for i in range(num_phases)])
    
    ax.set_ylabel("write stall (s)", labelpad=12, y=0.42)
    ax.set_xlabel("phase", labelpad=-1)
    
    output_file = CURR_DIR / "disk-based-write-stall.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.1)
    print(f"Saved plot to {output_file}")

def plot_overall_datamovement():
    tasks = [impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, wa_phases, ra_phases, stall_phases, movement_phases in executor.map(process_amplification_stats, tasks):
            if movement_phases:
                results[impl] = movement_phases

    labels = list(results.keys())
    if not labels: return
    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))

    csv_rows = [[impl] + results[impl] for impl in labels]
    headers = ["Implementation"] + [f"Phase {i+1} Movement (GB)" for i in range(num_phases)]
    dump_to_csv("stats_overall_datamovement.csv", headers, csv_rows)

    _, ax = plt.subplots(figsize=(4, 3))
    x = np.arange(num_phases)
    bar_width = 0.8 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = results[impl]
        xpos = x + j * bar_width
        ax.bar(xpos, heights, bar_width, facecolor="none" if impl != "vector-preallocated" else color, 
               edgecolor=color, hatch=hatch_map.get(key, None), label=style["label"])

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels([f"{i+1}" for i in range(num_phases)])
    
    ax.set_ylabel("data movement (GB)", labelpad=12, y=0.42)
    ax.set_xlabel("phase", labelpad=-1)
    
    output_file = CURR_DIR / "disk-based-overall-datamovement.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.1)
    print(f"Saved plot to {output_file}")

def run_fig4_analysis():
    # Latency files
    files = [
        ("stats_pq_latencies.csv", "Point Query Latency", "ms"),
        ("stats_rq_latencies.csv", "Range Query Latency", "ms"),
        ("stats_insert_latencies.csv", "Insert Latency", "us")
    ]
    for filename, label, unit in files:
        path = CURR_DIR / filename
        if not path.exists():
            continue
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        print(f"\n--- Detailed Pairwise Analysis: {label} ---")
        for i in range(len(data)):
            for j in range(len(data)):
                if i == j: continue
                val_i = float(data[i][f'Mean ({unit})'])
                val_j = float(data[j][f'Mean ({unit})'])
                name_i = data[i]['Implementation']
                name_j = data[j]['Implementation']
                if val_i < val_j:
                    factor = val_j / val_i if val_i != 0 else float('inf')
                    improvement = (1 - val_i / val_j) * 100 if val_j != 0 else 100.0
                    print(f"{name_i} vs {name_j}: {factor:.2f}x lower latency ({improvement:.1f}% reduction)")
                else:
                    factor = val_i / val_j if val_j != 0 else float('inf')
                    overhead = (val_i / val_j - 1) * 100 if val_j != 0 else float('inf')
                    print(f"{name_i} vs {name_j}: {factor:.2f}x higher latency ({overhead:.1f}% overhead)")

    # Phase-based files (TP, WA, RA, Stalls, Movement)
    phasewise_files = [
        ("stats_throughput.csv", "Throughput", "ops"),
        ("stats_write_amplification.csv", "Write Amplification", "ratio"),
        ("stats_read_amplification.csv", "Read Amplification", "ratio"),
        ("stats_write_stall.csv", "Write Stall", "s"),
        ("stats_overall_datamovement.csv", "Overall Data Movement", "GB")
    ]
    
    for filename, label, unit in phasewise_files:
        path = CURR_DIR / filename
        if not path.exists(): continue
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        
        phases = [k for k in data[0].keys() if "Phase" in k]
        for phase in phases:
            print(f"\n--- Detailed Pairwise Analysis: {label} ({phase}) ---")
            for i in range(len(data)):
                for j in range(len(data)):
                    if i == j: continue
                    val_i = float(data[i][phase])
                    val_j = float(data[j][phase])
                    name_i = data[i]['Implementation']
                    name_j = data[j]['Implementation']
                    
                    if label == "Throughput":
                        if val_i > val_j:
                            factor = val_i / val_j if val_j != 0 else float('inf')
                            print(f"{name_i} vs {name_j}: {factor:.2f}x higher throughput")
                        else:
                            factor = val_j / val_i if val_i != 0 else float('inf')
                            print(f"{name_i} vs {name_j}: {factor:.2f}x lower throughput")
                    else: # For WA, RA, Stalls, and Movement, lower is better
                        if val_i < val_j:
                            factor = val_j / val_i if val_i != 0 else float('inf')
                            print(f"{name_i} vs {name_j}: {factor:.2f}x lower {label}")
                        else:
                            factor = val_i / val_j if val_j != 0 else float('inf')
                            print(f"{name_i} vs {name_j}: {factor:.2f}x higher {label}")

    # Count files
    count_path = CURR_DIR / "stats_flush_compaction.csv"
    if count_path.exists():
        with open(count_path, 'r') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        metrics = ["Flush Count", "Compaction Count"]
        for metric in metrics:
            print(f"\n--- Detailed Pairwise Analysis: {metric} ---")
            for i in range(len(data)):
                for j in range(len(data)):
                    if i == j: continue
                    val_i = float(data[i][metric])
                    val_j = float(data[j][metric])
                    name_i = data[i]['Implementation']
                    name_j = data[j]['Implementation']
                    if val_i < val_j:
                        factor = val_j / val_i if val_i != 0 else float('inf')
                        print(f"{name_i} vs {name_j}: {factor:.2f}x fewer {metric}")
                    else:
                        factor = val_i / val_j if val_j != 0 else float('inf')
                        print(f"{name_i} vs {name_j}: {factor:.2f}x more {metric}")


if __name__ == "__main__":
    sys.stdout = Logger("fig4_analysis_results.log")
    plot_flush_and_compaction_counts()
    plot_throughput()
    # plot_insert_latencies_violin()
    # plot_PQ_latencies()
    # plot_RQ_latencies()
    # plot_write_amplification()
    # plot_read_amplification()
    plot_overall_datamovement()
    # plot_write_stall()
    # run_fig4_analysis()