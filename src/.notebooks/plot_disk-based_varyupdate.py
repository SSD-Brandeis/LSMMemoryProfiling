import os
import sys
import csv
import json
import re
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor

# ==============================================================================
# PLOT CONFIGURATION
# ==============================================================================
PLOT_SCALE = "linear"  # Set to "linear" or "log" to switch scales

# ==============================================================================
# SETUP OUTPUT DIRECTORY BASED ON SCRIPT NAME
# ==============================================================================
SCRIPT_NAME = Path(__file__).stem
CURR_DIR = Path.cwd()
OUTPUT_DIR = CURR_DIR / SCRIPT_NAME
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class Logger(object):
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(OUTPUT_DIR / filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

from plot.rocksdb_stats import parse_rocksdb_log
from plot.style import hatch_map, line_styles  # Triggers __init__.py font setup

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1] 

ROOT_DIR = PROJECT_ROOT / "data_new"
EXP_DIR = ROOT_DIR / "diskbased_vary_update"

implementations = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "skiplist",
    "simpleskiplist",
    "hashlinkedlist-H100000-X6",
    "hashskiplist-H100000-X6",
    "hashvector-H100000-X6",
]

def dump_to_csv(filename, headers, rows):
    """Helper to dump plot data to CSV for paper usage."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"[DATA DUMP] Saved numeric values to {filepath}")

def normalize_name(name):
    name = name.lower()
    if "hash_linked_list" in name or "hashlinkedlist" in name:
        return "hashlinkedlist"
    if "hash_skip_list" in name or "hashskiplist" in name:
        return "hashskiplist"
    if "hash_vector" in name or "hashvector" in name:
        return "hashvector"
    if "simple_skiplist" in name or "simpleskiplist" in name:
        return "simpleskiplist"
    if "skiplist" in name and "simple" not in name and "hash" not in name:
        return "skiplist"
    if "unsortedvector" in name:
        return "unsortedvector"
    if "sortedvector" in name:
        return "alwayssortedvector"
    if "vector" in name and "hash" not in name and "unsorted" not in name and "sorted" not in name:
        return "vector"
    if "linkedlist" in name and "hash" not in name:
        return "linkedlist"
    return None

# ==============================================================================
# CORE PARSER: Holistic Run Parsing (Sums across phases)
# ==============================================================================
def process_run_metrics(args):
    impl, path = args
    metrics = {
        "throughput": np.nan,
        "flush_count": np.nan,
        "compaction_count": np.nan,
        "data_movement_gb": np.nan,
        "space_amplification": np.nan
    }
    
    parent_dir = os.path.dirname(path)
    specs_file = os.path.join(parent_dir, "workload.specs.json")
    
    total_ops = 0.0
    total_unique_inserts = 0.0
    
    if os.path.exists(specs_file):
        try:
            with open(specs_file, 'r') as f:
                specs = json.load(f)
                for group in specs.get('sections', [{}])[0].get('groups', []):
                    for op_type, op_data in group.items():
                        if isinstance(op_data, dict) and 'op_count' in op_data:
                            count = float(op_data['op_count'])
                            total_ops += count
                            if op_type == 'inserts':
                                total_unique_inserts += count
        except Exception as e:
            print(f"[ERROR] Failed to parse JSON for {impl}: {e}")
    
    log_files_to_check = ["workload_run.log", "rocksdb_stats.log", "stats.log", "workload.log"]
    content = ""
    for fname in log_files_to_check:
        fpath = os.path.join(path, fname)
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                content += "\n" + f.read()
                
    if not content:
        return impl, metrics

    total_time_ns = sum(float(m.group(1)) for m in re.finditer(r'Workload Execution Time:\s*(\d+)', content))
    if total_time_ns > 0 and total_ops > 0:
        metrics["throughput"] = total_ops / (total_time_ns / 1e9)
        
    sizes = re.findall(r'Column Family Name: default, Size:\s*(\d+)', content)
    if sizes and total_unique_inserts > 0:
        final_size = float(sizes[-1])
        metrics["space_amplification"] = final_size / (total_unique_inserts * 32)
        
    metrics["flush_count"] = sum(float(m.group(1)) for m in re.finditer(r'rocksdb\.db\.flush\.micros.+?COUNT\s*:\s*(\d+)', content))
    metrics["compaction_count"] = sum(float(m.group(1)) for m in re.finditer(r'rocksdb\.compaction\.times\.micros.+?COUNT\s*:\s*(\d+)', content))
    
    bw = sum(float(m.group(1)) for m in re.finditer(r'rocksdb\.bytes\.written\s+COUNT\s*:\s*(\d+)', content))
    br = sum(float(m.group(1)) for m in re.finditer(r'rocksdb\.bytes\.read\s+COUNT\s*:\s*(\d+)', content))
    cwb = sum(float(m.group(1)) for m in re.finditer(r'rocksdb\.compact\.write\.bytes\s+COUNT\s*:\s*(\d+)', content))
    crb = sum(float(m.group(1)) for m in re.finditer(r'rocksdb\.compact\.read\.bytes\s+COUNT\s*:\s*(\d+)', content))
    
    metrics["data_movement_gb"] = (bw + br + cwb + crb) / (1024**3)
    
    return impl, metrics

def generate_metrics_table():
    """Generates a structured table in the log for Overleaf conversion."""
    if not os.path.exists(EXP_DIR):
        print(f"[ERROR] The directory {EXP_DIR} does not exist.")
        return
        
    exp_folders = [d for d in os.listdir(EXP_DIR) if os.path.isdir(os.path.join(EXP_DIR, d))]
    valid_targets = set(normalize_name(impl) for impl in implementations if normalize_name(impl))
    
    all_data = {}
    percentages = set()
    metrics_keys = ["throughput", "data_movement_gb", "flush_count", "compaction_count", "space_amplification"]
    
    display_names = {
        "throughput": "ops",
        "data_movement_gb": "data movement (GB)",
        "flush_count": "flush count",
        "compaction_count": "compaction count",
        "space_amplification": "space amplification"
    }

    print("[PROCESSING] Gathering all metrics for table generation...")
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        for exp in exp_folders:
            match = re.search(r'varyupdate_(\d+)', exp)
            if not match: continue
            pct = int(match.group(1))
            percentages.add(pct)
            
            exp_path = os.path.join(EXP_DIR, exp)
            tasks = []
            for item in os.listdir(exp_path):
                full_path = os.path.join(exp_path, item)
                if os.path.isdir(full_path) and "buffer-" in item:
                    norm_item = normalize_name(item)
                    if norm_item and norm_item in valid_targets:
                        tasks.append((item, full_path))
                        
            if not tasks: continue
            
            for impl, metrics in executor.map(process_run_metrics, tasks):
                norm_impl = normalize_name(impl)
                if norm_impl not in all_data:
                    all_data[norm_impl] = {m: {} for m in metrics_keys}
                
                for mk in metrics_keys:
                    all_data[norm_impl][mk][pct] = metrics.get(mk, np.nan)

    sorted_pcts = sorted(list(percentages))
    sorted_impls = sorted(list(all_data.keys()))

    print("\n" + "="*100)
    print("METRICS DATA TABLE (FOR OVERLEAF)")
    print("="*100)
    
    header = f"{'Buffer':<25}"
    for mk in metrics_keys:
        name = display_names[mk]
        header += f" | {name} ({', '.join([f'{p}%' for p in sorted_pcts])})"
    
    print(header)
    print("-" * len(header))

    for impl in sorted_impls:
        row_str = f"{impl:<25}"
        for mk in metrics_keys:
            vals = []
            for pct in sorted_pcts:
                v = all_data[impl][mk].get(pct, 0)
                if np.isnan(v):
                    vals.append("N/A")
                elif mk == "space_amplification":
                    vals.append(f"{v:.3f}")
                else:
                    vals.append(f"{int(round(v))}")
            
            cell_content = "  ".join(vals)
            row_str += f" | {cell_content}"
        print(row_str)
    
    print("-" * len(header))
    print("="*100 + "\n")

# ==============================================================================
# UNIFIED LINE PLOT GENERATOR
# ==============================================================================
def generate_lineplot_for_metric(metric_key, ylabel, filename_suffix, cap_y=None, y_ticks=None, divisor=1, scale="linear", y_min=None):
    if not os.path.exists(EXP_DIR):
        print(f"[ERROR] The directory {EXP_DIR} does not exist.")
        return
        
    exp_folders = [d for d in os.listdir(EXP_DIR) if os.path.isdir(os.path.join(EXP_DIR, d))]
    valid_targets = set(normalize_name(impl) for impl in implementations if normalize_name(impl))
    
    data_by_impl = {}
    percentages = set()
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        for exp in exp_folders:
            match = re.search(r'varyupdate_(\d+)', exp)
            if not match: continue
            pct = int(match.group(1))
            percentages.add(pct)
            
            exp_path = os.path.join(EXP_DIR, exp)
            tasks = []
            for item in os.listdir(exp_path):
                full_path = os.path.join(exp_path, item)
                if os.path.isdir(full_path) and "buffer-" in item:
                    norm_item = normalize_name(item)
                    if norm_item and norm_item in valid_targets:
                        tasks.append((item, full_path))
                        
            if not tasks: continue
            
            print(f"[PROCESSING] {ylabel} ({scale}) for percentage {pct}%")
            for impl, metrics in executor.map(process_run_metrics, tasks):
                val = metrics.get(metric_key, np.nan)
                if impl not in data_by_impl: data_by_impl[impl] = {}
                data_by_impl[impl][pct] = val

    if not data_by_impl:
        print(f"[WARNING] No data parsed for {metric_key}.")
        return

    sorted_pcts = sorted(list(percentages))
    labels = sorted(list(data_by_impl.keys()))

    if scale == "linear":
        csv_name = f"stats_{filename_suffix}.csv"
        csv_rows = []
        headers = ["Implementation", "Percentage", "Value"]
        for impl in labels:
            for pct in sorted_pcts:
                val = data_by_impl[impl].get(pct, 0)
                csv_rows.append([impl, pct, val])
        dump_to_csv(csv_name, headers, csv_rows)

    fig, ax = plt.subplots(figsize=(5, 4))
    x_positions = np.arange(len(sorted_pcts))

    for impl in labels:
        key = normalize_name(impl)
        if not key: key = "vector" 
        style = line_styles.get(key, {"color": "black", "label": impl}) 
        
        y_vals = [data_by_impl[impl].get(pct, np.nan) / divisor for pct in sorted_pcts]
        ax.plot(x_positions, y_vals, label=style["label"], color=style.get("color", "black"), marker=style.get("marker", "o"), markersize=5, linewidth=1.5)

    ax.set_xticks(x_positions)
    ax.set_xticklabels([str(p) for p in sorted_pcts])
    
    if scale == "log":
        ax.set_yscale('log')
        ax.set_ylim(bottom=y_min if y_min is not None else 10**1)
    else:
        current_ymin = y_min if y_min is not None else 0
        ax.set_ylim(bottom=current_ymin)
        if cap_y is not None:
            ax.set_ylim(current_ymin, cap_y)
    
    if y_ticks is not None and scale == "linear":
        ax.set_yticks(y_ticks)
        ax.set_yticklabels([str(t) for t in y_ticks])
        
    ax.set_ylabel(ylabel, labelpad=8)
    ax.set_xlabel("update percentage")
    
    output_file = OUTPUT_DIR / f"disk-based-{filename_suffix}-{scale}-lineplot-update.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.1)
    print(f"Saved plot to {output_file.resolve()}")

    fig_leg = plt.figure(figsize=(8, 1))
    handles, labels_leg = ax.get_legend_handles_labels()
    by_label = dict(zip(labels_leg, handles))
    fig_leg.legend(by_label.values(), by_label.keys(), loc='center', ncol=4, frameon=False)
    plt.axis('off')
    
    legend_file = OUTPUT_DIR / f"disk-based-{filename_suffix}-legend-update.pdf"
    fig_leg.savefig(legend_file, bbox_inches="tight")
    plt.close('all')


def plot_space_amplification():
    generate_lineplot_for_metric("space_amplification", "space amplification", "space-amplification", cap_y=2.0, y_ticks=[1.0, 1.5, 2.0], divisor=1, scale=PLOT_SCALE, y_min=1)

def plot_throughput():
    generate_lineplot_for_metric("throughput", "throughput (k ops)", "throughput", divisor=1000, scale=PLOT_SCALE)

def plot_overall_datamovement():
    generate_lineplot_for_metric("data_movement_gb", "data movement (GB)", "overall-datamovement", divisor=1, scale=PLOT_SCALE)

def plot_flush_counts():
    generate_lineplot_for_metric("flush_count", "flush count (k)", "flush-counts", divisor=1000, scale=PLOT_SCALE)

def plot_compaction_counts():
    generate_lineplot_for_metric("compaction_count", "compaction count (k)", "compaction-counts", divisor=1000, scale=PLOT_SCALE)

def plot_flush_and_compaction_counts():
    plot_flush_counts()
    plot_compaction_counts()

if __name__ == "__main__":
    log_filename = f"{SCRIPT_NAME}.log"
    sys.stdout = Logger(log_filename)
    
    # Generate the table as requested
    generate_metrics_table()
    
    # Print the full clickable path to the log file on terminal
    full_log_path = (OUTPUT_DIR / log_filename).resolve()
    sys.stdout.terminal.write(f"\n[TABLE GENERATED] Full log path: file://{full_log_path}\n")

    # INDEPENDENT MODULES
    # plot_flush_and_compaction_counts()
    # plot_throughput()
    # plot_overall_datamovement()
    # plot_space_amplification()