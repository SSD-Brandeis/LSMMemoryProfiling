# -*- coding: utf-8 -*-
"""
Plots range query throughput vs. common prefix length.
Targeting workload1.log for performance metrics.
"""
import re
import sys
from pathlib import Path
import numpy as np
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
from matplotlib import font_manager

# --- Import plotting setup ---
try:
    from plot import *
except ImportError:
    print("Warning: 'plot.py' not found. Using local style definitions.")
    EXP_DIR = Path(".")

# --- Path Setup ---
CURRENT_WORKING_DIR = Path.cwd()
CURRENT_DIRECTORY = Path(__file__).resolve().parent
BASE_DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_mar16_commonprefix_fig16D")
SCRIPT_NAME = Path(__file__).stem
PLOTS_OUTPUT_ROOT = CURRENT_DIRECTORY / "paper_plot" / SCRIPT_NAME

# --- Font Loading & Strict Control ---
FONT_CANDIDATES = [
    CURRENT_DIRECTORY / "LinLibertine_Mah.ttf",
    CURRENT_DIRECTORY.parent / "LinLibertine_Mah.ttf",
    Path.home() / "Desktop/tectonic/LinLibertine_Mah.ttf",
]

font_found = False
for fp in FONT_CANDIDATES:
    if fp.exists():
        prop = font_manager.FontProperties(fname=str(fp))
        plt.rcParams["font.family"] = prop.get_name()
        print(f"✅ Using font: {fp.resolve()}")
        font_found = True
        break

if not font_found:
    print("❌ CRITICAL ERROR: Publication font 'LinLibertine_Mah.ttf' not found. Aborting.")
    sys.exit(1)

plt.rcParams["text.usetex"] = True
plt.rcParams["font.size"] = 18

# --- STYLE DEFINITIONS ---
line_styles = {
    "Vector": {"label": "vector", "color": "#006d2c", "linestyle": (0, (1, 1)), "marker": "x", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "hash_vector": {"label": "hash vector", "color": "#006d2c", "linestyle": (0, (1, 1)), "marker": "x", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "skiplist": {"label": "skip list", "color": "#6a3d9a", "linestyle": "-", "marker": "o", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "hash_skip_list": {"label": "hash skip list", "color": "#1f78b4", "linestyle": "--", "marker": "^", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "hash_linked_list": {"label": "hash linked list", "color": "#b22222", "linestyle": "-.", "marker": "D", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "hash_linked_list_optimized": {"label": "hash linked list optimized", "color": "#d62728", "linestyle": "-", "marker": "s", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
}

# --- EXPERIMENT CONFIGS ---
DATASET_CONFIGURATIONS = [
    {
        "stats_directory": BASE_DATA_DIR / "common_prefix_varcD_sequential_totalorderseek_true-varC-Insert740000-PQ0-RQ1000-S0.1",
        "hybrid_directory": BASE_DATA_DIR / "common_prefix_varcD_sequential_totalorderseek_hybrid-varC-Insert740000-PQ0-RQ1000-S0.1",
        "plots_directory": PLOTS_OUTPUT_ROOT / "fig16D_results",
        "output_filename_prefix": "rq_tput_vs_common_prefix_fig16D",
        "y_axis_limit_rq": (0, None), 
        "x_axis_range": (0, 6),
        "single_point": True, 
    }
]

# --- CONSTANTS & REGEX ---
NANOSECONDS_TO_SECONDS = 1e-9
BUFFERS_TO_PLOT = ["hash_skip_list", "hash_linked_list", "hash_vector"]
FIGURE_SIZE = (5, 3.6)
NUM_RUNS = 1 

# Extracting counts from folder name
OPERATION_COUNT_PATTERNS = {
    "insert": re.compile(r"Insert(\d+)|I(\d+)"),
    "point": re.compile(r"PQ(\d+)|Q(\d+)"),
    "range": re.compile(r"RQ(\d+)|S(\d+)"),
}

# Matching exactly: "RangeQuery Execution Time: 133304836052"
EXECUTION_TIME_PATTERN = re.compile(r"(Inserts|PointQuery|RangeQuery|Scan)\s*Execution Time:\s*(\d+)", re.IGNORECASE)

def parse_operation_counts_from_dir_name(experiment_directory: Path):
    dir_name = experiment_directory.name
    counts = {"insert": 0, "point": 0, "range": 0}
    for key, pattern in OPERATION_COUNT_PATTERNS.items():
        match = pattern.search(dir_name)
        if match:
            val = next((g for g in match.groups() if g is not None), 0)
            counts[key] = int(val)
    return counts["insert"], counts["point"], counts["range"]

def parse_execution_times_from_log(log_content: str):
    times_in_ns = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0}
    for line in log_content.splitlines():
        match = EXECUTION_TIME_PATTERN.search(line)
        if match:
            op_kind, time_ns = match.groups()
            op_kind = op_kind.lower()
            if "insert" in op_kind: times_in_ns["Inserts"] = int(time_ns)
            elif "point" in op_kind: times_in_ns["PointQuery"] = int(time_ns)
            elif "range" in op_kind or "scan" in op_kind: times_in_ns["RangeQuery"] = int(time_ns)
    return times_in_ns

def calculate_average_metrics(data_directory: Path, phase: str):
    experiment_directory = data_directory.parent
    num_inserts, num_point_queries, num_range_queries = parse_operation_counts_from_dir_name(experiment_directory)
    throughput_runs = []
    
    for run_number in range(1, NUM_RUNS + 1):
        # Targeting workload1.log as per your input
        log_path = data_directory / f"workload{run_number}.log" 
        if not log_path.exists(): 
            continue
        
        log_content = log_path.read_text(errors="ignore")
        exec_times_ns = parse_execution_times_from_log(log_content)
        
        if phase == "I": op_count, time_ns = num_inserts, exec_times_ns["Inserts"]
        elif phase == "Q": op_count, time_ns = num_point_queries, exec_times_ns["PointQuery"]
        else: op_count, time_ns = num_range_queries, exec_times_ns["RangeQuery"]
        
        if op_count > 0 and time_ns > 0:
            throughput_runs.append(op_count / (time_ns * NANOSECONDS_TO_SECONDS))
        else:
            # Helpful diagnostic if data is still missing
            print(f"      ⚠️ Zero data for {data_directory.name}: OpCount={op_count}, Time={time_ns}ns")
            
    return float(np.mean(throughput_runs)) if throughput_runs else 0.0

def plot_throughput_vs_common_prefix(exp_dir, hybrid_dir, plots_dir, out_filename_base, y_axis_limit=None, x_axis_range=None, single_point=False):
    x_values = list(range(x_axis_range[0], x_axis_range[1] + 1))

    for buffer_type in BUFFERS_TO_PLOT:
        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.35)
        
        y_default, y_hybrid = [], []
        
        for prefix in x_values:
            p_std = exp_dir / f"{buffer_type}-X6-H1M-C{prefix}"
            y_default.append(calculate_average_metrics(p_std, "S"))

            val_hybrid = np.nan
            if single_point and hybrid_dir and prefix == 6:
                p_hybrid = hybrid_dir / f"{buffer_type}-X6-H1M-C{{6}}"
                val_hybrid = calculate_average_metrics(p_hybrid, "S")
            y_hybrid.append(val_hybrid)

        # Dynamic Headroom Logic
        all_y = [v for v in y_default if v > 0] + [v for v in y_hybrid if np.isfinite(v) and v > 0]
        max_y = max(all_y) if all_y else 1
        upper_limit = max_y * 1.15

        base_style = line_styles.get(buffer_type, {}).copy()
        base_label = base_style.pop("label", buffer_type)

        if any(v > 0 for v in y_default):
            ax.plot(x_values, y_default, label=f"{base_label} (true)", **base_style)

        if single_point and any(np.isfinite(y_hybrid)) and np.nanmax(y_hybrid) > 0:
            h_style = base_style.copy()
            h_style.update({"linestyle": "None"}) 
            if buffer_type == "hash_skip_list": h_style.update({"color": "tab:orange", "marker": "o"})
            elif buffer_type == "hash_linked_list": h_style.update({"color": "tab:green", "marker": "s"})
            elif buffer_type == "hash_vector": h_style.update({"color": "tab:red", "marker": "x"})
            
            valid_x = [x for x, y in zip(x_values, y_hybrid) if y > 0]
            valid_y = [y for y in y_hybrid if y > 0]
            if valid_x:
                ax.plot(valid_x, valid_y, label=f"{base_label} (hybrid)", **h_style)

        ax.set_xlabel("Common Prefix Length ($C$)")
        ax.set_ylabel("RQ Throughput (ops/s)")
        
        # Explicit Tick Control
        ax.set_xticks(x_values)
        ax.set_ylim(0, upper_limit)
        ax.set_yticks(np.linspace(0, upper_limit, 5))
        
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{x/1000:.1f}K' if x >= 1000 else f'{x:.0f}'))

        current_filename = f"{out_filename_base}_{buffer_type}"
        plot_path = plots_dir / f"{current_filename}.pdf"
        fig.savefig(plot_path, bbox_inches="tight", pad_inches=0.02)
        print(f"✅ Plot saved: {plot_path.name}")

        handles, labels = ax.get_legend_handles_labels()
        if handles:
            legend_fig = plt.figure(figsize=(9, 0.5))
            legend_fig.legend(handles, labels, loc="center", ncol=len(handles), frameon=False)
            plt.axis('off')
            legend_path = plots_dir / f"{current_filename}_legend.pdf"
            legend_fig.savefig(legend_path, bbox_inches="tight", pad_inches=0.01)
            plt.close(legend_fig)
        
        plt.close(fig)

if __name__ == "__main__":
    for config in DATASET_CONFIGURATIONS:
        plots_dir = config["plots_directory"]
        plots_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nProcessing: {config['stats_directory'].name}")
        
        plot_throughput_vs_common_prefix(
            config["stats_directory"],
            config.get("hybrid_directory"),
            plots_dir,
            config["output_filename_prefix"],
            y_axis_limit=config.get("y_axis_limit_rq"),
            x_axis_range=config.get("x_axis_range"),
            single_point=config.get("single_point", False)
        )