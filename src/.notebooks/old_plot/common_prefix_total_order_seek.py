# -*- coding: utf-8 -*-
"""
Plots range query throughput vs. common prefix length.
Simplified legend naming.
"""

import re
from pathlib import Path
import numpy as np
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt

# --- Import plotting setup ---
try:
    from plot import *
except ImportError:
    print("Warning: 'plot.py' not found. Using local style definitions.")
    EXP_DIR = Path(".")

# --- Path Setup ---
CURRENT_WORKING_DIR = Path.cwd()
CURRENT_DIRECTORY = Path(__file__).resolve().parent
BASE_DATA_DIR = EXP_DIR / "filter_result_diff_keysize_commonprefix"
TOTAL_ORDER_FALSE_ROOT = CURRENT_WORKING_DIR / "data" / "filter_result_totalorder_false"
PLOTS_OUTPUT_ROOT = CURRENT_DIRECTORY / "paper_plot" /"commonprefix_totalorderseek"

# --- STYLE DEFINITIONS ---
line_styles = {
    "Vector": {"label": "vector", "color": "#006d2c", "linestyle": (0, (1, 1)), "marker": "x", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "skiplist": {"label": "skip list", "color": "#6a3d9a", "linestyle": "-", "marker": "o", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "hash_skip_list": {"label": "hash skip list", "color": "#1f78b4", "linestyle": "--", "marker": "^", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "hash_linked_list": {"label": "hash linked list", "color": "#b22222", "linestyle": "-.", "marker": "D", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "hash_linked_list_optimized": {"label": "hash linked list optimized", "color": "#d62728", "linestyle": "-", "marker": "s", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "AlwaysSortedVector": {"label": "always sorted vector", "color": "#8b4513", "linestyle": (0, (3, 1, 1, 1)), "marker": "s", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
    "UnsortedVector": {"label": "unsorted vector", "color": "#4d4d4d", "linestyle": (0, (5, 2)), "marker": "v", "markersize": 12, "markerfacecolor": "none", "linewidth": 1},
}

# --- EXPERIMENT CONFIGS ---
DATASET_CONFIGURATIONS = [
    {
        "stats_directory": BASE_DATA_DIR / "common_prefix_keysize_128_value_896-insertPQRS_X8H1M_varC-lowpri_false-I480000-U0-Q0-S100-Y0.001-T10-P131072-B4-E1024",
        "total_order_false_directory": None,
        "plots_directory": PLOTS_OUTPUT_ROOT / "plots_keysize_128",
        "output_filename_prefix": "rq_tput_vs_common_prefix_keysize128",
        "caption": "k_128_v_896-_X8H1M_-I480K-S100-B4-E1024",
        "y_axis_limit_rq": (0, 400000),
        "x_axis_range": (0, 8),
        "single_point": True,
    },
    {
        "stats_directory": BASE_DATA_DIR / "common_prefix_keysize_8_value_1016-insertPQRS_X8H1M_varC-lowpri_false-I480000-U0-Q0-S100-Y0.001-T10-P131072-B4-E1024",
        "total_order_false_directory": None,
        "plots_directory": PLOTS_OUTPUT_ROOT / "plots_keysize_8_val_1016",
        "output_filename_prefix": "rq_tput_vs_common_prefix_keysize8_val1016",
        "caption": "k_8_v_1016-_X8H1M_-I480K-S100-B4-E1024",
        "y_axis_limit_rq": (0, 400000),
        "x_axis_range": (0, 8),
        "single_point": True,
    },
    {
        "stats_directory": BASE_DATA_DIR / "common_prefix_keysize_8_value_24-insertPQRS_X8H1M_varC-lowpri_false-I6000000-U0-Q0-S100-Y0.001-T10-P131072-B128-E32",
        "total_order_false_directory": TOTAL_ORDER_FALSE_ROOT / "common_prefix_keysize_8_value_24_totalorderseek_false-insertPQRS_X8H1M_varC-lowpri_false-I6000000-U0-Q0-S100-Y0.001-T10-P131072-B128-E32",
        "plots_directory": PLOTS_OUTPUT_ROOT / "plots_keysize_8_val_24",
        "output_filename_prefix": "rq_tput_vs_common_prefix_keysize8_val24",
        "caption": "k8_v_24-_X8H1M_-I6M-S100-B128-E32",
        "y_axis_limit_rq": (0, 400000),
        "x_axis_range": (0, 8),
        "single_point": True,
    },
]

# --- CONSTANTS & REGEX ---
NANOSECONDS_TO_SECONDS = 1e-9
BUFFERS_TO_PLOT = ["hash_skip_list", "hash_linked_list"]
FIGURE_SIZE = (5, 3.6)
DEFAULT_COMMON_PREFIX_VALUES = list(range(0, 9))
NUM_RUNS = 3

OPERATION_COUNT_PATTERNS = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point": re.compile(r"\bQ(\d+)\b"),
    "range": re.compile(r"\bS(\d+)\b"),
}
EXECUTION_TIME_PATTERN = re.compile(r"^(Inserts|PointQuery|RangeQuery)\s*Execution Time:\s*(\d+)\s*$", re.IGNORECASE)

# --- PARSING HELPER FUNCTIONS ---
def parse_operation_counts_from_dir_name(experiment_directory: Path):
    dir_name = experiment_directory.name
    try:
        num_inserts = int(OPERATION_COUNT_PATTERNS["insert"].search(dir_name).group(1))
        num_point_queries = int(OPERATION_COUNT_PATTERNS["point"].search(dir_name).group(1))
        num_range_queries = int(OPERATION_COUNT_PATTERNS["range"].search(dir_name).group(1))
        return num_inserts, num_point_queries, num_range_queries
    except AttributeError:
        return 0, 0, 0

def parse_execution_times_from_log(log_content: str):
    times_in_ns = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0}
    for line in log_content.splitlines():
        match = EXECUTION_TIME_PATTERN.match(line.strip())
        if match:
            op_kind, time_ns = match.groups()
            if op_kind.lower().startswith("insert"): times_in_ns["Inserts"] = int(time_ns)
            elif op_kind.lower().startswith("point"): times_in_ns["PointQuery"] = int(time_ns)
            elif op_kind.lower().startswith("range"): times_in_ns["RangeQuery"] = int(time_ns)
    return times_in_ns

def calculate_average_metrics(data_directory: Path, phase: str):
    experiment_directory = data_directory.parent
    num_inserts, num_point_queries, num_range_queries = parse_operation_counts_from_dir_name(experiment_directory)
    throughput_runs = []
    
    for run_number in range(1, NUM_RUNS + 1):
        workload_log_path = data_directory / f"workload{run_number}.log"
        if not workload_log_path.exists(): continue
        
        log_content = workload_log_path.read_text(errors="ignore")
        exec_times_ns = parse_execution_times_from_log(log_content)
        
        if phase == "I": op_count, time_ns = num_inserts, exec_times_ns["Inserts"]
        elif phase == "Q": op_count, time_ns = num_point_queries, exec_times_ns["PointQuery"]
        else: op_count, time_ns = num_range_queries, exec_times_ns["RangeQuery"]
        
        if op_count > 0 and time_ns > 0:
            throughput_runs.append(op_count / (time_ns * NANOSECONDS_TO_SECONDS))
            
    return float(np.mean(throughput_runs)) if throughput_runs else 0.0

# --- MAIN PLOTTING FUNCTION ---
def plot_throughput_vs_common_prefix(exp_dir, total_order_false_dir, plots_dir, out_filename_base, caption_text="", y_axis_limit=None, x_axis_range=None, single_point=False):
    
    x_values_to_plot = list(range(x_axis_range[0], x_axis_range[1] + 1)) if x_axis_range else DEFAULT_COMMON_PREFIX_VALUES

    for buffer_type in BUFFERS_TO_PLOT:
        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.35)
        
        y_default, y_hybrid, y_false = [], [], []
        
        # --- DATA COLLECTION ---
        for prefix in x_values_to_plot:
            # 1. Standard (C0-C8)
            p_std = exp_dir / f"{buffer_type}-X8-H1000000-C{prefix}"
            val_std = calculate_average_metrics(p_std, "S") if p_std.exists() else np.nan
            
            # 2. Old (C8 Default)
            val_old = np.nan
            if single_point and prefix == 8:
                p_old = exp_dir / f"{buffer_type}-X8-H1000000-C8-old"
                val_old = calculate_average_metrics(p_old, "S") if p_old.exists() else np.nan

            # 3. Seek False
            val_false = np.nan
            if total_order_false_dir:
                p_false = total_order_false_dir / f"{buffer_type}-X8-H1000000-C{prefix}"
                val_false = calculate_average_metrics(p_false, "S") if p_false.exists() else np.nan

            y_false.append(val_false)

            if prefix == 8 and single_point:
                y_default.append(val_old)     # C8 Default
                y_hybrid.append(val_std)      # C8 Hybrid
            else:
                y_default.append(val_std)     # C0-C7 Default
                y_hybrid.append(val_std)      # C0-C7 Hybrid (Overlay)

        # --- GET BASE STYLE ---
        base_style = line_styles.get(buffer_type, {}).copy()
        base_label_name = base_style.pop("label", buffer_type) # Remove label from dict so we can hardcode it below

        # --- 1. PLOT DEFAULT LINE ---
        if any(np.isfinite(y_default)):
            ax.plot(x_values_to_plot, y_default, 
                    label=f"{base_label_name} (total_order_seek true)",  # <--- HARDCODED
                    **base_style)

        # --- 2. PLOT HYBRID LINE ---
        if single_point and any(np.isfinite(y_hybrid)):
            hybrid_style = base_style.copy()
            # Hardcode overrides for Hybrid
            if buffer_type == "hash_skip_list":
                hybrid_style.update({"color": "tab:orange", "marker": "o"})
            elif buffer_type == "hash_linked_list":
                hybrid_style.update({"color": "tab:green", "marker": "s"})
            
            ax.plot(x_values_to_plot, y_hybrid, 
                    label=f"{base_label_name} (total_order_seek hybrid)", # <--- HARDCODED
                    **hybrid_style)

        # --- 3. PLOT FALSE LINE (Commented Block) ---
        """
        if any(np.isfinite(y_false)):
            false_style = base_style.copy()
            # Hardcode overrides for False
            if buffer_type == "hash_skip_list":
                false_style.update({"color": "#e377c2", "marker": "x"})
            elif buffer_type == "hash_linked_list":
                false_style.update({"color": "black", "marker": "p"})
            
            ax.plot(x_values_to_plot, y_false, 
                    label=f"{base_label_name} (total_order_seek false)", # <--- HARDCODED
                    **false_style)
        """
        
        # --- FORMATTING ---
        ax.set_xlabel("Common Prefix Length ($C$)")
        ax.set_ylabel("RQ Throughput (ops/s)")
        ax.set_xticks(x_values_to_plot)
        if y_axis_limit: ax.set_ylim(*y_axis_limit)
        if x_axis_range: ax.set_xlim(x_values_to_plot[0] - 0.5, x_values_to_plot[-1] + 0.5)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x/1000:.0f}K'))

        # --- SAVE ---
        current_filename = f"{out_filename_base}_{buffer_type}"
        plot_path = plots_dir / f"{current_filename}.pdf"
        fig.savefig(plot_path, bbox_inches="tight", pad_inches=0.02)
        print(f"✅ Plot saved: {plot_path.resolve()}")

        # Legend
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            legend_fig = plt.figure(figsize=(9, 0.5))
            legend_fig.legend(handles, labels, loc="center", ncol=len(handles), frameon=False, labelspacing=0.3, handletextpad=0.6, columnspacing=1.0)
            plt.axis('off')
            legend_path = plots_dir / f"{current_filename}_legend.pdf"
            legend_fig.savefig(legend_path, bbox_inches="tight", pad_inches=0.01)
            print(f"✅ Legend saved: {legend_path.resolve()}")
            plt.close(legend_fig)

        # Caption
        if caption_text:
            title_fig = plt.figure(figsize=(FIGURE_SIZE[0], 0.5))
            title_fig.text(0.5, 0.5, caption_text, ha='center', va='center')
            plt.axis('off')
            caption_path = plots_dir / f"{current_filename}_caption.pdf"
            title_fig.savefig(caption_path, bbox_inches="tight", pad_inches=0.02)
            print(f"✅ Caption saved: {caption_path.resolve()}")
            plt.close(title_fig)
        
        plt.close(fig)

# --- RUN ---
def format_caption(old_caption):
    insert = re.search(r"-I(\d+K|\d+M)\b", old_caption)
    scan = re.search(r"-S(\d+)\b", old_caption)
    parts = []
    if insert: parts.append(f"I={insert.group(1)}")
    if scan: parts.append(f"RQ={scan.group(1)}")
    return " ".join(parts) if parts else old_caption

if __name__ == "__main__":
    for config in DATASET_CONFIGURATIONS:
        plots_dir = config["plots_directory"]
        plots_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nProcessing: {config['stats_directory'].name}")
        
        plot_throughput_vs_common_prefix(
            config["stats_directory"],
            config["total_order_false_directory"],
            plots_dir,
            config["output_filename_prefix"],
            caption_text=format_caption(config.get("caption", "")),
            y_axis_limit=config.get("y_axis_limit_rq"),
            x_axis_range=config.get("x_axis_range"),
            single_point=config.get("single_point", False)
        )