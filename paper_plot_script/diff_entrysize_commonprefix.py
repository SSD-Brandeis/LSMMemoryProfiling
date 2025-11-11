# -*- coding: utf-8 -*-
"""
Plots range query throughput vs. entry size from experimental data.

This script processes data from benchmark runs where entry size is the
independent variable. It automatically discovers experiment directories,
calculates the average range query throughput for different buffer strategies,
and generates a plot comparing their performance.
"""

import sys
import re
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager

# --- 1. Configuration and Constants ---

# --- Path Setup ---
CURRENT_DIRECTORY = Path(__file__).resolve().parent
ROOT_DIRECTORY = CURRENT_DIRECTORY.parent if CURRENT_DIRECTORY.parent.name else CURRENT_DIRECTORY
sys.path.insert(0, str(ROOT_DIRECTORY))

# --- Data & Plot Directories ---
DATA_ROOT = ROOT_DIRECTORY / "data"
BASE_DATA_DIR = DATA_ROOT / "filter_result_diff_entrysize"
PLOTS_OUTPUT_ROOT = DATA_ROOT / "plots" / BASE_DATA_DIR.name

# --- Plot Styling ---
try:
    from notebooks.style_old import line_styles
except ImportError:
    print("⚠️ WARNING: 'style.py' not found. Plots will use default styles.", file=sys.stderr)
    line_styles = {}

# --- Constants ---
NANOSECONDS_TO_SECONDS = 1e-9
BUFFERS_TO_PLOT = ["hash_skip_list", "hash_linked_list"]
FIGURE_SIZE = (5, 3.6)
NUM_RUNS = 3

# --- Regular Expressions for Parsing ---
OPERATION_COUNT_PATTERNS = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point":  re.compile(r"\bQ(\d+)\b"),
    "range":  re.compile(r"\bS(\d+)\b"),
}
EXECUTION_TIME_PATTERN = re.compile(
    r"^(Inserts|PointQuery|RangeQuery)\s*Execution Time:\s*(\d+)\s*$",
    re.IGNORECASE
)
ENTRY_SIZE_PATTERN = re.compile(r'\bE(\d+)\b')

# --- 2. Setup Functions ---

def setup_publication_font():
    """Finds and sets the custom font for Matplotlib plots."""
    font_candidates = [
        CURRENT_DIRECTORY / "LinLibertine_Mah.ttf",
        ROOT_DIRECTORY / "LinLibertine_Mah.ttf",
        Path.home() / "Desktop/tectonic/LinLibertine_Mah.ttf",
    ]
    for font_path in font_candidates:
        if font_path.exists():
            prop = font_manager.FontProperties(fname=str(font_path))
            plt.rcParams["font.family"] = prop.get_name()
            print(f"✅ Using publication font found at: {font_path}")
            return
    print("❌ CRITICAL ERROR: Publication font 'LinLibertine_Mah.ttf' not found.", file=sys.stderr)
    sys.exit(1)

def setup_matplotlib_parameters():
    """Sets global Matplotlib parameters for consistent plot styling."""
    setup_publication_font()
    plt.rcParams["text.usetex"] = True
    plt.rcParams["font.weight"] = "normal"
    plt.rcParams["font.size"] = 22

# --- 3. Core Data Processing Logic ---

def parse_operation_counts_from_dir_name(experiment_directory: Path):
    dir_name = experiment_directory.name
    num_inserts = int(OPERATION_COUNT_PATTERNS["insert"].search(dir_name).group(1))
    num_point_queries = int(OPERATION_COUNT_PATTERNS["point"].search(dir_name).group(1))
    num_range_queries = int(OPERATION_COUNT_PATTERNS["range"].search(dir_name).group(1))
    return num_inserts, num_point_queries, num_range_queries

def parse_execution_times_from_log(log_content: str):
    times_in_ns = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0}
    for line in log_content.splitlines():
        match = EXECUTION_TIME_PATTERN.match(line.strip())
        if match:
            operation_kind, time_ns = match.groups()
            if operation_kind.lower().startswith("insert"): times_in_ns["Inserts"] = int(time_ns)
            elif operation_kind.lower().startswith("point"): times_in_ns["PointQuery"] = int(time_ns)
            elif operation_kind.lower().startswith("range"): times_in_ns["RangeQuery"] = int(time_ns)
    return times_in_ns

def calculate_average_metrics(data_directory: Path, phase: str, debug_context: str = ""):
    experiment_directory = data_directory.parent
    num_inserts, num_point_queries, num_range_queries = parse_operation_counts_from_dir_name(experiment_directory)

    throughput_runs, latency_runs = [], []
    for run_number in range(1, NUM_RUNS + 1):
        workload_log_path = data_directory / f"workload{run_number}.log"
        if not workload_log_path.exists():
            print(f"  [Debug] Warning: Log file not found at {workload_log_path}", file=sys.stderr)
            continue
        log_content = workload_log_path.read_text(errors="ignore")
        exec_times_ns = parse_execution_times_from_log(log_content)

        if phase == "I": op_count, time_ns = num_inserts, exec_times_ns["Inserts"]
        elif phase == "Q": op_count, time_ns = num_point_queries, exec_times_ns["PointQuery"]
        elif phase == "S": op_count, time_ns = num_range_queries, exec_times_ns["RangeQuery"]
        else: op_count, time_ns = 0, 0

        if op_count > 0 and time_ns > 0:
            time_sec = time_ns * NANOSECONDS_TO_SECONDS
            throughput = op_count / time_sec
            if phase == "S":
                print(f"  [Debug] Run {run_number} ({debug_context}): Ops={op_count} | Raw Time={time_ns:,} ns | Throughput={throughput:,.2f} ops/sec")
            throughput_runs.append(throughput)
            latency_runs.append(time_ns / op_count)

    avg_throughput = float(np.mean(throughput_runs)) if throughput_runs else 0.0
    return avg_throughput, float(np.mean(latency_runs)) if latency_runs else 0.0

# --- 4. Plotting Functions ---

def create_plot_figure():
    """Creates a Matplotlib figure and axis with standard adjustments."""
    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.95, bottom=0.4)
    return fig, ax

def apply_legend(figure):
    """Applies a standard bottom-centered legend to the figure."""
    figure.legend(
        loc="lower center",
        ncol=1,
        bbox_to_anchor=(0.5, -0.3),
        frameon=False,
        labelspacing=0.4,
        handletextpad=0.6,
        columnspacing=1.0,
    )

def plot_throughput_vs_entry_size(data, plots_dir, out_filename, y_axis_limit=None):
    """Plots Range Query throughput vs. Entry Size in a categorical manner."""
    fig, ax = create_plot_figure()

    if not any(data.values()):
        print("❌ Error: No data was successfully processed. Cannot generate plot.", file=sys.stderr)
        return

    first_buffer_with_data = next((b for b in BUFFERS_TO_PLOT if data.get(b)), None)
    if not first_buffer_with_data:
        print("❌ Error: No data found for any specified buffer.", file=sys.stderr)
        return
        
    sorted_entry_sizes = [item[0] for item in data[first_buffer_with_data]]
    x_indices = np.arange(len(sorted_entry_sizes))

    for buffer_type in BUFFERS_TO_PLOT:
        if buffer_type in data and data[buffer_type]:
            throughput_values = [item[1] for item in data[buffer_type]]
            style = line_styles.get(buffer_type, {}).copy()
            if 'label' not in style:
                style['label'] = buffer_type.replace('_', ' ').title()
            ax.plot(x_indices, throughput_values, **style)

    ax.set_xlabel("Entry Size (Bytes)", labelpad=15)
    ax.set_ylabel("RQ Throughput (ops/sec)")
    ax.set_xticks(x_indices)
    ax.set_xticklabels(sorted_entry_sizes, rotation=30, ha="right")

    if y_axis_limit:
        ax.set_ylim(*y_axis_limit)

    apply_legend(fig)

    output_path = plots_dir / f"{out_filename}.pdf"
    # FIX: Re-added bbox_inches='tight' to ensure the legend is included in the saved file.
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"✅ Plot saved to: {output_path}")

# --- 5. Main Execution ---

def main():
    """Main function to discover datasets, process them, and generate the plot."""
    setup_matplotlib_parameters()

    plots_directory = PLOTS_OUTPUT_ROOT
    plots_directory.mkdir(parents=True, exist_ok=True)
    print(f"Processing data from: {BASE_DATA_DIR}")
    print(f"  -> Saving plots to: {plots_directory}")

    results = {buffer_type: [] for buffer_type in BUFFERS_TO_PLOT}
    
    experiment_dirs = sorted(list(BASE_DATA_DIR.glob('exp_*')))
    if not experiment_dirs:
        print(f"❌ CRITICAL ERROR: No experiment directories matching 'exp_*' found in {BASE_DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    for exp_dir in experiment_dirs:
        match = ENTRY_SIZE_PATTERN.search(exp_dir.name)
        if not match:
            print(f"⚠️ Warning: Could not parse entry size from '{exp_dir.name}'. Skipping.")
            continue
        entry_size = int(match.group(1))

        for buffer_type in BUFFERS_TO_PLOT:
            data_subdir = exp_dir / buffer_type
            if not data_subdir.exists():
                print(f"⚠️ Warning: Data directory not found for {buffer_type} in {exp_dir.name}. Skipping.")
                continue

            debug_context_str = f"{buffer_type}, E={entry_size}"
            avg_throughput, _ = calculate_average_metrics(data_subdir, "S", debug_context=debug_context_str)
            if avg_throughput > 0:
                results[buffer_type].append((entry_size, avg_throughput))

    for buffer_type in BUFFERS_TO_PLOT:
        results[buffer_type].sort(key=lambda x: x[0])

    plot_throughput_vs_entry_size(
        results,
        plots_directory,
        "rq_tput_vs_entry_size",
        y_axis_limit=(0, 60000)
    )

if __name__ == "__main__":
    main()