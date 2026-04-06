# -*- coding: utf-8 -*-
"""
Plots range query throughput vs. common prefix length from experimental data.

This script processes data from benchmark runs, calculates throughput,
and generates plots to visualize the performance of different buffer strategies
under varying key prefix conditions.
"""

import re
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.ticker as ticker

# --- Import plotting setup ---
# This imports plt, matplotlib, EXP_DIR, line_styles, bar_styles
# and applies all rcParams (font, usetex, etc.) from notebooks/plot/__init__.py
try:
    from plot import *
except ImportError:
    print("Warning: 'plot.py' not found. Using default matplotlib styles.")
    import matplotlib.pyplot as plt
    # Define fallback styles if 'plot.py' is essential and missing
    from collections import defaultdict
    line_styles = defaultdict(lambda: {"linestyle": "-", "marker": "o"})
    # Define fallback EXP_DIR
    EXP_DIR = Path(".")


# --- Path Setup ---
CURRENT_DIRECTORY = Path(__file__).resolve().parent

# --- Data & Plot Directories ---
# Use EXP_DIR imported from plot/__init__.py
BASE_DATA_DIR = EXP_DIR / "filter_result_diff_keysize_commonprefix"
# Plots are saved relative to this script's location
PLOTS_OUTPUT_ROOT = CURRENT_DIRECTORY / "paper_plot"

# --- Plot Styling ---
# 'line_styles' is now imported via 'from plot import *'

# --- Experiment Configurations ---
# A list of dictionaries, each defining a set of experiments to plot.
DATASET_CONFIGURATIONS = [
    {
        "stats_directory": BASE_DATA_DIR
        / "common_prefix_keysize_128_value_896-insertPQRS_X8H1M_varC-lowpri_false-I480000-U0-Q0-S100-Y0.001-T10-P131072-B4-E1024",
        "plots_directory": PLOTS_OUTPUT_ROOT / "plots_keysize_128",
        "output_filename_prefix": "rq_tput_vs_common_prefix_keysize128",
        # MODIFIED: This will be parsed into "I=480K RQ=100"
        "caption": "k_128_v_896-_X8H1M_-I480K-S100-B4-E1024",
        "y_axis_limit_rq": (0, 400000),
        "x_axis_range": (0, 8),
        "single_point": True,
    },
    {
        "stats_directory": BASE_DATA_DIR
        / "common_prefix_keysize_8_value_1016-insertPQRS_X8H1M_varC-lowpri_false-I480000-U0-Q0-S100-Y0.001-T10-P131072-B4-E1024",
        "plots_directory": PLOTS_OUTPUT_ROOT / "plots_keysize_8_val_1016",
        "output_filename_prefix": "rq_tput_vs_common_prefix_keysize8_val1016",
        # MODIFIED: This will be parsed into "I=480K RQ=100"
        "caption": "k_8_v_1016-_X8H1M_-I480K-S100-B4-E1024",
        "y_axis_limit_rq": (0, 400000),
        "x_axis_range": (0, 8),
        "single_point": True,
    },
    {
        "stats_directory": BASE_DATA_DIR
        / "common_prefix_keysize_8_value_24-insertPQRS_X8H1M_varC-lowpri_false-I6000000-U0-Q0-S100-Y0.001-T10-P131072-B128-E32",
        "plots_directory": PLOTS_OUTPUT_ROOT / "plots_keysize_8_val_24",
        "output_filename_prefix": "rq_tput_vs_common_prefix_keysize8_val24",
        # MODIFIED: This will be parsed into "I=6M RQ=100"
        "caption": "k8_v_24-_X8H1M_-I6M-S100-B128-E32",
        "y_axis_limit_rq": (0, 400000),
        "x_axis_range": (0, 8),
        "single_point": True,
    },
    # {
    #     "stats_directory": BASE_DATA_DIR / "common_prefix_keysize_8_value_24-insertPQRS_X8H1M_varC-lowpri_false-I6000000-U0-Q0-S100-Y0.001-T10-P131072-B128-E32",
    #     "plots_directory": PLOTS_OUTPUT_ROOT / "plots_keysize_8_val_24",
    #     "output_filename_prefix": "rq_tput_vs_common_prefix_keysize8_val24_single_point_C8",
    #     "caption": "Key Size 8, Value Size 24 (Single Point)",
    #     "y_axis_limit_rq": (0, 400000),
    #     "x_axis_range": (0, 8),
    #     "single_point": True
    # },
]

# --- Constants ---
NANOsONDS_TO_sONDS = 1e-9
BUFFERS_TO_PLOT = ["hash_skip_list", "hash_linked_list"]
FIGURE_SIZE = (5, 3.6)  # This can remain script-specific
DEFAULT_COMMON_PREFIX_VALUES = list(range(0, 9))
NUM_RUNS = 3

# --- Regular Expressions for Parsing ---
OPERATION_COUNT_PATTERNS = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point": re.compile(r"\bQ(\d+)\b"),
    "range": re.compile(r"\bS(\d+)\b"),
}
EXECUTION_TIME_PATTERN = re.compile(
    r"^(Inserts|PointQuery|RangeQuery)\s*Execution Time:\s*(\d+)\s*$", re.IGNORECASE
)

# --- 2. Setup Functions ---

#
# REMOVED: setup_publication_font()
# REMOVED: setup_matplotlib_parameters()
# This is now handled automatically by 'from plot import *'
#

# --- 3. Core Data Processing Logic ---


def parse_operation_counts_from_dir_name(experiment_directory: Path):
    """Extracts operation counts (Inserts, Point Queries, Range Queries) from a directory name."""
    dir_name = experiment_directory.name
    num_inserts = int(OPERATION_COUNT_PATTERNS["insert"].search(dir_name).group(1))
    num_point_queries = int(OPERATION_COUNT_PATTERNS["point"].search(dir_name).group(1))
    num_range_queries = int(OPERATION_COUNT_PATTERNS["range"].search(dir_name).group(1))
    return num_inserts, num_point_queries, num_range_queries


def parse_execution_times_from_log(log_content: str):
    """Parses execution times for each operation phase from log file content."""
    times_in_ns = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0}
    for line in log_content.splitlines():
        match = EXECUTION_TIME_PATTERN.match(line.strip())
        if match:
            operation_kind, time_ns = match.groups()
            if operation_kind.lower().startswith("insert"):
                times_in_ns["Inserts"] = int(time_ns)
            elif operation_kind.lower().startswith("point"):
                times_in_ns["PointQuery"] = int(time_ns)
            elif operation_kind.lower().startswith("range"):
                times_in_ns["RangeQuery"] = int(time_ns)
    return times_in_ns


def calculate_average_metrics(
    data_directory: Path, phase: str, debug_context: str = ""
):
    """Calculates the average throughput and latency for a given phase across multiple runs."""
    experiment_directory = data_directory.parent
    num_inserts, num_point_queries, num_range_queries = (
        parse_operation_counts_from_dir_name(experiment_directory)
    )
    throughput_runs, latency_runs = [], []
    for run_number in range(1, NUM_RUNS + 1):
        workload_log_path = data_directory / f"workload{run_number}.log"
        if not workload_log_path.exists():
            continue
        log_content = workload_log_path.read_text(errors="ignore")
        exec_times_ns = parse_execution_times_from_log(log_content)
        if phase == "I":
            op_count, time_ns = num_inserts, exec_times_ns["Inserts"]
        elif phase == "Q":
            op_count, time_ns = num_point_queries, exec_times_ns["PointQuery"]
        else:
            op_count, time_ns = num_range_queries, exec_times_ns["RangeQuery"]
        if op_count > 0 and time_ns > 0:
            time_s = time_ns * NANOsONDS_TO_sONDS
            throughput = op_count / time_s
            if phase == "S":
                print(
                    f"  [Debug] Run {run_number} ({debug_context}): Raw Time = {time_ns:,} ns | Throughput = {throughput:,.2f} ops/s"
                )
            throughput_runs.append(throughput)
            latency_runs.append(time_ns / op_count)
    avg_throughput = float(np.mean(throughput_runs)) if throughput_runs else 0.0
    avg_latency = float(np.mean(latency_runs)) if latency_runs else 0.0
    return avg_throughput, avg_latency


# --- 4. Plotting Functions ---


def create_plot_figure():
    """Creates a Matplotlib figure and axis with standard adjustments."""
    # Note: plt is imported via 'from plot import *'
    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.35)
    return fig, ax


def apply_legend(figure):
    """Applies a standard bottom-centered legend to the figure."""
    figure.legend(
        loc="lower center",
        ncol=2,
        bbox_to_anchor=(0.5, -0.02),
        frameon=False,
        labelspacing=0.3,
        handletextpad=0.6,
        columnspacing=1.0,
    )


def plot_throughput_vs_common_prefix(
    exp_dir: Path,
    plots_dir: Path,
    out_filename: str,
    caption_text: str = "",
    y_axis_limit=None,
    x_axis_range=None,
    single_point=False,
):
    """Plots Range Query throughput vs. Common Prefix Length (C)."""
    if not exp_dir or not exp_dir.exists():
        print(f"❌ Error: Experiment directory not found, skipping: {exp_dir}")
        return

    if x_axis_range and len(x_axis_range) == 2:
        x_values_to_plot = list(range(x_axis_range[0], x_axis_range[1] + 1))
        print(f"ℹ️ Plotting custom x-axis range: {x_values_to_plot}")
    else:
        x_values_to_plot = DEFAULT_COMMON_PREFIX_VALUES

    fig, ax = create_plot_figure()

    for buffer_type in BUFFERS_TO_PLOT:
        throughput_values = []

        for prefix_length in x_values_to_plot:
            dir_suffix = ""
            if single_point and prefix_length == 8:
                dir_suffix = "-old"

            data_subdir = (
                exp_dir / f"{buffer_type}-X8-H1000000-C{prefix_length}{dir_suffix}"
            )

            if not data_subdir.exists():
                throughput_values.append(np.nan)
                print(
                    f"⚠️ Warning: Data directory not found for line plot: {data_subdir}"
                )
                continue

            debug_context_str = f"{buffer_type}, C={prefix_length}{dir_suffix}"
            avg_throughput, _ = calculate_average_metrics(
                data_subdir, "S", debug_context=debug_context_str
            )
            throughput_values.append(avg_throughput)

        if any(np.isfinite(y) for y in throughput_values):
            # line_styles is imported from plot/style.py via plot/__init__.py
            style = line_styles.get(buffer_type, {}).copy()
            ax.plot(x_values_to_plot, throughput_values, **style)

            if single_point:
                single_point_dir = exp_dir / f"{buffer_type}-X8-H1000000-C8"
                if single_point_dir.exists():
                    sp_debug_str = f"{buffer_type}, C=8 (Single Point)"
                    sp_throughput, _ = calculate_average_metrics(
                        single_point_dir, "S", debug_context=sp_debug_str
                    )

                    point_style = style.copy()
                    point_style["linestyle"] = "None"
                    if "marker" not in point_style:
                        point_style["marker"] = "o"

                    if "label" in point_style:
                        del point_style["label"]

                    ax.plot(8, sp_throughput, **point_style)
                else:
                    print(
                        f"⚠️ Warning: Single point data directory not found: {single_point_dir}"
                    )
    
    # <-- MODIFIED: Get handles and labels for separate legend
    handles, labels = ax.get_legend_handles_labels()

    ax.set_xlabel("Common Prefix Length ($C$)")
    ax.set_ylabel("RQ Throughput (ops/s)") 
    ax.set_xticks(x_values_to_plot)

    if y_axis_limit:
        ax.set_ylim(*y_axis_limit)
    if x_axis_range:
        ax.set_xlim(x_values_to_plot[0] - 0.5, x_values_to_plot[-1] + 0.5)

    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{x/1000:.0f}K'))

    # <-- MODIFIED: Title removed from main plot
    # ax.set_title(caption_text, pad=10)

    # <-- MODIFIED: Legend removed from main plot
    # apply_legend(fig)

    # --- MODIFICATION: Save main plot in PDF only ---
    output_path = plots_dir / f"{out_filename}.pdf"
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
    print(f"✅ Plot saved to: {output_path.name}")
    # --- END MODIFICATION ---

    plt.close(fig)  # Close the main figure

    # --- MODIFIED: Start new block to save legend separately ---
    # This logic is now consistent with plot_write_stall_components.py
    if handles:
        # Use 2 columns since there are only 2 buffers
        ncol = min(len(handles), 2) 
        figsize_height = 0.4 * (1 + (len(handles) - 1) // ncol)
        figsize_width = 2.5 * ncol

        legend_fig = plt.figure(figsize=(figsize_width, figsize_height))
        legend_fig.legend(
            handles,
            labels,
            loc="center",
            ncol=ncol,
            frameon=False,
            labelspacing=0.3,       # Consistent
            handletextpad=0.6,      # Consistent
            columnspacing=1.0,      # Consistent
        )
        plt.axis('off')

        # --- MODIFICATION: Save legend in PDF only ---
        legend_output_path = plots_dir / f"{out_filename}_legend.pdf"
        legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0.01) # Consistent
        print(f"✅ Legend saved to: {legend_output_path.name}")
        # --- END MODIFICATION ---
        
        plt.close(legend_fig)
    # --- End new block ---

    # --- MODIFIED: Start new block to save caption separately ---
    # This logic was already consistent.
    if caption_text:
        # Use main figure's width, arbitrary small height
        title_fig = plt.figure(figsize=(FIGURE_SIZE[0], 0.5)) 
        title_fig.text(
            0.5, 
            0.5, 
            caption_text, 
            ha='center', 
            va='center', 
            fontsize=plt.rcParams['font.size'] # Use global font size
        )
        plt.axis('off')

        # --- MODIFICATION: Save caption in PDF only ---
        caption_output_path = plots_dir / f"{out_filename}_caption.pdf"
        title_fig.savefig(caption_output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"✅ Caption saved to: {caption_output_path.name}")
        # --- END MODIFICATION ---
        
        plt.close(title_fig)
    # --- End new block ---


# --- 5. Main Execution ---

# --- NEW FUNCTION ---
def format_caption(old_caption: str) -> str:
    """
    Parses the old, complex caption string to extract Insert (I) and
    Range Query (S) counts, returning a simplified, readable caption.
    """
    insert_match = re.search(r"-I(\d+K|\d+M)\b", old_caption)
    scan_match = re.search(r"-S(\d+)\b", old_caption)
    
    parts = []
    if insert_match:
        parts.append(f"I={insert_match.group(1)}")
    if scan_match:
        parts.append(f"RQ={scan_match.group(1)}")
        
    if parts:
        return " ".join(parts)
    
    # Fallback if no matches are found
    return old_caption
# --- END NEW FUNCTION ---


def main():
    """Main function to iterate through datasets and generate plots."""
    # REMOVED: setup_matplotlib_parameters()
    # Styling is applied automatically on import.

    for config in DATASET_CONFIGURATIONS:
        experiment_directory = config["stats_directory"]
        plots_directory = config["plots_directory"]
        output_prefix = config["output_filename_prefix"]
        y_limit = config.get("y_axis_limit_rq")
        x_range = config.get("x_axis_range")
        single_point_flag = config.get("single_point", False)
        
        # --- MODIFICATION: Use format_caption function ---
        original_caption = config.get("caption", "")
        caption = format_caption(original_caption)
        # --- END MODIFICATION ---

        plots_directory.mkdir(parents=True, exist_ok=True)
        print(f"\nProcessing data from: {experiment_directory.name}")
        if single_point_flag:
            print("  -> Single point plotting for C=8 is ENABLED.")
        print(f"  -> Saving plots to: {plots_directory}")
        print(f"  -> Caption: '{caption}' (from '{original_caption}')")


        plot_throughput_vs_common_prefix(
            experiment_directory,
            plots_directory,
            output_prefix,
            caption_text=caption,
            y_axis_limit=y_limit,
            x_axis_range=x_range,
            single_point=single_point_flag,
        )


if __name__ == "__main__":
    main()