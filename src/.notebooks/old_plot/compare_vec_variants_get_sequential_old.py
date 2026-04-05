#!/usr/bin/env python3

from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.ticker as ticker
import math

# --- Import plotting setup ---
try:
    from plot import *
except ImportError:
    # print("Warning: 'plot.py' not found. Using default matplotlib and placeholder styles.")
    import matplotlib.pyplot as plt

# --- Configuration ---
ROLLING_AVERAGE_WINDOW = 1  # Set to > 1 to smooth lines if data is noisy

# Manual Y-axis limits. Set to None to auto-scale, or a number to fix the limit.
# Example: Y_MIN = 0, Y_MAX = 5000
Y_MIN = 0
# Resetting Y_MAX to None because 10M items will be much slower than 100k
Y_MAX = 1e5



# --- Path Setup ---
CURR_DIR = Path(__file__).resolve().parent

# Base directory for the specific experiment provided
DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_compare_vec_variants_get_internal_debug")

# --- MODIFIED: Switched to the 10M experiment folder ---
# Old 100k: "compare_vec_variants-lowpri_true-I100000-U0-Q100-S0-Y0-T10-P131072-B4-E1024"
# EXP_FOLDER = "compare_vec_variants_10m-lowpri_true-I10000000-U0-Q100-S0-Y0-T10-P524288-B512-E8"
EXP_FOLDER ="fixed_pq_vec_variants-lowpri_true-I100000-U0-Q100-S0-Y0-T10-P524288-B512-E8"
BASE_EXP_DIR = DATA_ROOT / EXP_FOLDER

# --- Data Directories Map ---
# Mapping internal keys to the specific subdirectories provided in the tree
EXPERIMENT_DIRS = {
    "vector": BASE_EXP_DIR / "Vector-dynamic",
    "always_sorted": BASE_EXP_DIR / "AlwayssortedVector-dynamic",
    "unsorted": BASE_EXP_DIR / "UnsortedVector-dynamic",
}

# --- MODIFIED: Updated output directory for 10M results ---
PLOTS_DIR = CURR_DIR / "paper_plot" / "fixed_vec_variants_get_latency"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# --- Constants ---
FIGSIZE = (5, 3.6)

# Matches "GetTime:", "Get Time:", or "GetTime :" (case-insensitive)
LATENCY_RE = re.compile(r"Get\s*Time\s*:\s*([\d\.e\+\-]+)", re.IGNORECASE)


# --- Helper Functions ---
def parse_stats_log(file_path: Path):
    """
    Extracts a list of 'GetTime' latencies (floats) from a stats.log file.
    """
    latencies = []
    
    if not file_path.exists():
        print(f"Warning: Log file not found: {file_path}")
        return latencies
    try:
        with open(file_path, "r") as f:
            content = f.read()
            matches = LATENCY_RE.findall(content)
            latencies = [float(m) for m in matches]
            print(f"--- Parsed {file_path.parent.name}: Found {len(matches)} data points.")
    except Exception as e:
        print(f"Error reading or parsing {file_path}: {e}")
    return latencies


def save_plot_legend(handles, labels, base_output_path):
    """Saves a plot's legend to PDF only."""
    if not handles:
        return
    legend_fig = plt.figure(figsize=(10, 0.5))
    legend_fig.legend(
        handles,
        labels,
        loc="center",
        ncol=len(handles),
        frameon=False,
        labelspacing=0.1,
        handletextpad=0.1,
        columnspacing=0.1,
    )
    plt.axis("off")

    # Only PDF
    for ext in ["pdf"]:
        legend_output_path = base_output_path.with_name(
            f"{base_output_path.name}_legend.{ext}"
        )
        legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0.01)
    plt.close(legend_fig)


def save_plot_caption(caption_text, base_output_path):
    """Saves a plot's caption to PDF only."""
    if not caption_text:
        return
    title_fig = plt.figure(figsize=(FIGSIZE[0], 0.5))
    title_fig.text(
        0.5,
        0.5,
        caption_text,
        ha="center",
        va="center",
        fontsize=plt.rcParams.get("font.size", 10),
    )
    plt.axis("off")

    # Only PDF
    for ext in ["pdf"]:
        caption_output_path = base_output_path.with_name(
            f"{base_output_path.name}_caption.{ext}"
        )
        title_fig.savefig(caption_output_path, bbox_inches="tight", pad_inches=0.02)
    plt.close(title_fig)


def save_individual_legend(handle, label, base_output_path):
    """Saves a single legend item to a compact PDF file."""
    legend_fig = plt.figure(figsize=(4, 0.2))
    legend_fig.legend(
        [handle],
        [label],
        loc="center",
        frameon=False,
        borderaxespad=0,
        labelspacing=0,
        borderpad=0,
        columnspacing=0.4,
        handletextpad=0.2,
        handlelength=1,
        handleheight=1,
    )
    plt.axis("off")

    # Changed from .svg to .pdf
    legend_output_path = base_output_path.with_suffix(".pdf")
    legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0)
    plt.close(legend_fig)


# --- Main Plotting Function ---
def plot_vec_variants_latency():
    """Plots GetTime latency for Vector, AlwayssortedVector, and UnsortedVector."""

    fig, ax = plt.subplots(figsize=FIGSIZE)
    # Adjust margins to match your style convention
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.15)

    # Order of plotting
    experiment_keys = ["vector", "always_sorted", "unsorted"]

    # --- Style Definitions ---
    color_map = {
        "vector": "#377eb8",         # Blue
        "always_sorted": "#006d2c",  # Green
        "unsorted": "#e41a1c",       # Red
    }

    linestyle_map = {
        "vector": "-",
        "always_sorted": "--",
        "unsorted": ":",
    }

    label_map = {
        "vector": "Vector",
        "always_sorted": "AlwaysSorted Vector",
        "unsorted": "Unsorted Vector",
    }

    all_max_y = []

    for exp_key in experiment_keys:
        buffer_dir = EXPERIMENT_DIRS.get(exp_key)
        
        # Determine stats.log path
        log_file = buffer_dir / "stats.log"
        latencies = parse_stats_log(log_file)

        if not latencies:
            print(f"Skipping {exp_key} (no data)")
            continue

        min_len = len(latencies)
        avg_latencies = latencies

        # Rolling Average Logic
        if ROLLING_AVERAGE_WINDOW > 1 and min_len > ROLLING_AVERAGE_WINDOW:
            avg_latencies_series = pd.Series(avg_latencies)
            plot_latencies_series = avg_latencies_series.rolling(
                window=ROLLING_AVERAGE_WINDOW
            ).mean()
            plot_latencies = plot_latencies_series.dropna().values
            start_x = ROLLING_AVERAGE_WINDOW
            x_ops = np.arange(start_x, len(avg_latencies_series) + 1)
        else:
            plot_latencies = avg_latencies
            x_ops = np.arange(1, min_len + 1)

        if len(plot_latencies) > 0:
            all_max_y.append(np.max(plot_latencies))

        # Apply styles
        style = {}
        style["color"] = color_map.get(exp_key, "black")
        style["linestyle"] = linestyle_map.get(exp_key, "-")
        style["label"] = label_map.get(exp_key, exp_key)
        style["alpha"] = 0.8 if style["linestyle"] == "-" else 1.0

        if ROLLING_AVERAGE_WINDOW > 1:
            style["marker"] = ""

        # Plot the line
        (line,) = ax.plot(x_ops, plot_latencies, **style)

        # Save individual legend for this line
        safe_label = re.sub(r"[\s\(\)/]+", "_", style["label"]).strip("_")
        legend_name = f"legend_{safe_label}"
        legend_out_path = PLOTS_DIR / legend_name
        save_individual_legend(line, style["label"], legend_out_path)

    # --- Axis Formatting ---
    ax.set_xlabel("number of point queries") # Lower case as requested
    ax.set_ylabel("Latency (ns)", labelpad=0)

    # Manual Y-Axis Control
    if Y_MIN is not None:
        ax.set_ylim(bottom=Y_MIN)
    if Y_MAX is not None:
        ax.set_ylim(top=Y_MAX)

    # Scientific Notation for Y-axis
    # (Calculated based on actual max_y unless manually overridden, but formatting logic remains)
    if all_max_y:
        # If Y_MAX is manually set, use that to determine power, otherwise use data max
        effective_max = Y_MAX if Y_MAX is not None else np.max(all_max_y)
    else:
        effective_max = 1

    if effective_max > 999:
        y_power = math.floor(math.log10(effective_max)) if effective_max > 0 else 0
        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.1f}")
        )
        # Place scientific notation multiplier (e.g., x 10^3) inside top-left
        ax.text(
            0.05,
            0.98,
            r"$\times 10^{{{}}}$".format(y_power),
            transform=ax.transAxes,
            fontsize=plt.rcParams.get("font.size", 10),
            ha="left",
            va="top",
        )

    # --- Save Outputs ---
    base_out = PLOTS_DIR / "fixed_compare_vectors_latency"
    caption_text = "Comparison of Get Latency across Vector Variants"

    if all_max_y:
        # Only PDF
        for ext in ["pdf"]:
            output_path = base_out.with_suffix(f".{ext}")
            fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
            print(f"[saved] {output_path.name}")
    else:
        print("No data plotted.")

    plt.close(fig)
    save_plot_caption(caption_text, base_out)

if __name__ == "__main__":
    plot_vec_variants_latency()