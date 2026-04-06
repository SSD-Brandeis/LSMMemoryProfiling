import matplotlib
matplotlib.use("Agg")

from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.ticker as ticker
import math
import matplotlib.pyplot as plt
from math import ceil

# --- Configuration ---

ROLLING_AVERAGE_WINDOW = 100
FIGSIZE = (5, 3.6)

# 1. SET THE PATH TO YOUR NEW DATA DIRECTORY
DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/ondisk/ondisk_sanitycheck-lowpri_false-I250000-U0-Q0-S0-Y0-T2-P1024-B32-E128/Vector-dynamic")

# 2. SET THE OUTPUT DIRECTORY FOR PLOTS
try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    print("⚠️  __file__ not defined, using Path.cwd() as fallback.")
    CURR_DIR = Path.cwd()

# MODIFIED: Updated to your new requested folder name
PLOTS_DIR = CURR_DIR / "ondisk_vector_time"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# 3. REGEX FOR PARSING LATENCY
# Searches for "InsertTime: <number>"
LATENCY_RE = re.compile(r"InsertTime:\s*(\d+)")

# --- Helper Functions (Unchanged) ---

def parse_run_log(file_path: Path):
    """
    Parses a log file for all matches of LATENCY_RE.
    """
    latencies = []
    if not file_path.exists():
        print(f"Warning: Log file not found: {file_path}")
        return latencies
    try:
        with open(file_path, "r") as f:
            content = f.read()
            matches = LATENCY_RE.findall(content)
            latencies = [int(m) for m in matches]
    except Exception as e:
        print(f"Error reading or parsing {file_path}: {e}")
    return latencies


def save_plot_legend(handles, labels, base_output_path):
    if not handles:
        print("Warning: No legend handles found. Skipping legend save.")
        return
    ncol = ceil(len(handles) / 2)
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend(
        handles,
        labels,
        loc="center",
        ncol=ncol,
        frameon=False,
        fontsize=plt.rcParams["font.size"],
        borderpad=0.1,
        columnspacing=0.8,
        handletextpad=0.5,
    )
    plt.axis("off")
    for ext in ["pdf"]:
        legend_output_path = base_output_path.with_name(
            f"{base_output_path.name}_legend.{ext}"
        )
        legend_fig.savefig(
            legend_output_path, 
            bbox_inches="tight", 
            pad_inches=0.01,
            bbox_extra_artists=[leg]
        )
        print(f"[saved] {legend_output_path.name}")
    plt.close(legend_fig)


DECIMALS_RE = re.compile(r"\.(\d+)")

def save_plot_caption(caption_text, base_output_path):
    if not caption_text:
        return
    has_decimals = DECIMALS_RE.search(caption_text)
    caption_fontsize = plt.rcParams["font.size"]
    if has_decimals:
        caption_fontsize *= 1
    title_fig = plt.figure(figsize=(0.1, 0.1))
    txt = title_fig.text(
        0,
        0,
        caption_text,
        ha="left",
        va="bottom",
        fontsize=caption_fontsize,
    )
    plt.axis("off")
    for ext in ["pdf"]:
        caption_output_path = base_output_path.with_name(
            f"{base_output_path.name}_caption.{ext}"
        )
        title_fig.savefig(
            caption_output_path, 
            bbox_inches="tight", 
            pad_inches=0.01,
            bbox_extra_artists=[txt]
        )
        print(f"[saved] {caption_output_path.name}")
    plt.close(title_fig)


def save_individual_legend(handle, label, base_output_path):
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend(
        [handle],
        [label],
        loc="center",
        ncol=1,
        frameon=False,
        fontsize=plt.rcParams["font.size"],
        borderpad=0.1, 
        handletextpad=0.5,
    )
    plt.axis("off")
    for ext in ["pdf"]:
        legend_output_path = base_output_path.with_suffix(f".{ext}")
        legend_fig.savefig(
            legend_output_path, 
            bbox_inches="tight", 
            pad_inches=0.01,
            bbox_extra_artists=[leg]
        )
        print(f"[saved] {legend_output_path.name}")
    plt.close(legend_fig)


# --- Main Plotting Function ---

def plot_single_experiment_latency():
    """
    Plots InsertTime latency from a single stats.log file.
    """
    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.15)
 
    print(f"\n--- Processing: {DATA_DIR.name} ---")

    if not DATA_DIR.is_dir():
        print(f"Error: Directory not found: {DATA_DIR}, exiting.")
        return

    # --- Load data from the single stats.log file ---
    log_file = DATA_DIR / "stats.log"
    
    print(f"Parsing {log_file}...")
    latencies = parse_run_log(log_file)
    
    if not latencies:
        print(f"Error: No valid latency data found in {log_file}, skipping plot.")
        return

    data_len = len(latencies)
    if data_len == 0:
        print(f"Error: Data length is 0, skipping plot.")
        return
    
    # --- Apply rolling average ---
    if ROLLING_AVERAGE_WINDOW > 1 and data_len > ROLLING_AVERAGE_WINDOW:
        print(f"Applying rolling average with window {ROLLING_AVERAGE_WINDOW}")
        latencies_series = pd.Series(latencies)
        plot_latencies_series = latencies_series.rolling(
            window=ROLLING_AVERAGE_WINDOW
        ).mean()
        plot_latencies = plot_latencies_series.dropna().values
        start_x = ROLLING_AVERAGE_WINDOW
        x_ops = np.arange(start_x, data_len + 1)
    else:
        if ROLLING_AVERAGE_WINDOW > 1:
            print(
                f"Warning: Data length ({data_len}) less than window ({ROLLING_AVERAGE_WINDOW}). Plotting raw data."
            )
        plot_latencies = latencies
        x_ops = np.arange(1, data_len + 1)

    if len(plot_latencies) == 0:
        print("Error: No data points left after rolling average. Skipping plot.")
        return
        
    all_max_y = [np.max(plot_latencies)]

    # --- Define plot style for this single line ---
    style = {
        "color": "#006d2c",  # Dark green
        "linestyle": "-",    # Solid line
        "alpha": 0.8,
        "label": "InsertTime (dynamic vector)"
    }

    if ROLLING_AVERAGE_WINDOW > 1:
        style["marker"] = ""

    (line,) = ax.plot(x_ops, plot_latencies, **style)
    print(f"Plotted {style['label']} with {len(plot_latencies)} data points.")

    # --- Save individual legend ---
    safe_label = re.sub(r"[\s\(\)]+", "_", style['label']).strip("_")
    legend_name = f"legend_{safe_label}"
    legend_out_path = PLOTS_DIR / legend_name
    save_individual_legend(line, style["label"], legend_out_path)

    # --- Format axes ---
    ax.set_xlabel("Operation Count")
    ax.set_ylabel("InsertTime Latency (ns)", labelpad=0)
    
    ax.tick_params(axis='x', labelsize=plt.rcParams["font.size"])
    ax.tick_params(axis='y', labelsize=plt.rcParams["font.size"])

    #
    # THIS IS THE FIX
    #
    max_y = max([y for y in all_max_y if not math.isnan(y)] + [1])
    #
    #
    #
    
    if not all_max_y:
        max_y = 1
    else:
        max_y = np.max(all_max_y)

    # Scientific notation for y-axis if needed
    if max_y > 999:
        y_power = math.floor(math.log10(max_y)) if max_y > 0 else 0
        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.1f}")
        )
        ax.text(
            0.02,
            0.98,
            r"$\times 10^{{{}}}$".format(y_power),
            transform=ax.transAxes,
            fontsize=plt.rcParams["font.size"],
            ha="left",
            va="top",
        )

    # --- Save plot and caption ---
    base_out = PLOTS_DIR / "sanity_check_insert_latency"
    caption_text = "Vector InsertTime Latency (Sanity Check)"

    for ext in ["pdf"]:
        output_path = base_out.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {output_path.name}")
    plt.close(fig)

    save_plot_caption(caption_text, base_out)


if __name__ == "__main__":
    plot_single_experiment_latency()