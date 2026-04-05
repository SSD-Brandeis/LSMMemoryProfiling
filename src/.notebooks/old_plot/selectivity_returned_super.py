#!/usr/bin/env python3

from pathlib import Path
import re
import numpy as np
import pandas as pd  # Kept for structural consistency
import matplotlib.ticker as ticker
import math

# --- Import plotting setup ---
try:
    from plot import *
except ImportError:
    print(
        "Warning: 'plot.py' not found. Using default matplotlib and placeholder styles."
    )
    import matplotlib.pyplot as plt

    # Define placeholder line_styles if import fails
    line_styles = {
        "vectorrep": {"color": "#006d2c", "linestyle": "-"},
        "skiplist": {"color": "#6a3d9a", "linestyle": ":"},
    }

# --- MODIFICATION: User Configuration ---
ROLLING_AVERAGE_WINDOW = 1
# --- End MODIFICATION ---

# --- Path Setup ---
CURR_DIR = Path(__file__).resolve().parent

# --- Data & Plot Directories ---
EXPERIMENT_DIRS = {
    "interleave": Path(
        "/Users/cba/Desktop/LSMMemoryBuffer/data/write_stall_data/write_stall_pq_interleave-lowpri_true-I60000-U0-Q10000-S0-Y0-T10-P16384-B4-E1024/Vector-dynamic"
    ),
    "rawop": Path(
        "/Users/cba/Desktop/LSMMemoryBuffer/data/write_stall_data/write_stall_pq_rawop-lowpri_true-I60000-U0-Q10000-S0-Y0-T10-P16384-B4-E1024/Vector-dynamic"
    ),
}

PLOTS_DIR = CURR_DIR / "paper_plot" / "get_latency_plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


# --- Constants ---
FIGSIZE = (5, 3.6)

# This now matches "GetTime:" OR "Get Time:" (case-insensitive)
LATENCY_RE = re.compile(r"Get\s*Time:\s*([\d\.e\+\-]+)", re.IGNORECASE)


# --- Helper Functions (Copied from your reference script) ---
def parse_stats_log(file_path: Path):
    """
    MODIFIED: Extracts a list of 'GetTime' latencies (floats) from a stats.log file.
    """
    latencies = []
    print(f"--- Parsing log file: {file_path}")
    
    if not file_path.exists():
        print(f"Warning: Log file not found: {file_path}")
        return latencies
    try:
        with open(file_path, "r") as f:
            content = f.read()
            matches = LATENCY_RE.findall(content)
            
            print(f"--- Found {len(matches)} matches for 'Get Time'.")
            
            latencies = [float(m) for m in matches]
    except Exception as e:
        print(f"Error reading or parsing {file_path}: {e}")
    return latencies


def save_plot_legend(handles, labels, base_output_path):
    """Saves a plot's legend to PDF and SVG files."""
    if not handles:
        print(
            "Warning: No legend handles found. Skipping legend save."
        )
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

    for ext in ["pdf", "svg"]:
        legend_output_path = base_output_path.with_name(
            f"{base_output_path.name}_legend.{ext}"
        )
        legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0.01)
        print(f"[saved] {legend_output_path.name}")
    plt.close(legend_fig)


def save_plot_caption(caption_text, base_output_path):
    """Saves a plot's caption to PDF and SVG files."""
    if not caption_text:
        return
    title_fig = plt.figure(figsize=(FIGSIZE[0], 0.5))
    title_fig.text(
        0.5,
        0.5,
        caption_text,
        ha="center",
        va="center",
        fontsize=plt.rcParams["font.size"],
    )
    plt.axis("off")

    for ext in ["pdf", "svg"]:
        caption_output_path = base_output_path.with_name(
            f"{base_output_path.name}_caption.{ext}"
        )
        title_fig.savefig(caption_output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {caption_output_path.name}")
    plt.close(title_fig)


def save_individual_legend(handle, label, base_output_path):
    """Saves a single legend item to a compact SVG file."""
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

    legend_output_path = base_output_path.with_suffix(".svg")
    legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0)
    print(f"[saved] {legend_output_path.name}")
    plt.close(legend_fig)


# --- Main Plotting Function ---
def plot_get_latency():
    """Plots a combined average 'GetTime' latency over operations."""

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.15)

    experiment_keys = ["interleave", "rawop"]

    color_map = {
        "interleave": line_styles.get("vectorrep", {}).get(
            "color", "#006d2c"
        ),  # Green
        "rawop": line_styles.get("skiplist", {}).get("color", "#6a3d9a"),  # Purple
    }

    linestyle_map = {
        "interleave": "-",  # Solid
        "rawop": ":",  # Dotted
    }

    # === 1. MODIFICATION: Using '/' for the legend ===
    label_map = {
        "rawop": "Sequential (Insert / Point Query)",
        "interleave": "Interleaved (Insert / Point Query)",
    }
    # =================================================

    all_max_y = []

    for exp_key in experiment_keys:
        print(f"\n--- Processing: {exp_key} ---")

        buffer_dir = EXPERIMENT_DIRS.get(exp_key)
        if not buffer_dir:
            print(f"Warning: No base directory found for {exp_key}, skipping.")
            continue

        if not buffer_dir.is_dir():
            print(f"Warning: Directory not found: {buffer_dir}, skipping.")
            continue

        log_file = buffer_dir / "stats.log"
        latencies = parse_stats_log(log_file) # This now has better regex

        if not latencies:
            print(f"Warning: No valid latency data found, skipping plot.")
            continue

        min_len = len(latencies)
        avg_latencies = latencies

        if ROLLING_AVERAGE_WINDOW > 1 and min_len > ROLLING_AVERAGE_WINDOW:
            print(f"Applying rolling average with window {ROLLING_AVERAGE_WINDOW}")
            avg_latencies_series = pd.Series(avg_latencies)
            plot_latencies_series = avg_latencies_series.rolling(
                window=ROLLING_AVERAGE_WINDOW
            ).mean()
            plot_latencies = plot_latencies_series.dropna().values
            start_x = ROLLING_AVERAGE_WINDOW
            x_ops = np.arange(start_x, len(avg_latencies_series) + 1)[
                start_x - 1 :
            ]
        else:
            if ROLLING_AVERAGE_WINDOW > 1:
                print(
                    f"Warning: Data length ({min_len}) less than window ({ROLLING_AVERAGE_WINDOW}). Plotting raw data."
                )
            plot_latencies = avg_latencies
            x_ops = np.arange(1, min_len + 1)

        if len(plot_latencies) > 0:
            all_max_y.append(np.max(plot_latencies))

        style = {}
        style["color"] = color_map[exp_key]
        style["linestyle"] = linestyle_map[exp_key]

        if style["linestyle"] == "-":
            style["alpha"] = 0.7
        else:
            style["alpha"] = 1.0

        style["label"] = label_map.get(exp_key, "Unknown Label")

        if ROLLING_AVERAGE_WINDOW > 1:
            style["marker"] = ""

        (line,) = ax.plot(x_ops, plot_latencies, **style)
        print(f"Plotted {style['label']} with {len(plot_latencies)} data points.")

        # Updated safe_label to handle the '/' character
        safe_label = re.sub(r"[\s\(\)/]+", "_", style["label"]).strip("_")
        legend_name = f"legend_{safe_label}"
        legend_out_path = PLOTS_DIR / legend_name
        save_individual_legend(line, style["label"], legend_out_path)

    ax.set_xlabel("Number of Point Queries")
    
    # === MODIFICATION: Updated Y-axis label ===
    ax.set_ylabel("Latency (ns)", labelpad=0)
    # ==========================================

    max_y = max([y for y in all_max_y if not math.isnan(y)] + [1])
    if not all_max_y:
        max_y = 1
    else:
        max_y = np.max(all_max_y)

    if max_y > 999:
        y_power = math.floor(math.log10(max_y)) if max_y > 0 else 0
        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.1f}")
        )
        
        # === 2. KEPT THIS CHANGE: Moved scientific notation to the right ===
        ax.text(
            0.05,  # Kept at 0.05 (changed from 0.02)
            0.98,
            r"$\times 10^{{{}}}$".format(y_power),
            transform=ax.transAxes,
            fontsize=plt.rcParams["font.size"],
            ha="left",
            va="top",
        )
        # ================================================================

    base_out = PLOTS_DIR / "get_latency_comparison"
    caption_text = "Point Query 'GetTime' Latency Comparison"

    # Save the main plot only if data was actually plotted
    if all_max_y: 
        for ext in ["pdf", "svg"]:
            output_path = base_out.with_suffix(f".{ext}")
            fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
            print(f"[saved] {output_path.name}")
    else:
        print("\nNo data was plotted. Skipping save of main plot file.")
        
    plt.close(fig)

    save_plot_caption(caption_text, base_out)


if __name__ == "__main__":
    plot_get_latency()