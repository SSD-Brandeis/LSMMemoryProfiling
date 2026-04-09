import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from pathlib import Path
from math import ceil
import re
import sys
import numpy as np

try:
    from plot import *
except ImportError:
    print("⚠️  'plot.py' not found. Using default matplotlib settings.")
    plt.rcParams.update({"font.size": 20})
    EXP_DIR = Path.cwd()
    bar_styles = {}


try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

USE_LOG_SCALE = True
# Data is in micros, so we might want to keep it in micros or convert. 
# The prompt implies plotting the extracted values directly (micros).
# Setting this to 1 to keep units as micros.
MICROS_TO_S = 1e-6 

# Adjusted Limits for Latency Stats (P50 is ~100, SUM is ~10^8)
# We need a wide log range.
YLIM_LOG_STATS = (1e0, 1e10) 
# YLIM_LOG_STATS = (0, 20000) 

RAWOP_DIR = EXP_DIR / "filter_result_fixed_sequential_throughput"
INTERLEAVE_DIR = EXP_DIR / "filter_result_fixed_interleave_throughput"

PLOTS_DIR = Path(
    "/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/poster"
)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    "Vector-dynamic",
    "skiplist",
    "hash_skip_list",
    "hash_linked_list",
    "UnsortedVector-dynamic",
    "AlwaysSortedVector-dynamic",
]

# --- REGEX FOR ROCKSDB STATS ---
# Pattern: rocksdb.db.get.micros P50 : 138.57... P95 : ... SUM : 179316162
ROCKS_GET_RE = re.compile(
    r"rocksdb\.db\.get\.micros\s+P50\s*:\s*([\d\.]+)\s+P95\s*:\s*([\d\.]+)\s+P99\s*:\s*([\d\.]+)\s+P100\s*:\s*([\d\.]+)\s+COUNT\s*:\s*(\d+)\s+SUM\s*:\s*(\d+)"
)

ROCKS_WRITE_RE = re.compile(
    r"rocksdb\.db\.write\.micros\s+P50\s*:\s*([\d\.]+)\s+P95\s*:\s*([\d\.]+)\s+P99\s*:\s*([\d\.]+)\s+P100\s*:\s*([\d\.]+)\s+COUNT\s*:\s*(\d+)\s+SUM\s*:\s*(\d+)"
)

def safe_div(numer, denom):
    if numer and denom:
        return numer / denom
    return 0

def bar_style(buf_name):
    global bar_styles
    if "bar_styles" not in globals():
        bar_styles = {}
    default_style = {"color": "None", "edgecolor": "black", "hatch": ""}
    return bar_styles.get(buf_name, default_style).copy()

def apply_axis_style(ax, y_limit_tuple, is_latency_plot, formatter=None):
    if USE_LOG_SCALE:
        if y_limit_tuple:
            ax.set_ylim(*y_limit_tuple)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        if y_limit_tuple:
            ax.set_ylim(*y_limit_tuple)
        if formatter:
            ax.yaxis.set_major_formatter(formatter)
        else:
            ax.ticklabel_format(style="plain", axis="y", useOffset=False)

    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

def save_plot_legend(handles, labels, base_output_path):
    if not handles:
        return
    ncol = ceil(len(handles) / 2)
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend(
        handles, labels, loc="center", ncol=ncol, frameon=False,
        fontsize=plt.rcParams["font.size"], borderpad=0.1,
        columnspacing=0.8, handletextpad=0.5,
    )
    plt.axis("off")
    for ext in ["pdf"]:
        legend_output_path = base_output_path.with_name(f"{base_output_path.name}_legend.{ext}")
        legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0.01, bbox_extra_artists=[leg])
    plt.close(legend_fig)

def save_plot_caption(caption_text, base_output_path):
    if not caption_text:
        return
    caption_fontsize = plt.rcParams["font.size"]
    title_fig = plt.figure(figsize=(0.1, 0.1))
    txt = title_fig.text(0, 0, caption_text, ha="left", va="bottom", fontsize=caption_fontsize)
    plt.axis("off")
    for ext in ["pdf"]:
        caption_output_path = base_output_path.with_name(f"{base_output_path.name}_caption.{ext}")
        title_fig.savefig(caption_output_path, bbox_inches="tight", pad_inches=0.01, bbox_extra_artists=[txt])
    plt.close(title_fig)

def save_individual_legend(handle, label, base_output_path):
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend(
        [handle], [label], loc="center", ncol=1, frameon=False,
        fontsize=plt.rcParams["font.size"], borderpad=0.1, handletextpad=0.5,
    )
    plt.axis("off")
    for ext in ["pdf"]:
        legend_output_path = base_output_path.with_name(f"{base_output_path.name}.{ext}")
        legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0.01, bbox_extra_artists=[leg])
    plt.close(legend_fig)

def parse_rocksdb_stats(text):
    """
    Extracts P-stats and SUM for Get and Write from the log text.
    Returns a dictionary with keys like 'get_p50', 'write_sum', etc.
    """
    out = {}
    
    # Parse Get
    m_get = ROCKS_GET_RE.search(text)
    if m_get:
        out["get_p50"] = float(m_get.group(1))
        out["get_p95"] = float(m_get.group(2))
        out["get_p99"] = float(m_get.group(3))
        out["get_p100"] = float(m_get.group(4))
        count = float(m_get.group(5))
        out["get_sum"] = float(m_get.group(6))
        # Optional: Calculate Mean if needed
        out["get_mean"] = safe_div(out["get_sum"], count)
    
    # Parse Write
    m_write = ROCKS_WRITE_RE.search(text)
    if m_write:
        out["write_p50"] = float(m_write.group(1))
        out["write_p95"] = float(m_write.group(2))
        out["write_p99"] = float(m_write.group(3))
        out["write_p100"] = float(m_write.group(4))
        count = float(m_write.group(5))
        out["write_sum"] = float(m_write.group(6))
        # Optional: Calculate Mean if needed
        out["write_mean"] = safe_div(out["write_sum"], count)
        
    return out

def collect_records(data_dir):
    records = []
    print(f"\n--- Collecting records from: {data_dir} ---")
    for log_path in data_dir.rglob("workload*.log"):
        buffer_name = log_path.parent.name

        # Normalize buffer name
        if buffer_name.startswith("hash_skip_list"):
            buffer_key = "hash_skip_list"
        elif buffer_name.startswith("hash_linked_list"):
            buffer_key = "hash_linked_list"
        elif buffer_name.startswith("AlwayssortedVector"):
            buffer_key = "AlwaysSortedVector-dynamic"
        else:
            buffer_key = buffer_name

        # Parse metrics directly from the log file
        stats = parse_rocksdb_stats(log_path.read_text())
        
        # Only add record if we successfully parsed data
        if stats:
            record = {"buffer": buffer_key}
            record.update(stats)
            records.append(record)
            
    return pd.DataFrame(records)

def plot_combined_barchart(
    df, metric_cols, y_label, x_labels, filename_base, buffers, y_limit
):
    fig, ax = plt.subplots(1, 1, figsize=(8, 4.5)) # Slightly wider for 5 groups

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)

    num_groups = len(metric_cols)
    num_buffers = len(buffers)

    group_width = 0.9
    bar_width_ratio = 0.9
    width_of_one_bar_slot = group_width / num_buffers
    bar_width = width_of_one_bar_slot * bar_width_ratio

    group_x_positions = np.arange(num_groups)

    log_bottom = 1.0
    if USE_LOG_SCALE and y_limit:
        log_bottom = y_limit[0]

    for group_idx, metric_col in enumerate(metric_cols):
        group_center = group_x_positions[group_idx]
        group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot

            value = log_bottom
            try:
                if metric_col in df.columns:
                    value = df.loc[df["buffer"] == buf_name, metric_col].iat[0]
            except (IndexError, KeyError):
                pass

            # Safety for log scale
            if USE_LOG_SCALE and value <= 0:
                value = log_bottom
            elif USE_LOG_SCALE and value < log_bottom:
                value = log_bottom

            ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(group_x_positions)
    ax.set_xticklabels(x_labels)

    apply_axis_style(ax, y_limit, is_latency_plot=True) # Always latency here

    ax.set_ylabel(y_label)
    fig.tight_layout()

    for ext in ["pdf"]:
        output_path = filename_base.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        print(f"[saved] {output_path.name}")

    plt.close(fig)

def process_and_plot(data_dir, file_suffix, y_limit):
    raw_df = collect_records(data_dir)
    if raw_df.empty:
        print(f"No records found in {data_dir}. Skipping plots.")
        return None, False

    grouped_df = raw_df.groupby("buffer", as_index=False).mean()

    if FILTER_BUFFERS:
        grouped_df = grouped_df[grouped_df["buffer"].isin(FILTER_BUFFERS)]

    if grouped_df.empty:
        print(f"No data left after filtering for {data_dir}. Skipping plots.")
        return None, False

    csv_filename = PLOTS_DIR / f"rocksdb_stats_metrics{file_suffix}.csv"
    grouped_df.to_csv(csv_filename, index=False)

    buffers_to_plot = [b for b in FILTER_BUFFERS if b in grouped_df["buffer"].values]

    # --- Plot 1: Write (Insert) Statistics ---
    # Metrics: p50, p95, p99, p100, SUM
    write_labels = ["P50", "P95", "P99", "P100", "Total Latency"]
    write_metrics = ["write_p50", "write_p95", "write_p99", "write_p100", "write_sum"]
    
    write_filename = PLOTS_DIR / f"rocksdb_stats_write{file_suffix}"
    print(f"Generating Write Stats plot: {write_filename.name}...")
    plot_combined_barchart(
        df=grouped_df,
        metric_cols=write_metrics,
        y_label="write latency (micros)",
        x_labels=write_labels,
        filename_base=write_filename,
        buffers=buffers_to_plot,
        y_limit=y_limit,
    )

    # --- Plot 2: Get (Point Query) Statistics ---
    # Metrics: p50, p95, p99, p100, SUM
    get_labels = ["P50", "P95", "P99", "P100", "Total Latency"]
    get_metrics = ["get_p50", "get_p95", "get_p99", "get_p100", "get_sum"]
    
    get_filename = PLOTS_DIR / f"rocksdb_stats_get{file_suffix}"
    print(f"Generating Get Stats plot: {get_filename.name}...")
    plot_combined_barchart(
        df=grouped_df,
        metric_cols=get_metrics,
        y_label="get latency (micros)",
        x_labels=get_labels,
        filename_base=get_filename,
        buffers=buffers_to_plot,
        y_limit=y_limit,
    )

    # Save caption (Reuse base caption or custom)
    caption_text = f"RocksDB Stats (Micros) {file_suffix}"
    save_plot_caption(caption_text, write_filename)
    save_plot_caption(caption_text, get_filename)

    return grouped_df, True

def main():
    # Process Raw Op (Sequential)
    df_rawop, success_rawop = process_and_plot(
        data_dir=RAWOP_DIR,
        file_suffix="_rawop",
        y_limit=YLIM_LOG_STATS,
    )

    # Process Interleaved
    df_interleave, success_interleave = process_and_plot(
        data_dir=INTERLEAVE_DIR,
        file_suffix="_interleave",
        y_limit=YLIM_LOG_STATS,
    )

    if not (success_rawop or success_interleave):
        print("No plots were generated. Skipping legend.")
        return

    print("\nGenerating legends...")
    buffers_for_legend = FILTER_BUFFERS
    all_legend_handles = []
    all_legend_labels = []

    # Generate Common Legend
    for b in buffers_for_legend:
        style = bar_style(b)
        legend_style = {
            "facecolor": style.get("facecolor"),
            "edgecolor": style.get("edgecolor"),
            "hatch": style.get("hatch"),
            "linewidth": style.get("linewidth"),
        }
        # Filter None values
        legend_style = {k: v for k, v in legend_style.items() if v is not None}
        
        all_legend_handles.append(plt.Rectangle((0, 0), 1, 1, **legend_style))
        all_legend_labels.append(bar_style(b).get("label", b).lower())

    save_plot_legend(
        all_legend_handles, all_legend_labels, PLOTS_DIR / "rocksdb_stats_legend"
    )

    # Generate Individual Legends
    for b in buffers_for_legend:
        style = bar_style(b)
        legend_style = {
            "facecolor": style.get("facecolor"),
            "edgecolor": style.get("edgecolor"),
            "hatch": style.get("hatch"),
            "linewidth": style.get("linewidth"),
        }
        legend_style = {k: v for k, v in legend_style.items() if v is not None}

        handle = plt.Rectangle((0, 0), 1, 1, **legend_style)
        label = bar_style(b).get("label", b).lower()
        output_path = PLOTS_DIR / f"rocksdb_stats_legend_{b}"
        save_individual_legend(handle, label, output_path)

if __name__ == "__main__":
    main()