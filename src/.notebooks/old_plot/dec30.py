import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from pathlib import Path
from math import ceil
import re
import numpy as np

try:
    from plot import *
except ImportError:
    plt.rcParams.update({"font.size": 20})
    bar_styles = {}

USE_LOG_SCALE = True
NS_TO_S = 1e-9

# Updated Paths
DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data")
EXP_FOLDER = "filter_result_dec30_inmemory_interleave-lowpri_true-I450000-U0-Q450000-S0-Y0-T10-P131072-B4-E1024"
DATA_DIR = DATA_ROOT / EXP_FOLDER
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/dec30_totals")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    # "Vector-dynamic",
    "hash_skip_list",
    "hash_linked_list",
    # "skiplist",
    # "UnsortedVector-dynamic",
    # "AlwaysSortedVector-dynamic",
]

TIME_RE = re.compile(r"^(Inserts|PointQuery) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")


def bar_style(buf_name):
    global bar_styles
    default_style = {"color": "None", "edgecolor": "black", "hatch": ""}
    return bar_styles.get(buf_name, default_style).copy()

def apply_axis_style(ax, y_limit_tuple, is_latency_plot):
    if USE_LOG_SCALE:
        if y_limit_tuple: ax.set_ylim(*y_limit_tuple)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        if y_limit_tuple: ax.set_ylim(*y_limit_tuple)
        ax.ticklabel_format(style="plain", axis="y", useOffset=False)
    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

def parse_exec_times(text):
    out = {"Inserts": 0, "PointQuery": 0, "Workload": 0}
    for line in text.splitlines():
        line = line.strip()
        m = TIME_RE.match(line)
        if m:
            out[m.group(1)] = int(m.group(2))
        else:
            m_workload = WORKLOAD_TIME_RE.match(line)
            if m_workload:
                out["Workload"] = int(m_workload.group(1))
    return out

def collect_records(data_dir):
    records = []
    for log_path in data_dir.rglob("*.log"):
        if "workload" not in log_path.name and "run" not in log_path.name:
            continue
        
        buffer_name = log_path.parent.name
        if "AlwayssortedVector" in buffer_name: buffer_key = "AlwaysSortedVector-dynamic"
        elif "hash_skip_list" in buffer_name: buffer_key = "hash_skip_list"
        elif "hash_linked_list" in buffer_name: buffer_key = "hash_linked_list"
        elif "UnsortedVector" in buffer_name: buffer_key = "UnsortedVector-dynamic"
        elif "Vector" in buffer_name: buffer_key = "Vector-dynamic"
        else: buffer_key = buffer_name

        exec_ns = parse_exec_times(log_path.read_text())
        
        records.append({
            "buffer": buffer_key,
            "total_ins_ns": exec_ns["Inserts"],
            "total_pq_ns": exec_ns["PointQuery"],
            "total_workload_ns": exec_ns["Workload"]
        })
    return pd.DataFrame(records).groupby("buffer", as_index=False).mean()

def plot_combined_barchart(df, metric_cols, y_label, x_labels, filename_base, buffers, y_limit):
    # Using your original figure size and logic
    fig, ax = plt.subplots(1, 1, figsize=(5, 3.6))
    if USE_LOG_SCALE: ax.set_yscale("log", base=10)

    num_groups = len(metric_cols)
    num_buffers = len(buffers)
    group_width, bar_width_ratio = 0.9, 0.9
    width_of_one_bar_slot = group_width / num_buffers
    bar_width = width_of_one_bar_slot * bar_width_ratio
    group_x_positions = np.arange(num_groups)

    log_bottom = y_limit[0] if y_limit else 1.0

    for group_idx, metric_col in enumerate(metric_cols):
        group_center = group_x_positions[group_idx]
        group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot
            try:
                value = df.loc[df["buffer"] == buf_name, metric_col].iat[0]
            except:
                value = log_bottom
            
            if USE_LOG_SCALE and value <= 0: value = log_bottom
            ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(group_x_positions)
    ax.set_xticklabels(x_labels)
    apply_axis_style(ax, y_limit, True)
    ax.set_ylabel(y_label)
    fig.tight_layout()
    fig.savefig(filename_base.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)

def main():
    df = collect_records(DATA_DIR)
    if df.empty:
        print("No data found.")
        return

    # Use the specific order from FILTER_BUFFERS
    buffers_to_plot = [b for b in FILTER_BUFFERS if b in df["buffer"].values]

    # Plot 1: Total Latency for Inserts and Point Queries
    # Y-axis in ns (as in your logs) or change to seconds by multiplying df columns by NS_TO_S
    plot_combined_barchart(
        df=df,
        metric_cols=["total_ins_ns", "total_pq_ns"],
        y_label="operation latency (ns)",
        x_labels=["insert", "point query"],
        filename_base=PLOTS_DIR / "total_latency_ops",
        buffers=buffers_to_plot,
        y_limit=(1e7, 1e14) # Adjusted for total ns
    )

    # Plot 2: Total Workload Execution Time
    plot_combined_barchart(
        df=df,
        metric_cols=["total_workload_ns"],
        y_label="time (ns)",
        x_labels=["total workload execution"],
        filename_base=PLOTS_DIR / "total_workload_execution",
        buffers=buffers_to_plot,
        y_limit=(1e7, 1e14)
    )

    print(f"Finished. Plots saved in {PLOTS_DIR}")

if __name__ == "__main__":
    main()