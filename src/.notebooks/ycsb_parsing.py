import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from pathlib import Path
import re
import sys
import numpy as np
from math import ceil

# --- Font and Style Integrity ---
try:
    from plot import *
except ImportError:
    print("Error: 'plot.py' not found. Specified font and styles are missing.")
    sys.exit(1)

# Abort if the academic font doesn't load
if plt.rcParams["font.family"][0] == "sans-serif":
    print("Error: Specified academic font not found. Aborting program.")
    sys.exit(1)

# --- Configuration ---
DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/results_ycsb")
OUTPUT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/output_plots/ycsb_execution_time")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

USE_LOG_SCALE = False
NS_TO_S = 1e-9
YLIM_EXEC = (1e0, 1e3) 

FILTER_BUFFERS = [
    "skiplist",
    "vector-preallocated",
    "hash_skip_list-X6-H100000",
    "hash_linked_list-X6-H100000",
    "unsortedvector-preallocated",
    "simple_skiplist",
    "hash_vector-X6-H100000",
]

WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")
BUF_PATTERN = re.compile(r"^buffer-\d+MB-\d+-(.*)", re.IGNORECASE)

def bar_style(buf_name):
    if "bar_styles" in globals():
        return bar_styles.get(buf_name, {"color": "white", "edgecolor": "black"}).copy()
    return {"color": "white", "edgecolor": "black"}

def parse_workload_time(file_path):
    if not file_path.exists():
        return None
    content = file_path.read_text()
    for line in content.splitlines():
        m = WORKLOAD_TIME_RE.match(line.strip())
        if m:
            return int(m.group(1)) * NS_TO_S
    return None

def collect_workload_data(workload_dir):
    records = []
    for buf_path in workload_dir.iterdir():
        if not buf_path.is_dir():
            continue
        match = BUF_PATTERN.match(buf_path.name)
        if match:
            buffer_key = match.group(1)
            log_file = buf_path / "workload_run.log"
            exec_time = parse_workload_time(log_file)
            if exec_time is not None:
                records.append({"buffer": buffer_key, "exec_time": exec_time})
    return pd.DataFrame(records)

def save_separate_legend(buffers, output_path):
    handles = []
    labels = []
    for b in buffers:
        style = bar_style(b)
        style.pop('label', None)
        handles.append(plt.Rectangle((0, 0), 1, 1, **style))
        clean_label = b.replace("-preallocated", "").replace("hash_", "h_")
        labels.append(clean_label)
    
    ncol = ceil(len(buffers) / 2)
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend(
        handles,
        labels,
        loc="center",
        ncol=ncol,
        frameon=False,
        fontsize=plt.rcParams["font.size"],
        handletextpad=0.5,
        columnspacing=0.8
    )
    plt.axis("off")
    legend_fig.savefig(output_path, bbox_inches="tight", pad_inches=0.01)
    plt.close(legend_fig)
    print(f"[saved] {output_path.name}")

def plot_workload(workload_id, df):
    df = df[df["buffer"].isin(FILTER_BUFFERS)].copy()
    if df.empty:
        return

    df["buffer"] = pd.Categorical(df["buffer"], categories=FILTER_BUFFERS, ordered=True)
    df = df.sort_values("buffer")

    fig, ax = plt.subplots(figsize=(5, 3.2)) # Adjusted height slightly for label clarity

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        ax.set_ylim(*YLIM_EXEC)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))

    # Strict Tick Control - Removing x-tick labels as they are in the separate legend
    x_pos = np.arange(len(df))
    ax.set_xticks(x_pos)
    ax.set_xticklabels([]) 
    
    # Ensure individual buffer names don't appear on the axis
    ax.tick_params(axis='x', which='both', bottom=True, top=False, labelbottom=False)
    
    for i, row in enumerate(df.itertuples()):
        style = bar_style(row.buffer)
        ax.bar(i, row.exec_time, width=0.7, **style)

    # Label Control
    ax.set_xlabel("buffer")
    ax.set_ylabel("Total Execution Time (s)")
    ax.set_title(f"YCSB Workload {workload_id.upper()}")

    fig.tight_layout()
    
    # PDF only
    output_path = OUTPUT_DIR / f"ycsb_workload_{workload_id}.pdf"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"[saved] {output_path.name}")

def main():
    for wl in ['a', 'b', 'c', 'd', 'e', 'f']:
        wl_path = DATA_ROOT / f"ycsb-{wl}-1MB"
        if not wl_path.exists():
            continue
        df = collect_workload_data(wl_path)
        plot_workload(wl, df)

    # Separate legend remains the only place where buffer names appear
    legend_output = OUTPUT_DIR / "ycsb_workload_legend.pdf"
    save_separate_legend(FILTER_BUFFERS, legend_output)

if __name__ == "__main__":
    main()