import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from pathlib import Path
import re
import sys
import numpy as np
import os
from math import ceil

try:
    from plot import *
    from plot.style import bar_styles, line_styles, hatch_map
except ImportError:
    print("Error: 'plot' package or 'style.py' not found. Specified font and styles are missing.")
    sys.exit(1)

if plt.rcParams["font.family"][0] == "sans-serif":
    print("Error: Specified academic font not found. Aborting program.")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_NAME = Path(__file__).stem
OUTPUT_DIR = SCRIPT_DIR / "paperplot" / SCRIPT_NAME
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/results_ycsb")
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

def get_mapped_style(buf_name):
    clean = buf_name.lower().replace("-preallocated", "").replace("-x6-h100000", "").replace("_", "").replace("-", "")
    
    plot_style = {"edgecolor": "black", "facecolor": "white", "hatch": ""}
    
    if clean in line_styles:
        plot_style["facecolor"] = line_styles[clean]["color"]
    
    if buf_name in bar_styles:
        plot_style["hatch"] = bar_styles[buf_name].get("hatch", "")
    elif clean in hatch_map:
        plot_style["hatch"] = hatch_map[clean]
        
    return plot_style

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
        style = get_mapped_style(b)
        handles.append(plt.Rectangle((0, 0), 1, 1, facecolor=style["facecolor"], edgecolor=style["edgecolor"], hatch=style["hatch"]))
        
        clean_label = b.replace("-preallocated", "").replace("hash_", "h_").replace("-X6-H100000", "")
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
    print(f"[saved] {output_path.resolve()}")

def plot_workload(workload_id, df):
    df = df[df["buffer"].isin(FILTER_BUFFERS)].copy()
    if df.empty:
        return

    df["buffer"] = pd.Categorical(df["buffer"], categories=FILTER_BUFFERS, ordered=True)
    df = df.sort_values("buffer")

    fig, ax = plt.subplots(figsize=(5, 3.2))

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        ax.set_ylim(*YLIM_EXEC)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))

    x_pos = np.arange(len(df))
    ax.set_xticks(x_pos)
    ax.set_xticklabels([]) 
    ax.tick_params(axis='x', which='both', bottom=True, top=False, labelbottom=False)
    
    for i, row in enumerate(df.itertuples()):
        style = get_mapped_style(row.buffer)
        ax.bar(i, row.exec_time, width=0.7, facecolor=style["facecolor"], edgecolor=style["edgecolor"], hatch=style["hatch"])

    ax.set_xlabel("buffer")
    ax.set_ylabel("Total Execution Time (s)")
    ax.set_title(f"YCSB Workload {workload_id.upper()}")

    fig.tight_layout()
    
    output_path = OUTPUT_DIR / f"ycsb_workload_{workload_id}.pdf"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"[saved] {output_path.resolve()}")

def main():
    for wl in ['a', 'b', 'c', 'd', 'e', 'f']:
        wl_path = DATA_ROOT / f"ycsb-{wl}-1MB"
        if not wl_path.exists():
            continue
        df = collect_workload_data(wl_path)
        plot_workload(wl, df)

    legend_output = OUTPUT_DIR / "ycsb_workload_legend.pdf"
    save_separate_legend(FILTER_BUFFERS, legend_output)

if __name__ == "__main__":
    main()