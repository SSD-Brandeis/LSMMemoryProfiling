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

# --- Configuration ---
SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_NAME = Path(__file__).stem
OUTPUT_DIR = SCRIPT_DIR / "paperplot" / SCRIPT_NAME
# Nested folder for tail latency plots
LATENCY_DIR = OUTPUT_DIR / "ycsb tail latency plot"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LATENCY_DIR.mkdir(parents=True, exist_ok=True)

DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/results_ycsb")

# Parameter to switch between linear and log
USE_LOG_SCALE = True
NS_TO_S = 1e-9

# Control which percentiles to plot - comment out to remove from the boxplot
PLOT_PERCENTILES = [
    # "P50", 
    # "P95", 
    # "P99", 
    "P100"
]


FILTER_BUFFERS = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "skiplist",
    "simple_skiplist",
    "hash_linked_list-X6-H100000",
    "hash_skip_list-X6-H100000",
    "hash_vector-X6-H100000",
]

WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")
BUF_PATTERN = re.compile(r"^buffer-\d+MB-\d+-(.*)", re.IGNORECASE)

# Regex for rocksdb latency metrics (P50, P95, P99, P100)
LATENCY_RE = re.compile(
    r"rocksdb\.db\.(get|write)\.micros\s+P50\s+:\s+([\d.]+)\s+P95\s+:\s+([\d.]+)\s+P99\s+:\s+([\d.]+)\s+P100\s+:\s+([\d.]+)"
)

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

def parse_latency_metrics(file_path):
    if not file_path.exists():
        return []
    results = []
    content = file_path.read_text()
    for line in content.splitlines():
        m = LATENCY_RE.search(line)
        if m:
            results.append({
                "op": m.group(1),
                "P50": float(m.group(2)),
                "P95": float(m.group(3)),
                "P99": float(m.group(4)),
                "P100": float(m.group(5))
            })
    return results

def collect_workload_data(workload_dir):
    records = []
    latency_records = []
    for buf_path in workload_dir.iterdir():
        if not buf_path.is_dir():
            continue
        match = BUF_PATTERN.match(buf_path.name)
        if match:
            buffer_key = match.group(1)
            log_file = buf_path / "workload_run.log"
            
            # Execution time
            exec_time = parse_workload_time(log_file)
            if exec_time is not None:
                records.append({"buffer": buffer_key, "exec_time": exec_time})
            
            # Tail Latency
            latencies = parse_latency_metrics(log_file)
            for lat in latencies:
                lat["buffer"] = buffer_key
                latency_records.append(lat)
                
    return pd.DataFrame(records), pd.DataFrame(latency_records)

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
    print(f"[saved legend] {output_path.resolve()}")

def save_latency_legend(output_path):
    from matplotlib.lines import Line2D
    legend_elements = []
    
    if "P100" in PLOT_PERCENTILES:
        legend_elements.append(Line2D([0], [0], color='black', marker='o', linestyle='None', markersize=4, label='P100 (Max)'))
    if "P99" in PLOT_PERCENTILES:
        legend_elements.append(Line2D([0], [0], color='black', linestyle='-', label='Whisker (P99)'))
    
    # Show box meaning based on P50/P95 presence
    box_label = "Box"
    if "P50" in PLOT_PERCENTILES and "P95" in PLOT_PERCENTILES:
        box_label = "Box (P50-P95)"
    elif "P95" in PLOT_PERCENTILES:
        box_label = "Box (to P95)"

    legend_elements.append(plt.Rectangle((0, 0), 1, 1, facecolor='lightgray', edgecolor='black', label=box_label))

    legend_fig = plt.figure(figsize=(0.1, 0.1))
    legend_fig.legend(
        handles=legend_elements,
        loc="center",
        ncol=len(legend_elements),
        frameon=False,
        fontsize=plt.rcParams["font.size"]
    )
    plt.axis("off")
    legend_fig.savefig(output_path, bbox_inches="tight", pad_inches=0.01)
    plt.close(legend_fig)
    print(f"[saved latency legend] {output_path.resolve()}")

def plot_workload(workload_id, df):
    df = df[df["buffer"].isin(FILTER_BUFFERS)].copy()
    if df.empty:
        return

    df["buffer"] = pd.Categorical(df["buffer"], categories=FILTER_BUFFERS, ordered=True)
    df = df.sort_values("buffer")

    fig, ax = plt.subplots(figsize=(5, 3.2))

    y_max = df["exec_time"].max()

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        ax.set_ylim(bottom=10, top=y_max * 5 if y_max > 10 else 100)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        ax.set_ylim(bottom=0, top=y_max * 1.1)

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

def plot_latency_workload(workload_id, df, op_type):
    """Generates box plots for requested percentiles with dynamic autoscaling."""
    df = df[(df["buffer"].isin(FILTER_BUFFERS)) & (df["op"] == op_type)].copy()
    if df.empty:
        return

    df["buffer"] = pd.Categorical(df["buffer"], categories=FILTER_BUFFERS, ordered=True)
    df = df.sort_values("buffer")

    fig, ax = plt.subplots(figsize=(6, 3.5))

    stats_list = []
    for row in df.itertuples():
        # Map values based on which percentiles are enabled in PLOT_PERCENTILES
        med = getattr(row, "P50") if "P50" in PLOT_PERCENTILES else 0
        q1 = getattr(row, "P50") if "P50" in PLOT_PERCENTILES else 0
        q3 = getattr(row, "P95") if "P95" in PLOT_PERCENTILES else med
        whislo = med
        whishi = getattr(row, "P99") if "P99" in PLOT_PERCENTILES else q3
        fliers = [getattr(row, "P100")] if "P100" in PLOT_PERCENTILES else []

        stats = {
            "med": med,
            "q1": q1,
            "q3": q3,
            "whislo": whislo,
            "whishi": whishi,
            "fliers": fliers,
            "label": ""
        }
        stats_list.append(stats)

    # --- Fixed Autoscaling Logic ---
    # Only calculate the max based on percentiles that are actually being plotted
    y_data_max = df[PLOT_PERCENTILES].max().max()

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
        # Headroom for log scale visibility
        ax.set_ylim(bottom=10, top=y_data_max * 5 if y_data_max > 10 else 100)
    else:
        # 10% headroom for linear scale
        ax.set_ylim(bottom=0, top=y_data_max * 1.1)

    for i, (stats, buf_name) in enumerate(zip(stats_list, df["buffer"])):
        style = get_mapped_style(buf_name)
        
        bp = ax.bxp([stats], positions=[i], widths=0.6, patch_artist=True,
                    showfliers=True, manage_ticks=False)
        
        for patch in bp['boxes']:
            patch.set_facecolor(style["facecolor"])
            patch.set_edgecolor(style["edgecolor"])
            patch.set_hatch(style["hatch"])
        
        plt.setp(bp['whiskers'], color='black', linestyle='-')
        plt.setp(bp['caps'], color='black')
        plt.setp(bp['medians'], color='black', linewidth=1.5)
        plt.setp(bp['fliers'], markeredgecolor='black', marker='o', markersize=4)

    ax.set_xticks(range(len(df)))
    ax.set_xticklabels([])
    ax.tick_params(axis='x', which='both', bottom=True, labelbottom=False)
    
    ax.set_xlabel("buffer")
    ax.set_ylabel(r"Latency ($\mu$s)")
    ax.set_title(f"YCSB {workload_id.upper()} - {op_type.capitalize()} Tail Latency")

    fig.tight_layout()
    output_path = LATENCY_DIR / f"ycsb_latency_{workload_id}_{op_type}.pdf"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"[saved] {output_path.resolve()}")

def main():
    for wl in ['a', 'b', 'c', 'd', 'e', 'f']:
        wl_path = DATA_ROOT / f"ycsb-{wl}-1MB"
        if not wl_path.exists():
            continue
        df_exec, df_lat = collect_workload_data(wl_path)
        
        plot_workload(wl, df_exec)
        
        for op in ['get', 'write']:
            plot_latency_workload(wl, df_lat, op)

    # Legends are strictly generated in separate files
    legend_output = OUTPUT_DIR / "ycsb_workload_legend.pdf"
    save_separate_legend(FILTER_BUFFERS, legend_output)

    latency_legend_output = LATENCY_DIR / "ycsb_latency_legend.pdf"
    save_latency_legend(latency_legend_output)

if __name__ == "__main__":
    main()