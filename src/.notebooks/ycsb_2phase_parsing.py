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

# Ensure the script's directory is in sys.path to find the sibling 'plot' package
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# Import directly from style to avoid broken plot/__init__.py
from plot.style import line_styles, hatch_map

if plt.rcParams["font.family"][0] == "sans-serif":
    print("Error: Specified academic font not found. Aborting program.")
    sys.exit(1)

# --- Configuration ---
# Parameter to switch between linear and log
USE_LOG_SCALE = False
NS_TO_S = 1e-9

SCRIPT_NAME = Path(__file__).stem
OUTPUT_DIR = SCRIPT_DIR / "paperplot" / SCRIPT_NAME
LATENCY_DIR = OUTPUT_DIR / "ycsb tail latency plot"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LATENCY_DIR.mkdir(parents=True, exist_ok=True)

DATA_ROOT = Path("/Users/cba/Desktop/LSM/LSMMemoryProfiling/data_new/filter_result_ycsb_2phase")

PLOT_PERCENTILES = ["P100"]

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

LATENCY_RE = re.compile(
    r"rocksdb\.db\.(get|write)\.micros\s+P50\s+:\s+([\d.]+)\s+P95\s+:\s+([\d.]+)\s+P99\s+:\s+([\d.]+)\s+P100\s+:\s+([\d.]+)"
)

# --- Helpers matching your reference script ---

def normalize_name(name):
    """Exactly copied from your reference script for mapping consistency."""
    name = name.lower()
    if "hashlinkedlist" in name or "hash_linked_list" in name:
        return "hashlinkedlist"
    if "hashskiplist" in name or "hash_skip_list" in name:
        return "hashskiplist"
    if "hashvector" in name or "hash_vector" in name:
        return "hashvector"
    if "simpleskiplist" in name or "simple_skiplist" in name:
        return "simpleskiplist"
    if name == "skiplist":
        return "skiplist"
    if "linkedlist" in name:
        return "linkedlist"
    if "unsortedvector" in name:
        return "unsortedvector"
    if "alwayssortedvector" in name or "sortedvector" in name:
        return "alwayssortedvector"
    if "vector" in name:
        return "vector"
    return None

def get_mapped_style(buf_name):
    """Applies styling: solid for vector, hollow with colored edges and hatches for others."""
    key = normalize_name(buf_name)
    style = line_styles.get(key, {"color": "black", "label": buf_name})
    color = style["color"]
    hatch = hatch_map.get(key, None)
    
    # Strictly follow reference logic: Only vector is solid, others are hollow
    facecolor = color if key == "vector" else "none"
    
    return {
        "facecolor": facecolor,
        "edgecolor": color,
        "hatch": hatch,
        "label": style.get("label", buf_name)
    }

# --- Data Parsing ---

def parse_workload_time(file_path):
    if not file_path.exists(): return []
    content = file_path.read_text()
    times = []
    for line in content.splitlines():
        m = WORKLOAD_TIME_RE.match(line.strip())
        if m: times.append(int(m.group(1)) * NS_TO_S)
    return times

def parse_latency_metrics(file_path):
    if not file_path.exists(): return []
    results = []
    content = file_path.read_text()
    op_counts = {}
    for line in content.splitlines():
        m = LATENCY_RE.search(line)
        if m:
            op_type = m.group(1)
            op_counts[op_type] = op_counts.get(op_type, 0) + 1
            phase = f"p{op_counts[op_type]}"
            results.append({
                "op": op_type, "phase": phase,
                "P50": float(m.group(2)), "P95": float(m.group(3)),
                "P99": float(m.group(4)), "P100": float(m.group(5))
            })
    return results

def collect_workload_data(workload_dir):
    records, latency_records = [], []
    for buf_path in workload_dir.iterdir():
        if not buf_path.is_dir(): continue
        match = BUF_PATTERN.match(buf_path.name)
        if match:
            buffer_key = match.group(1)
            log_file = buf_path / "workload_run.log"
            exec_times = parse_workload_time(log_file)
            for i, exec_time in enumerate(exec_times):
                records.append({"buffer": buffer_key, "exec_time": exec_time, "phase": f"p{i+1}"})
            latencies = parse_latency_metrics(log_file)
            for lat in latencies:
                lat["buffer"] = buffer_key
                latency_records.append(lat)
    return pd.DataFrame(records), pd.DataFrame(latency_records)

# --- Plotting ---

def plot_workload(workload_id, df):
    df = df[df["buffer"].isin(FILTER_BUFFERS)].copy()
    if df.empty: return
    unique_phases = sorted(df["phase"].unique())
    num_phases, num_impls = len(unique_phases), len(FILTER_BUFFERS)
    
    fig, ax = plt.subplots(figsize=(4, 3))
    
    y_max = df["exec_time"].max()
    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        # Starting at 10^0 (1)
        ax.set_ylim(bottom=1, top=y_max * 10)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        ax.set_ylim(bottom=0, top=y_max * 1.1)

    x = np.arange(num_phases)
    bar_width = 0.9 / num_impls

    for j, buf in enumerate(FILTER_BUFFERS):
        style = get_mapped_style(buf)
        heights = []
        for phase in unique_phases:
            val = df[(df["buffer"] == buf) & (df["phase"] == phase)]["exec_time"]
            heights.append(val.values[0] if not val.empty else 0)
        
        xpos = x + j * bar_width
        ax.bar(xpos, heights, bar_width - 0.008, 
               facecolor=style["facecolor"], edgecolor=style["edgecolor"], 
               hatch=style["hatch"], linewidth=1.5)

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels([f"{i+1}" for i in range(num_phases)]) 
    
    ax.set_xlabel("phase", labelpad=-1)
    ax.set_ylabel("Total Execution Time (s)", labelpad=-1)
    ax.set_title(f"YCSB Workload {workload_id.upper()}")
    
    fig.tight_layout(pad=0.02)
    output_path = OUTPUT_DIR / f"ycsb_workload_{workload_id}.pdf"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"[saved] {output_path.resolve()}")

def plot_latency_workload(workload_id, df, op_type):
    df = df[(df["buffer"].isin(FILTER_BUFFERS)) & (df["op"] == op_type)].copy()
    if df.empty: return
    unique_phases = sorted(df["phase"].unique())
    num_phases, num_impls = len(unique_phases), len(FILTER_BUFFERS)
    
    fig, ax = plt.subplots(figsize=(4, 3))
    
    y_data_max = df[PLOT_PERCENTILES].max().max()
    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
        # Starting at 10^0 (1)
        ax.set_ylim(bottom=1, top=y_data_max * 10)
    else:
        ax.set_ylim(bottom=0, top=y_data_max * 1.1)

    x = np.arange(num_phases)
    width = 0.8 / num_impls

    for i, phase in enumerate(unique_phases):
        for j, buf in enumerate(FILTER_BUFFERS):
            style = get_mapped_style(buf)
            subset = df[(df["buffer"] == buf) & (df["phase"] == phase)]
            if subset.empty: continue
            row = subset.iloc[0]
            
            stats = {
                "med": row["P50"], "q1": row["P50"], "q3": row["P95"],
                "whislo": row["P50"], "whishi": row["P99"],
                "fliers": [row["P100"]] if "P100" in PLOT_PERCENTILES else [], "label": ""
            }
            
            bp = ax.bxp([stats], positions=[i + j * width], widths=width * 0.9, patch_artist=True, showfliers=True, manage_ticks=False)
            
            for patch in bp['boxes']:
                patch.set_facecolor("none")
                patch.set_edgecolor(style["edgecolor"])
                patch.set_hatch(style["hatch"])
                patch.set_linewidth(1.5)
            
            plt.setp(bp['whiskers'], color=style["edgecolor"], linewidth=1.5)
            plt.setp(bp['caps'], color=style["edgecolor"], linewidth=1.5)
            plt.setp(bp['medians'], color=style["edgecolor"], linewidth=2)
            plt.setp(bp['fliers'], markeredgecolor=style["edgecolor"], marker='o', markersize=4)

    ax.set_xticks(x + width * (num_impls - 1) / 2)
    ax.set_xticklabels([f"{i+1}" for i in range(num_phases)])
    ax.set_ylabel(r"Latency ($\mu$s)", labelpad=-1)
    ax.set_xlabel("phase", labelpad=-1)
    
    fig.tight_layout(pad=0.02)
    output_path = LATENCY_DIR / f"ycsb_latency_{workload_id}_{op_type}.pdf"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"[saved] {output_path.resolve()}")

def save_separate_legend(buffers, output_path):
    from matplotlib.patches import Patch
    legend_elements = []
    for b in buffers:
        style = get_mapped_style(b)
        legend_elements.append(
            Patch(facecolor=style["facecolor"], edgecolor=style["edgecolor"], 
                  hatch=style["hatch"], label=style["label"], linewidth=1.5)
        )
    
    fig_legend = plt.figure(figsize=(10, 1.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(handles=legend_elements, loc="center", ncol=8, frameon=False, 
                      columnspacing=0.5, handletextpad=0.2)
    fig_legend.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"[saved legend] {output_path.resolve()}")

def save_latency_legend(output_path):
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], color='black', marker='o', linestyle='None', markersize=4, label='P100 (Max)'),
        plt.Rectangle((0, 0), 1, 1, facecolor='none', edgecolor='black', label='Box')
    ]
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    legend_fig.legend(handles=legend_elements, loc="center", ncol=2, frameon=False)
    plt.axis("off")
    legend_fig.savefig(output_path, bbox_inches="tight", pad_inches=0.01)
    plt.close(legend_fig)
    print(f"[saved latency legend] {output_path.resolve()}")

def main():
    for wl in ['a', 'b', 'c', 'd', 'e', 'f']:
        wl_path = DATA_ROOT / f"ycsb-{wl}-1MB"
        if not wl_path.exists(): continue
        df_exec, df_lat = collect_workload_data(wl_path)
        plot_workload(wl, df_exec)
        for op in ['get', 'write']: plot_latency_workload(wl, df_lat, op)
    save_separate_legend(FILTER_BUFFERS, OUTPUT_DIR / "ycsb_workload_legend.pdf")
    save_latency_legend(LATENCY_DIR / "ycsb_latency_legend.pdf")

if __name__ == "__main__":
    main()