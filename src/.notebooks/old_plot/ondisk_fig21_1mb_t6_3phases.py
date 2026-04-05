import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from pathlib import Path
import re
import sys
import numpy as np

# --- Path Injection for style.py ---
STYLE_PATH = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/plot")
if str(STYLE_PATH) not in sys.path:
    sys.path.insert(0, str(STYLE_PATH))

# Import predefined styles from style.py
try:
    from style import bar_styles, line_styles
except ImportError:
    print(f"Error: 'style.py' not found at {STYLE_PATH}. Aborting.")
    sys.exit(1)

# Enforcement of font settings and abort logic as per saved preferences
try:
    from plot import *
except ImportError:
    print(f"Error: 'plot.py' not found at {STYLE_PATH}. Aborting program to prevent unauthorized fallback styles.")
    sys.exit(1)

# ==============================================================================
# Y-LIMIT CONFIGURATION (Hard-code bottom and top here)
# ==============================================================================
# Format: "filename_base": (bottom, top) or None for auto
Y_LIMITS = {
    "resource_counts": (0, 100),
    "p1_mean_latency_insert": (0, 8000),
    "p1_total_latency_insert": None,
    "p1_workload_time": None,
    "p2_mean_latency_insert": (0, 8000),
    "p2_mean_latency_pq": (0, 1500),
    "p2_total_latency_insert": None,
    "p2_total_latency_pq": None,
    "p2_workload_time": None,
    "total_workload_execution": None,
    "p3_mean_latency_range_query": (0, 8000),
    "p3_total_latency_range_query": None,
    "p3_mean_latency_insert": (0, 8000),
}

NS_TO_S = 1e-9

PHASE1_INSERTS = 90000000
PHASE2_INSERTS = 10000000
PHASE2_PQ = 10000

# Phase 3 workload: 1 million insert, 1000 range query
PHASE3_INSERTS = 1000000
PHASE3_RQ = 1000

# ==============================================================================
# DATA PATHS (Updated to 128MB setup)
# ==============================================================================
DATA_BASE_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_multiphase_ondisk_3phase_t6_varybuffer")

DATA_DIRS = {
    "varybuffer_128MB": DATA_BASE_DIR / "-multiphase_ondisk_3phase_varybuffer_t6_128MB-I-1000000-U-0-PQ-0-RQ-1000-S-0.0001-Low_Pri0-Size_Ratio6",
}

PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/ondisk_fig21_1mb_t6_3phases")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# BUFFER FILTERING (Toggle by commenting/uncommenting)
# ==============================================================================
FILTER_BUFFERS = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    # "alwayssortedVector-preallocated",
    "skiplist",
    "simple_skiplist",
    "hash_vector-X6-H100000",
    # "hash_skip_list-X6-H100000",
    "hash_linked_list-X6-H100000",
]

TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery|PointDelete) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")

COUNT_SUM_RE = re.compile(
    r"rocksdb\.(?P<metric>[\w\.]+)\s*.*"
    r"COUNT\s*:\s*(?P<count>\d+)\s*"
    r"SUM\s*:\s*(?P<sum>\d+)"
)

def safe_div(numer, denom):
    if numer and denom:
        return numer / denom
    return 0

def parse_exec_times(text):
    phases = []
    current_phase = None
    for line in text.splitlines():
        m_workload = WORKLOAD_TIME_RE.match(line.strip())
        if m_workload:
            if current_phase is not None:
                phases.append(current_phase)
            current_phase = {
                "Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "PointDelete": 0, 
                "Workload": int(m_workload.group(1))
            }
        else:
            m = TIME_RE.match(line.strip())
            if m and current_phase is not None:
                current_phase[m.group(1)] = int(m.group(2))
    
    if current_phase is not None:
        phases.append(current_phase)
        
    while len(phases) < 3: 
        phases.append({"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "PointDelete": 0, "Workload": 0})
        
    return phases

def collect_records():
    records = []
    
    for setup_name, data_dir in DATA_DIRS.items():
        if not data_dir.exists():
            print(f"Warning: Directory {data_dir} does not exist.")
            continue
        for log_path in data_dir.rglob("workload_run.log"):
            buffer_key = get_buffer_key(log_path)
            if buffer_key not in FILTER_BUFFERS: continue

            content = log_path.read_text()
            phases = parse_exec_times(content)
            phase1, phase2, phase3 = phases[0], phases[1], phases[2]
            
            f_count, c_count = parse_counts(content)

            records.append({
                "setup": setup_name, "buffer": buffer_key,
                "p1_lat_ins": safe_div(phase1["Inserts"], PHASE1_INSERTS),
                "p2_lat_ins": safe_div(phase2["Inserts"], PHASE2_INSERTS),
                "p2_lat_pq": safe_div(phase2["PointQuery"], PHASE2_PQ),
                "p1_time_ins": phase1["Inserts"] * NS_TO_S,
                "p2_time_ins": phase2["Inserts"] * NS_TO_S,
                "p2_time_pq": phase2["PointQuery"] * NS_TO_S,
                "p1_time_workload": phase1["Workload"] * NS_TO_S,
                "p2_time_workload": phase2["Workload"] * NS_TO_S,
                "total_time_workload": (phase1["Workload"] + phase2["Workload"] + phase3["Workload"]) * NS_TO_S,
                "flush_count": f_count, "compaction_count": c_count,
                "p3_lat_rq": safe_div(phase3["RangeQuery"], PHASE3_RQ),
                "p3_time_rq": phase3["RangeQuery"] * NS_TO_S,
                "p3_lat_ins": safe_div(phase3["Inserts"], PHASE3_INSERTS)
            })

    return pd.DataFrame(records)

def get_buffer_key(log_path):
    buffer_name = log_path.parent.name
    # Updated regex to match "buffer-128MB-1-skiplist"
    m_buf = re.match(r"^buffer-\d+MB-\d+-(.*)", buffer_name, re.IGNORECASE)
    return m_buf.group(1) if m_buf else buffer_name

def parse_counts(content):
    f_count = 0
    c_count = 0
    for match in COUNT_SUM_RE.finditer(content):
        metric_name = match.group("metric")
        count = int(match.group("count"))
        if metric_name == "db.flush.micros":
            f_count = count
        elif metric_name == "compaction.times.micros":
            c_count = count
    return f_count, c_count

def generate_separate_legend():
    """Creates a standalone PDF containing only the buffer legend using bar_styles."""
    fig_leg = plt.figure(figsize=(10, 1))
    ax_leg = fig_leg.add_subplot(111)
    
    for buf in FILTER_BUFFERS:
        style = bar_styles.get(buf, {"label": buf})
        clean_label = style["label"].replace("_", "\\_")
        
        ax_leg.bar(0, 0, 
                   facecolor=style.get("facecolor", "white"),
                   edgecolor=style.get("edgecolor", "black"),
                   hatch=style.get("hatch"),
                   linewidth=style.get("linewidth", 1),
                   label=clean_label)
    
    handles, labels = ax_leg.get_legend_handles_labels()
    fig_leg.legend(handles, labels, loc='center', frameon=False, ncol=4)
    ax_leg.axis('off')
    
    output_path = PLOTS_DIR / "standalone_legend.pdf"
    fig_leg.savefig(output_path, bbox_inches='tight')
    plt.close(fig_leg)
    print(f"  Standalone legend saved to: {str(output_path.resolve())}")

def plot_grouped_barchart(df, metric_cols, x_labels, y_label, filename_base, y_limit=None):
    setups = df["setup"].unique()
    
    for setup in setups:
        fig, ax = plt.subplots(figsize=(6, 4))
        setup_df = df[df["setup"] == setup]
        
        active_buffers = [b for b in FILTER_BUFFERS if b in setup_df["buffer"].unique()]
        num_buffers = len(active_buffers)
        num_metrics = len(metric_cols)
        
        group_width = 0.8
        bar_width = group_width / num_buffers if num_buffers > 0 else group_width
        group_x = np.arange(num_metrics)
        
        print(f"\n--- Plotting {setup} for: {filename_base.name} ---")
        
        for idx, buf in enumerate(active_buffers):
            buf_data = setup_df[setup_df["buffer"] == buf]
            values = [buf_data[col].mean() if not buf_data.empty else 0 for col in metric_cols]
            x_pos = group_x - (group_width/2) + (idx * bar_width) + (bar_width/2)
            
            style = bar_styles.get(buf, {})
            ax.bar(x_pos, values, 
                   width=bar_width, 
                   facecolor=style.get("facecolor", "white"),
                   edgecolor=style.get("edgecolor", "black"),
                   hatch=style.get("hatch"),
                   linewidth=style.get("linewidth", 1))
            
            for i, val in enumerate(values):
                print(f"  Setup: {setup:12} | Buffer: {buf:30} | Metric: {metric_cols[i]:18} | Value: {val}")

        ax.set_xticks(group_x)
        ax.set_xticklabels(x_labels)
        
        # Apply the hard-coded limit if provided
        if y_limit is not None:
            ax.set_ylim(bottom=y_limit[0], top=y_limit[1])
        
        ax.yaxis.set_major_locator(mticker.AutoLocator())
        y_ticks = ax.get_yticks()
        ax.set_yticks(y_ticks)
        
        ax.set_ylabel(y_label)
        ax.set_title(f"Setup: {setup}")
        
        fig.tight_layout()
        setup_filename = f"{filename_base.name}_{setup}.pdf"
        output_path = PLOTS_DIR / setup_filename
        fig.savefig(output_path, bbox_inches="tight")
        
        print(f"  Saved to: {str(output_path.resolve())}")
        plt.close(fig)

def main():
    df = collect_records()
    if df.empty:
        print("Error: No data records found. Check directory paths or log availability.")
        sys.exit(0)

    generate_separate_legend()

    grouped_df = df.replace(0, np.nan).groupby(["setup", "buffer"], as_index=False).mean().fillna(0)

    plot_configs = [
        (["flush_count", "compaction_count"], ["Flush", "Compaction"], "count", "resource_counts"),
        (["p1_lat_ins"], ["Phase I Insert"], "mean latency (ns/op)", "p1_mean_latency_insert"),
        (["p1_time_ins"], ["Phase I Insert"], "total latency (s)", "p1_total_latency_insert"),
        (["p1_time_workload"], ["Phase I Workload"], "execution time (s)", "p1_workload_time"),
        (["p2_lat_ins"], ["Phase II Insert"], "mean latency (ns/op)", "p2_mean_latency_insert"),
        (["p2_lat_pq"], ["Phase II Point Query"], "mean latency (ns/op)", "p2_mean_latency_pq"),
        (["p2_time_ins"], ["Phase II Insert"], "total latency (s)", "p2_total_latency_insert"),
        (["p2_time_pq"], ["Phase II Point Query"], "total latency (s)", "p2_total_latency_pq"),
        (["p2_time_workload"], ["Phase II Workload"], "execution time (s)", "p2_workload_time"),
        (["total_time_workload"], ["Total Workload"], "execution time (s)", "total_workload_execution"),
        (["p3_lat_rq"], ["Phase III Range Query"], "mean latency (ns/op)", "p3_mean_latency_range_query"),
        (["p3_time_rq"], ["Phase III Range Query"], "total latency (s)", "p3_total_latency_range_query"),
        (["p3_lat_ins"], ["Phase III Insert"], "mean latency (ns/op)", "p3_mean_latency_insert"),
    ]

    for metrics, x_labs, y_lab, file_base in plot_configs:
        # Retrieve the limit from the configuration defined at the beginning
        y_lim = Y_LIMITS.get(file_base, None)
        
        plot_grouped_barchart(
            df=grouped_df,
            metric_cols=metrics,
            x_labels=x_labs,
            y_label=y_lab,
            filename_base=PLOTS_DIR / file_base,
            y_limit=y_lim
        )

if __name__ == "__main__":
    main()