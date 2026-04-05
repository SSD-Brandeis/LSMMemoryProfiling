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

# Enforcement of font settings and abort logic as per saved preferences
try:
    from plot import *
except ImportError:
    print("Error: 'plot.py' not found. Aborting program to prevent unauthorized fallback styles.")
    sys.exit(1)

NS_TO_S = 1e-9

PHASE1_INSERTS = 90000000
PHASE2_INSERTS = 10000000
PHASE2_PQ = 10000
#lowpri1
# DATA_DIRS = {
#     "setup1_t2": Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_setup1_t2"),
#     "setup1_t10": Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_setup1_t10"),
#     "setup2_t2": Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_setup2_t2"),
#     "setup2_t10": Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_setup2_t10"),
# }
#lowpri0
DATA_DIRS = {
    "setup1_t2": Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_setup1_t2_lowpri0"),
    "setup1_t10": Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_setup1_t10_lowpri0"),
    "setup2_t2": Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_setup2_t2_lowpri0"),
    "setup2_t10": Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_setup2_t10_lowpri0"),
}

PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/new_ondisk_setups")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = ["skiplist"]

TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery|PointDelete) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")

# Added from old script for resource metric parsing
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
        
    while len(phases) < 2:
        phases.append({"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "PointDelete": 0, "Workload": 0})
        
    return phases[0], phases[1]

def collect_records():
    records = []
    for setup_name, data_dir in DATA_DIRS.items():
        if not data_dir.exists():
            continue

        for log_path in data_dir.rglob("workload*.log"):
            buffer_name = log_path.parent.name
            m_buf = re.match(r"^buffer-\d+-(.*)", buffer_name, re.IGNORECASE)
            buffer_key = m_buf.group(1) if m_buf else buffer_name

            if buffer_key not in FILTER_BUFFERS:
                continue

            content = log_path.read_text()
            phase1, phase2 = parse_exec_times(content)
            
            # Extract compaction and flush counts
            f_count = 0
            c_count = 0
            for match in COUNT_SUM_RE.finditer(content):
                metric_name = match.group("metric")
                count = int(match.group("count"))
                if metric_name == "db.flush.micros":
                    f_count = count
                elif metric_name == "compaction.times.micros":
                    c_count = count

            record = {
                "setup": setup_name,
                "buffer": buffer_key,
                "p1_lat_ins": safe_div(phase1["Inserts"], PHASE1_INSERTS),
                "p2_lat_ins": safe_div(phase2["Inserts"], PHASE2_INSERTS),
                "p2_lat_pq": safe_div(phase2["PointQuery"], PHASE2_PQ),
                "p1_time_ins": phase1["Inserts"] * NS_TO_S,
                "p2_time_ins": phase2["Inserts"] * NS_TO_S,
                "p2_time_pq": phase2["PointQuery"] * NS_TO_S,
                "p1_time_workload": phase1["Workload"] * NS_TO_S,
                "p2_time_workload": phase2["Workload"] * NS_TO_S,
                "total_time_workload": (phase1["Workload"] + phase2["Workload"]) * NS_TO_S,
                "flush_count": f_count,
                "compaction_count": c_count
            }
            records.append(record)
    return pd.DataFrame(records)

def plot_grouped_barchart(df, metric_cols, x_labels, y_label, filename_base):
    fig, ax = plt.subplots(figsize=(6, 4))
    setups = list(DATA_DIRS.keys())
    num_groups = len(metric_cols)
    num_setups = len(setups)
    
    group_width = 0.8
    bar_width = group_width / num_setups
    group_x = np.arange(num_groups)
    
    print(f"\n--- Plotting Data for: {filename_base.name} ---")
    for idx, setup in enumerate(setups):
        setup_data = df[df["setup"] == setup]
        values = []
        for col in metric_cols:
            if not setup_data.empty:
                val = setup_data[col].iloc[0]
                values.append(val)
                print(f"  Setup: {setup:12} | Metric: {col:18} | Value: {val}")
            else:
                values.append(0)
                print(f"  Setup: {setup:12} | Metric: {col:18} | Value: 0 (No Data)")
        
        x_pos = group_x - (group_width/2) + (idx * bar_width) + (bar_width/2)
        ax.bar(x_pos, values, width=bar_width, label=setup, edgecolor="black")
    
    ax.set_xticks(group_x)
    ax.set_xticklabels(x_labels)
    
    # Explicit tick control as per requirements
    y_ticks = ax.get_yticks()
    ax.set_yticks(y_ticks)
    
    ax.set_ylabel(y_label)
    ax.legend(loc="upper left", bbox_to_anchor=(1, 1), frameon=False)
    
    fig.tight_layout()
    output_path = filename_base.with_suffix(".pdf")
    fig.savefig(output_path, bbox_inches="tight")
    
    print(f"  Saved to: {str(output_path.resolve())}")
    plt.close(fig)

def main():
    df = collect_records()
    if df.empty:
        print("Error: No data records found. Check directory paths or log availability.")
        sys.exit(0)

    grouped_df = df.groupby(["setup", "buffer"], as_index=False).mean()

    # Resource Metrics Plots
    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["flush_count", "compaction_count"],
        x_labels=["Flush", "Compaction"],
        y_label="count",
        filename_base=PLOTS_DIR / "resource_counts"
    )

    # Phase 1 Plots
    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["p1_lat_ins"],
        x_labels=["Insert"],
        y_label="mean latency (ns/op)",
        filename_base=PLOTS_DIR / "p1_mean_latency_insert"
    )

    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["p1_time_ins"],
        x_labels=["Insert"],
        y_label="total latency (s)",
        filename_base=PLOTS_DIR / "p1_total_latency_insert"
    )

    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["p1_time_workload"],
        x_labels=["Phase 1"],
        y_label="execution time (s)",
        filename_base=PLOTS_DIR / "p1_workload_time"
    )

    # Phase 2 Plots
    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["p2_lat_ins"],
        x_labels=["Insert"],
        y_label="mean latency (ns/op)",
        filename_base=PLOTS_DIR / "p2_mean_latency_insert"
    )

    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["p2_lat_pq"],
        x_labels=["Point Query"],
        y_label="mean latency (ns/op)",
        filename_base=PLOTS_DIR / "p2_mean_latency_pq"
    )

    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["p2_time_ins"],
        x_labels=["Insert"],
        y_label="total latency (s)",
        filename_base=PLOTS_DIR / "p2_total_latency_insert"
    )

    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["p2_time_pq"],
        x_labels=["Point Query"],
        y_label="total latency (s)",
        filename_base=PLOTS_DIR / "p2_total_latency_pq"
    )

    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["p2_time_workload"],
        x_labels=["Phase 2"],
        y_label="execution time (s)",
        filename_base=PLOTS_DIR / "p2_workload_time"
    )

    # Total Execution Plot
    plot_grouped_barchart(
        df=grouped_df,
        metric_cols=["total_time_workload"],
        x_labels=["Total"],
        y_label="execution time (s)",
        filename_base=PLOTS_DIR / "total_workload_execution"
    )

if __name__ == "__main__":
    main()