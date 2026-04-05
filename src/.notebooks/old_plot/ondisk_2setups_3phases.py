import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from pathlib import Path
import re
import sys
import numpy as np

# --- Path Injection for style.py and plot.py ---
STYLE_PATH = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/plot")
if str(STYLE_PATH) not in sys.path:
    sys.path.insert(0, str(STYLE_PATH))

try:
    from style import line_styles
except ImportError:
    print(f"Error: 'style.py' not found at {STYLE_PATH}. Aborting.")
    sys.exit(1)

try:
    from plot import *
except ImportError:
    print(f"Error: 'plot.py' not found at {STYLE_PATH}. Aborting.")
    sys.exit(1)

NS_TO_S = 1e-9

# Standard workload constants for mean latency calculation
PHASE1_INSERTS = 90000000
PHASE2_INSERTS = 10000000
PHASE2_PQ = 10000
PHASE3_INSERTS = 1000000
PHASE3_RQ = 1000

DATA_BASE_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_multiphase_ondisk_3phase_t6_varybuffer")
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/vary_buffer_size_line")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    "skiplist",
    "vector-preallocated",
    "hash_skip_list-X6-H100000",
    "hash_linked_list-X6-H100000",
    "unsortedvector-preallocated",
    "alwayssortedVector-preallocated",
    "simple_skiplist",
    "hash_vector-X6-H100000",
]

TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery|PointDelete) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")

def safe_div(numer, denom):
    return numer / denom if numer and denom else 0

def get_buffer_details(sub_dir_name):
    # Pattern: buffer-<size>MB-<index>-<type>
    m = re.match(r"buffer-(\d+)MB-\d+-(.*)", sub_dir_name)
    if m:
        return int(m.group(1)), m.group(2)
    return None, None

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
    if not DATA_BASE_DIR.exists():
        print(f"Error: Path {DATA_BASE_DIR} does not exist.")
        return pd.DataFrame()

    for exp_dir in sorted(DATA_BASE_DIR.iterdir()):
        if not exp_dir.is_dir(): continue
        
        for buffer_dir in exp_dir.iterdir():
            if not buffer_dir.is_dir(): continue
                
            size, buffer_key = get_buffer_details(buffer_dir.name)
            if size is None or buffer_key not in FILTER_BUFFERS:
                continue

            log_path = buffer_dir / "workload_run.log"
            if not log_path.exists(): continue

            content = log_path.read_text()
            phases = parse_exec_times(content)
            p1, p2, p3 = phases[0], phases[1], phases[2]

            records.append({
                "buffer_size": size,
                "buffer_type": buffer_key,
                "p1_ins_lat": safe_div(p1["Inserts"], PHASE1_INSERTS),
                "p2_ins_lat": safe_div(p2["Inserts"], PHASE2_INSERTS),
                "p3_ins_lat": safe_div(p3["Inserts"], PHASE3_INSERTS),
                "p2_pq_lat": safe_div(p2["PointQuery"], PHASE2_PQ),
                "p3_rq_lat": safe_div(p3["RangeQuery"], PHASE3_RQ),
                "total_time": (p1["Workload"] + p2["Workload"] + p3["Workload"]) * NS_TO_S
            })

    return pd.DataFrame(records)

def generate_separate_legend():
    fig_leg = plt.figure(figsize=(10, 1))
    ax_leg = fig_leg.add_subplot(111)
    
    for buf in FILTER_BUFFERS:
        style = line_styles.get(buf, {"label": buf})
        clean_label = style.get("label", buf).replace("_", "\\_")
        
        ax_leg.plot([], [], 
                    color=style.get("color"),
                    marker=style.get("marker", "o"),
                    linestyle=style.get("linestyle", "-"),
                    linewidth=1.5,
                    label=clean_label)
    
    handles, labels = ax_leg.get_legend_handles_labels()
    fig_leg.legend(handles, labels, loc='center', frameon=False, ncol=4)
    ax_leg.axis('off')
    
    output_path = PLOTS_DIR / "line_legend_standalone.pdf"
    fig_leg.savefig(output_path, bbox_inches='tight')
    plt.close(fig_leg)
    print(f"  Legend saved to: {output_path}")

def plot_lines(df, metric_col, y_label, filename_base):
    # Mean aggregation and sorting
    df_grouped = df.groupby(["buffer_size", "buffer_type"])[metric_col].mean().reset_index()
    df_grouped = df_grouped.sort_values("buffer_size")
    
    all_sizes = sorted(df_grouped["buffer_size"].unique())
    size_to_idx = {size: i for i, size in enumerate(all_sizes)}
    x_indices = np.arange(len(all_sizes))

    fig, ax = plt.subplots(figsize=(6, 4))
    
    for buf in df_grouped["buffer_type"].unique():
        buf_df = df_grouped[df_grouped["buffer_type"] == buf]
        style = line_styles.get(buf, {})
        
        # Categorical plotting using indices
        plot_x = [size_to_idx[s] for s in buf_df["buffer_size"]]
        
        ax.plot(plot_x, buf_df[metric_col], 
                color=style.get("color"),
                marker=style.get("marker", "o"),
                linestyle=style.get("linestyle", "-"),
                linewidth=1.5)

    # Tick and Formatting
    ax.set_xticks(x_indices)
    ax.set_xticklabels([f"{s}MB" for s in all_sizes])
    
    # Y-axis MUST start at 0
    ax.set_ylim(bottom=0)
    
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=8))
    ax.set_yticks(ax.get_yticks())

    ax.set_xlabel("Buffer Size")
    ax.set_ylabel(y_label)
    ax.grid(False) # No background dash lines

    fig.tight_layout()
    output_path = PLOTS_DIR / f"{filename_base}.pdf"
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved plot: {output_path}")

def main():
    df = collect_records()
    if df.empty:
        print("Error: No data records found.")
        sys.exit(0)

    generate_separate_legend()

    configs = [
        ("p1_ins_lat", "Phase I Mean Insert Latency (ns/op)", "p1_insert_latency"),
        ("p2_ins_lat", "Phase II Mean Insert Latency (ns/op)", "p2_insert_latency"),
        ("p3_ins_lat", "Phase III Mean Insert Latency (ns/op)", "p3_insert_latency"),
        ("p2_pq_lat", "Phase II Mean PQ Latency (ns/op)", "p2_pq_latency"),
        ("p3_rq_lat", "Phase III Mean RQ Latency (ns/op)", "p3_rq_latency"),
        ("total_time", "Total Execution Time (s)", "total_execution_time"),
    ]

    for col, y_lab, f_name in configs:
        plot_lines(df, col, y_lab, f_name)

if __name__ == "__main__":
    main()