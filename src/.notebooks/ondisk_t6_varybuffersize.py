import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from pathlib import Path
import re
import sys
import numpy as np

# ==============================================================================
# GLOBAL LOG SCALE CONTROL
# ==============================================================================
LOG_SCALE = True # Set to True for log scale, False for linear scale (global)

# ==============================================================================
# Y-AXIS TICKER CONTROL (ADJUST EACH PLOT HERE)
# ==============================================================================
# ylim: (bottom, top) or None. If top is None, script adds padding automatically.
# yticks: [list of specific values] or None
# Y_AXIS_CONTROL = {
#     "ph1_ins":    {"use_log": False, "ylim": (0, 200000), "yticks": [0, 50000, 100000, 150000, 200000], "num_ticks": 6},
#     "ph2_ins":    {"use_log": False, "ylim": (0, None),   "yticks": None, "num_ticks": 6},
#     "ph3_ins":    {"use_log": False, "ylim": (0, 250000), "yticks": [0, 50000, 100000, 150000, 200000, 250000], "num_ticks": 10},
#     "ph2_pq":     {"use_log": False, "ylim": (0, None),   "yticks": None, "num_ticks": 8},
#     "ph3_rq":     {"use_log": True,  "ylim": (1, None),   "yticks": None, "num_ticks": 8},
#     "total_time": {"use_log": False, "ylim": (0, 25000),  "yticks": [0, 5000, 10000, 15000, 20000, 25000], "num_ticks": 5},
# }
# ==============================================================================

# --- Path Injection and Style Enforcement ---
STYLE_PATH = Path("/Users/cba/Desktop/LSM/LSMMemoryProfiling/src/.notebooks/plot")

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
    print(f"Error: 'plot.py' not found at {STYLE_PATH}. Aborting to prevent unauthorized fallback styles.")
    sys.exit(1)

NS_TO_S = 1e-9

# PHASE1_INSERTS = 90000000
# PHASE2_INSERTS = 10000000
# PHASE2_PQ = 10000
# PHASE3_INSERTS = 1000000
# PHASE3_RQ = 100

PHASE1_INSERTS = 80000000
PHASE2_INSERTS = 10000000
PHASE2_PQ = 10000
PHASE3_INSERTS = 10000000
PHASE3_RQ = 1000


DATA_BASE_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_multiphase_ondisk_3phase_t6_varybuffer")
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/vary_buffer_size_line")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "alwayssortedVector-preallocated",
    "skiplist",
    "simple_skiplist",
    "hash_vector-X6-H100000",
    "hash_skip_list-X6-H100000",
    "hash_linked_list-X6-H100000",
]

TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery|PointDelete) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")

def safe_div(numer, denom):
    return numer / denom if numer and denom else 0

def get_buffer_details(sub_dir_name):
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
            current_phase = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "Workload": int(m_workload.group(1))}
        else:
            m = TIME_RE.match(line.strip())
            if m and current_phase is not None:
                current_phase[m.group(1)] = int(m.group(2))
    
    if current_phase: phases.append(current_phase)
    while len(phases) < 3:
        phases.append({"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "Workload": 0})
    return phases

def collect_records():
    records = []
    if not DATA_BASE_DIR.exists():
        print(f"Error: {DATA_BASE_DIR} not found.")
        return pd.DataFrame()

    for exp_dir in sorted(DATA_BASE_DIR.iterdir()):
        if not exp_dir.is_dir(): continue
        for buffer_dir in exp_dir.iterdir():
            if not buffer_dir.is_dir(): continue
            size, buffer_key = get_buffer_details(buffer_dir.name)
            if size is None or buffer_key not in FILTER_BUFFERS: continue

            log_path = buffer_dir / "workload_run.log"
            if not log_path.exists(): continue

            content = log_path.read_text()
            ph = parse_exec_times(content)

            records.append({
                "buffer_size": size,
                "buffer_type": buffer_key,
                "ph1_ins_lat": safe_div(ph[0]["Inserts"], PHASE1_INSERTS),
                "ph2_ins_lat": safe_div(ph[1]["Inserts"], PHASE2_INSERTS),
                "ph3_ins_lat": safe_div(ph[2]["Inserts"], PHASE3_INSERTS),
                "ph2_pq_lat": safe_div(ph[1]["PointQuery"], PHASE2_PQ),
                "ph3_rq_lat": safe_div(ph[2]["RangeQuery"], PHASE3_RQ),
                "total_time": sum(p["Workload"] for p in ph) * NS_TO_S
            })
    return pd.DataFrame(records)

def generate_separate_legend():
    """Generates standalone legend based on line_styles from style.py."""
    fig_leg = plt.figure(figsize=(12, 1))
    ax_leg = fig_leg.add_subplot(111)
    for buf in FILTER_BUFFERS:
        style = line_styles.get(buf, {"label": buf})
        ax_leg.plot([], [], color=style.get("color"), marker=style.get("marker", "o"),
                    linestyle=style.get("linestyle", "-"), label=style.get("label", buf).replace("_", "\\_"))
    
    handles, labels = ax_leg.get_legend_handles_labels()
    fig_leg.legend(handles, labels, loc='center', frameon=False, ncol=4)
    ax_leg.axis('off')
    
    save_path = PLOTS_DIR / "standalone_legend.pdf"
    fig_leg.savefig(save_path, bbox_inches='tight')
    plt.close(fig_leg)
    print(f"  Standalone legend saved: {save_path.resolve()}")

def plot_lines(df, metric_col, y_label, filename_base, use_log, num_ticks, ylim, yticks):
    df_grouped = df.groupby(["buffer_size", "buffer_type"])[metric_col].mean().reset_index()
    df_grouped = df_grouped.sort_values("buffer_size")
    all_sizes = sorted(df_grouped["buffer_size"].unique())
    size_to_idx = {size: i for i, size in enumerate(all_sizes)}

    fig, ax = plt.subplots(figsize=(6, 5))
    for buf in df_grouped["buffer_type"].unique():
        buf_df = df_grouped[df_grouped["buffer_type"] == buf]
        style = line_styles.get(buf, {})
        plot_x = [size_to_idx[s] for s in buf_df["buffer_size"]]
        
        ax.plot(plot_x, buf_df[metric_col], color=style.get("color"),
                marker=style.get("marker", "o"), linestyle=style.get("linestyle", "-"), linewidth=1.5)

    ax.set_xticks(range(len(all_sizes)))
    ax.set_xticklabels([str(s) for s in all_sizes])
    
    # --- Y-AXIS ENFORCEMENT ---
    if use_log:
        ax.set_yscale('log')
        data_max = df_grouped[metric_col].max()
        
        if ylim is not None:
            # Handle user-defined ylim, allowing for partial None
            bottom = ylim[0] if ylim[0] is not None else 10
            top = ylim[1] if ylim[1] is not None else data_max * 10
            ax.set_ylim(bottom=bottom, top=top)
        else:
            # Enforce start at 10^1 (10) as per preference
            ax.set_ylim(bottom=10, top=data_max * 10) 
            
        ax.yaxis.set_major_locator(mticker.LogLocator(base=10.0, numticks=num_ticks))
        ax.yaxis.set_major_formatter(mticker.LogFormatterSciNotation())
    else:
        if ylim is not None:
            ax.set_ylim(ylim)
        else:
            ax.set_ylim(bottom=0)

        if yticks is not None:
            ax.set_yticks(yticks)
        else:
            ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=num_ticks, prune='lower'))
            y_ticks_auto = ax.get_yticks()
            ax.set_yticks([t for t in y_ticks_auto if t >= (ylim[0] if (ylim and ylim[0] is not None) else 0)])

    ax.set_xlabel("buffer size (mb)")
    ax.set_ylabel(y_label.lower())
    ax.grid(False) 

    fig.tight_layout()
    save_path = PLOTS_DIR / f"{filename_base}.pdf"
    fig.savefig(save_path, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved plot: {save_path.resolve()}")

def main():
    df = collect_records()
    if df.empty:
        print("No data records found."); sys.exit(0)

    generate_separate_legend()

    configs = [
        ("ph1_ins_lat", "P1 Mean Insert Latency (ns)", "ph1_ins"),
        ("ph2_ins_lat", "P2 Mean Insert Latency (ns)", "ph2_ins"),
        ("ph3_ins_lat", "P3 Mean Insert Latency (ns)", "ph3_ins"),
        ("ph2_pq_lat",  "P2 Mean PQ Latency (ns)", "ph2_pq"),
        ("ph3_rq_lat",  "P3 Mean RQ Latency (ns)", "ph3_rq"),
        ("total_time",  "Total Execution Time (s)", "total_time")
    ]

    y_ctrl = globals().get('Y_AXIS_CONTROL', {})

    for col, y_cap, f_name in configs:
        ctrl = y_ctrl.get(f_name, {})
        use_log = ctrl.get("use_log", LOG_SCALE)
        num_ticks = ctrl.get("num_ticks", 8)
        ylim = ctrl.get("ylim", None)
        yticks = ctrl.get("yticks", None)
        
        plot_lines(
            df=df, 
            metric_col=col, 
            y_label=y_cap, 
            filename_base=f_name, 
            use_log=use_log, 
            num_ticks=num_ticks,
            ylim=ylim,
            yticks=yticks
        )

if __name__ == "__main__":
    main()