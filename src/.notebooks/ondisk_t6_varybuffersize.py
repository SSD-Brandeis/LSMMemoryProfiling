import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.font_manager as font_manager
from matplotlib.ticker import MaxNLocator, NullLocator, FixedLocator
from matplotlib.lines import Line2D
import pandas as pd
from pathlib import Path
import re
import sys
import numpy as np

# --- FONT AND STYLE ALIGNMENT (from your __init__.py) ---
CURR_DIR_FONT = Path("/Users/cba/Desktop/LSM/LSMMemoryProfiling/src/.notebooks/plot") # Adjust to your actual font path
prop = font_manager.FontProperties(fname=CURR_DIR_FONT / "LinLibertine_Mah.ttf")
plt.rcParams["font.family"] = prop.get_name()
plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "bold"
plt.rcParams["font.size"] = 24  # Enforcing the large font size

# ==============================================================================
# GLOBAL LOG SCALE CONTROL
# ==============================================================================
LOG_SCALE = True # Set to True for log scale, False for linear scale (global)

# ==============================================================================
# Y-AXIS TICKER CONTROL (ADJUST EACH PLOT HERE)
# ==============================================================================
# ylim: (bottom, top) or None. 
# yticks: [list of specific values] or None.
# NOTE: For Log Scale, bottom ylim and yticks MUST be > 0.
# Y_AXIS_CONTROL = {
#     "ph1_ins":    {"use_log": False, "ylim": (10, 200000), "yticks": None},
#     "ph2_ins":    {"use_log": False, "ylim": (1, None),   "yticks": None},
#     "ph3_ins":    {"use_log": False, "ylim": (0, 250000), "yticks": None},
#     "ph2_pq":     {"use_log": False, "ylim": (0, None),   "yticks": None},
#     "ph3_rq":     {"use_log": False, "ylim": (0, None),    "yticks": None},
#     "total_time": {"use_log": False, "ylim": (0, 10000), "yticks": [0, 5000, 10000]},
# }

# ==============================================================================
# Y-AXIS TICKER CONTROL (INDEPENDENT CONTROL)
# ==============================================================================
Y_AXIS_CONTROL = {
    # Example: P1 Inserts - Specific log range
    "ph1_ins":    {
        "use_log": True, 
        "ylim": (1, 10**6),   
        "yticks": [10**0, 10**2, 10**4, 10**6]
    },
    
    # Example: P2 Inserts - Autoscale top, but fixed log ticks
    "ph2_ins":    {
        "use_log": True, 
        "ylim": (1, 10**6),   
        "yticks": [10**0, 10**2, 10**4, 10**6]
    },
    
    # Example: P3 Inserts - Linear Scale (Style A from before)
    "ph3_ins":    {
        "use_log": True, 
        "ylim": (1, 10**6),   
        "yticks": [10**0, 10**2, 10**4, 10**6]
    },

    "ph2_pq":     {
        "use_log": True, 
        "ylim": (1, 10**10),   
        "yticks": [10**0, 10**5, 10**10]
    },

    "ph3_rq":     {
        "use_log": True, 
        "ylim": (1, 10**12),   
        "yticks": [10**0, 10**4, 10**8, 10**12]
    },

    # Example: Total Time - Deep Log Scale
    "total_time": {
        "use_log": True, 
        "ylim": (1, 10**6), 
        "yticks": [10**0, 10**2, 10**4, 10**6]
    },
}

# --- Path Injection and Style Enforcement ---
STYLE_PATH = Path("/Users/cba/Desktop/LSM/LSMMemoryProfiling/src/.notebooks/plot")

if str(STYLE_PATH) not in sys.path:
    sys.path.insert(0, str(STYLE_PATH))

try:
    from style import line_styles
except ImportError:
    print(f"Error: 'style.py' not found at {STYLE_PATH}. Aborting.")
    sys.exit(1)

NS_TO_S = 1e-9

PHASE1_INSERTS = 80000000
PHASE2_INSERTS = 10000000
PHASE2_PQ = 10000
PHASE3_INSERTS = 10000000
PHASE3_RQ = 1000

DATA_BASE_DIR = Path("/Users/cba/Desktop/LSM/LSMMemoryProfiling/data_new/filter_result_multiphase_ondisk_3phase_t6_varybuffer_newsetting")
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

def normalize_name(name):
    name = name.lower()
    if "hash_linked_list" in name or "hashlinkedlist" in name:
        return "hashlinkedlist"
    if "hash_skip_list" in name or "hashskiplist" in name:
        return "hashskiplist"
    if "hash_vector" in name or "hashvector" in name:
        return "hashvector"
    if "simple_skiplist" in name or "simpleskiplist" in name:
        return "simpleskiplist"
    if name == "skiplist":
        return "skiplist"
    if "linkedlist" in name:
        return "linkedlist"
    if "unsortedvector" in name:
        return "unsortedvector"
    if "sortedvector" in name or "alwayssortedvector" in name:
        return "alwayssortedvector"
    if "vector" in name:
        return "vector"
    return None

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
    """Generates standalone legend matching the style logic of shifting workload script."""
    legend_elements = []
    unique_buffers = FILTER_BUFFERS 
    
    for buf in unique_buffers:
        key = normalize_name(buf)
        style = line_styles.get(key)
        if style:
            legend_elements.append(Line2D([0], [0], **style))

    fig_legend = plt.figure(figsize=(10, 0.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")

    ax_legend.legend(
        handles=legend_elements,
        loc="center",
        ncol=4,
        frameon=False,
        borderaxespad=0,
        labelspacing=0.2,
        borderpad=0,
    )

    save_path = PLOTS_DIR / "standalone_legend.pdf"
    fig_legend.savefig(save_path, bbox_inches='tight', pad_inches=0.02)
    plt.close(fig_legend)
    print(f"  Standalone legend saved: {save_path.resolve()}")

def plot_lines(df, metric_col, y_label, filename_base, use_log, ylim, yticks):
    df_grouped = df.groupby(["buffer_size", "buffer_type"])[metric_col].mean().reset_index()
    df_grouped = df_grouped.sort_values("buffer_size")
    all_sizes = sorted(df_grouped["buffer_size"].unique())
    size_to_idx = {size: i for i, size in enumerate(all_sizes)}

    fig, ax = plt.subplots(figsize=(6, 5))
    for buf in df_grouped["buffer_type"].unique():
        buf_df = df_grouped[df_grouped["buffer_type"] == buf]
        key = normalize_name(buf)
        style = line_styles.get(key, {})
        plot_x = [size_to_idx[s] for s in buf_df["buffer_size"]]
        ax.plot(plot_x, buf_df[metric_col], **style)

    ax.set_xticks(range(len(all_sizes)))
    ax.set_xticklabels([str(s) for s in all_sizes])
    
    # --- SCALE AND LIMIT CONTROL ---
    if use_log:
        ax.set_yscale('log')
        # Log scale safety: bottom must be > 0
        bottom = ylim[0] if (ylim and ylim[0] is not None and ylim[0] > 0) else 1
        top = ylim[1] if (ylim and ylim[1] is not None) else None
        ax.set_ylim(bottom=bottom, top=top)
        
        # RESTORE 10^X AND REMOVE 1eX AT TOP
        ax.yaxis.set_major_formatter(mticker.LogFormatterMathtext())
        ax.yaxis.get_offset_text().set_visible(False) # Strict suppression of 1eX
    else:
        bottom = ylim[0] if (ylim and ylim[0] is not None) else 0
        top = ylim[1] if (ylim and ylim[1] is not None) else None
        ax.set_ylim(bottom=bottom, top=top)

    ax.relim()
    ax.autoscale_view()

    # --- TICK CONTROL (STRICT ENFORCEMENT) ---
    if yticks is not None:
        ax.yaxis.set_major_locator(FixedLocator(yticks))
        # Ensure labels are explicitly 10^x for log or whole numbers for linear
        if use_log:
            ax.yaxis.set_major_formatter(mticker.LogFormatterMathtext())
        else:
            ax.yaxis.set_major_formatter(mticker.ScalarFormatter())
        ax.yaxis.set_minor_locator(NullLocator())
    else:
        if use_log:
            ax.yaxis.set_major_locator(mticker.LogLocator(base=10.0, numticks=8))
        else:
            y_max = int(ax.get_ylim()[1])
            ax.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=5))
            fig.canvas.draw() 
            current_ticks = ax.get_yticks().tolist()
            gap_threshold = y_max * 0.12
            final_ticks = [int(t) for t in current_ticks if t >= 0 and (y_max - t) > gap_threshold]
            final_ticks.append(y_max)
            ax.set_yticks(sorted(list(set(final_ticks))))
            ax.set_yticklabels([f"{int(t)}" for t in ax.get_yticks()])

    ax.set_xlabel("buffer size (MB)")
    ax.set_ylabel(y_label, labelpad=-1)
    ax.grid(False) 

    fig.tight_layout()
    save_path = PLOTS_DIR / f"{filename_base}.pdf"
    fig.savefig(save_path, bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)
    print(f"  Saved plot: {save_path.resolve()}")

def main():
    df = collect_records()
    if df.empty:
        print("No data records found."); sys.exit(0)

    generate_separate_legend()

    configs = [
        ("ph1_ins_lat", "P1 insert mean latency (ns)", "ph1_ins"),
        ("ph2_ins_lat", "P2 insert mean latency  (ns)", "ph2_ins"),
        ("ph3_ins_lat", "P3 insert mean latency (ns)", "ph3_ins"),
        ("ph2_pq_lat",  "P2 PQ mean latency (ns)", "ph2_pq"),
        ("ph3_rq_lat",  "P3 RQ mean latency (ns)", "ph3_rq"),
        ("total_time",  "total execution time (s)", "total_time")
    ]

    y_ctrl = globals().get('Y_AXIS_CONTROL', {})

    for col, y_cap, f_name in configs:
        ctrl = y_ctrl.get(f_name, {})
        use_log = ctrl.get("use_log", LOG_SCALE)
        ylim = ctrl.get("ylim", None)
        yticks = ctrl.get("yticks", None)
        
        plot_lines(
            df=df, 
            metric_col=col, 
            y_label=y_cap, 
            filename_base=f_name, 
            use_log=use_log, 
            ylim=ylim,
            yticks=yticks
        )

if __name__ == "__main__":
    main()