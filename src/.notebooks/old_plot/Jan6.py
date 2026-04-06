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

# 1. Import external styles
try:
    from plot import *
except ImportError:
    plt.rcParams.update({"font.size": 20})
    bar_styles = {}


bar_styles["hash_skip_list-H1"] =   {"color": "lightskyblue", "edgecolor": "black", "hatch": "///"}
bar_styles["hash_skip_list-H2"] =   {"color": "steelblue", "edgecolor": "black", "hatch": "\\\\"}
bar_styles["hash_linked_list-H1"] = {"color": "navajowhite", "edgecolor": "black", "hatch": "///"}
bar_styles["hash_linked_list-H2"] = {"color": "darkorange", "edgecolor": "black", "hatch": "\\\\"}

# 3. CONFIGURATION
USE_LOG_SCALE = False
NS_TO_S = 1e-9

DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data")

FOLDER_DEC30 = "filter_result_dec30_inmemory_interleave-lowpri_true-I450000-U0-Q450000-S0-Y0-T10-P131072-B4-E1024"
FOLDER_JAN6 = "filter_result_Jan6"

EXP_FOLDERS = [FOLDER_DEC30, FOLDER_JAN6]

PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Jan6_total")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# 4. FILTER LIST
FILTER_BUFFERS = [
    "skiplist",
    "hash_skip_list",       # Original (Dec30)
    "hash_skip_list-H1",    # New (Jan6)
    "hash_skip_list-H2",    # New (Jan6)
    "hash_linked_list",     # Original (Dec30)
    "hash_linked_list-H1",  # New (Jan6)
    "hash_linked_list-H2",  # New (Jan6)
]

TIME_RE = re.compile(r"^(Inserts|PointQuery) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")

def bar_style(buf_name):
    global bar_styles
    default_style = {"color": "white", "edgecolor": "black", "hatch": ""}
    
    style = bar_styles.get(buf_name, default_style).copy()
    style.pop("label", None)
    return style

def apply_axis_style(ax, y_limit_tuple, is_latency_plot):
    if USE_LOG_SCALE:
        if y_limit_tuple: ax.set_ylim(*y_limit_tuple)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        ax.set_ylim(bottom=0)
        if y_limit_tuple: ax.set_ylim(top=y_limit_tuple[1])
        # Scientific notation with 0,0 limits forces it for almost all values
        ax.ticklabel_format(style="scientific", axis="y", scilimits=(0,0))
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

def collect_records(data_root, folders):
    records = []
    
    for folder_name in folders:
        data_dir = data_root / folder_name
        
        for log_path in data_dir.rglob("*.log"):
            if "workload" not in log_path.name and "run" not in log_path.name:
                continue
            
            dir_name = log_path.parent.name
            
            # --- NAMING LOGIC ---
            
            # CASE 1: Dec30 Folder (Originals)
            # STRICT MAPPING: We ignore suffixes like -H100000 here to preserve the original name
            if folder_name == FOLDER_DEC30:
                if "hash_skip_list" in dir_name:
                    buffer_key = "hash_skip_list"
                elif "hash_linked_list" in dir_name:
                    buffer_key = "hash_linked_list"
                elif "AlwayssortedVector" in dir_name: buffer_key = "AlwaysSortedVector-dynamic"
                elif "UnsortedVector" in dir_name: buffer_key = "UnsortedVector-dynamic"
                elif "Vector" in dir_name: buffer_key = "Vector-dynamic"
                else: buffer_key = dir_name # Fallback

            # CASE 2: Jan6 Folder (New Variants)
            # We explicitly check for H1/H2 and append suffix
            elif folder_name == FOLDER_JAN6:
                # First determine base
                if "hash_skip_list" in dir_name:
                    base = "hash_skip_list"
                elif "hash_linked_list" in dir_name:
                    base = "hash_linked_list"
                else:
                    base = dir_name

                # Append suffix based on folder string
                if "-H1" in dir_name:
                    buffer_key = base + "-H1"
                elif "-H2" in dir_name:
                    buffer_key = base + "-H2"
                else:
                    buffer_key = base

            exec_ns = parse_exec_times(log_path.read_text())
            
            records.append({
                "buffer": buffer_key,
                "total_ins_s": exec_ns["Inserts"] * NS_TO_S,
                "total_pq_s": exec_ns["PointQuery"] * NS_TO_S,
                "total_workload_s": exec_ns["Workload"] * NS_TO_S
            })
            
    return pd.DataFrame(records).groupby("buffer", as_index=False).mean()

def plot_combined_barchart(df, metric_cols, y_label, x_labels, filename_base, buffers, y_limit):
    fig, ax = plt.subplots(1, 1, figsize=(8, 4.5))
    if USE_LOG_SCALE: ax.set_yscale("log", base=10)

    num_groups = len(metric_cols)
    num_buffers = len(buffers)
    group_width, bar_width_ratio = 0.9, 0.9
    width_of_one_bar_slot = group_width / num_buffers
    bar_width = width_of_one_bar_slot * bar_width_ratio
    group_x_positions = np.arange(num_groups)

    bottom_val = 1.0 if USE_LOG_SCALE else 0.0

    for group_idx, metric_col in enumerate(metric_cols):
        group_center = group_x_positions[group_idx]
        group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot
            try:
                value = df.loc[df["buffer"] == buf_name, metric_col].iat[0]
            except:
                value = bottom_val
            
            if USE_LOG_SCALE and value <= 0: value = bottom_val
            
            ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(group_x_positions)
    ax.set_xticklabels(x_labels)
    apply_axis_style(ax, y_limit, True)
    ax.set_ylabel(y_label)
    
    fig.tight_layout()
    fig.savefig(filename_base.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)

def save_individual_legends(buffers, output_dir):
    legend_dir = output_dir / "legends"
    legend_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Generating individual legends in: {legend_dir}")
    
    for buf_name in buffers:
        fig_dummy, ax_dummy = plt.subplots()
        style = bar_style(buf_name)
        handle = ax_dummy.bar([0], [1], **style, label=buf_name)
        
        fig_leg, ax_leg = plt.subplots(figsize=(4, 1))
        ax_leg.axis('off')
        ax_leg.legend([handle], [buf_name], loc='center', frameon=False, fontsize=20)
        
        safe_name = buf_name.replace(" ", "_").replace("/", "-")
        fig_leg.savefig(legend_dir / f"legend_{safe_name}.pdf", bbox_inches='tight')
        
        plt.close(fig_dummy)
        plt.close(fig_leg)

def main():
    df = collect_records(DATA_ROOT, EXP_FOLDERS)
    if df.empty:
        print("No data found.")
        return

    buffers_to_plot = [b for b in FILTER_BUFFERS if b in df["buffer"].values]
    print(f"Plotting: {buffers_to_plot}")

    # Plot 1: Total Latency
    plot_combined_barchart(
        df=df,
        metric_cols=["total_ins_s", "total_pq_s"],
        y_label="operation latency (s)",
        x_labels=["insert", "point query"],
        filename_base=PLOTS_DIR / "total_latency_ops",
        buffers=buffers_to_plot,
        y_limit=None 
    )

    # Plot 2: Total Workload Execution Time
    plot_combined_barchart(
        df=df,
        metric_cols=["total_workload_s"],
        y_label="time (s)",
        x_labels=["total workload execution"],
        filename_base=PLOTS_DIR / "total_workload_execution",
        buffers=buffers_to_plot,
        y_limit=None
    )
    
    save_individual_legends(buffers_to_plot, PLOTS_DIR)

    print(f"Finished. Plots and legends saved in {PLOTS_DIR}")

if __name__ == "__main__":
    main()