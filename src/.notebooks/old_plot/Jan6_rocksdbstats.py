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

# 1. Import external styles or set defaults
try:
    from plot import *
except ImportError:
    plt.rcParams.update({"font.size": 20})
    bar_styles = {}

# --- DEFINE STYLES (Added H100000) ---
# Note: "skiplist" style is NOT defined here, so it uses plot.py or default.
bar_styles["hash_skip_list-H1"] =       {"color": "lightskyblue", "edgecolor": "black", "hatch": "///"}
bar_styles["hash_skip_list-H2"] =       {"color": "steelblue", "edgecolor": "black", "hatch": "\\\\"}
bar_styles["hash_skip_list-H100000"] =  {"color": "mediumseagreen", "edgecolor": "black", "hatch": "..."}

bar_styles["hash_linked_list-H1"] =     {"color": "navajowhite", "edgecolor": "black", "hatch": "///"}
bar_styles["hash_linked_list-H2"] =     {"color": "darkorange", "edgecolor": "black", "hatch": "\\\\"}
bar_styles["hash_linked_list-H100000"] = {"color": "olivedrab", "edgecolor": "black", "hatch": "..."}

# 2. CONFIGURATION
USE_LOG_SCALE = False
MICROS_TO_S = 1e-6  # Changed from NS_TO_S to microsecond conversion

# Update Data Root to the new path provided
DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_Jan6_hashhybrid")

# Specific subfolders from your tree output
FOLDER_SKIP = "Jan6_inmemory_interleave_scalabilitytest1_rerun-lowpri_true-I1500000-U0-Q450-S0-Y0-T10-P131072-B16-E256"
FOLDER_LINK = "Jan6_inmemory_interleave_scalabilitytest2-lowpri_true-I450000-U0-Q450-S0-Y0-T10-P131072-B16-E256"

EXP_FOLDERS = [FOLDER_SKIP, FOLDER_LINK]

# Output directory
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Jan6_rocksdbstats")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# 3. REGEX (UPDATED to target SUM in RocksDB stats)
GET_SUM_RE = re.compile(r"rocksdb\.db\.get\.micros .* SUM : (\d+)")
WRITE_SUM_RE = re.compile(r"rocksdb\.db\.write\.micros .* SUM : (\d+)")

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
        # Scientific notation
        ax.ticklabel_format(style="scientific", axis="y", scilimits=(0,0))
    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

def parse_exec_times(text):
    out = {"Inserts": 0, "PointQuery": 0}
    for line in text.splitlines():
        line = line.strip()
        
        # Check for Point Query SUM
        m_get = GET_SUM_RE.search(line)
        if m_get:
            out["PointQuery"] = int(m_get.group(1))
            continue
            
        # Check for Insert SUM
        m_write = WRITE_SUM_RE.search(line)
        if m_write:
            out["Inserts"] = int(m_write.group(1))
            
    # Total workload is sum of inserts + point queries
    out["Workload"] = out["Inserts"] + out["PointQuery"]
    return out

def collect_records(data_root, folders):
    records = []
    
    for folder_name in folders:
        data_dir = data_root / folder_name
        if not data_dir.exists():
            print(f"Warning: Folder not found: {data_dir}")
            continue

        for log_path in data_dir.rglob("*.log"):
            # Ensure we are reading workload1.log as per your instruction
            if "workload1" not in log_path.name:
                continue
            
            dir_name = log_path.parent.name
            
            # --- NAMING LOGIC ---
            # Identify Base Type
            if "hash_skip_list" in dir_name:
                base = "hash_skip_list"
            elif "hash_linked_list" in dir_name:
                base = "hash_linked_list"
            elif "skiplist" in dir_name:
                base = "skiplist"
            else:
                continue

            # Identify Suffix (H settings)
            if base == "skiplist":
                buffer_key = "skiplist"
            else:
                # Check H100000 FIRST because "H1" is a substring of "H100000"
                if "H100000" in dir_name:
                    buffer_key = base + "-H100000"
                elif "H2" in dir_name:
                    buffer_key = base + "-H2"
                elif "H1" in dir_name:
                    buffer_key = base + "-H1"
                else:
                    buffer_key = base 

            exec_micros = parse_exec_times(log_path.read_text())
            
            records.append({
                "buffer": buffer_key,
                "total_ins_s": exec_micros["Inserts"] * MICROS_TO_S,
                "total_pq_s": exec_micros["PointQuery"] * MICROS_TO_S,
                "total_workload_s": exec_micros["Workload"] * MICROS_TO_S
            })
            
    return pd.DataFrame(records).groupby("buffer", as_index=False).mean()

def plot_combined_barchart(df, metric_cols, y_label, x_labels, filename_base, buffers, y_limit):
    # Filter data to only valid buffers
    valid_buffers = [b for b in buffers if b in df["buffer"].values]
    if not valid_buffers:
        print(f"Skipping plot {filename_base.name}: No matching buffers found in data.")
        return

    fig, ax = plt.subplots(1, 1, figsize=(8, 4.5))
    if USE_LOG_SCALE: ax.set_yscale("log", base=10)

    num_groups = len(metric_cols)
    num_buffers = len(valid_buffers)
    group_width, bar_width_ratio = 0.9, 0.9
    width_of_one_bar_slot = group_width / num_buffers
    bar_width = width_of_one_bar_slot * bar_width_ratio
    group_x_positions = np.arange(num_groups)

    bottom_val = 1.0 if USE_LOG_SCALE else 0.0

    for group_idx, metric_col in enumerate(metric_cols):
        group_center = group_x_positions[group_idx]
        group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

        for buf_index, buf_name in enumerate(valid_buffers):
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

    # --- DEFINE BUFFER GROUPS ---
    buffers_skip = [
        "skiplist",
        "hash_skip_list-H1",
        "hash_skip_list-H2",
        "hash_skip_list-H100000"
    ]

    buffers_link = [
        "skiplist",
        "hash_linked_list-H1",
        "hash_linked_list-H2",
        "hash_linked_list-H100000"
    ]
    
    print("Generating plots...")

    # ==========================================
    # SET 1: HASH SKIP LIST
    # ==========================================
    # 1. Insert Only
    plot_combined_barchart(
        df=df,
        metric_cols=["total_ins_s"],
        y_label="insert latency (s)",
        x_labels=["insert"],
        filename_base=PLOTS_DIR / "latency_insert_hash_skip_list",
        buffers=buffers_skip,
        y_limit=None 
    )
    # 2. Point Query Only
    plot_combined_barchart(
        df=df,
        metric_cols=["total_pq_s"],
        y_label="point query latency (s)",
        x_labels=["point query"],
        filename_base=PLOTS_DIR / "latency_pq_hash_skip_list",
        buffers=buffers_skip,
        y_limit=None 
    )
    # 3. Workload
    plot_combined_barchart(
        df=df,
        metric_cols=["total_workload_s"],
        y_label="time (s)",
        x_labels=["total workload"],
        filename_base=PLOTS_DIR / "workload_hash_skip_list",
        buffers=buffers_skip,
        y_limit=None
    )

    # ==========================================
    # SET 2: HASH LINKED LIST
    # ==========================================
    # 1. Insert Only
    plot_combined_barchart(
        df=df,
        metric_cols=["total_ins_s"],
        y_label="insert latency (s)",
        x_labels=["insert"],
        filename_base=PLOTS_DIR / "latency_insert_hash_linked_list",
        buffers=buffers_link,
        y_limit=None 
    )
    # 2. Point Query Only
    plot_combined_barchart(
        df=df,
        metric_cols=["total_pq_s"],
        y_label="point query latency (s)",
        x_labels=["point query"],
        filename_base=PLOTS_DIR / "latency_pq_hash_linked_list",
        buffers=buffers_link,
        y_limit=None 
    )
    # 3. Workload
    plot_combined_barchart(
        df=df,
        metric_cols=["total_workload_s"],
        y_label="time (s)",
        x_labels=["total workload"],
        filename_base=PLOTS_DIR / "workload_hash_linked_list",
        buffers=buffers_link,
        y_limit=None
    )
    
    # Save legends
    all_buffers = sorted(list(set(buffers_skip + buffers_link)))
    save_individual_legends(all_buffers, PLOTS_DIR)

    print(f"Finished. Plots and legends saved in {PLOTS_DIR}")

if __name__ == "__main__":
    main()