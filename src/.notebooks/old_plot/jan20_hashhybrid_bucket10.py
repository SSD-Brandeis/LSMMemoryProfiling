import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from matplotlib import font_manager
from pathlib import Path
import re
import numpy as np
import sys

# --- Directory Setup ---
try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

ROOT = CURR_DIR.parent

# --- Style and Font Configuration ---
try:
    from plot import *
except ImportError:
    print("Warning: plot.py not found. Using default styles.")
    plt.rcParams.update({"font.size": 20})
    bar_styles = {}

FONT_CANDIDATES = [
    CURR_DIR / "LinLibertine_Mah.ttf",
    ROOT / "LinLibertine_Mah.ttf",
    Path.home() / "Desktop/tectonic/LinLibertine_Mah.ttf",
]

font_found = False
for fp in FONT_CANDIDATES:
    if fp.exists():
        prop = font_manager.FontProperties(fname=str(fp))
        plt.rcParams["font.family"] = prop.get_name()
        print(f"✅ Using font: {fp.resolve()}")
        font_found = True
        break

if not font_found:
    print("❌ CRITICAL ERROR: Publication font 'LinLibertine_Mah.ttf' not found.")
    sys.exit(1)

plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "normal"
plt.rcParams["font.size"] = 24

# --- Bar Style Definitions ---
bar_styles["hash_skip_list-H5"] =       {"color": "lightskyblue", "edgecolor": "black", "hatch": "///"}
bar_styles["hash_skip_list-H10"] =      {"color": "steelblue", "edgecolor": "black", "hatch": "\\\\"}
bar_styles["hash_skip_list-H100000"] =  {"color": "mediumseagreen", "edgecolor": "black", "hatch": "..."}

bar_styles["hash_linked_list-H5"] =     {"color": "navajowhite", "edgecolor": "black", "hatch": "///"}
bar_styles["hash_linked_list-H10"] =    {"color": "darkorange", "edgecolor": "black", "hatch": "\\\\"}
bar_styles["hash_linked_list-H100000"] = {"color": "olivedrab", "edgecolor": "black", "hatch": "..."}

# --- Paths ---
DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_Jan20_clock")
FOLDER_MAIN = "Jan20_inmemory_interleave_scalabilitytest1_rerun-lowpri_true-I1500000-U0-Q450-S0-Y0-T10-P131072-B16-E256"
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Jan20_clock")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

WRITE_RE = re.compile(r"rocksdb\.db\.write\.micros.*?SUM\s*:\s*(\d+)")
GET_RE = re.compile(r"rocksdb\.db\.get\.micros.*?SUM\s*:\s*(\d+)")

def get_bar_style(buf_name):
    default_style = {"color": "white", "edgecolor": "black", "hatch": ""}
    style = bar_styles.get(buf_name, default_style).copy()
    style.pop("label", None)
    return style

def parse_log_file(text):
    insert_micros, get_micros = 0, 0
    m_write = WRITE_RE.search(text)
    if m_write: insert_micros = int(m_write.group(1))
    m_get = GET_RE.search(text)
    if m_get: get_micros = int(m_get.group(1))
    return insert_micros, get_micros

def load_data():
    records = []
    target_dir = DATA_ROOT / FOLDER_MAIN
    if not target_dir.exists():
        return pd.DataFrame()

    for log_path in target_dir.rglob("workload1.log"):
        dir_name = log_path.parent.name
        if "hash_skip_list" in dir_name: base = "hash_skip_list"
        elif "hash_linked_list" in dir_name: base = "hash_linked_list"
        elif "skiplist" in dir_name: base = "skiplist"
        else: continue

        if base == "skiplist":
            key = "skiplist"
        else:
            if "H100000" in dir_name: key = base + "-H100000"
            elif "H10" in dir_name: key = base + "-H10"
            elif "H5" in dir_name: key = base + "-H5"
            else: key = base 

        ins_micros, pq_micros = parse_log_file(log_path.read_text())
        records.append({
            "buffer": key,
            "total_ins_s": ins_micros * 1e-6,
            "total_pq_s": pq_micros * 1e-6,
            "total_workload_s": (ins_micros + pq_micros) * 1e-6
        })
    return pd.DataFrame(records).groupby("buffer", as_index=False).mean()

def plot_chart(df, col, ylabel, xlabel, out_name, buffers):
    valid_bufs = [b for b in buffers if b in df["buffer"].values]
    if not valid_bufs:
        return

    fig, ax = plt.subplots(figsize=(8, 4.5))
    n_bars = len(valid_bufs)
    width_slot = 0.9 / n_bars
    width_bar = width_slot * 0.9
    x_pos = 0 
    start_x = x_pos - (0.9 / 2) + (width_slot / 2)

    for i, buf in enumerate(valid_bufs):
        val = df.loc[df["buffer"] == buf, col].iat[0]
        ax.bar(start_x + i * width_slot, val, width=width_bar, **get_bar_style(buf))

    ax.set_xticks([0])
    ax.set_xticklabels([xlabel])
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)
    ax.ticklabel_format(style="scientific", axis="y", scilimits=(0,0))
    ax.tick_params(axis="y", labelsize=24)
    
    fig.tight_layout()
    
    # Resolve and print absolute path
    abs_path = Path(out_name).resolve()
    fig.savefig(abs_path, bbox_inches="tight")
    print(f"📄 Plot saved: {abs_path}")
    plt.close(fig)

def create_legends(buffers):
    leg_dir = PLOTS_DIR / "legends"
    leg_dir.mkdir(parents=True, exist_ok=True)
    
    for buf in buffers:
        fig, ax = plt.subplots()
        h = ax.bar([0], [1], **get_bar_style(buf))
        
        fig_l, ax_l = plt.subplots(figsize=(4, 1))
        ax_l.axis('off')
        ax_l.legend([h], [buf], loc='center', frameon=False, fontsize=20)
        
        clean_name = buf.replace(" ", "_").replace("/", "-")
        save_path = leg_dir / f"legend_{clean_name}.pdf"
        abs_leg_path = save_path.resolve()
        
        fig_l.savefig(abs_leg_path, bbox_inches='tight')
        print(f"🏷️  Legend saved: {abs_leg_path}")
        
        plt.close(fig)
        plt.close(fig_l)

def main():
    df = load_data()
    if df.empty:
        print("No data found.")
        return

    print("--- Data Summary ---")
    print(df)
    print("--------------------")

    group_skip = ["skiplist", "hash_skip_list-H5", "hash_skip_list-H10", "hash_skip_list-H100000"]
    group_link = ["skiplist", "hash_linked_list-H5", "hash_linked_list-H10", "hash_linked_list-H100000"]

    # Hash Skip List Plots
    plot_chart(df, "total_ins_s", "insert latency (s)", "insert", PLOTS_DIR / "latency_insert_hash_skip_list.pdf", group_skip)
    plot_chart(df, "total_pq_s", "point query latency (s)", "point query", PLOTS_DIR / "latency_pq_hash_skip_list.pdf", group_skip)
    plot_chart(df, "total_workload_s", "time (s)", "total workload", PLOTS_DIR / "workload_hash_skip_list.pdf", group_skip)

    # Hash Linked List Plots
    plot_chart(df, "total_ins_s", "insert latency (s)", "insert", PLOTS_DIR / "latency_insert_hash_linked_list.pdf", group_link)
    plot_chart(df, "total_pq_s", "point query latency (s)", "point query", PLOTS_DIR / "latency_pq_hash_linked_list.pdf", group_link)
    plot_chart(df, "total_workload_s", "time (s)", "total workload", PLOTS_DIR / "workload_hash_linked_list.pdf", group_link)

    create_legends(sorted(list(set(group_skip + group_link))))

if __name__ == "__main__":
    main()