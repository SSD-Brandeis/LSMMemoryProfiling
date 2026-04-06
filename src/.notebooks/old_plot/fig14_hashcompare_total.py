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

# --- Configuration ---
SHOW_VALUES_ON_BARS = True 

try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

ROOT = CURR_DIR.parent

# --- Style & plot.py Integration ---
# Initialize bar_styles so plot.py can populate it
bar_styles = {}

try:
    # This should populate bar_styles if defined there
    from plot import *
    print("✅ Successfully imported styles from plot.py")
except ImportError:
    print("Warning: plot.py not found. Using default styles.")
    plt.rcParams.update({"font.size": 20})

# Helper to set styles only if not already defined in plot.py
def set_default_style(key, style_dict):
    if key not in bar_styles:
        bar_styles[key] = style_dict

# Define defaults for Hash Skip List
set_default_style("hash_skip_list-H1",       {"color": "thistle", "edgecolor": "black", "hatch": "..."})
set_default_style("hash_skip_list-H2",       {"color": "plum", "edgecolor": "black", "hatch": "///"})
set_default_style("hash_skip_list-H5",       {"color": "lightskyblue", "edgecolor": "black", "hatch": "\\\\"})
set_default_style("hash_skip_list-H10",      {"color": "steelblue", "edgecolor": "black", "hatch": "xx"})
set_default_style("hash_skip_list-H100000",  {"color": "mediumseagreen", "edgecolor": "black", "hatch": "--"})

# Define defaults for Hash Linked List
set_default_style("hash_linked_list-H1",     {"color": "lemonchiffon", "edgecolor": "black", "hatch": "..."})
set_default_style("hash_linked_list-H2",     {"color": "khaki", "edgecolor": "black", "hatch": "///"})
set_default_style("hash_linked_list-H5",     {"color": "navajowhite", "edgecolor": "black", "hatch": "\\\\"})
set_default_style("hash_linked_list-H10",    {"color": "darkorange", "edgecolor": "black", "hatch": "xx"})
set_default_style("hash_linked_list-H100000", {"color": "olivedrab", "edgecolor": "black", "hatch": "--"})

# Define defaults for the new Hash Vector
set_default_style("hash_vector-H1",          {"color": "mistyrose", "edgecolor": "black", "hatch": "..."})
set_default_style("hash_vector-H2",          {"color": "lightcoral", "edgecolor": "black", "hatch": "///"})
set_default_style("hash_vector-H5",          {"color": "indianred", "edgecolor": "black", "hatch": "\\\\"})
set_default_style("hash_vector-H10",         {"color": "firebrick", "edgecolor": "black", "hatch": "xx"})
set_default_style("hash_vector-H100000",     {"color": "maroon", "edgecolor": "black", "hatch": "--"})

# Only default skiplist to white if plot.py didn't define it
set_default_style("skiplist", {"color": "white", "edgecolor": "black", "hatch": ""})

# --- Font Loading ---
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
plt.rcParams["font.size"] = 20

# --- Data Paths ---
DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_Mar17_fig14")
TARGET_FOLDERS = ["hashhybrid_newsetting_fig14_316-I740000-U0-PQ500-RQ0-S0-lowpri_true-I740000-P32768"]
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Mar17_fig14_plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

WRITE_RE = re.compile(r"rocksdb\.db\.write\.micros.*?Sum\s*[:\s]\s*(\d+)", re.IGNORECASE)
GET_RE = re.compile(r"rocksdb\.db\.get\.micros.*?Sum\s*[:\s]\s*(\d+)", re.IGNORECASE)

def get_bar_style(buf_name):
    default_style = {"color": "gray", "edgecolor": "black", "hatch": ""}
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
    for folder_name in TARGET_FOLDERS:
        target_dir = DATA_ROOT / folder_name
        if not target_dir.exists(): continue
        
        for log_path in target_dir.rglob("workload.log"):
            dir_name = log_path.parent.name
            if "skiplist" in dir_name:
                key = "skiplist"
            else:
                # Cleaning the -X6- from names like hash_vector-X6-H1
                base_match = re.search(r"(hash_linked_list|hash_skip_list|hash_vector)", dir_name)
                h_match = re.search(r"-H(\d+)", dir_name)
                if base_match and h_match:
                    key = f"{base_match.group(1)}-H{h_match.group(1)}"
                else:
                    continue

            ins_micros, pq_micros = parse_log_file(log_path.read_text())
            records.append({
                "buffer": key,
                "total_ins_s": ins_micros * 1e-6,
                "total_pq_s": pq_micros * 1e-6,
                "total_workload_s": (ins_micros + pq_micros) * 1e-6
            })
    if not records: return pd.DataFrame()
    return pd.DataFrame(records).groupby("buffer", as_index=False).mean()

def plot_chart(df, col, ylabel, xlabel, out_name, buffers):
    valid_bufs = [b for b in buffers if b in df["buffer"].values]
    if not valid_bufs: return

    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    n_bars = len(valid_bufs)
    group_width, bar_ratio = 0.9, 0.85
    width_slot = group_width / n_bars
    width_bar = width_slot * bar_ratio
    start_x = 0 - (group_width / 2) + (width_slot / 2)

    for i, buf in enumerate(valid_bufs):
        val = df.loc[df["buffer"] == buf, col].iat[0]
        rects = ax.bar(start_x + i * width_slot, val, width=width_bar, **get_bar_style(buf))

        if SHOW_VALUES_ON_BARS and val > 0:
            for rect in rects:
                height = rect.get_height()
                label = f'{height:.1e}' if (height > 1000 or (0 < height < 0.1)) else f'{height:.2f}'
                ax.annotate(label, xy=(rect.get_x() + rect.get_width()/2, height),
                            xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=12)

    ax.set_xticks([0])
    ax.set_xticklabels([xlabel])
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)
    ax.ticklabel_format(style="scientific", axis="y", scilimits=(0,0))
    
    fig.tight_layout()
    abs_path = Path(out_name).resolve()
    fig.savefig(abs_path, bbox_inches="tight", pad_inches=0.1)
    print(f"📊 Saved Plot: {abs_path}")
    plt.close(fig)

def create_legends(buffers):
    leg_dir = PLOTS_DIR / "legends"
    leg_dir.mkdir(parents=True, exist_ok=True)
    for buf in buffers:
        fig, ax = plt.subplots(); h = ax.bar([0], [1], **get_bar_style(buf))
        
        label_text = buf
        if label_text == "skiplist":
            label_text = "Skip List"
        else:
            label_text = label_text.replace("hash_skip_list", "Hash Skip List")
            label_text = label_text.replace("hash_linked_list", "Hash Linked List")
            label_text = label_text.replace("hash_vector", "Hash Vector")
            label_text = label_text.replace("-H", " H")
            label_text = label_text.replace("100000", "100k")
        
        fig_l, ax_l = plt.subplots(figsize=(4, 1)); ax_l.axis('off')
        ax_l.legend([h], [label_text], loc='center', frameon=False, fontsize=20)
        
        clean_name = buf.replace(" ", "_").replace("/", "-")
        save_path = leg_dir / f"legend_{clean_name}.pdf"
        abs_leg_path = save_path.resolve()
        fig_l.savefig(abs_leg_path, bbox_inches='tight')
        print(f"🏷️  Saved Legend: {abs_leg_path}")
        plt.close(fig); plt.close(fig_l)

def main():
    df = load_data()
    if df.empty: 
        print("No data found. Check workload.log files.")
        return

    print("\n--- Data Loaded ---")
    print(df)
    
    h_vals = ["H1", "H2", "H5", "H10", "H100000"]
    group_skip = ["skiplist"] + [f"hash_skip_list-{h}" for h in h_vals]
    group_link = ["skiplist"] + [f"hash_linked_list-{h}" for h in h_vals]
    group_vector = ["skiplist"] + [f"hash_vector-{h}" for h in h_vals]

    configs = [
        (group_skip, "hash_skip_list"),
        (group_link, "hash_linked_list"),
        (group_vector, "hash_vector")
    ]

    for bufs, name in configs:
        plot_chart(df, "total_ins_s", "insert latency (s)", "insert", PLOTS_DIR / f"latency_insert_{name}.pdf", bufs)
        plot_chart(df, "total_pq_s", "point query latency (s)", "point query", PLOTS_DIR / f"latency_pq_{name}.pdf", bufs)
        plot_chart(df, "total_workload_s", "time (s)", "total workload", PLOTS_DIR / f"workload_{name}.pdf", bufs)

    create_legends(sorted(list(set(group_skip + group_link + group_vector))))

if __name__ == "__main__":
    main()