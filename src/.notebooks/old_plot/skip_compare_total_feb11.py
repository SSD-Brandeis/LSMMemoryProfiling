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

# Set to True to write numeric values on top of bars
SHOW_VALUES_ON_BARS = True 

try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

ROOT = CURR_DIR.parent

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
        print(f"✅ Using font: {fp}")
        font_found = True
        break

if not font_found:
    print("❌ CRITICAL ERROR: Publication font 'LinLibertine_Mah.ttf' not found.")
    sys.exit(1)

plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "normal"
plt.rcParams["font.size"] = 24

# --- STYLES ---

bar_styles["hash_skip_list-H1"] =       {"color": "thistle", "edgecolor": "black", "hatch": "..."}
bar_styles["hash_skip_list-H2"] =       {"color": "plum", "edgecolor": "black", "hatch": "///"}
bar_styles["hash_skip_list-H5"] =       {"color": "lightskyblue", "edgecolor": "black", "hatch": "\\\\"}
bar_styles["hash_skip_list-H10"] =      {"color": "steelblue", "edgecolor": "black", "hatch": "xx"}
bar_styles["hash_skip_list-H100000"] =  {"color": "mediumseagreen", "edgecolor": "black", "hatch": "--"}

bar_styles["hash_linked_list-H1"] =     {"color": "lemonchiffon", "edgecolor": "black", "hatch": "..."}
bar_styles["hash_linked_list-H2"] =     {"color": "khaki", "edgecolor": "black", "hatch": "///"}
bar_styles["hash_linked_list-H5"] =     {"color": "navajowhite", "edgecolor": "black", "hatch": "\\\\"}
bar_styles["hash_linked_list-H10"] =    {"color": "darkorange", "edgecolor": "black", "hatch": "xx"}
bar_styles["hash_linked_list-H100000"] = {"color": "olivedrab", "edgecolor": "black", "hatch": "--"}

if "skiplist" not in bar_styles:
    bar_styles["skiplist"] = {"color": "white", "edgecolor": "black", "hatch": ""}


if "simpleskiplist" not in bar_styles:
    bar_styles["simpleskiplist"] = {"color": "gainsboro", "edgecolor": "black", "hatch": "||"}



DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_feb11_hash_compare")

TARGET_FOLDERS = [
    "skiplist_compare_feb11_nothrottling_small_insert-lowpri_true-I1000000-U0-Q0-S0-Y0-T5-P131072-B32-E128"
]

PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/feb11_skip_compare_total")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

WRITE_RE = re.compile(r"rocksdb\.db\.write\.micros.*?SUM\s*:\s*(\d+)")
GET_RE = re.compile(r"rocksdb\.db\.get\.micros.*?SUM\s*:\s*(\d+)")

def get_bar_style(buf_name):
    default_style = {"color": "gray", "edgecolor": "black", "hatch": ""}
    style = bar_styles.get(buf_name, default_style).copy()
    style.pop("label", None)
    return style

def parse_log_file(text):
    insert_micros = 0
    get_micros = 0
    
    m_write = WRITE_RE.search(text)
    if m_write:
        insert_micros = int(m_write.group(1))
        
    m_get = GET_RE.search(text)
    if m_get:
        get_micros = int(m_get.group(1))
        
    return insert_micros, get_micros

def load_data():
    records = []
    # Track if baseline buffers are already added to avoid duplicates if multiple folders exist
    added_buffers = set()

    for folder_name in TARGET_FOLDERS:
        target_dir = DATA_ROOT / folder_name
        if not target_dir.exists():
            print(f"Warning: Directory not found: {target_dir}")
            continue
        
        # is_test3 logic removed as it was specific to Jan23 folders
        
        for log_path in target_dir.rglob("workload1.log"):
            dir_name = log_path.parent.name
            
            if "hash_skip_list" in dir_name:
                base = "hash_skip_list"
            elif "hash_linked_list" in dir_name:
                base = "hash_linked_list"
            elif "simpleskipList" in dir_name: # Note: Case sensitive check based on `tree` output
                base = "simpleskiplist"
            elif "skiplist" in dir_name:
                base = "skiplist"
            else:
                continue

            # Determine key
            if base == "skiplist":
                key = "skiplist"
            elif base == "simpleskiplist":
                key = "simpleskiplist"
            else:
                # Handle H-values
                h_val = "0"
                if "-H" in dir_name:
                    parts = dir_name.split("-H")
                    if len(parts) > 1:
                        h_val = parts[1]
                
                key = f"{base}-H{h_val}"

            # Prevent duplicate entries for non-H buffers if multiple runs exist 
            # (though in this specific tree structure, they seem unique)
            if key in added_buffers and (base == "skiplist" or base == "simpleskiplist"):
                continue

            try:
                content = log_path.read_text()
                ins_micros, pq_micros = parse_log_file(content)
                
                records.append({
                    "buffer": key,
                    "total_ins_s": ins_micros * 1e-6,
                    "total_pq_s": pq_micros * 1e-6,
                    "total_workload_s": (ins_micros + pq_micros) * 1e-6
                })
                added_buffers.add(key)
            except Exception as e:
                print(f"Error reading {log_path}: {e}")
            
    if not records:
        return pd.DataFrame()
        
    return pd.DataFrame(records).groupby("buffer", as_index=False).mean()

def plot_chart(df, col, ylabel, xlabel, out_name, buffers):
    valid_bufs = [b for b in buffers if b in df["buffer"].values]
    if not valid_bufs:
        print(f"Skipping plot {out_name.name} (No valid data found for requested buffers)")
        return

    fig, ax = plt.subplots(figsize=(5, 3.6))
    
    n_bars = len(valid_bufs)
    
    group_width = 0.9
    bar_width_ratio = 0.9
    
    width_of_one_bar_slot = group_width / n_bars
    width_bar = width_of_one_bar_slot * bar_width_ratio
    
    group_center = 0
    start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

    for i, buf in enumerate(valid_bufs):
        val = df.loc[df["buffer"] == buf, col].iat[0]
        style = get_bar_style(buf)
        
        bar_x_pos = start_x + i * width_of_one_bar_slot
        
        rects = ax.bar(bar_x_pos, val, width=width_bar, **style)

        if SHOW_VALUES_ON_BARS:
            for rect in rects:
                height = rect.get_height()
                if height > 1000 or height < 0.01:
                    label_txt = f'{height:.1e}'
                else:
                    label_txt = f'{height:.2f}'
                
                ax.annotate(label_txt,
                            xy=(rect.get_x() + rect.get_width() / 2, height),
                            xytext=(0, 3),  
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=12)

    ax.set_xticks([0])
    ax.set_xticklabels([xlabel])
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)
    ax.ticklabel_format(style="scientific", axis="y", scilimits=(0,0))
    ax.tick_params(axis="y", labelsize=24)
    
    fig.tight_layout()

    fig.savefig(out_name, bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)
    print(f"Saved {out_name}")

def create_legends(buffers):
    leg_dir = PLOTS_DIR / "legends"
    leg_dir.mkdir(parents=True, exist_ok=True)
    
    for buf in buffers:
        fig, ax = plt.subplots()
        h = ax.bar([0], [1], **get_bar_style(buf))
        
        label_text = buf
        
        if label_text == "skiplist":
            label_text = "Skip List"
        elif label_text == "simpleskiplist":
            label_text = "Simple Skip List"
        else:
            label_text = label_text.replace("hash_skip_list", "Hash Skip List")
            label_text = label_text.replace("hash_linked_list", "Hash Linked List")
            label_text = label_text.replace("-H", " H")
            label_text = label_text.replace("100000", "100k")
            
        fig_l, ax_l = plt.subplots(figsize=(4, 1))
        ax_l.axis('off')
        ax_l.legend([h], [label_text], loc='center', frameon=False, fontsize=20)
        
        clean_name = buf.replace(" ", "_").replace("/", "-")
        fig_l.savefig(leg_dir / f"legend_{clean_name}.pdf", bbox_inches='tight')
        plt.close(fig)
        plt.close(fig_l)

def main():
    df = load_data()
    if df.empty:
        print("No data found.")
        return

    print("Data Loaded. Available buffers:")
    print(df["buffer"].unique())

    # Included simpleskiplist in the grouping
    group_skip = [
        "skiplist", 
        "simpleskiplist",
        "hash_skip_list-H1", 
        "hash_skip_list-H2", 
        "hash_skip_list-H5", 
        "hash_skip_list-H10", 
        "hash_skip_list-H100000"
    ]
    
    group_link = [
        "skiplist", 
        "simpleskiplist",
        "hash_linked_list-H1", 
        "hash_linked_list-H2", 
        "hash_linked_list-H5", 
        "hash_linked_list-H10", 
        "hash_linked_list-H100000"
    ]

    plot_chart(df, "total_ins_s", "insert latency (s)", "insert", PLOTS_DIR / "latency_insert_hash_skip_list.pdf", group_skip)
    plot_chart(df, "total_pq_s", "point query latency (s)", "point query", PLOTS_DIR / "latency_pq_hash_skip_list.pdf", group_skip)
    plot_chart(df, "total_workload_s", "time (s)", "total workload", PLOTS_DIR / "workload_hash_skip_list.pdf", group_skip)

    plot_chart(df, "total_ins_s", "insert latency (s)", "insert", PLOTS_DIR / "latency_insert_hash_linked_list.pdf", group_link)
    plot_chart(df, "total_pq_s", "point query latency (s)", "point query", PLOTS_DIR / "latency_pq_hash_linked_list.pdf", group_link)
    plot_chart(df, "total_workload_s", "time (s)", "total workload", PLOTS_DIR / "workload_hash_linked_list.pdf", group_link)

    all_buffers = sorted(list(set(group_skip + group_link)))
    create_legends(all_buffers)

if __name__ == "__main__":
    main()