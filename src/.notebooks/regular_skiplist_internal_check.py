import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import font_manager
from pathlib import Path
import re
import numpy as np

# --- Configuration ---
SHOW_VALUES = True
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

# Data and Plot Directories
DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_collisioncost_inmemory_internal_clock_feb4/regular_skiplist_test_collisioncost_sequential_inmemory_modifiedskiplist_innerclock_feb4-lowpri_true-I100000-U0-Q100-S10-Y0.01-T10-P131072-B4-E1024")
PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Feb5_modified_skiplist_internalclock")  

LEGEND_DIR = PLOT_DIR / "legends"

# Scaling: Nanoseconds -> Seconds for internal costs
COST_SCALE = 1e-9 

PATTERNS = {
    # HashSkipList Internal Costs (Nanoseconds)
    "ins_hashing":   re.compile(r"HashSkipList_Insert_Hashing:\s*(\d+)"),
    "ins_collision": re.compile(r"HashSkipList_Insert_Collision:\s*(\d+)"),
    "get_hashing":   re.compile(r"HashSkipList_Get_Hashing:\s*(\d+)"),
    "get_collision": re.compile(r"HashSkipList_Get_Collision:\s*(\d+)"),
    
    # Regular SkipList Internal Costs (Nanoseconds)
    "sl_insert": re.compile(r"SkipList_InsertKey:\s*(\d+)"),
    "sl_get":    re.compile(r"SkipList_Get:\s*(\d+)")
}

# --- Styling ---
STYLES = {
    # Main Buffers
    "Skip List":         {"facecolor": "white",   "edgecolor": "black", "hatch": ""},
    "Hash Skip List H1": {"facecolor": "#e0e0e0", "edgecolor": "black", "hatch": "///"},
    
    # Internal Metrics (treated as separate bars)
    "Hashing Cost":      {"facecolor": "white",   "edgecolor": "black", "hatch": "xx"},
    "Collision Cost":    {"facecolor": "white",   "edgecolor": "black", "hatch": ".."}
}

def setup_styles():
    font_path = BASE_DIR / "LinLibertine_Mah.ttf"
    if font_path.exists():
        prop = font_manager.FontProperties(fname=str(font_path))
        plt.rcParams["font.family"] = prop.get_name()

    plt.rcParams.update({
        "text.usetex": True,
        "font.size": 24,
        "axes.linewidth": 1.5
    })

def get_buffer_key(name):
    # Only plotting Hash Skip List and Skip List
    if "hash_skip_list" in name and "H1" in name:
        return "Hash Skip List H1"
    if "skiplist" in name and "simple" not in name:
        return "Skip List"
    return None

def load_data():
    records = []
    if not DATA_DIR.exists():
        print(f"Directory not found: {DATA_DIR}")
        return pd.DataFrame()

    for item in DATA_DIR.iterdir():
        if not item.is_dir(): continue
        key = get_buffer_key(item.name)
        if not key: continue
            
        run_file = item / "run1.log"
        ins_total, get_total = 0, 0
        ins_hash, ins_coll = 0, 0
        get_hash, get_coll = 0, 0
        
        if run_file.exists():
            run_text = run_file.read_text()
            
            if key == "Hash Skip List H1":
                # HashSkipList: Sum hashing and collision for the total
                ins_hash = sum(int(x) for x in PATTERNS["ins_hashing"].findall(run_text)) * COST_SCALE
                ins_coll = sum(int(x) for x in PATTERNS["ins_collision"].findall(run_text)) * COST_SCALE
                get_hash = sum(int(x) for x in PATTERNS["get_hashing"].findall(run_text)) * COST_SCALE
                get_coll = sum(int(x) for x in PATTERNS["get_collision"].findall(run_text)) * COST_SCALE
                ins_total = ins_hash + ins_coll
                get_total = get_hash + get_coll
            else:
                # Regular SkipList: Sum SkipList_InsertKey and SkipList_Get
                ins_total = sum(int(x) for x in PATTERNS["sl_insert"].findall(run_text)) * COST_SCALE
                get_total = sum(int(x) for x in PATTERNS["sl_get"].findall(run_text)) * COST_SCALE

        records.append({
            "buffer": key,
            "Insert_Total": ins_total,
            "Insert_Hashing": ins_hash,
            "Insert_Collision": ins_coll,
            "Get_Total": get_total,
            "Get_Hashing": get_hash,
            "Get_Collision": get_coll
        })
    return pd.DataFrame(records)

def plot_simple_bars(df, operation, ylabel, fname):
    """
    Constructs a list of 4 items:
    1. Skip List Total
    2. Hash Skip List Total
    3. Hashing Cost (extracted from Hash Skip List data)
    4. Collision Cost (extracted from Hash Skip List data)
    """
    bars_data = []
    
    # 1. Skip List
    row_sl = df.loc[df["buffer"] == "Skip List"]
    if not row_sl.empty:
        bars_data.append(("Skip List", row_sl[f"{operation}_Total"].iat[0]))
        
    # 2. Hash Skip List (Total)
    row_hsl = df.loc[df["buffer"] == "Hash Skip List H1"]
    hsl_hashing = 0
    hsl_collision = 0
    
    if not row_hsl.empty:
        bars_data.append(("Hash Skip List H1", row_hsl[f"{operation}_Total"].iat[0]))
        hsl_hashing = row_hsl[f"{operation}_Hashing"].iat[0]
        hsl_collision = row_hsl[f"{operation}_Collision"].iat[0]
    
    # 3. Hashing Cost
    bars_data.append(("Hashing Cost", hsl_hashing))
    
    # 4. Collision Cost
    bars_data.append(("Collision Cost", hsl_collision))

    if not bars_data: return

    # Plotting
    fig, ax = plt.subplots(figsize=(8, 4))
    
    names = [b[0] for b in bars_data]
    values = [b[1] for b in bars_data]
    x_pos = range(len(names))
    
    for i, (name, val) in enumerate(bars_data):
        style = STYLES.get(name, STYLES["Skip List"])
        rect = ax.bar(i, val, width=0.5, **style)
        
        if SHOW_VALUES and val > 0:
            txt = f'{val:.1e}' if (val < 0.01 or val > 1000) else f'{val:.2f}'
            ax.bar_label(rect, labels=[txt], padding=3, fontsize=12)

    ax.set_xticks(x_pos)
    ax.set_xticklabels([n.replace(" ", "\n") for n in names], fontsize=16)
    
    ax.set_ylabel(ylabel)
    ax.set_xlabel(f"{operation} Operations")
    ax.set_ylim(bottom=0)
    ax.ticklabel_format(style="scientific", axis="y", scilimits=(0,0))
    
    out_path = PLOT_DIR / fname
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight", pad_inches=0.1)
    plt.close(fig)
    print(f"Saved plot: {out_path}")

def save_legends():
    LEGEND_DIR.mkdir(parents=True, exist_ok=True)
    for name, style in STYLES.items():
        fig, ax = plt.subplots(figsize=(3, 0.8))
        rect = plt.Rectangle((0, 0), 1, 1, **style)
        ax.legend([rect], [name], loc='center', frameon=False, fontsize=22)
        ax.axis('off')
        
        safe_name = name.replace(" ", "_").lower()
        fig.savefig(LEGEND_DIR / f"legend_{safe_name}.pdf", bbox_inches='tight')
        plt.close(fig)

def main():
    setup_styles()
    df = load_data()
    if df.empty: return

    # Plot Insert
    plot_simple_bars(df, "Insert", "Time (s)", "latency_insert_4bars.pdf")
    
    # Plot Get
    plot_simple_bars(df, "Get", "Time (s)", "latency_pq_4bars.pdf")
    
    save_legends()

if __name__ == "__main__":
    main()