import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import font_manager
from pathlib import Path
import re
import numpy as np


SHOW_VALUES = True
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()
    

   
# DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/skiplist_compare_feb11_nothrottling_orderedwl-lowpri_true-I100000-U0-Q0-S0-Y0-T5-P131072-B32-E128")
# PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Feb11_skipcompare_orderedwl")  
   

   
DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/skiplist_compare_feb12_nothrottling_hashing-lowpri_true-I100000-U0-Q0-S0-Y0-T5-P131072-B32-E128")
PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Feb12_skipcompare_hashingcost")  
   

   
   
   
# DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/skiplist_compare_feb11_nothrottling-lowpri_true-I100000-U0-Q0-S0-Y0-T5-P131072-B32-E128")
# PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Feb11_skipcompare_randomwl")  
   

   
  
# DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/skiplist_compare_feb11-lowpri_true-I100000-U0-Q0-S0-Y0-T5-P131072-B32-E128")
# PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Feb11_skipcompare")  



# DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_collisioncost_inmemory_feb4/regular_skiplist_test_collisioncost_sequential_inmemory_modifiedskiplist_feb4-lowpri_true-I100000-U0-Q100-S10-Y0.01-T10-P131072-B4-E1024")
# PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Feb4_modified_skiplist")  
    
# DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_collisioncost_ondisk_feb3/regular_skiplist_test_collisioncost_sequential_ondisk_1gb_feb3-lowpri_true-I1048576-U0-Q100-S10-Y0.01-T10-P4096-B4-E1024")
# PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Feb3_skip_ondisk_results")

# DATA_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_collisioncost_feb2/regular_skiplist_test_collisioncost_sequential_feb2-lowpri_true-I100000-U0-Q100-S10-Y0.01-T10-P131072-B4-E1024")
# PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/Feb2_test4_results")
LEGEND_DIR = PLOT_DIR / "legends"


COST_SCALE = 1e-9 

PATTERNS = {
  
    "write": re.compile(r"rocksdb\.db\.write\.micros.*?SUM\s*:\s*(\d+)"),
    "get":   re.compile(r"rocksdb\.db\.get\.micros.*?SUM\s*:\s*(\d+)"),
  
    "ins_hashing":   re.compile(r"HashSkipList_Insert_Hashing:\s*(\d+)"),
    "ins_collision": re.compile(r"HashSkipList_Insert_Collision:\s*(\d+)"),
    "get_hashing":   re.compile(r"HashSkipList_Get_Hashing:\s*(\d+)"),
    "get_collision": re.compile(r"HashSkipList_Get_Collision:\s*(\d+)")
}


STYLES = {
 
    "Skip List":         {"facecolor": "white",   "edgecolor": "black", "hatch": ""},
    "Simple Skip List":  {"facecolor": "#808080", "edgecolor": "black", "hatch": "\\\\"},
    "Hash Skip List H1": {"facecolor": "#e0e0e0", "edgecolor": "black", "hatch": "///"},
    

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
    if "hash_skip_list" in name and "H1" in name:
        return "Hash Skip List H1"
    if "simpleskiplist" in name:
        return "Simple Skip List"
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
            
      
        log_file = item / "workload1.log"
        ins_total, get_total = 0, 0
        if log_file.exists():
            text = log_file.read_text()
            m_write = PATTERNS["write"].search(text)
            m_get = PATTERNS["get"].search(text)
            ins_total = int(m_write.group(1)) * 1e-6 if m_write else 0
            get_total = int(m_get.group(1)) * 1e-6 if m_get else 0

       
        run_file = item / "run1.log"
        ins_hash, ins_coll = 0, 0
        get_hash, get_coll = 0, 0
        if run_file.exists():
            run_text = run_file.read_text()
            ins_hash = sum(int(x) for x in PATTERNS["ins_hashing"].findall(run_text)) * COST_SCALE
            ins_coll = sum(int(x) for x in PATTERNS["ins_collision"].findall(run_text)) * COST_SCALE
            get_hash = sum(int(x) for x in PATTERNS["get_hashing"].findall(run_text)) * COST_SCALE
            get_coll = sum(int(x) for x in PATTERNS["get_collision"].findall(run_text)) * COST_SCALE

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

 
    bars_data = []
    

    row_sl = df.loc[df["buffer"] == "Skip List"]
    if not row_sl.empty:
        bars_data.append(("Skip List", row_sl[f"{operation}_Total"].iat[0]))
        

    row_ssl = df.loc[df["buffer"] == "Simple Skip List"]
    if not row_ssl.empty:
        bars_data.append(("Simple Skip List", row_ssl[f"{operation}_Total"].iat[0]))
        

    row_hsl = df.loc[df["buffer"] == "Hash Skip List H1"]
    hsl_hashing = 0
    hsl_collision = 0
    
    if not row_hsl.empty:
        bars_data.append(("Hash Skip List H1", row_hsl[f"{operation}_Total"].iat[0]))
        hsl_hashing = row_hsl[f"{operation}_Hashing"].iat[0]
        hsl_collision = row_hsl[f"{operation}_Collision"].iat[0]
    

    bars_data.append(("Hashing Cost", hsl_hashing))
    
 
    bars_data.append(("Collision Cost", hsl_collision))

    if not bars_data: return


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
    plot_simple_bars(df, "Insert", "Time (s)", "latency_insert_5bars.pdf")
    
 
    # plot_simple_bars(df, "Get", "Time (s)", "latency_pq_5bars.pdf")
    
    save_legends()

if __name__ == "__main__":
    main()