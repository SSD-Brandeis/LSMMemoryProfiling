import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib import font_manager
from pathlib import Path
import re
import sys

# --- CONFIGURATION ---
USE_LOG_SCALE = True  # Set to True for Log Scale, False for Linear
SHOW_VALUES_ON_BARS = True
DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_feb24_linklist_insert")
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/feb24_linklist_results")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


BUFFERS_TO_PLOT = [
    "skiplist",
    "simple_skiplist",
    "linkedlist",
    "Vector-dynamic",
    "UnsortedVector-dynamic",
    "AlwayssortedVector-dynamic",
    "hash_skip_list-H1",
    "hash_linked_list-H1",
    # "hash_vector-H1",
]


TIME_RE = re.compile(r"Workload Execution Time:\s*(\d+)")

# --- FONT LOADING ---
try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

FONT_CANDIDATES = [
    CURR_DIR / "LinLibertine_Mah.ttf",
    Path.home() / "Desktop/tectonic/LinLibertine_Mah.ttf",
]

font_found = False
for fp in FONT_CANDIDATES:
    if fp.exists():
        prop = font_manager.FontProperties(fname=str(fp))
        plt.rcParams["font.family"] = prop.get_name()
        font_found = True
        break

if not font_found:
    print("❌ Publication font not found. Falling back to serif.")
    plt.rcParams["font.family"] = "serif"

plt.rcParams["text.usetex"] = True
plt.rcParams["font.size"] = 18

# --- STYLES ---
bar_styles = {
    "skiplist": {"color": "white", "edgecolor": "black", "hatch": ""},
    "Vector-dynamic": {"color": "lightskyblue", "edgecolor": "black", "hatch": "///"},
    "hash_skip_list-H1": {"color": "thistle", "edgecolor": "black", "hatch": "xx"},
    "hash_linked_list-H1": {"color": "lemonchiffon", "edgecolor": "black", "hatch": ".."},
    "UnsortedVector-dynamic": {"color": "salmon", "edgecolor": "black", "hatch": "\\\\"},
    "AlwayssortedVector-dynamic": {"color": "mediumseagreen", "edgecolor": "black", "hatch": "++"},
    "linkedlist": {"color": "navajowhite", "edgecolor": "black", "hatch": "--"},
    "simple_skiplist": {"color": "gainsboro", "edgecolor": "black", "hatch": "||"},
    "hash_vector-H1": {"color": "steelblue", "edgecolor": "black", "hatch": "oo"},
}

def get_style(buffer_name):
    return bar_styles.get(buffer_name, {"color": "gray", "edgecolor": "black"})

def load_data():
    records = []
    for log_path in DATA_ROOT.rglob("workload_run.log"):
        buffer_folder_name = log_path.parent.name
        
        try:
            content = log_path.read_text()
            match = TIME_RE.search(content)
            if match:
                exec_time = int(match.group(1))
                clean_name = re.sub(r"buffer-\d+-", "", buffer_folder_name)
                
                # Only add if it's in our allowed list
                if clean_name in BUFFERS_TO_PLOT:
                    records.append({
                        "buffer": clean_name,
                        "time": exec_time
                    })
        except Exception as e:
            print(f"Error reading {log_path}: {e}")
            
    df = pd.DataFrame(records)
    if df.empty: return df

    # This ensures the dataframe follows the order defined in BUFFERS_TO_PLOT
    df['buffer'] = pd.Categorical(df['buffer'], categories=BUFFERS_TO_PLOT, ordered=True)
    return df.sort_values("buffer")

def plot_workload(df):
    if df.empty:
        print("No data found for the selected buffers.")
        return

    # Adjusting figure width based on number of buffers to keep it clean
    fig_width = max(6, len(df) * 1.2)
    fig, ax = plt.subplots(figsize=(fig_width, 6))
    
    buffers = df["buffer"].tolist()
    values = df["time"].tolist()
    x_pos = range(len(buffers))

    for i, buf in enumerate(buffers):
        style = get_style(buf)
        rects = ax.bar(i, values[i], **style)

        if SHOW_VALUES_ON_BARS:
            for rect in rects:
                height = rect.get_height()
                # Format: 1.2e11 if huge, otherwise standard number
                label_txt = f'{height:.1e}' if height > 1e7 else f'{int(height)}'
                ax.annotate(label_txt,
                            xy=(rect.get_x() + rect.get_width() / 2, height),
                            xytext=(0, 3), textcoords="offset points",
                            ha='center', va='bottom', fontsize=12)

    if USE_LOG_SCALE:
        ax.set_yscale('log')
        ax.set_ylim(bottom=1) 
        ax.set_ylabel("Execution Time (Log Scale)")
    else:
        ax.set_ylim(bottom=0)
        ax.set_ylabel("Execution Time (Linear)")
        ax.ticklabel_format(style="scientific", axis="y", scilimits=(0,0))

    ax.set_xticks(x_pos)
    ax.set_xticklabels(buffers, rotation=30, ha="right")
    
    plt.tight_layout()
    out_path = PLOTS_DIR / "workload_execution_comparison.pdf"
    fig.savefig(out_path, bbox_inches="tight")
    print(f"✅ Plot saved to: {out_path}")

if __name__ == "__main__":
    data_df = load_data()
    plot_workload(data_df)