from pathlib import Path
import re
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

# --- USER CONFIGURATION ---

# 1. Y-AXIS CONTROL
# Set the UPPER limit for latency. Set to None to auto-scale.
MANUAL_Y_LIMIT_TOP = None

# 2. X-AXIS CONTROL (DATA POINTS)
# Start plotting from this query number (0 is the beginning)
X_AXIS_START_INDEX = 0 

# Stop plotting at this query number. 
# Set to an integer (e.g., 200) to stop there. 
# Set to None to plot until the very end of the data.
X_AXIS_END_INDEX = 10001

# --- Import plotting setup ---
try:
    from plot import *
except ImportError:
    # Fallback if plot.py is missing to prevent crash
    print("Warning: 'plot.py' not found. Using default matplotlib styles.")

# --- Path Setup ---
CURR_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURR_DIR.parent 

# Target: /Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_getpath_sequential
STATS_DIR = PROJECT_ROOT / "data" / "filter_result_getpath_sequential"

PLOTS_DIR = CURR_DIR / "paper_plot" / "sequential_get_path_plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# --- Constants ---
USE_LOG_SCALE_Y = False
BUFFER_TO_PLOT = "Vector-dynamic"
FIGSIZE = (5, 3) 
NUM_RUNS = 1 
LATENCY_RE = re.compile(r"([\w:]+):\s*(\d+)")

# --- Component Plotting Control ---
# Ordered by expected size (Largest to Smallest) 
COMPONENT_STYLE_MAP = {
    "DB::Get":              {"color": "black",  "linestyle": "-", "linewidth": 3.5}, # Background (Thickest)
    "DBImpl::Get":          {"color": "pink",   "linestyle": "-", "linewidth": 3.0},
    "MemTable::GetImplSpent": {"color": "grey",   "linestyle": "--","linewidth": 2.5},
    "MemTable::Get":        {"color": "red",    "linestyle": "-", "linewidth": 2.0},
    "MemTable::GetFromTable": {"color": "purple", "linestyle": "-", "linewidth": 1.5},
    "VectorRep::Get":       {"color": "green",  "linestyle": "-", "linewidth": 1.0},
    "VectorRep::Seek":      {"color": "blue",   "linestyle": "-", "linewidth": 0.8}, # Foreground (Thinnest)
}

# --- Helper Functions ---
def parse_run_log(file_path: Path):
    data = defaultdict(list)
    if not file_path.exists():
        return {}
    
    try:
        with open(file_path, "r") as f:
            lines = f.readlines()
            # Exclude last line just in case it's incomplete
            if len(lines) > 0:
                lines = lines[:-1] 
            
            for line in lines:
                matches = LATENCY_RE.findall(line)
                for key, value in matches:
                    if key in COMPONENT_STYLE_MAP:
                        data[key].append(int(value))
                        
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return dict(data)


def average_runs(run_data_list):
    all_keys = set().union(*(d.keys() for d in run_data_list))
    avg_data = {}
    for key in all_keys:
        if key not in COMPONENT_STYLE_MAP: continue
        lists_for_key = [run[key] for run in run_data_list if key in run and len(run[key]) > 0]
        if not lists_for_key: continue
        min_len = min(len(l) for l in lists_for_key)
        if min_len == 0: continue
        trimmed_lists = [l[:min_len] for l in lists_for_key]
        avg_data[key] = np.mean(trimmed_lists, axis=0)
    return avg_data

def save_plot_legend(handles, labels, base_output_path):
    if not handles: return
    ncol = min(len(handles), 3)
    figsize_height = 0.4 * (1 + (len(handles) - 1) // ncol)
    figsize_width = 3.5 * ncol
    legend_fig = plt.figure(figsize=(figsize_width, figsize_height)) 
    legend_fig.legend(handles, labels, loc="center", ncol=ncol, frameon=False)
    plt.axis("off")
    
    # Save PDF only
    ext = "pdf"
    legend_fig.savefig(base_output_path.with_name(f"{base_output_path.name}_legend.{ext}"), bbox_inches="tight")
    plt.close(legend_fig)

def plot_component_latency(avg_data, output_filename):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.15, right=0.98, top=0.98, bottom=0.15) # Adjusted top margin since title is removed

    # Sort keys by value (Largest First -> Smallest Last) to ensure correct z-order
    def sort_key(item):
        key, style = item
        if key in avg_data:
            return np.mean(avg_data[key])
        return -1
        
    sorted_styles = sorted(COMPONENT_STYLE_MAP.items(), key=sort_key, reverse=True)

    max_x_encountered = 0

    for key, style in sorted_styles:
        if key not in avg_data: continue 
        values = avg_data[key]
        
        # --- Logic for Slicing X-Axis ---
        start = X_AXIS_START_INDEX
        end = X_AXIS_END_INDEX if X_AXIS_END_INDEX is not None else len(values)
        
        # Slice the values array
        sliced_values = values[start:end]
        
        if len(sliced_values) == 0:
            continue

        # Create proper X indices so the plot shows (e.g.) 50, 51, 52 instead of 0, 1, 2
        x_indices = np.arange(start, start + len(sliced_values))
        
        # Track max x for setting limits later
        if len(x_indices) > 0:
            max_x_encountered = max(max_x_encountered, x_indices[-1])

        ax.plot(x_indices, sliced_values, label=key, **style)

    if USE_LOG_SCALE_Y:
        ax.set_yscale('log')
    else:
        ax.set_yscale('linear')
        
    # Set Bottom to 0 unless manual limit overrides
    if MANUAL_Y_LIMIT_TOP is not None:
        ax.set_ylim(0, MANUAL_Y_LIMIT_TOP)
    else:
        ax.set_ylim(bottom=0)
    
    # Set X-Axis Limits tightly
    ax.set_xlim(left=X_AXIS_START_INDEX, right=max_x_encountered)

    ax.set_xlabel("point query numbers")
    ax.set_ylabel("latency (ns)")
    
    handles, labels = ax.get_legend_handles_labels()
    
    # Save PDF
    ext = "pdf"
    output_path = output_filename.with_suffix(f".{ext}")
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
    
    # Print absolute path of the saved plot
    print(f"[saved] {output_path.resolve()}")
    
    plt.close(fig)
    save_plot_legend(handles, labels, output_filename)

def main():
    # Patterns for both workloads
    patterns = {
        "sequential": r"sequential_get_path-lowpri_true-.*",
        "interleave": r"interleave_get_path-lowpri_true-.*"
    }
    
    found_any = False

    if not STATS_DIR.exists():
        print(f"Error: Stats directory does not exist: {STATS_DIR}")
        return

    # Iterate through all items in the directory
    for d in STATS_DIR.iterdir():
        if not d.is_dir():
            continue
            
        # Check if directory matches Sequential or Interleave
        workload_type = None
        for w_name, w_pattern in patterns.items():
            if re.match(w_pattern, d.name):
                workload_type = w_name
                break
        
        if not workload_type:
            continue
            
        found_any = True
        print(f"\n--- Processing: {workload_type.upper()} ({d.name}) ---")
        
        buffer_dir = d / BUFFER_TO_PLOT
        if not buffer_dir.is_dir():
            print(f"Error: {buffer_dir} not found. Skipping.")
            continue
            
        run_data = []
        log_file = buffer_dir / "run1.log"
        if log_file.exists():
            log_data = parse_run_log(log_file)
            if log_data: run_data.append(log_data)
        
        if not run_data:
            print("Error: run1.log not found or empty.")
            continue
            
        avg_data = average_runs(run_data)
        if not avg_data:
            print("Error: Data empty.")
            continue
            
        # Define Filename (Title removed)
        filename_base = PLOTS_DIR / f"latency_breakdown_{BUFFER_TO_PLOT}_{workload_type}"
        
        plot_component_latency(avg_data, filename_base)

    if not found_any:
        print(f"Warning: No matching directories found in {STATS_DIR}")

if __name__ == "__main__":
    main()