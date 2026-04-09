from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.ticker as ticker
import math
from collections import defaultdict

# --- Import plotting setup ---
try:
    from plot import *
except ImportError:
    print("Warning: 'plot.py' not found. Using default matplotlib styles.")
    # Define fallback styles if 'plot.py' is essential and missing
    # For example:
    import matplotlib.pyplot as plt
    line_styles = defaultdict(lambda: {"color": "blue", "linestyle": "-"})


# --- Path Setup ---
CURR_DIR = Path(__file__).resolve().parent

# --- Data & Plot Directories ---
# Assuming EXP_DIR is defined in plot.py or needs to be defined here
try:
    EXP_DIR
except NameError:
    print("Warning: EXP_DIR not defined, defaulting to CURR_DIR")
    EXP_DIR = CURR_DIR

STATS_DIR = EXP_DIR / "write_stall_data"

# Updated directory patterns
LOWPRI_TRUE_PATTERN = r"internal_function_call-lowpri_true-.*"
LOWPRI_TRUE_DIRS = [
    d
    for d in STATS_DIR.iterdir()
    if d.is_dir() and re.match(LOWPRI_TRUE_PATTERN, d.name)
]
if not LOWPRI_TRUE_DIRS:
    raise FileNotFoundError(f"Could not find 'lowpri_true' dir in {STATS_DIR}")
LOWPRI_TRUE_DIR = LOWPRI_TRUE_DIRS[0]

LOWPRI_FALSE_PATTERN = r"internal_function_call-lowpri_false-.*"
LOWPRI_FALSE_DIRS = [
    d
    for d in STATS_DIR.iterdir()
    if d.is_dir() and re.match(LOWPRI_FALSE_PATTERN, d.name)
]
if not LOWPRI_FALSE_DIRS:
    raise FileNotFoundError(f"Could not find 'lowpri_false' dir in {STATS_DIR}")
LOWPRI_FALSE_DIR = LOWPRI_FALSE_DIRS[0]

EXPERIMENT_DIRS = {"true": LOWPRI_TRUE_DIR, "false": LOWPRI_FALSE_DIR}
PLOTS_DIR = CURR_DIR / "paper_plot" / "write_stall_component_plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# Debug plot directory
DEBUG_PLOTS_DIR = CURR_DIR / "paper_plot" / "debug"
DEBUG_PLOTS_DIR.mkdir(parents=True, exist_ok=True)


# --- Constants ---

# Plotting Scale Control
USE_LOG_SCALE_Y = True

BUFFER_TO_PLOT = "Vector-dynamic"
FIGSIZE = (10, 5) # Use a wider size
NUM_RUNS = 3
LATENCY_RE = re.compile(r"(\w+):\s*(\d+)")

# Component Plotting Control
COMPONENT_STYLE_MAP = {
    "Lock": {"color": line_styles.get("hash_skip_list", {}).get("color", "blue"), "linewidth":1},
    "VectorRep": {"color": line_styles.get("Vector", {}).get("color", "green"), "linewidth": 1},
    "MemTableRep": {"color": line_styles.get("skiplist", {}).get("color", "purple"), "linewidth": 1},
    "MemTable": {"color": line_styles.get("hash_linked_list", {}).get("color", "red"), "linewidth": 1},
    "PutCFImpl": {"color": line_styles.get("UnsortedVector", {}).get("color", "grey"), "linewidth": 1},
    # "WriteBatchInternal": {"color": line_styles.get("AlwaysSortedVector", {}).get("color", "brown"), "linewidth": 1},
    "DBImpl": {"color": line_styles.get("hash_linked_list_optimized", {}).get("color", "pink"), "linewidth": 1},
}


# --- Helper Functions ---
def parse_run_log(file_path: Path):
    """
    Extracts all key-value latency pairs from a run*.log file.
    """
    data = defaultdict(list)
    if not file_path.exists():
        print(f"Warning: Log file not found: {file_path}")
        return {}
    
    try:
        with open(file_path, "r") as f:
            for line in f:
                matches = LATENCY_RE.findall(line)
                for key, value in matches:
                    if key in COMPONENT_STYLE_MAP:
                        data[key].append(int(value))
    except Exception as e:
        print(f"Error reading or parsing {file_path}: {e}")
    
    return dict(data)


def average_runs(run_data_list):
    """
    Averages the data from multiple runs.
    """
    all_keys = set().union(*(d.keys() for d in run_data_list))
    avg_data = {}
    
    for key in all_keys:
        if key not in COMPONENT_STYLE_MAP:
            continue

        lists_for_key = [
            run[key] for run in run_data_list if key in run and len(run[key]) > 0
        ]
        
        if not lists_for_key:
            print(f"Warning: No data found for component '{key}' in any run.")
            continue
            
        min_len = min(len(l) for l in lists_for_key)
        
        if min_len == 0:
            print(f"Warning: Zero-length data for component '{key}'. Skipping.")
            continue

        trimmed_lists = [l[:min_len] for l in lists_for_key]
        avg_data[key] = np.mean(trimmed_lists, axis=0)
        
    return avg_data

def save_plot_legend(handles, labels, base_output_path):
    """Saves a plot's legend to PDF and SVG files."""
    if not handles:
        print("Warning: No legend handles found. Skipping legend save.")
        return
    
    #ncol = min(len(handles), 4)
    #3 cols legend
    ncol = min(len(handles), 3)
    figsize_height = 0.4 * (1 + (len(handles) - 1) // ncol)
    figsize_width = 2.5 * ncol
    
    legend_fig = plt.figure(figsize=(figsize_width, figsize_height)) 
    legend_fig.legend(
        handles,
        labels,
        loc="center",
        ncol=ncol,
        frameon=False,
        labelspacing=0.3,
        handletextpad=0.6,
        columnspacing=1.0,
    )
    plt.axis("off")

    for ext in ["pdf", "svg"]:
        legend_output_path = base_output_path.with_name(
            f"{base_output_path.name}_legend.{ext}"
        )
        legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0.01)
        print(f"[saved] {legend_output_path.name}")
    plt.close(legend_fig)


def save_plot_caption(caption_text, base_output_path):
    """Saves a plot's caption to PDF and SVG files."""
    if not caption_text:
        return
    title_fig = plt.figure(figsize=(FIGSIZE[0], 0.5))
    title_fig.text(
        0.5,
        0.5,
        caption_text,
        ha="center",
        va="center",
        fontsize=plt.rcParams["font.size"],
    )
    plt.axis("off")

    for ext in ["pdf", "svg"]:
        caption_output_path = base_output_path.with_name(
            f"{base_output_path.name}_caption.{ext}"
        )
        title_fig.savefig(caption_output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {caption_output_path.name}")
    plt.close(title_fig)

# --- Plotting Function ---
def plot_component_latency(avg_data, title, output_filename):
    """
    Generates and saves a single plot for all components.
    """
    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.1, right=0.98, top=0.95, bottom=0.1) 

    def sort_key(item):
        key, style = item
        if key in avg_data:
            return np.mean(avg_data[key])
        return -1
        
    sorted_styles = sorted(COMPONENT_STYLE_MAP.items(), key=sort_key, reverse=True)

    for key, style in sorted_styles:
        if key not in avg_data:
            continue 
            
        values = avg_data[key]
        ax.plot(values, label=key, **style)

    if USE_LOG_SCALE_Y:
        ax.set_yscale('log')
        ax.set_ylim(1e0, 1e6)
    else:
        ax.set_yscale('linear')
        ax.set_ylim(bottom=0)
    
    ax.set_xlabel("operation")
    ax.set_ylabel("time (ns)")
    
    handles, labels = ax.get_legend_handles_labels()
    
    for ext in ["pdf", "svg"]:
        output_path = output_filename.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        print(f"[saved] {output_path.name}")
    plt.close(fig)
    
    save_plot_legend(handles, labels, output_filename)
    save_plot_caption(title, output_filename)


# --- Main Execution ---
def main():
    """
    Main function to generate the two requested plots.
    """
    # --- Standard plot tasks ---
    plot_tasks = [("true", LOWPRI_TRUE_DIR), ("false", LOWPRI_FALSE_DIR)]

    # --- MODIFICATION: Add mapping for captions ---
    caption_map = {
        "true": "priority compaction",
        "false": "priority write"
    }
    # --- END MODIFICATION ---


    for setting, data_dir in plot_tasks:
        
        # --- MODIFICATION: Use map for print ---
        caption_suffix = caption_map.get(setting, f"lowpri_{setting}")
        print(f"\n--- Processing: {BUFFER_TO_PLOT} ({caption_suffix}) ---")
        # --- END MODIFICATION ---
        
        buffer_dir = data_dir / BUFFER_TO_PLOT
        if not buffer_dir.is_dir():
            print(f"Error: Directory not found: {buffer_dir}. Skipping.")
            continue
            
        run_data = []
        for i in range(1, NUM_RUNS + 1):
            log_file = buffer_dir / f"run{i}.log"
            log_data = parse_run_log(log_file)
            if log_data:
                run_data.append(log_data)
        
        if len(run_data) < NUM_RUNS:
            print(f"Warning: Found data for only {len(run_data)} out of {NUM_RUNS} runs.")
        
        if not run_data:
            print("Error: No data found for any run. Skipping plot.")
            continue
            
        avg_data = average_runs(run_data)
        
        if not avg_data:
            print("Error: Averaged data is empty. Skipping plot.")
            continue
            
        # --- MODIFICATION: Update title logic ---
        title = f"{BUFFER_TO_PLOT} ({caption_suffix})"
        # --- END MODIFICATION ---
        
        filename_base = PLOTS_DIR / f"stall_latency_{BUFFER_TO_PLOT}_lowpri_{setting}"
        
        plot_component_latency(avg_data, title, filename_base)
        
    # --- MODIFIED: Added separate debug plot task ---
    print(f"\n--- Processing: Debug Plot (1000ms refill) ---")
    
    # Define the specific path from your request
    debug_buffer_dir = STATS_DIR / "internal_function_call_1000ms_refill-lowpri_true-I10000-U0-Q0-S0-Y0-T10-P16384-B4-E1024" / BUFFER_TO_PLOT
    
    if not debug_buffer_dir.is_dir():
        print(f"Error: Debug directory not found: {debug_buffer_dir}. Skipping debug plot.")
    else:
        debug_run_data = []
        # --- MODIFIED: Corrected loop to use NUM_RUNS ---
        for i in range(1, NUM_RUNS + 1):
            log_file = debug_buffer_dir / f"run{i}.log"
            log_data = parse_run_log(log_file)
            if log_data:
                debug_run_data.append(log_data)
        
        if not debug_run_data:
            print("Error: No data found for debug run. Skipping plot.")
        else:
            debug_avg_data = average_runs(debug_run_data)
            
            if not debug_avg_data:
                print("Error: Averaged debug data is empty. Skipping plot.")
            else:
                debug_title = f"{BUFFER_TO_PLOT} (1000ms_refill)"
                # Save to the new debug directory
                debug_filename_base = DEBUG_PLOTS_DIR / "stall_latency_1000ms_refill_debug"
                
                plot_component_latency(debug_avg_data, debug_title, debug_filename_base)
    # --- End Modification ---

if __name__ == "__main__":
    main()