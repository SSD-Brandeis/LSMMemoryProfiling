import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib.ticker as mticker
import numpy as np
import sys
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
USE_LOG_SCALE = True 
NUM_BINS = 1

# Memory Instruction: Font enforcement
SPECIFIED_FONT = 'Arial' 

PLOTTED_BUFFERS = [
    # "buffer-1-skiplist",
    "buffer-2-vector-preallocated",
    # "buffer-3-hash_skip_list-X6-H100000",
    # "buffer-4-hash_linked_list-X6-H100000",
    # "buffer-5-unsortedvector-preallocated",
    # "buffer-6-alwayssortedVector-preallocated",
    # "buffer-9-hash_vector-X6-H100000",
]

BASE_DATA_PATH = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/ondisk_interleaved_insert_debug")
PLOTS_DIR = Path.cwd() / "latency_plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# FONT ENFORCEMENT & STYLE
# =============================================================================
available_fonts = {f.name for f in fm.fontManager.ttflist}
if SPECIFIED_FONT not in available_fonts:
    print(f"CRITICAL ERROR: Font '{SPECIFIED_FONT}' not found. Aborting.")
    sys.exit(1)

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": [SPECIFIED_FONT],
    "axes.unicode_minus": False,
    "axes.grid": False 
})

# =============================================================================
# DATA PARSING
# =============================================================================

def parse_latencies(stats_path):
    latencies = []
    if not stats_path.exists(): return []
    try:
        with open(stats_path, 'r', errors='ignore') as f:
            for line in f:
                clean = line.strip()
                if clean.startswith('I:'):
                    try:
                        val = int(clean.split(':')[1].strip())
                        if val < 1: val = 1
                        latencies.append(val)
                    except (ValueError, IndexError): continue
    except Exception: pass
    return latencies

# =============================================================================
# PLOTTING
# =============================================================================

def generate_comparison_plot(exp_folders):
    print("\n--- Generating Comparison Plot ---")
    fig, ax = plt.subplots(figsize=(18, 10))
    ax.grid(False)

    exp_data = {}
    global_max = 0
    
    for folder in exp_folders:
        data = parse_latencies(folder / PLOTTED_BUFFERS[0] / "stats.log")
        if data:
            bin_size = len(data) // NUM_BINS
            exp_data[folder.name] = [data[i*bin_size : (i+1)*bin_size] for i in range(NUM_BINS)]
            global_max = max(global_max, max(data))

    if not exp_data:
        print("[!] No data found to compare.")
        return

    num_exps = len(exp_data)
    group_width = 0.8 
    box_width = group_width / num_exps
    colors = ['#3498db', '#e74c3c', '#2ecc71', '#f1c40f']
    bin_indices = np.arange(NUM_BINS)
    
    legend_handles = []
    legend_labels = []

    for idx, (full_name, binned_list) in enumerate(exp_data.items()):
        color = colors[idx % len(colors)]
        offset = (idx - (num_exps - 1) / 2) * box_width
        positions = bin_indices + offset
        
        # FIX: Removed tick_labels from inside boxplot to prevent Dimension ValueError
        bp = ax.boxplot(binned_list, 
                        positions=positions, 
                        widths=box_width * 0.85, 
                        patch_artist=True,
                        tick_labels=None, 
                        boxprops={'color': color, 'linewidth': 1.0},
                        medianprops={'color': 'black', 'linewidth': 3.0},
                        whiskerprops={'color': color, 'linewidth': 1.5, 'alpha': 0.7},
                        capprops={'color': color, 'linewidth': 1.5},
                        flierprops={'marker': 'o', 'markersize': 3.0, 'alpha': 0.8, 
                                    'markerfacecolor': color, 'markeredgecolor': 'none'})

        for patch in bp['boxes']:
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        legend_handles.append(bp['boxes'][0])
        # REVERTED: Using full folder name for legend
        legend_labels.append(full_name)

    # AXIS & TICK CONTROL
    if USE_LOG_SCALE:
        ax.set_yscale('log', base=10)
        ax.set_ylim(1, global_max) 
        ax.yaxis.set_major_formatter(mticker.LogFormatterMathtext())

    ax.set_xticks(bin_indices)
    ax.set_xticklabels([f"{i*100}k" for i in range(NUM_BINS)])
    
    ax.set_xlabel(r"$\mathrm{Insert\ Count}$", fontsize=12)
    ax.set_ylabel(r"$\mathrm{Latency\ (ns)}$", fontsize=12)
    ax.set_title(f"Grouped Latency Comparison: {PLOTTED_BUFFERS[0]}", fontsize=14)
    
    # Legend placement: Absolute full names, placed below plot
    ax.legend(legend_handles, legend_labels, loc='upper center', 
              bbox_to_anchor=(0.5, -0.15), ncol=1, fontsize='x-small', 
              frameon=True, shadow=False)
    
    plt.tight_layout()
    save_path = PLOTS_DIR / "FINAL_FULL_LABEL_COMPARISON.pdf"
    plt.savefig(save_path, bbox_inches='tight')
    
    print(f"[SUCCESS] Saved Plot: file://{save_path.resolve()}")
    plt.close(fig)

def main():
    if not BASE_DATA_PATH.exists():
        sys.exit(f"Data path not found: {BASE_DATA_PATH}")

    exp_folders = sorted([d for d in BASE_DATA_PATH.iterdir() if d.is_dir() and d.name.startswith("-multiphase")])
    
    if not exp_folders:
        print("No valid experiment folders found.")
        return

    generate_comparison_plot(exp_folders)

if __name__ == "__main__":
    main()