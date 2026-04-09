import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from pathlib import Path
import re
import sys
import math

# =============================================================================
# STYLE & FONT ENFORCEMENT (Strict adherence to plot.py)
# =============================================================================
try:
    # Font registration and rcParams are handled within this module
    sys.path.append("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/plot")
    from plot import *
except ImportError:
    print("Error: 'plot.py' not found. Aborting program to prevent unauthorized fallback styles.")
    sys.exit(1)

# =============================================================================
# CONFIGURATION
# =============================================================================
USE_LOG_SCALE = False 
FIGSIZE = (4, 3.2)

EXP_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/vary_rqs_exp_interleave_mar29")
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/vary_rangequery_selectivity_rqs")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    # "vector-preallocated",
    # "unsortedvector-preallocated",
    # "alwayssortedVector-preallocated",
    "skiplist",
    # "simple_skiplist",
    "hash_vector-X6-H100000",
    "hash_skip_list-X6-H100000",
    "hash_linked_list-X6-H100000",
]

DIR_PATTERN = re.compile(r"-RQ-(\d+)-S-([0-9.eE+]+)")
# Regex to capture the per-op latency from stats.log
STATS_LATENCY_RE = re.compile(r"S:\s*(\d+)")

# =============================================================================
# HELPERS
# =============================================================================
def apply_axis_settings(ax, x_ticks, y_ticks=None):
    """Explicit control for x and y ticks as requested."""
    ax.set_xticks(x_ticks)
    # ax.set_yticks(y_ticks) # Commented out to allow auto-scaling
    
    if USE_LOG_SCALE:
        ax.set_yscale('log', base=10)
        ax.set_ylim(bottom=1) # 10^0
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        ax.set_yscale('linear')
        # ax.set_ylim(bottom=y_ticks[0], top=y_ticks[-1])
        ax.set_ylim(bottom=0) # Starting at 0 for auto-scale
        formatter = mticker.ScalarFormatter(useMathText=True)
        formatter.set_scientific(True)
        formatter.set_powerlimits((-3, 4))
        ax.yaxis.set_major_formatter(formatter)
    
    ax.margins(x=0, y=0.1)

def save_plot_pdf(fig, base_filename):
    """Saves PDF only. No captions. No SVG."""
    fig.tight_layout(pad=0.08)
    output_path = PLOTS_DIR / f"{base_filename}.pdf"
    fig.savefig(output_path, bbox_inches="tight")
    print(f"[saved plot] {output_path.name}")

# =============================================================================
# MAIN
# =============================================================================
def plot_per_query_latency(data_dir, target_selectivity):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    plotted = False
    rq_count_found = 1000

    for path in sorted(data_dir.iterdir()):
        if not path.is_dir() or "sanitycheck" not in path.name:
            continue
            
        m_dir = DIR_PATTERN.search(path.name)
        if not m_dir:
            continue

        try:
            found_sel = float(m_dir.group(2))
        except ValueError:
            continue

        if not math.isclose(found_sel, target_selectivity, rel_tol=1e-7):
            continue
        
        rq_count_found = int(m_dir.group(1))

        for buffer_dir in sorted(path.iterdir()):
            m_buf = re.match(r"^buffer-\d+-(.*)", buffer_dir.name, re.IGNORECASE)
            buffer_key = m_buf.group(1) if m_buf else buffer_dir.name
            
            if buffer_key not in FILTER_BUFFERS:
                continue
            
            stats_log = buffer_dir / "stats.log"
            if not stats_log.exists():
                continue

            with open(stats_log, "r") as f:
                content = f.read()
                # Find all latencies matching "S: [number]"
                latencies_ns = STATS_LATENCY_RE.findall(content)
                
                if latencies_ns:
                    # Convert to float and ms
                    latencies_ms = [int(val) / 1_000_000.0 for val in latencies_ns]
                    x_values = range(len(latencies_ms))
                    
                    # Local change: enforce no markers for this specific script
                    style = line_styles.get(buffer_key, {"label": buffer_key}).copy()
                    style['marker'] = None 
                    
                    ax.plot(x_values, latencies_ms, **style)
                    plotted = True

    if not plotted:
        plt.close(fig)
        return

    ax.set_xlabel("range query number")
    ax.set_ylabel("latency (ms)")
    ax.set_xlim(0, rq_count_found)

    # Applying specified ticks
    apply_axis_settings(ax, 
                        x_ticks=[0, 250, 500, 750, 1000])
                        # y_ticks=[0, 50, 100, 150, 200, 250]) # Auto-scaling requested
                        
    save_plot_pdf(fig, f"fig1_per_query_latency_S_{target_selectivity}")
    plt.close(fig)

def main():
    if not EXP_DIR.exists():
        print(f"Error: Experiment directory {EXP_DIR} not found.")
        return

    target_selectivities = [0.1, 0.01, 0.001, 0.0001, 0.00001, 0.000001]
    for s in target_selectivities:
        plot_per_query_latency(EXP_DIR, target_selectivity=s)

if __name__ == "__main__":
    main()