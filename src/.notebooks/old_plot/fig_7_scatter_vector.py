import matplotlib
matplotlib.use("Agg")

import os
import re
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.font_manager as font_manager
from pathlib import Path
from math import ceil

# --- ROBUST PATH RESOLUTION ---
# Path to this script: /Users/cba/Desktop/LSMMemoryBuffer/notebooks/fig_7_scatter_vector.py
try:
    here = Path(__file__).resolve().parent
except NameError:
    here = Path.cwd()

# This goes up one level to: /Users/cba/Desktop/LSMMemoryBuffer
repo_root = here.parent 

# Font configuration
font_path = here / "LinLibertine_Mah.ttf"
if font_path.exists():
    prop = font_manager.FontProperties(fname=str(font_path))
    plt.rcParams["font.family"] = prop.get_name()
else:
    plt.rcParams["font.family"] = "DejaVu Sans"

plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "bold"
plt.rcParams["font.size"] = 20

# Updated Data Discovery: Look specifically inside the project folder first
env_dir = os.environ.get("LSMMB_STATS_DIR")
candidates = [
    Path(env_dir) if env_dir else None,
    repo_root / "data",               # /Users/cba/Desktop/LSMMemoryBuffer/data
    repo_root / ".result",
    repo_root / ".vstats",
    Path("/Users/cba/Desktop/data"),  # Fallback
]
EXP_DIR = next((p for p in candidates if p and p.exists()), repo_root / "data")

# --- PLOT CONFIGURATION ---
ROLLING_AVERAGE_WINDOW = 100
Y_AXIS_MAX = 700
STATS_DIR = EXP_DIR / "write_stall_data"
PLOTS_DIR = here / "paper_plot" / "march7_scatter_vec"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

BUFFERS_TO_PLOT = ["Vector-dynamic", "Vector-preallocated"]
NUM_RUNS = 3
FIGSIZE = (5, 3.6)
LATENCY_RE = re.compile(r"VectorRep:\s*(\d+),\s*")

def parse_run_log(file_path: Path):
    latencies = []
    if not file_path.exists():
        return latencies
    try:
        with open(file_path, "r") as f:
            content = f.read()
            matches = LATENCY_RE.findall(content)
            latencies = [int(m) for m in matches]
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
    return latencies

def save_individual_legend(handle, label, base_output_path):
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend(
        [handle], [label], loc="center", frameon=False,
        fontsize=plt.rcParams["font.size"], borderpad=0.1, handletextpad=0.5,
    )
    plt.axis("off")
    legend_fig.savefig(base_output_path.with_suffix(".pdf"), bbox_inches="tight", pad_inches=0.01)
    plt.close(legend_fig)

def plot_write_stall_latency():
    print(f"🔍 Searching for data in: {STATS_DIR}")
    
    if not STATS_DIR.exists():
        print(f"❌ CRITICAL ERROR: The directory '{STATS_DIR}' does not exist.")
        print("Please ensure your data is located at the path above.")
        return

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.15)

    lowpri_settings = ["true", "false"]
    
    color_map = {
        "Vector-dynamic": "#006d2c",
        "Vector-preallocated": "#6a3d9a",
    }
    marker_map = {
        "true": "o",  # Priority Compactions
        "false": "x", # Priority Writes
    }
    label_map = {
        ("true", "Vector-dynamic"): "dynamic vector (priority compactions)",
        ("true", "Vector-preallocated"): "static vector (priority compactions)",
        ("false", "Vector-dynamic"): "dynamic vector (priority writes)",
        ("false", "Vector-preallocated"): "static vector (priority writes)",
    }

    all_max_y = []

    for lowpri in lowpri_settings:
        pattern = f"write_stall_vectorrep-lowpri_{lowpri}-.*"
        matching_dirs = [d for d in STATS_DIR.iterdir() if d.is_dir() and re.match(pattern, d.name)]
        
        if not matching_dirs:
            print(f"⚠️ No matching directory for lowpri={lowpri}")
            continue
        
        target_dir = matching_dirs[0]
        print(f"✅ Found directory: {target_dir.name}")

        for buffer_type in BUFFERS_TO_PLOT:
            buffer_dir = target_dir / buffer_type
            if not buffer_dir.is_dir():
                continue

            run_latencies = []
            for i in range(1, NUM_RUNS + 1):
                latencies = parse_run_log(buffer_dir / f"run{i}.log")
                if latencies:
                    run_latencies.append(latencies)

            if not run_latencies:
                continue

            min_len = min(len(l) for l in run_latencies)
            avg_latencies = np.mean([l[:min_len] for l in run_latencies], axis=0)

            if ROLLING_AVERAGE_WINDOW > 1 and min_len > ROLLING_AVERAGE_WINDOW:
                series = pd.Series(avg_latencies).rolling(window=ROLLING_AVERAGE_WINDOW).mean().dropna()
                plot_latencies = series.values
                x_ops = np.arange(ROLLING_AVERAGE_WINDOW, min_len + 1)
            else:
                plot_latencies = avg_latencies
                x_ops = np.arange(1, min_len + 1)

            all_max_y.append(np.max(plot_latencies))

            # --- SCATTER PLOT SETTINGS ---
            scatter_style = {
                "color": color_map.get(buffer_type, "#333333"),
                "marker": marker_map[lowpri],
                "alpha": 0.6 if lowpri == "true" else 0.8,
                "s": 12, # Size of points
                "label": label_map.get((lowpri, buffer_type), "Unknown")
            }

            scatter_handle = ax.scatter(x_ops, plot_latencies, **scatter_style)

            # Annotation logic for values exceeding Y_AXIS_MAX
            if Y_AXIS_MAX is not None and len(x_ops) > 0:
                last_annotated_x = -np.inf
                x_range = np.max(x_ops) - np.min(x_ops)
                min_x_gap = x_range * 0.12

                for i in range(len(plot_latencies)):
                    y = plot_latencies[i]
                    if y > Y_AXIS_MAX:
                        x = x_ops[i]
                        if x > last_annotated_x + min_x_gap:
                            ax.text(x + (x_range * 0.02), Y_AXIS_MAX * 0.97, f"{int(y)}",
                                    ha="left", va="top", fontsize=plt.rcParams["font.size"] * 0.6,
                                    color=scatter_style["color"], fontweight="bold")
                            last_annotated_x = x

            safe_label = re.sub(r"[\s\(\)]+", "_", scatter_style["label"]).strip("_")
            save_individual_legend(scatter_handle, scatter_style["label"], PLOTS_DIR / f"legend_{safe_label}")

    ax.set_xlabel("Operation Count")
    ax.set_ylabel("Latency (ns)", labelpad=0)

    if all_max_y:
        max_val = max(all_max_y)
        if max_val > 999:
            y_power = math.floor(math.log10(max_val))
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.1f}"))
            ax.text(0.02, 0.98, r"$\times 10^{{{}}}$".format(y_power), transform=ax.transAxes,
                    fontsize=plt.rcParams["font.size"], ha="left", va="top")

    if Y_AXIS_MAX:
        ax.set_ylim(0, Y_AXIS_MAX)
    else:
        ax.set_ylim(bottom=0)

    save_path = PLOTS_DIR / "write_stall_latency_combined.pdf"
    fig.savefig(save_path, bbox_inches="tight", pad_inches=0.02)
    print(f"✨ Successfully saved scatter plot to: {save_path}")
    plt.close(fig)

if __name__ == "__main__":
    plot_write_stall_latency()