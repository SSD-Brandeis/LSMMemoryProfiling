import matplotlib
matplotlib.use("Agg")

from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.ticker as ticker
import math
import matplotlib.pyplot as plt
from math import ceil

from plot import *

ROLLING_AVERAGE_WINDOW = 100

try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    print("⚠️  __file__ not defined, using Path.cwd() as fallback.")
    CURR_DIR = Path.cwd()

STATS_DIR = EXP_DIR / "write_stall_data"

LOWPRI_TRUE_PATTERN = r"write_stall_vectorrep-lowpri_true-.*"
LOWPRI_TRUE_DIRS = [
    d
    for d in STATS_DIR.iterdir()
    if d.is_dir() and re.match(LOWPRI_TRUE_PATTERN, d.name)
]
if not LOWPRI_TRUE_DIRS:
    raise FileNotFoundError(f"Could not find 'lowpri_true' dir in {STATS_DIR}")
LOWPRI_TRUE_DIR = LOWPRI_TRUE_DIRS[0]

LOWPRI_FALSE_PATTERN = r"write_stall_vectorrep-lowpri_false-.*"
LOWPRI_FALSE_DIRS = [
    d
    for d in STATS_DIR.iterdir()
    if d.is_dir() and re.match(LOWPRI_FALSE_PATTERN, d.name)
]
if not LOWPRI_FALSE_DIRS:
    raise FileNotFoundError(f"Could not find 'lowpri_false' dir in {STATS_DIR}")
LOWPRI_FALSE_DIR = LOWPRI_FALSE_DIRS[0]

EXPERIMENT_DIRS = {"true": LOWPRI_TRUE_DIR, "false": LOWPRI_FALSE_DIR}

PLOTS_DIR = CURR_DIR / "paper_plot" / "write_stall_vectorrepcombined_plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


BUFFERS_TO_PLOT = [
    "Vector-dynamic",
    "Vector-preallocated",
]
FIGSIZE = (5, 3.6)
NUM_RUNS = 3
LATENCY_RE = re.compile(r"VectorRep:\s*(\d+),\s*")


def parse_run_log(file_path: Path):
    latencies = []
    if not file_path.exists():
        print(f"Warning: Log file not found: {file_path}")
        return latencies
    try:
        with open(file_path, "r") as f:
            content = f.read()
            matches = LATENCY_RE.findall(content)
            latencies = [int(m) for m in matches]
    except Exception as e:
        print(f"Error reading or parsing {file_path}: {e}")
    return latencies


def save_plot_legend(handles, labels, base_output_path):
    if not handles:
        print("Warning: No legend handles found. Skipping legend save.")
        return

    ncol = ceil(len(handles) / 2)
    
    legend_fig = plt.figure(figsize=(0.1, 0.1))

    leg = legend_fig.legend(
        handles,
        labels,
        loc="center",
        ncol=ncol,
        frameon=False,
        fontsize=plt.rcParams["font.size"],
        borderpad=0.1,
        columnspacing=0.8,
        handletextpad=0.5,
    )
    plt.axis("off")

    for ext in ["pdf"]:
        legend_output_path = base_output_path.with_name(
            f"{base_output_path.name}_legend.{ext}"
        )
        legend_fig.savefig(
            legend_output_path, 
            bbox_inches="tight", 
            pad_inches=0.01,
            bbox_extra_artists=[leg]
        )
        print(f"[saved] {legend_output_path.name}")
    plt.close(legend_fig)


DECIMALS_RE = re.compile(r"\.(\d+)")

def save_plot_caption(caption_text, base_output_path):
    if not caption_text:
        return

    has_decimals = DECIMALS_RE.search(caption_text)

    caption_fontsize = plt.rcParams["font.size"]
    if has_decimals:
        caption_fontsize *= 1

    title_fig = plt.figure(figsize=(0.1, 0.1))

    txt = title_fig.text(
        0,
        0,
        caption_text,
        ha="left",
        va="bottom",
        fontsize=caption_fontsize,
    )

    plt.axis("off")

    for ext in ["pdf"]:
        caption_output_path = base_output_path.with_name(
            f"{base_output_path.name}_caption.{ext}"
        )
        title_fig.savefig(
            caption_output_path, 
            bbox_inches="tight", 
            pad_inches=0.01,
            bbox_extra_artists=[txt]
        )
        print(f"[saved] {caption_output_path.name}")
    plt.close(title_fig)


def save_individual_legend(handle, label, base_output_path):
    legend_fig = plt.figure(figsize=(0.1, 0.1))

    leg = legend_fig.legend(
        [handle],
        [label],
        loc="center",
        ncol=1,
        frameon=False,
        fontsize=plt.rcParams["font.size"],
        borderpad=0.1, 
        handletextpad=0.5,
    )
    plt.axis("off")

    for ext in ["pdf"]:
        legend_output_path = base_output_path.with_suffix(f".{ext}")
        legend_fig.savefig(
            legend_output_path, 
            bbox_inches="tight", 
            pad_inches=0.01,
            bbox_extra_artists=[leg]
        )
        print(f"[saved] {legend_output_path.name}")
    plt.close(legend_fig)



def plot_write_stall_latency():
    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.15)

    lowpri_settings = ["true", "false"]

    color_map = {
        "Vector-dynamic": line_styles.get("vectorrep", {}).get(
            "color", "#006d2c"
        ),
        "Vector-preallocated": line_styles.get("skiplist", {}).get(
            "color", "#6a3d9a"
        ),
    }

    linestyle_map = {
        "true": "-",
        "false": ":",
    }

    label_map = {
        ("true", "Vector-dynamic"): "dynamic vector (priority compactions)",
        ("true", "Vector-preallocated"): "static vector (priority compactions)",
        ("false", "Vector-dynamic"): "dynamic vector (priority writes)",
        ("false", "Vector-preallocated"): "static vector (priority writes)",
    }
 
    all_max_y = []
 
    for lowpri in lowpri_settings:
        for buffer_type in BUFFERS_TO_PLOT:

            print(f"\n--- Processing: lowpri={lowpri}, buffer={buffer_type} ---")

            base_experiment_dir = EXPERIMENT_DIRS.get(lowpri)
            if not base_experiment_dir:
                print(
                    f"Warning: No base directory found for lowpri={lowpri}, skipping."
                )
                continue

            buffer_dir = base_experiment_dir / buffer_type
            if not buffer_dir.is_dir():
                print(f"Warning: Directory not found: {buffer_dir}, skipping.")
                continue

            run_latencies = []
            for i in range(1, NUM_RUNS + 1):
                log_file = buffer_dir / f"run{i}.log"
                latencies = parse_run_log(log_file)
                if latencies:
                    run_latencies.append(latencies)
                else:
                    print(f"Warning: No latencies found in {log_file}")

            if not run_latencies:
                print(f"Warning: No valid latency data found, skipping plot.")
                continue

            min_len = min(len(l) for l in run_latencies) if run_latencies else 0
            if min_len == 0:
                print(f"Warning: Minimum length is 0, skipping plot.")
                continue

            trimmed_latencies = [l[:min_len] for l in run_latencies]
            avg_latencies = np.mean(trimmed_latencies, axis=0)

            if ROLLING_AVERAGE_WINDOW > 1 and min_len > ROLLING_AVERAGE_WINDOW:
                print(f"Applying rolling average with window {ROLLING_AVERAGE_WINDOW}")
                avg_latencies_series = pd.Series(avg_latencies)
                plot_latencies_series = avg_latencies_series.rolling(
                    window=ROLLING_AVERAGE_WINDOW
                ).mean()
                plot_latencies = plot_latencies_series.dropna().values
                start_x = ROLLING_AVERAGE_WINDOW
                x_ops = np.arange(start_x, min_len + 1)
            else:
                if ROLLING_AVERAGE_WINDOW > 1:
                    print(
                        f"Warning: Data length ({min_len}) less than window ({ROLLING_AVERAGE_WINDOW}). Plotting raw data."
                    )
                plot_latencies = avg_latencies
                x_ops = np.arange(1, min_len + 1)

            if len(plot_latencies) > 0:
                all_max_y.append(np.max(plot_latencies))

            style = line_styles.get("vectorrep", {}).copy()
            if not style:
                print(
                    f"Warning: Style 'vectorrep' not found in style.py. Using default."
                )

            style["color"] = color_map[buffer_type]
            style["linestyle"] = linestyle_map[lowpri]

            if style["linestyle"] == "-":
                style["alpha"] = 0.7
            else:
                style["alpha"] = 1.0
 
            style["label"] = label_map.get((lowpri, buffer_type), "Unknown Label")

            if ROLLING_AVERAGE_WINDOW > 1:
                style["marker"] = ""

            (line,) = ax.plot(x_ops, plot_latencies, **style)
            print(f"Plotted {style['label']} with {len(plot_latencies)} data points.")

            safe_label = re.sub(r"[\s\(\)]+", "_", style["label"]).strip("_")
            legend_name = f"legend_{safe_label}"
            legend_out_path = PLOTS_DIR / legend_name
            save_individual_legend(line, style["label"], legend_out_path)

    ax.set_xlabel("Operation Count")
    ax.set_ylabel("Latency (ns)", labelpad=0)
    
    ax.tick_params(axis='x', labelsize=plt.rcParams["font.size"])
    ax.tick_params(axis='y', labelsize=plt.rcParams["font.size"])

    max_y = max([y for y in all_max_y if not math.isnan(y)] + [1])
    if not all_max_y:
        max_y = 1
    else:
        max_y = np.max(all_max_y)

    if max_y > 999:
        y_power = math.floor(math.log10(max_y)) if max_y > 0 else 0

        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.1f}")
        )

        ax.text(
            0.02,
            0.98,
            r"$\times 10^{{{}}}$".format(y_power),
            transform=ax.transAxes,
            fontsize=plt.rcParams["font.size"],
            ha="left",
            va="top",
        )

    base_out = PLOTS_DIR / "write_stall_latency_combined"
    caption_text = "Vector Write Stall Latency (Combined)"

    for ext in ["pdf"]:
        output_path = base_out.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {output_path.name}")
    plt.close(fig)

    save_plot_caption(caption_text, base_out)



if __name__ == "__main__":
    plot_write_stall_latency()