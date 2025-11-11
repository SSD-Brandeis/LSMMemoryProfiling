import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from pathlib import Path
from math import ceil
import re
import sys
import numpy as np
import math

from plot import *

try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    print("⚠️  __file__ not defined, using Path.cwd() as fallback.")
    CURR_DIR = Path.cwd()


EXPERIMENT_PATH = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/ondisk/ondisk_sanitycheck-lowpri_false-I250000-U0-Q0-S0-Y0-T2-P1024-B32-E128")

PLOTS_DIR = CURR_DIR / "paper_plot" / "ondisk_plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

NUM_RUNS = 3
FIGSIZE = (8, 4)
USE_LOG_SCALE_TIME = False
USE_LOG_SCALE_BYTES = True

BUFFER_ABBREVIATIONS = {
    "vector dynamic": "VD",
    "skip list": "SL",
    "hash skip list": "HSL",
    "hash linked list": "HLL",
    "unsorted vector dynamic": "UVD",
    "always-sorted vector dynamic": "ASVD",
}


METRIC_STYLES = {
    "flush_count": {
        "label": "flush count",
        "color": "None",
        "edgecolor": "#1f78b4",
        "hatch": "////",
        "linewidth": 1
    },
    "compaction_count": {
        "label": "compaction count",
        "color": "None",
        "edgecolor": "#b22222",
        "hatch": "\\\\",
        "linewidth": 1
    },
    "avg_flush_time": {
        "label": "avg flush time",
        "color": "None",
        "edgecolor": "#1f78b4",
        "hatch": "////",
        "linewidth": 1
    },
    "avg_compaction_time": {
        "label": "avg compaction time",
        "color": "None",
        "edgecolor": "#b22222",
        "hatch": "\\\\",
        "linewidth": 1
    },
    "compact_read_bytes": {
        "label": "compact read",
        "color": "None",
        "edgecolor": "#006d2c",
        "hatch": "----",
        "linewidth": 1
    },
    "compact_write_bytes": {
        "label": "compact write",
        "color": "None",
        "edgecolor": "#6a3d9a",
        "hatch": "++++",
        "linewidth": 1
    },
    "flush_write_bytes": {
        "label": "flush write",
        "color": "None",
        "edgecolor": "#ff7f00",
        "hatch": "xxxx",
        "linewidth": 1
    }
}

STATS_RE = re.compile(r"rocksdb\.(?P<metric>[\w\.]+):\s*(?P<value>\d+)")
COUNT_SUM_RE = re.compile(
    r"rocksdb\.(?P<metric>[\w\.]+)\s*.*"
    r"COUNT\s*:\s*(?P<count>\d+)\s*"
    r"SUM\s*:\s*(?P<sum>\d+)"
)

def parse_workload_log(file_path: Path):
    metrics = {
        "flush_count": 0, "compaction_count": 0,
        "avg_flush_time": 0, "avg_compaction_time": 0,
        "compact_read_bytes": 0, "compact_write_bytes": 0, "flush_write_bytes": 0
    }
    
    if not file_path.exists():
        print(f"Warning: Log file not found: {file_path}")
        return metrics

    with open(file_path, 'r') as f:
        content = f.read()

    for match in COUNT_SUM_RE.finditer(content):
        metric_name = match.group("metric")
        count = int(match.group("count"))
        total_sum = int(match.group("sum"))
        
        if metric_name == "db.flush.micros":
            metrics["flush_count"] = count
            if count > 0:
                metrics["avg_flush_time"] = total_sum / count
        elif metric_name == "compaction.times.micros":
            metrics["compaction_count"] = count
            if count > 0:
                metrics["avg_compaction_time"] = total_sum / count

    for match in STATS_RE.finditer(content):
        metric_name = match.group("metric")
        value = int(match.group("value"))
        
        if metric_name == "compact.read.bytes":
            metrics["compact_read_bytes"] = value
        elif metric_name == "compact.write.bytes":
            metrics["compact_write_bytes"] = value
        elif metric_name == "flush.write.bytes":
            metrics["flush_write_bytes"] = value
            
    return metrics

def collect_data(base_path: Path):
    all_data = []
    buffer_dirs = [d for d in base_path.iterdir() if d.is_dir() and d.name != "__pycache__"]
    
    if not buffer_dirs:
        print(f"Error: No buffer subdirectories found in {base_path}")
        return pd.DataFrame()

    sorted_style_keys = sorted(bar_styles.keys(), key=len, reverse=True)

    for buffer_dir in buffer_dirs:
        buffer_name = buffer_dir.name
        buffer_key = None
        
        if buffer_name in bar_styles:
            buffer_key = buffer_name
        
        if buffer_key is None:
            for key in sorted_style_keys:
                if buffer_name.startswith(key):
                    buffer_key = key
                    break
        
        if buffer_key is None:
            for key in sorted_style_keys:
                if buffer_name.lower().startswith(key.lower()):
                    buffer_key = key
                    break

        if buffer_key is None:
            print(f"Warning: No matching style key found for folder '{buffer_name}'. Skipping.")
            continue
        
        style_dict = bar_styles.get(buffer_key, {})
        buffer_label = style_dict.get("label", buffer_key).lower()
        
        buffer_abbr = BUFFER_ABBREVIATIONS.get(buffer_label)
        
        if buffer_abbr is None:
            print(f"Info: Skipping buffer '{buffer_label}' as it is not in BUFFER_ABBREVIATIONS map.")
            continue
        
        run_metrics = []
        for i in range(1, NUM_RUNS + 1):
            log_file = buffer_dir / f"workload{i}.log"
            run_metrics.append(parse_workload_log(log_file))
        
        if run_metrics:
            df_runs = pd.DataFrame(run_metrics)
            avg_metrics = df_runs.mean().to_dict()
            avg_metrics["buffer"] = buffer_label
            avg_metrics["buffer_abbr"] = buffer_abbr
            all_data.append(avg_metrics)
        else:
            print(f"Warning: No data found for buffer {buffer_name}")

    return pd.DataFrame(all_data)

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

def plot_grouped_barchart(df, metrics, title, y_label, file_base, use_log_scale=False):
    if df.empty:
        print(f"Skipping plot '{title}' due to empty data.")
        return

    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.15, right=0.98, top=0.95, bottom=0.25)
    
    num_buffers = len(df)
    num_metrics = len(metrics)
    
    bar_width = 0.8 / num_metrics
    group_width = 0.8
    buffer_x_positions = np.arange(num_buffers)

    for i, metric in enumerate(metrics):
        metric_style = METRIC_STYLES[metric]
        bar_x_offsets = (i - (num_metrics - 1) / 2) * bar_width
        values = df[metric]
        
        if use_log_scale:
            values = values.replace(0, 1)
            
        ax.bar(buffer_x_positions + bar_x_offsets, values, width=bar_width, **metric_style)

    ax.set_xticks(buffer_x_positions)
    ax.set_xticklabels(df["buffer_abbr"], rotation=90)
    ax.set_xlabel("buffer type")
    
    ax.set_ylabel(y_label)
    
    ax.tick_params(axis='x', labelsize=plt.rcParams["font.size"])
    ax.tick_params(axis='y', labelsize=plt.rcParams["font.size"])
    
    ax.grid(True, linestyle="--", alpha=0.3, axis="y")

    if use_log_scale:
        ax.set_yscale('log')
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
        
        if y_label == "bytes":
            ax.set_ylim(1e6, 1e9)
        else:
            ax.set_ylim(bottom=1) 
    else:
        max_y = 0
        if y_label == "average time (micros)":
            all_values = pd.concat([df[m] for m in metrics])
            max_y = all_values.max()
            
            if max_y > 999:
                y_power = math.floor(math.log10(max_y))
                ax.yaxis.set_major_formatter(
                    mticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.1f}")
                )
                ax.text(
                    0.02, # Changed from 0.05 to 0.02
                    0.95, 
                    r"$\times 10^{{{}}}$".format(y_power),
                    transform=ax.transAxes,
                    fontsize=plt.rcParams["font.size"],
                    ha="left",
                    va="top"
                )
                ax.set_ylim(bottom=0, top=max_y * 1.05)
            else:
                ax.set_ylim(bottom=0)
        else:
             ax.set_ylim(bottom=0)

    for ext in ["pdf"]:
        output_path = file_base.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        print(f"[saved] {output_path.name}")
    plt.close(fig)
    
    metric_handles = []
    metric_labels = []
    for metric in metrics:
        style = METRIC_STYLES[metric]
        legend_style = {
            'facecolor': 'white',
            'edgecolor': style.get('edgecolor'),
            'hatch': style.get('hatch'),
            'linewidth': style.get('linewidth')
        }
        metric_handles.append(plt.Rectangle((0, 0), 1, 1, **legend_style))
        metric_labels.append(style.get('label'))

    legend_filename = file_base.with_name(f"{file_base.name}_legend")
    save_plot_legend(metric_handles, metric_labels, legend_filename)
    
    caption_filename = file_base.with_name(f"{file_base.name}_caption")
    save_plot_caption(title, caption_filename)

def main():
    print(f"--- Starting On-Disk Plot Generation ---")
    print(f"Reading data from: {EXPERIMENT_PATH}")
    
    data_df = collect_data(EXPERIMENT_PATH)
    
    if data_df.empty:
        print("No data collected. Exiting.")
        return
        
    sorter = list(BUFFER_ABBREVIATIONS.keys())
    data_df['buffer'] = pd.Categorical(data_df['buffer'], categories=sorter, ordered=True)
    data_df = data_df.sort_values(by="buffer").reset_index(drop=True)
    
    data_df = data_df[data_df['buffer'].notna()].reset_index(drop=True)
    
    if data_df.empty:
        print("No data left after filtering for known buffers. Exiting.")
        return

    plot_grouped_barchart(
        df=data_df,
        metrics=["flush_count", "compaction_count"],
        title="flush and compaction counts",
        y_label="count",
        file_base=PLOTS_DIR / "ondisk_counts",
        use_log_scale=False
    )
    
    plot_grouped_barchart(
        df=data_df,
        metrics=["avg_flush_time", "avg_compaction_time"],
        title="average flush and compaction time",
        y_label="average time (micros)",
        file_base=PLOTS_DIR / "ondisk_avg_time",
        use_log_scale=USE_LOG_SCALE_TIME
    )
    
    plot_grouped_barchart(
        df=data_df,
        metrics=["compact_read_bytes", "compact_write_bytes", "flush_write_bytes"],
        title="write and read amplification bytes",
        y_label="bytes",
        file_base=PLOTS_DIR / "ondisk_bytes",
        use_log_scale=USE_LOG_SCALE_BYTES
    )

    print("\nGenerating common buffer legend...")
    
    legend_lines = []
    plotted_buffers = data_df["buffer"].unique()
    
    for full_label in BUFFER_ABBREVIATIONS.keys():
        if full_label not in plotted_buffers:
            continue
            
        abbr = BUFFER_ABBREVIATIONS[full_label]
        legend_lines.append(f"{abbr}: {full_label}")
        
    num_items = len(legend_lines)
    items_per_line = ceil(num_items / 2)
    
    line1_items = legend_lines[:items_per_line]
    line2_items = legend_lines[items_per_line:]
    
    padding = "    "
    line1_text = padding.join(line1_items)
    line2_text = padding.join(line2_items)
    
    save_plot_caption(line1_text, PLOTS_DIR / "ondisk_buffer_legend_line1")
    save_plot_caption(line2_text, PLOTS_DIR / "ondisk_buffer_legend_line2")
    
    caption_text = "compaction/flush data read/write"
    save_plot_caption(caption_text, PLOTS_DIR / "ondisk_caption")
        
    # --- ADDED FOR NEW CAPTION ---
    save_plot_caption("K=32 V=96 T=2 I=250k", PLOTS_DIR / "workload")
    # --- END ---

    print(f"--- On-Disk Plot Generation Complete ---")

if __name__ == "__main__":
    main()