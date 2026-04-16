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
import csv
import numpy as np

# Enforcement of font settings and abort logic as per saved preferences
try:
    from plot import *
except ImportError:
    print("Error: 'plot.py' not found. Aborting program to prevent unauthorized fallback styles.")
    sys.exit(1)

# Set up script name and directory paths
try:
    CURR_DIR = Path(__file__).resolve().parent
    SCRIPT_STEM = Path(__file__).stem
except NameError:
    CURR_DIR = Path.cwd()
    SCRIPT_STEM = "default_script_name"

# Logger to capture analysis results in a log file
class Logger(object):
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

USE_LOG_SCALE = True
NS_TO_S = 1e-9

YLIM_LOG_THR_RAWOP = (1e0, 1e9)
YLIM_LOG_LAT_RAWOP = (1e0, 1e9)
YLIM_LOG_THR_INTERLEAVE = (1e0, 1e9)
YLIM_LOG_LAT_INTERLEAVE = (1e0, 1e9)
YLIM_LOG_LAT_WORKLOAD = (1e0, 1e9)

WORKLOAD_INSERTS = 740000
WORKLOAD_PQ = 100000  # 50k EmptyPQ + 50k nonEmptyPQ
WORKLOAD_RQ = 1000

EXP_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data")
#old setting
# RAWOP_DIR = EXP_DIR / "filter_result_mar15_multiphase_rawop"
# INTERLEAVE_DIR = EXP_DIR / "filter_result_multiphase_interleave_inmemory_314"

#new setting

RAWOP_DIR = EXP_DIR / "multiphase_sequential_inmem_newsetting_mar25"
INTERLEAVE_DIR = EXP_DIR / "multiphase_interleave_inmem_newsetting_mar25"

# Save in current project directory / paper_plot / script_name
PLOTS_DIR = CURR_DIR / "paper_plot" / SCRIPT_STEM
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "alwayssortedVector-preallocated",
    "skiplist",
    "simple_skiplist",
    "hash_vector-X6-H100000",
    "hash_skip_list-X6-H100000",
    "hash_linked_list-X6-H100000",
]

TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")
GETTIME_RE = re.compile(r"^GetTime:\s*(\d+)")

ROCKS_RE = re.compile(
    r"rocksdb\.db\.(?P<op>get|write)\.micros\s+P50\s+:\s+(?P<p50>[\d.e+-]+)\s+P95\s+:\s+(?P<p95>[\d.e+-]+)\s+P99\s+:\s+(?P<p99>[\d.e+-]+)\s+P100\s+:\s+(?P<p100>[\d.e+-]+)",
    re.IGNORECASE
)

def dump_to_csv(filename, dataframe):
    """Saves dataframe to CSV in the plots directory."""
    filepath = PLOTS_DIR / filename
    dataframe.to_csv(filepath, index=False)
    print(f"[DATA DUMP] Saved stats to {filepath.resolve()}")

def perform_pairwise_analysis(df, metrics, label_suffix):
    """Performs modular pairwise factor (x) analysis."""
    print(f"\n--- Pairwise Analysis for {label_suffix} ---")
    buffers = df["buffer"].tolist()
    
    for metric in metrics:
        print(f"\nMetric: {metric}")
        is_throughput = "thr" in metric
        
        for i in range(len(buffers)):
            for j in range(len(buffers)):
                if i == j: continue
                
                name_i, name_j = buffers[i], buffers[j]
                val_i = df.loc[df["buffer"] == name_i, metric].values[0]
                val_j = df.loc[df["buffer"] == name_j, metric].values[0]
                
                if val_i == 0 or val_j == 0: continue

                if is_throughput:
                    if val_i > val_j:
                        factor = val_i / val_j
                        print(f"{name_i} vs {name_j}: {factor:.2f}x higher throughput")
                else:
                    if val_i < val_j:
                        factor = val_j / val_i
                        print(f"{name_i} vs {name_j}: {factor:.2f}x lower value/latency")

def safe_div(numer, denom):
    if numer and denom:
        return numer / denom
    return 0

def bar_style(buf_name):
    global bar_styles
    if "bar_styles" not in globals():
        bar_styles = {}
    default_style = {"color": "None", "edgecolor": "black", "hatch": ""}
    return bar_styles.get(buf_name, default_style).copy()

def parse_workload_log(log_path):
    exec_out = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "Workload": 0}
    rocks_stats = {}
    if not log_path.exists():
        return exec_out, rocks_stats
    content = log_path.read_text()
    for line in content.splitlines():
        line_clean = line.strip()
        m_time = TIME_RE.match(line_clean)
        if m_time:
            kind = m_time.group(1)
            ns = int(m_time.group(2))
            exec_out[kind] = ns
        else:
            m_workload = WORKLOAD_TIME_RE.match(line_clean)
            if m_workload:
                exec_out["Workload"] = int(m_workload.group(1))
        m_rocks = ROCKS_RE.search(line_clean)
        if m_rocks:
            op = m_rocks.group('op').lower()
            rocks_stats[f"{op}_p50"] = float(m_rocks.group('p50'))
            rocks_stats[f"{op}_p95"] = float(m_rocks.group('p95'))
            rocks_stats[f"{op}_p99"] = float(m_rocks.group('p99'))
            rocks_stats[f"{op}_p100"] = float(m_rocks.group('p100'))
    return exec_out, rocks_stats

def collect_records(data_dir):
    records = []
    if not data_dir.exists(): return pd.DataFrame()
    for log_path in data_dir.rglob("workload_run.log"):
        buffer_name = log_path.parent.name
        m_buf = re.match(r"^buffer-\d+-(.*)", buffer_name, re.IGNORECASE)
        buffer_key = m_buf.group(1) if m_buf else buffer_name
        
        exec_ns, rocks_stats = parse_workload_log(log_path)
        t_ins, t_pq, t_rq = exec_ns["Inserts"]*NS_TO_S, exec_ns["PointQuery"]*NS_TO_S, exec_ns["RangeQuery"]*NS_TO_S
        
        record = {
            "buffer": buffer_key,
            "thr_insert": safe_div(WORKLOAD_INSERTS, t_ins),
            "thr_pq": safe_div(WORKLOAD_PQ, t_pq),
            "thr_rq": safe_div(WORKLOAD_RQ, t_rq),
            "lat_insert": safe_div(exec_ns["Inserts"], WORKLOAD_INSERTS),
            "lat_pq": safe_div(exec_ns["PointQuery"], WORKLOAD_PQ),
            "lat_rq": safe_div(exec_ns["RangeQuery"], WORKLOAD_RQ),
            "lat_workload": safe_div(exec_ns["Workload"], (WORKLOAD_INSERTS + WORKLOAD_PQ + WORKLOAD_RQ)),
            "time_insert": t_ins, "time_pq": t_pq, "time_rq": t_rq,
            "total_workload_time_s": exec_ns["Workload"] * NS_TO_S,
        }
        record.update(rocks_stats)
        records.append(record)
    return pd.DataFrame(records)

def apply_axis_style(ax, y_limit_tuple, is_latency_plot, formatter=None):
    if USE_LOG_SCALE:
        if y_limit_tuple: ax.set_ylim(*y_limit_tuple)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        if y_limit_tuple: ax.set_ylim(*y_limit_tuple)
        ax.ticklabel_format(style="plain", axis="y", useOffset=False)
    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

def save_plot_legend(handles, labels, base_output_path):
    if not handles: return
    ncol = ceil(len(handles) / 2)
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend(handles, labels, loc="center", ncol=ncol, frameon=False, fontsize=plt.rcParams["font.size"])
    plt.axis("off")
    legend_output_path = base_output_path.with_name(f"{base_output_path.name}_legend.pdf")
    legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0.01)
    plt.close(legend_fig)

def plot_combined_barchart(df, metric_cols, y_label, x_labels, filename_base, buffers, y_limit, is_latency_plot):
    fig, ax = plt.subplots(1, 1, figsize=(5, 3.6))
    if USE_LOG_SCALE: ax.set_yscale("log", base=10)
    num_groups, num_buffers = len(metric_cols), len(buffers)
    group_width, bar_width_ratio = 0.9, 0.9
    width_of_one_bar_slot = group_width / num_buffers
    bar_width = width_of_one_bar_slot * bar_width_ratio
    group_x_positions = np.arange(num_groups)
    log_bottom = y_limit[0] if (USE_LOG_SCALE and y_limit) else 1e-4

    for group_idx, metric_col in enumerate(metric_cols):
        group_start_x = group_x_positions[group_idx] - (group_width / 2) + (width_of_one_bar_slot / 2)
        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot
            try:
                value = df.loc[df["buffer"] == buf_name, metric_col].iat[0]
            except:
                value = log_bottom
            if USE_LOG_SCALE and value <= log_bottom: value = log_bottom
            ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(group_x_positions)
    ax.set_xticklabels(x_labels)
    apply_axis_style(ax, y_limit, is_latency_plot)
    ax.set_ylabel(y_label)
    fig.savefig(filename_base.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)

def plot_workload_latency_comparison(df_seq, df_interleave, buffers, filename_base, y_limit, is_latency_plot, metric="lat_workload", y_label="total wl latency (ns/op)"):
    fig, ax = plt.subplots(figsize=(5, 3.6))
    if USE_LOG_SCALE: ax.set_yscale("log", base=10)
    group_x_positions = np.arange(2)
    group_width, bar_width = 0.9, (0.9 / len(buffers)) * 0.9
    log_bottom = y_limit[0] if (USE_LOG_SCALE and y_limit) else 1e-4

    for group_idx, df in enumerate([df_seq, df_interleave]):
        if df is None: continue
        group_start_x = group_x_positions[group_idx] - (group_width / 2) + ((0.9/len(buffers)) / 2)
        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * (0.9/len(buffers))
            try:
                value = df.loc[df["buffer"] == buf_name, metric].iat[0]
            except:
                value = log_bottom
            if USE_LOG_SCALE and value <= log_bottom: value = log_bottom
            ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(group_x_positions)
    ax.set_xticklabels(["sequential", "interleaved"])
    apply_axis_style(ax, y_limit, is_latency_plot)
    ax.set_ylabel(y_label)
    fig.savefig(filename_base.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)

def process_and_plot(data_dir, file_suffix, y_limit_thr, y_limit_lat):
    raw_df = collect_records(data_dir)
    if raw_df.empty: return None, False
    grouped_df = raw_df.groupby("buffer", as_index=False).mean()
    if FILTER_BUFFERS:
        grouped_df = grouped_df[grouped_df["buffer"].isin(FILTER_BUFFERS)]
    if grouped_df.empty: return None, False

    # Dump data to CSV with clear naming
    dump_to_csv(f"{SCRIPT_STEM}_stats{file_suffix}.csv", grouped_df)

    # Modular Pairwise Analysis
    metrics_to_analyze = ["thr_insert", "thr_pq", "thr_rq", "lat_insert", "lat_pq", "lat_rq"]
    perform_pairwise_analysis(grouped_df, metrics_to_analyze, file_suffix)

    buffers_to_plot = [b for b in FILTER_BUFFERS if b in grouped_df["buffer"].values]
    
    # Combined plots
    plot_combined_barchart(grouped_df, ["thr_insert", "thr_pq", "thr_rq"], "throughput (ops/sec)", ["insert", "pq", "rq"], PLOTS_DIR / f"multi_thr_combined{file_suffix}", buffers_to_plot, y_limit_thr, False)
    plot_combined_barchart(grouped_df, ["lat_insert", "lat_pq", "lat_rq"], "mean latency (ns/op)", ["insert", "pq", "rq"], PLOTS_DIR / f"multi_lat_combined{file_suffix}", buffers_to_plot, y_limit_lat, True)
    
    return grouped_df, True

def get_formatted_buffer_label(buf_name):
    match = re.search(r"(hash_[^-]+)-X(\d+)-H(\d+)", buf_name, re.IGNORECASE)
    if match:
        h_val = int(match.group(3))
        h_str = f"{h_val // 1000}k" if h_val >= 1000 else str(h_val)
        return f"{match.group(1).replace('_', ' ')} (X={match.group(2)}, H={h_str})"
    return buf_name.lower()

def main():
    sys.stdout = Logger(PLOTS_DIR / f"{SCRIPT_STEM}_analysis.log")
    
    df_rawop, success_rawop = process_and_plot(RAWOP_DIR, "_rawop", YLIM_LOG_THR_RAWOP, YLIM_LOG_LAT_RAWOP)
    df_interleave, success_interleave = process_and_plot(INTERLEAVE_DIR, "_interleave", YLIM_LOG_THR_INTERLEAVE, YLIM_LOG_LAT_INTERLEAVE)

    if success_rawop and success_interleave:
        common_buffers = [b for b in FILTER_BUFFERS if b in set(df_rawop["buffer"]).intersection(set(df_interleave["buffer"]))]
        if common_buffers:
            plot_workload_latency_comparison(df_rawop, df_interleave, common_buffers, PLOTS_DIR / "multi_latency_workload_comparison", YLIM_LOG_LAT_WORKLOAD, True)

    # Consolidated Legend Generation
    all_legend_handles, all_legend_labels = [], []
    for b in FILTER_BUFFERS:
        style = bar_style(b)
        all_legend_handles.append(plt.Rectangle((0, 0), 1, 1, facecolor=style.get("facecolor"), edgecolor=style.get("edgecolor"), hatch=style.get("hatch")))
        all_legend_labels.append(get_formatted_buffer_label(b))
    
    save_plot_legend(all_legend_handles, all_legend_labels, PLOTS_DIR / "multi_throughput_legend")

    # Print full path of the directory containing all saved artifacts
    print(f"\n[INFO] All plots and logs saved to: {PLOTS_DIR.resolve()}")

if __name__ == "__main__":
    main()