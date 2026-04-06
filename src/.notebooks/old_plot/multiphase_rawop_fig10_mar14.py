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

# Enforcement of font settings and abort logic as per saved preferences
try:
    from plot import *
except ImportError:
    print("Error: 'plot.py' not found. Aborting program to prevent unauthorized fallback styles.")
    sys.exit(1)

try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

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

PLOTS_DIR = Path(
    "/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/multi_throughput_multiphase_326"
)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    # "Vector-dynamic",
    "vector-preallocated",
    # "UnsortedVector-dynamic",
    "unsortedvector-preallocated",
    # "AlwaysSortedVector-dynamic",
    "alwayssortedVector-preallocated",
    "skiplist",
    "simple_skiplist",
    # "linkedlist",
    # "hash_vector-X2-H1000",
    "hash_vector-X6-H100000",
    # "hash_skip_list-X2-H1000",
    "hash_skip_list-X6-H100000",
    # "hash_linked_list-X2-H1000",
    "hash_linked_list-X6-H100000",
]

TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")
GETTIME_RE = re.compile(r"^GetTime:\s*(\d+)")

# Robust regex to match RocksDB micros distribution lines in workload_run.log
ROCKS_RE = re.compile(
    r"rocksdb\.db\.(?P<op>get|write)\.micros\s+P50\s+:\s+(?P<p50>[\d.e+-]+)\s+P95\s+:\s+(?P<p95>[\d.e+-]+)\s+P99\s+:\s+(?P<p99>[\d.e+-]+)\s+P100\s+:\s+(?P<p100>[\d.e+-]+)",
    re.IGNORECASE
)

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

def create_formatter(scale, suffix):
    def formatter(val, pos):
        if val == 0:
            return "0"
        scaled_val = val / scale
        return f"{scaled_val:g}{suffix}"

    return mticker.FuncFormatter(formatter)

def parse_workload_log(log_path):
    """Parses execution times and all RocksDB P-values from workload_run.log"""
    exec_out = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "Workload": 0}
    rocks_stats = {}
    
    if not log_path.exists():
        return exec_out, rocks_stats

    content = log_path.read_text()
    for line in content.splitlines():
        line_clean = line.strip()
        
        # Parse Execution Times
        m_time = TIME_RE.match(line_clean)
        if m_time:
            kind = m_time.group(1)
            ns = int(m_time.group(2))
            exec_out[kind] = ns
        else:
            m_workload = WORKLOAD_TIME_RE.match(line_clean)
            if m_workload:
                exec_out["Workload"] = int(m_workload.group(1))
        
        # Parse RocksDB micros P-values
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
    print(f"\n--- Collecting records from: {data_dir.resolve()} ---")
    if not data_dir.exists():
        print(f"Directory not found: {data_dir.resolve()}")
        return pd.DataFrame()

    for log_path in data_dir.rglob("workload_run.log"):
        buffer_name = log_path.parent.name

        m_buf = re.match(r"^buffer-\d+-(.*)", buffer_name, re.IGNORECASE)
        if m_buf:
            raw_key = m_buf.group(1)
            if raw_key.lower() == "vector-dynamic":
                buffer_key = "Vector-dynamic"
            elif raw_key.lower() == "unsortedvector-dynamic":
                buffer_key = "UnsortedVector-dynamic"
            elif raw_key.lower() == "alwayssortedvector-dynamic":
                buffer_key = "AlwaysSortedVector-dynamic"
            else:
                buffer_key = raw_key
        else:
            buffer_key = buffer_name

        n_ins = WORKLOAD_INSERTS
        n_pq = WORKLOAD_PQ
        n_rq = WORKLOAD_RQ
        n_total = n_ins + n_pq + n_rq

        exec_ns, rocks_stats = parse_workload_log(log_path)
        
        t_ins = exec_ns["Inserts"] * NS_TO_S
        t_pq = exec_ns["PointQuery"] * NS_TO_S
        t_rq = exec_ns["RangeQuery"] * NS_TO_S

        record = {
            "buffer": buffer_key,
            "thr_insert": safe_div(n_ins, t_ins),
            "thr_pq": safe_div(n_pq, t_pq),
            "thr_rq": safe_div(n_rq, t_rq),
            "lat_insert": safe_div(exec_ns["Inserts"], n_ins),
            "lat_pq": safe_div(exec_ns["PointQuery"], n_pq),
            "lat_rq": safe_div(exec_ns["RangeQuery"], n_rq),
            "lat_workload": safe_div(exec_ns["Workload"], n_total),
            "time_insert": t_ins,
            "time_pq": t_pq,
            "time_rq": t_rq,
            "total_workload_time_s": exec_ns["Workload"] * NS_TO_S,
            "n_total_ops": n_total,
        }
        record.update(rocks_stats)
        records.append(record)
    return pd.DataFrame(records)

def apply_axis_style(ax, y_limit_tuple, is_latency_plot, formatter=None):
    if USE_LOG_SCALE:
        if y_limit_tuple:
            ax.set_ylim(*y_limit_tuple)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        if y_limit_tuple:
            ax.set_ylim(*y_limit_tuple)

        if formatter:
            ax.yaxis.set_major_formatter(formatter)
        else:
            ax.ticklabel_format(style="plain", axis="y", useOffset=False)

    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

def save_plot_legend(handles, labels, base_output_path):
    if not handles:
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
            bbox_extra_artists=[leg],
        )
        print(f"[saved] {legend_output_path.resolve()}")
    plt.close(legend_fig)

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
        legend_output_path = base_output_path.with_name(
            f"{base_output_path.name}.{ext}"
        )
        legend_fig.savefig(
            legend_output_path,
            bbox_inches="tight",
            pad_inches=0.01,
            bbox_extra_artists=[leg],
        )
        print(f"[saved] {legend_output_path.resolve()}")
    plt.close(legend_fig)

def plot_combined_barchart(
    df, metric_cols, y_label, x_labels, filename_base, buffers, y_limit, is_latency_plot
):
    fig, ax = plt.subplots(1, 1, figsize=(5, 3.6))

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)

    num_groups = len(metric_cols)
    num_buffers = len(buffers)

    group_width = 0.9
    bar_width_ratio = 0.9
    width_of_one_bar_slot = group_width / num_buffers
    bar_width = width_of_one_bar_slot * bar_width_ratio

    group_x_positions = np.arange(num_groups)

    log_bottom = 1.0
    if USE_LOG_SCALE and y_limit:
        log_bottom = y_limit[0]
    elif USE_LOG_SCALE:
        log_bottom = 1e-4

    for group_idx, metric_col in enumerate(metric_cols):
        group_center = group_x_positions[group_idx]
        group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot

            value = 0
            try:
                if metric_col in df.columns:
                    value = df.loc[df["buffer"] == buf_name, metric_col].iat[0]
                else:
                    value = log_bottom
            except (IndexError, KeyError):
                value = log_bottom

            if USE_LOG_SCALE and value < log_bottom:
                value = log_bottom

            if USE_LOG_SCALE and value <= 0:
                value = log_bottom

            ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(group_x_positions)
    ax.set_xticklabels(x_labels)

    apply_axis_style(ax, y_limit, is_latency_plot, formatter=None)
    ax.set_ylabel(y_label) 

    fig.tight_layout()

    for ext in ["pdf"]:
        output_path = filename_base.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        print(f"[saved] {output_path.resolve()}")

    plt.close(fig)

def plot_individual_barchart(
    df, metric_col, y_label, x_label, filename_base, buffers, y_limit, is_latency_plot
):
    fig, ax = plt.subplots(1, 1, figsize=(3.5, 3.6))

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)

    num_buffers = len(buffers)
    group_width = 0.8
    bar_width_ratio = 0.9
    width_of_one_bar_slot = group_width / num_buffers
    bar_width = width_of_one_bar_slot * bar_width_ratio

    group_center = 0
    group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

    log_bottom = 1.0
    if USE_LOG_SCALE and y_limit:
        log_bottom = y_limit[0]
    elif USE_LOG_SCALE:
        log_bottom = 1e-4 

    for buf_index, buf_name in enumerate(buffers):
        bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot
        value = 0
        try:
            value = df.loc[df["buffer"] == buf_name, metric_col].iat[0]
        except IndexError:
            value = log_bottom

        if USE_LOG_SCALE and (value < log_bottom or value <= 0):
            value = log_bottom

        ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks([0])
    ax.set_xticklabels([x_label])
    apply_axis_style(ax, y_limit, is_latency_plot)
    ax.set_ylabel(y_label) 
    fig.tight_layout()

    for ext in ["pdf"]:
        output_path = filename_base.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        print(f"[saved individual] {output_path.resolve()}")
    plt.close(fig)

def plot_workload_latency_comparison(
    df_seq, df_interleave, buffers, filename_base, y_limit, is_latency_plot, metric="lat_workload", y_label="total wl latency (ns/op)"
):
    fig, ax = plt.subplots(figsize=(5, 3.6))

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)

    num_groups = 2
    num_buffers = len(buffers)

    group_width = 0.9
    bar_width_ratio = 0.9
    width_of_one_bar_slot = group_width / num_buffers
    bar_width = width_of_one_bar_slot * bar_width_ratio

    group_x_positions = np.arange(num_groups)
    x_labels = ["sequential", "interleaved"]

    log_bottom = 1.0
    if USE_LOG_SCALE and y_limit:
        log_bottom = y_limit[0]

    data_frames = [df_seq, df_interleave]

    for group_idx, df in enumerate(data_frames):
        if df is None:
            continue

        group_center = group_x_positions[group_idx]
        group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot

            value = log_bottom
            try:
                if metric in df.columns:
                    value = df.loc[df["buffer"] == buf_name, metric].iat[0]
                else:
                    value = log_bottom
            except (IndexError, KeyError):
                value = log_bottom

            if USE_LOG_SCALE and value < log_bottom:
                value = log_bottom
            if USE_LOG_SCALE and value <= 0:
                value = log_bottom

            ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(group_x_positions)
    ax.set_xticklabels(x_labels)

    apply_axis_style(ax, y_limit, is_latency_plot, formatter=None)
    ax.set_ylabel(y_label) 
    fig.tight_layout()

    for ext in ["pdf"]:
        output_path = filename_base.with_suffix(f".{ext}")
        fig.savefig(output_path)
        print(f"[saved comparison] {output_path.resolve()}")

    plt.close(fig)

def process_and_plot(data_dir, file_suffix, y_limit_thr, y_limit_lat):
    raw_df = collect_records(data_dir)
    if raw_df.empty:
        return None, False

    grouped_df = raw_df.groupby("buffer", as_index=False).mean()

    if FILTER_BUFFERS:
        grouped_df = grouped_df[grouped_df["buffer"].isin(FILTER_BUFFERS)]

    if grouped_df.empty:
        return None, False

    buffers_to_plot = [b for b in FILTER_BUFFERS if b in grouped_df["buffer"].values]

    thr_labels = ["insert", "point\nqueries", "range\nqueries"]
    thr_metrics = ["thr_insert", "thr_pq", "thr_rq"]
    thr_filename_base = PLOTS_DIR / f"multi_throughput_combined{file_suffix}"
    print(f"Generating combined throughput plot: {thr_filename_base.resolve()}...")
    plot_combined_barchart(
        df=grouped_df,
        metric_cols=thr_metrics,
        y_label="throughput (ops/sec)",
        x_labels=thr_labels,
        filename_base=thr_filename_base,
        buffers=buffers_to_plot,
        y_limit=y_limit_thr,
        is_latency_plot=False,
    )

    for metric, label in zip(thr_metrics, thr_labels):
        name = label.replace("\n", "_").replace(" ", "_")
        plot_individual_barchart(
            df=grouped_df,
            metric_col=metric,
            y_label="throughput (ops/sec)",
            x_label=label,
            filename_base=PLOTS_DIR / f"multi_throughput_{name}{file_suffix}",
            buffers=buffers_to_plot,
            y_limit=y_limit_thr,
            is_latency_plot=False
        )

    lat_labels = ["insert", "point\nqueries", "range\nqueries"]
    lat_metrics = ["lat_insert", "lat_pq", "lat_rq"]
    lat_filename_base = PLOTS_DIR / f"multi_latency_combined{file_suffix}"
    print(f"Generating combined latency plot: {lat_filename_base.resolve()}...")
    plot_combined_barchart(
        df=grouped_df,
        metric_cols=lat_metrics,
        y_label="mean latency (ns/op)",
        x_labels=lat_labels,
        filename_base=lat_filename_base,
        buffers=buffers_to_plot,
        y_limit=y_limit_lat,
        is_latency_plot=True,
    )

    for metric, label in zip(lat_metrics, lat_labels):
        name = label.replace("\n", "_").replace(" ", "_")
        plot_individual_barchart(
            df=grouped_df,
            metric_col=metric,
            y_label="mean latency (ns/op)",
            x_label=label,
            filename_base=PLOTS_DIR / f"multi_latency_{name}{file_suffix}",
            buffers=buffers_to_plot,
            y_limit=y_limit_lat,
            is_latency_plot=True
        )

    time_labels = ["insert", "point\nqueries", "range\nqueries"]
    time_metrics = ["time_insert", "time_pq", "time_rq"]
    time_filename_base = PLOTS_DIR / f"multi_execution_time_combined{file_suffix}"
    print(f"Generating combined execution time plot: {time_filename_base.resolve()}...")
    plot_combined_barchart(
        df=grouped_df,
        metric_cols=time_metrics,
        y_label="total execution time (s)",
        x_labels=time_labels,
        filename_base=time_filename_base,
        buffers=buffers_to_plot,
        y_limit=None, 
        is_latency_plot=False,
    )

    print(f"[DONE] Plots for {file_suffix} saved in {PLOTS_DIR.resolve()}")
    return grouped_df, True

def get_formatted_buffer_label(buf_name):
    match = re.search(r"(hash_[^-]+)-X(\d+)-H(\d+)", buf_name, re.IGNORECASE)
    if match:
        base = match.group(1).replace("_", " ")
        x_val = match.group(2)
        h_val = int(match.group(3))
        
        if h_val >= 1000:
            h_str = f"{h_val // 1000}k"
        else:
            h_str = str(h_val)
            
        return f"{base} (X={x_val}, H={h_str})"
    
    return buf_name.lower()

def main():
    df_rawop, success_rawop = process_and_plot(
        data_dir=RAWOP_DIR,
        file_suffix="_rawop",
        y_limit_thr=YLIM_LOG_THR_RAWOP,
        y_limit_lat=YLIM_LOG_LAT_RAWOP,
    )

    df_interleave, success_interleave = process_and_plot(
        data_dir=INTERLEAVE_DIR,
        file_suffix="_interleave",
        y_limit_thr=YLIM_LOG_THR_INTERLEAVE,
        y_limit_lat=YLIM_LOG_LAT_INTERLEAVE,
    )

    if success_rawop and success_interleave:
        buffers_rawop = set(df_rawop["buffer"])
        buffers_interleave = set(df_interleave["buffer"])
        common_buffers = buffers_rawop.intersection(buffers_interleave)
        buffers_to_plot_workload = [b for b in FILTER_BUFFERS if b in common_buffers]

        if buffers_to_plot_workload:
            # Standard workload latency comparison
            plot_workload_latency_comparison(
                df_seq=df_rawop,
                df_interleave=df_interleave,
                buffers=buffers_to_plot_workload,
                filename_base=PLOTS_DIR / "multi_latency_workload_comparison",
                y_limit=YLIM_LOG_LAT_WORKLOAD,
                is_latency_plot=True,
            )

            # --- ROCKSDB COMPARISON PLOTS (P50, P95, P99, P100) ---
            percentiles = ["p50", "p95", "p99", "p100"]
            ops = ["get", "write"]
            
            for op in ops:
                for p in percentiles:
                    metric_key = f"{op}_{p}"
                    if metric_key in df_rawop.columns:
                        filename = f"rocksdb_{op}_{p}_comparison"
                        y_label_str = f"db.{op} {p.upper()} (micros)"
                        
                        plot_workload_latency_comparison(
                            df_seq=df_rawop,
                            df_interleave=df_interleave,
                            buffers=buffers_to_plot_workload,
                            filename_base=PLOTS_DIR / filename,
                            y_limit=(1e0, 1e7), # Dynamic range for micros
                            is_latency_plot=True,
                            metric=metric_key,
                            y_label=y_label_str
                        )

    if not (success_rawop or success_interleave):
        return

    buffers_for_legend = FILTER_BUFFERS
    all_legend_handles = []
    all_legend_labels = []

    for b in buffers_for_legend:
        style = bar_style(b)
        legend_style = {
            "facecolor": style.get("facecolor"),
            "edgecolor": style.get("edgecolor"),
            "hatch": style.get("hatch"),
            "linewidth": style.get("linewidth"),
        }
        legend_style = {k: v for k, v in legend_style.items() if v is not None}
        all_legend_handles.append(plt.Rectangle((0, 0), 1, 1, **legend_style))
        all_legend_labels.append(get_formatted_buffer_label(b))

    save_plot_legend(all_legend_handles, all_legend_labels, PLOTS_DIR / "multi_throughput_legend")

    for b in buffers_for_legend:
        style = bar_style(b)
        legend_style = {
            "facecolor": style.get("facecolor"),
            "edgecolor": style.get("edgecolor"),
            "hatch": style.get("hatch"),
            "linewidth": style.get("linewidth"),
        }
        legend_style = {k: v for k, v in legend_style.items() if v is not None}
        handle = plt.Rectangle((0, 0), 1, 1, **legend_style)
        save_individual_legend(handle, get_formatted_buffer_label(b), PLOTS_DIR / f"multi_throughput_legend_{b}")

if __name__ == "__main__":
    main()