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

try:
    from plot import *
except ImportError:
    print("⚠️  'plot.py' not found. Using default matplotlib settings.")
    plt.rcParams.update({"font.size": 20})
    EXP_DIR = Path.cwd()
    bar_styles = {}


try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

USE_LOG_SCALE = True
NS_TO_S = 1e-9

YLIM_LOG_THR_RAWOP = (1e0, 1e8)
YLIM_LOG_LAT_RAWOP = (1e0, 1e9)
YLIM_LOG_THR_INTERLEAVE = (1e0, 1e8)
YLIM_LOG_LAT_INTERLEAVE = (1e0, 1e9)
# YLIM_LOG_LAT_WORKLOAD = (0, 10000000)
YLIM_LOG_LAT_WORKLOAD = (1e0, 1e7)


# YLIM_LOG_THR_RAWOP = (0, 400000)
# YLIM_LOG_LAT_RAWOP = (0, 4000000)
# YLIM_LOG_THR_INTERLEAVE = (0, 400000)
# YLIM_LOG_LAT_INTERLEAVE = (0, 400000)


RAWOP_DIR = EXP_DIR / "filter_result_rawop"
INTERLEAVE_DIR = EXP_DIR / "filter_result_interleave"

PLOTS_DIR = Path(
    "/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/multi_throughput"
)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    "Vector-dynamic",
    "skiplist",
    "hash_skip_list",
    "hash_linked_list",
    "UnsortedVector-dynamic",
    "AlwaysSortedVector-dynamic",
]

TOKEN_PAT = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point": re.compile(r"\bQ(\d+)\b"),
    "range": re.compile(r"\bS(\d+)\b"),
}
# Regex for individual operation times
TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery) Execution Time:\s*(\d+)")
# --- New Regex for Workload Execution Time ---
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")


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


def parse_operation_counts(folder_name):
    m_ins = TOKEN_PAT["insert"].search(folder_name)
    m_pq = TOKEN_PAT["point"].search(folder_name)
    m_rq = TOKEN_PAT["range"].search(folder_name)
    if m_ins and m_pq and m_rq:
        n_ins = int(m_ins.group(1))
        n_pq = int(m_pq.group(1))
        n_rq = int(m_rq.group(1))
        return n_ins, n_pq, n_rq
    return None


def parse_exec_times(text):
    # --- Updated to include "Workload" ---
    out = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "Workload": 0}
    for line in text.splitlines():
        m = TIME_RE.match(line.strip())
        if m:
            kind = m.group(1)
            ns = int(m.group(2))
            out[kind] = ns
        else:
            # --- Check for new workload time ---
            m_workload = WORKLOAD_TIME_RE.match(line.strip())
            if m_workload:
                # --- CORRECTED LINE ---
                # The regex has one group, so we use group(1)
                out["Workload"] = int(m_workload.group(1))
    return out


def collect_records(data_dir):
    records = []
    print(f"\n--- Collecting records from: {data_dir} ---")
    for log_path in data_dir.rglob("workload*.log"):
        buffer_name = log_path.parent.name

        if buffer_name.startswith("hash_skip_list"):
            buffer_key = "hash_skip_list"
        elif buffer_name.startswith("hash_linked_list"):
            buffer_key = "hash_linked_list"
        elif buffer_name.startswith("AlwayssortedVector"):
            buffer_key = "AlwaysSortedVector-dynamic"
        else:
            buffer_key = buffer_name

        parent_folder = log_path.parent.parent.name
        counts = parse_operation_counts(parent_folder)
        if counts is None:
            continue

        n_ins, n_pq, n_rq = counts
        # --- New: Calculate total operations ---
        n_total = n_ins + n_pq + n_rq

        exec_ns = parse_exec_times(log_path.read_text())
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
            # --- New: Add workload latency calculation ---
            "lat_workload": safe_div(exec_ns["Workload"], n_total),
            "total_workload_time_s": exec_ns["Workload"] * NS_TO_S,
            "n_total_ops": n_total,
        }
        records.append(record)
    return pd.DataFrame(records)


def apply_axis_style(ax, y_limit_tuple, is_latency_plot, formatter=None):
    if USE_LOG_SCALE:
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
            bbox_extra_artists=[leg],
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
            bbox_extra_artists=[txt],
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
        legend_output_path = base_output_path.with_name(
            f"{base_output_path.name}.{ext}"
        )
        legend_fig.savefig(
            legend_output_path,
            bbox_inches="tight",
            pad_inches=0.01,
            bbox_extra_artists=[leg],
        )
        print(f"[saved] {legend_output_path.name}")
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

    for group_idx, metric_col in enumerate(metric_cols):
        group_center = group_x_positions[group_idx]

        group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot

            value = 0
            try:
                value = df.loc[df["buffer"] == buf_name, metric_col].iat[0]
            except IndexError:
                print(
                    f"Warning: No data for buffer '{buf_name}' with metric '{metric_col}'. Skipping bar."
                )
                value = log_bottom

            if USE_LOG_SCALE and value < log_bottom:
                print(
                    f"Warning: Metric '{metric_col}' for buffer '{buf_name}' is {value}. "
                    f"Clipping to log scale bottom {log_bottom}."
                )
                value = log_bottom

            if USE_LOG_SCALE and value <= 0:
                print(
                    f"Warning: Metric '{metric_col}' for buffer '{buf_name}' is {value}. Cannot plot on log scale. Setting to {log_bottom}."
                )
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
        print(f"[saved] {output_path.name}")

    plt.close(fig)


# --- New Plotting Function for Workload Latency Comparison ---
def plot_workload_latency_comparison(
    df_seq, df_interleave, buffers, filename_base, y_limit, is_latency_plot
):
    """
    Plots a new chart comparing 'lat_workload' between sequential and interleaved data.
    X-axis: "Sequential", "Interleaved"
    Y-axis: "total Workload Latency (ns/op)"
    Bars: Grouped by buffer
    """
    fig, ax = plt.subplots( figsize=(5, 3.6))

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)

    num_groups = 2  # Sequential, Interleaved
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

    data_frames = [df_seq, df_interleave]  # Data to plot for each group

    for group_idx, df in enumerate(data_frames):
        if df is None:  # Add a check in case one dataframe failed to load
            print(f"Skipping plot group '{x_labels[group_idx]}' due to missing data.")
            continue

        group_center = group_x_positions[group_idx]
        group_start_x = group_center - (group_width / 2) + (width_of_one_bar_slot / 2)

        for buf_index, buf_name in enumerate(buffers):
            bar_x_pos = group_start_x + buf_index * width_of_one_bar_slot

            value = log_bottom
            try:
                # --- Plot the new 'lat_workload' metric ---
                value = df.loc[df["buffer"] == buf_name, "lat_workload"].iat[0]
            except (IndexError, KeyError):
                print(
                    f"Warning: No 'lat_workload' data for buffer '{buf_name}' in group '{x_labels[group_idx]}'. Skipping bar."
                )
                value = log_bottom

            if USE_LOG_SCALE and value < log_bottom:
                value = log_bottom
            if USE_LOG_SCALE and value <= 0:
                value = log_bottom

            ax.bar(bar_x_pos, value, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(group_x_positions)
    ax.set_xticklabels(x_labels)

    apply_axis_style(ax, y_limit, is_latency_plot, formatter=None)
    # --- Set new Y-axis label ---
    ax.set_ylabel("total wl latency (ns/op)")
    fig.tight_layout()
    #total wl latency
    for ext in ["pdf"]:
        output_path = filename_base.with_suffix(f".{ext}")
        # fig.savefig(output_path, bbox_inches="tight", pad_inches=0.1)
        fig.savefig(output_path)
        print(f"[saved] {output_path.name}")

    plt.close(fig)


def process_and_plot(data_dir, file_suffix, y_limit_thr, y_limit_lat):
    raw_df = collect_records(data_dir)
    if raw_df.empty:
        print(f"No records found in {data_dir}. Skipping plots.")
        # --- Modified: Return None and False on failure ---
        return None, False

    grouped_df = raw_df.groupby("buffer", as_index=False).mean()

    if FILTER_BUFFERS:
        grouped_df = grouped_df[grouped_df["buffer"].isin(FILTER_BUFFERS)]

    if grouped_df.empty:
        print(f"No data left after filtering for {data_dir}. Skipping plots.")
        # --- Modified: Return None and False on failure ---
        return None, False

    csv_filename = PLOTS_DIR / f"multi_throughput_metrics{file_suffix}.csv"
    grouped_df.to_csv(csv_filename, index=False)

    buffers_to_plot = [b for b in FILTER_BUFFERS if b in grouped_df["buffer"].values]

    # --- Existing Plot 1: Throughput ---
    thr_labels = ["insert", "point\nqueries", "range\nqueries"]
    thr_metrics = ["thr_insert", "thr_pq", "thr_rq"]
    thr_filename_base = PLOTS_DIR / f"multi_throughput_combined{file_suffix}"
    print(f"Generating combined plot: {thr_filename_base.name}...")
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

    # --- Existing Plot 2: Latency ---
    lat_labels = ["insert", "point\nqueries", "range\nqueries"]
    lat_metrics = ["lat_insert", "lat_pq", "lat_rq"]
    lat_filename_base = PLOTS_DIR / f"multi_latency_combined{file_suffix}"
    print(f"Generating combined plot: {lat_filename_base.name}...")
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

    base_caption = "I=450K PQ=10k RQ=1k S=0.1"
    if file_suffix == "_rawop":
        caption_text = f"Sequential {base_caption}"
    elif file_suffix == "_interleave":
        caption_text = f"Interleaved {base_caption}"
    else:
        caption_text = base_caption

    save_plot_caption(caption_text, thr_filename_base)
    save_plot_caption(caption_text, lat_filename_base)

    print(f"[DONE] Plots for {file_suffix} saved in {PLOTS_DIR}")
    # --- Modified: Return dataframe and True on success ---
    return grouped_df, True


def main():
    # --- Modified: Capture dataframe and success flag ---
    df_rawop, success_rawop = process_and_plot(
        data_dir=RAWOP_DIR,
        file_suffix="_rawop",
        y_limit_thr=YLIM_LOG_THR_RAWOP,
        y_limit_lat=YLIM_LOG_LAT_RAWOP,
    )

    # --- Modified: Capture dataframe and success flag ---
    df_interleave, success_interleave = process_and_plot(
        data_dir=INTERLEAVE_DIR,
        file_suffix="_interleave",
        y_limit_thr=YLIM_LOG_THR_INTERLEAVE,
        y_limit_lat=YLIM_LOG_LAT_INTERLEAVE,
    )

    # --- New block to generate the comparison plot ---
    if success_rawop and success_interleave:
        print("\nGenerating workload latency comparison plot...")
        buffers_rawop = set(df_rawop["buffer"])
        buffers_interleave = set(df_interleave["buffer"])
        common_buffers = buffers_rawop.intersection(buffers_interleave)

        # Use the master FILTER_BUFFERS list to set order and filter
        buffers_to_plot_workload = [b for b in FILTER_BUFFERS if b in common_buffers]

        if not buffers_to_plot_workload:
            print("No common buffers found. Skipping workload latency plot.")
        else:
            plot_workload_latency_comparison(
                df_seq=df_rawop,
                df_interleave=df_interleave,
                buffers=buffers_to_plot_workload,
                filename_base=PLOTS_DIR / "multi_latency_workload_comparison",
                y_limit=YLIM_LOG_LAT_WORKLOAD,  # Use new limit
                is_latency_plot=True,
            )
    else:
        print(
            "\nSkipping workload latency comparison plot (missing data from one or both runs)."
        )
    # --- End of new block ---

    if not (success_rawop or success_interleave):
        print("No plots were generated. Skipping legend and caption.")
        return

    print("\nGenerating common legend...")

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
        all_legend_labels.append(bar_style(b).get("label", b).lower())

    save_plot_legend(
        all_legend_handles, all_legend_labels, PLOTS_DIR / "multi_throughput_legend"
    )

    print("\nGenerating individual legends...")
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
        label = bar_style(b).get("label", b).lower()

        output_path = PLOTS_DIR / f"multi_throughput_legend_{b}"

        save_individual_legend(handle, label, output_path)


if __name__ == "__main__":
    main()
