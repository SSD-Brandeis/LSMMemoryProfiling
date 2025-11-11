from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.ticker as ticker
import math  # Import math for log10 and floor

# --- Import plotting setup ---
# This imports plt, matplotlib, EXP_DIR, line_styles
# and applies all rcParams (font, usetex, etc.) from notebooks/plot/__init__.py
from plot import *

# --- Path Setup ---
CURR_DIR = Path(__file__).resolve().parent
# 'line_styles' is imported from 'from plot import *'

# --- Data & Plot Directories ---
# STATS_DIR uses EXP_DIR imported from plot
STATS_DIR = EXP_DIR / "medium_varied_bucketprefix"
# STATS_DIR = EXP_DIR / "filter_result_400kbucket"
# STATS_DIR = EXP_DIR / "filter_result_1GB_400kbucket"
# STATS_DIR = EXP_DIR / "filter_result_fixed_rq_selectivity"

# PLOTS_DIR saves to the 'paper_plot' folder in the script's directory
PLOTS_DIR = CURR_DIR / "paper_plot" / "medium_variedbucketplot"
# PLOTS_DIR = CURR_DIR / "paper_plot" / "variedbucketplot_400k"
# PLOTS_DIR = CURR_DIR / "paper_plot" / "variedbucketplot_fixed_rq_selectivity"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


# --- Constants ---
NS_TO_S = 1e-9
# <-- REMOVED: SECONDS_PER_MIN = 60

YLIM_INSERT = (0, 600000)
YLIM_PQ = (0, 400000)
YLIM_PQ_VARX = (0, 400000)
YLIM_RQ = (0, 78000)
# YLIM_INSERT   = (0, 1000000)
# YLIM_PQ       = (0, 1000000)
# YLIM_PQ_VARX  = (0, 1000000)
# YLIM_RQ       = (0, 1000000)

# BUCKET_COUNTS_TO_PLOT = [100, 1000, 10000, 100000, 250000, 500000, 1000000, 2000000, 4000000]

BUCKET_COUNTS_TO_PLOT = [100, 1000, 10000, 100000, 250000, 500000, 1000000]
BUFFERS_TO_PLOT = ["hash_skip_list", "hash_linked_list"]
# BUFFERS_TO_PLOT = [ "hash_linked_list"]

FIGSIZE = (4.5, 3.6)

TOKEN_PAT = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point": re.compile(r"\bQ(\d+)\b"),
    "range": re.compile(r"\bS(\d+)\b"),
}
TIME_RE = re.compile(
    r"^(Inserts|PointQuery|RangeQuery)\s*Execution Time:\s*(\d+)\s*$", re.IGNORECASE
)

# --- Helper Functions (Restored categorical_axis) ---

# <-- REMOVED format_bucket_count_label function -->

def find_dir_ci(root: Path, regex: str):
    cands = [
        p
        for p in root.iterdir()
        if p.is_dir() and re.match(regex, p.name, re.IGNORECASE)
    ]
    return sorted(cands, key=lambda p: p.name)[:1][0] if cands else None


def parse_operation_counts_from_exp_dir(exp_dir: Path):
    name = exp_dir.name
    m_ins = TOKEN_PAT["insert"].search(name)
    m_pq = TOKEN_PAT["point"].search(name)
    m_rq = TOKEN_PAT["range"].search(name)
    if not (m_ins and m_pq and m_rq):
        return None, None, None
    return int(m_ins.group(1)), int(m_pq.group(1)), int(m_rq.group(1))


def parse_exec_times(text: str):
    out = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0}
    for line in text.splitlines():
        m = TIME_RE.match(line.strip())
        if m:
            kind = m.group(1)
            ns = int(m.group(2))
            if kind.lower().startswith("insert"):
                out["Inserts"] = ns
            elif kind.lower().startswith("point"):
                out["PointQuery"] = ns
            elif kind.lower().startswith("range"):
                out["RangeQuery"] = ns
    return out


def safe_div(numer, denom):
    if numer and denom:
        return numer / denom
    return 0


def avg_metric_from_dir(d: Path, phase: str):
    exp_dir = d.parent
    if exp_dir.name.startswith(
        ("hash_", "hash", "skiplist", "Vector", "Unsorted", "Always")
    ):
        exp_dir = d.parent.parent
    nI, nQ, nS = parse_operation_counts_from_exp_dir(exp_dir)
    thr_runs, lat_runs = [], []
    for r in (1, 2, 3):
        w = d / f"workload{r}.log"
        if not w.exists():
            continue
        execs = parse_exec_times(w.read_text(errors="ignore"))
        tI, tQ, tS = (
            execs["Inserts"] * NS_TO_S,
            execs["PointQuery"] * NS_TO_S,
            execs["RangeQuery"] * NS_TO_S,
        )

        if phase == "I":
            thr = safe_div(nI, tI)
            lat = safe_div(execs["Inserts"], nI)
        elif phase == "Q":
            thr = safe_div(nQ, tQ)
            lat = safe_div(execs["PointQuery"], nQ)
        else:
            thr = safe_div(nS, tS)
            lat = safe_div(execs["RangeQuery"], nS)
        thr_runs.append(thr)
        lat_runs.append(lat)
    thr_avg = float(np.mean(thr_runs)) if thr_runs else 0.0
    lat_avg = float(np.mean(lat_runs)) if lat_runs else 0.0
    return thr_avg, lat_avg


def fig_ax():
    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.35)
    return fig, ax


def legend_bottom(fig):
    fig.legend(
        loc="lower center",
        ncol=1,
        bbox_to_anchor=(0.5, -0.02),
        frameon=False,
        labelspacing=0.3,
        handletextpad=0.6,
        columnspacing=1.0,
    )

# <-- MODIFIED: Restored categorical_axis, takes numerical labels -->
def categorical_axis(ax, numbers):
    x_positions = np.arange(len(numbers))
    ax.set_xlim(-0.5, len(numbers) - 0.5)
    ax.set_xticks(x_positions)
    # Labels will be set by a formatter in the plotting function
    ax.margins(x=0)
    return x_positions


def save_plot_legend(handles, labels, base_output_path):
    """Saves a plot's legend to PDF and SVG files."""
    if not handles:
        return
    legend_fig = plt.figure(figsize=(3, 0.25))
    legend_fig.legend(
        handles,
        labels,
        loc="center",
        ncol=1,
        frameon=False,
        labelspacing=0.2,
        handletextpad=0.2,
        columnspacing=0.1,
    )
    plt.axis("off")
    for ext in ["pdf", "svg"]:
        legend_output_path = base_output_path.with_name(
            f"{base_output_path.name}_legend.{ext}"
        )
        legend_fig.tight_layout()
        legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0.02)
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


# --- Plotting Functions (Modified) ---


def plot_varH_insert():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_X8_varH-lowpri_false-")
    if not exp:
        print("Warning: Skipping 'plot_varH_insert', directory not found.")
        return
    buckets = BUCKET_COUNTS_TO_PLOT
    fig, ax = fig_ax()
    # <-- MODIFIED: Use categorical axis with numbers -->
    x_positions = categorical_axis(ax, buckets)

    all_ys = []
    for buf in BUFFERS_TO_PLOT:
        ys = []
        for H in buckets:
            d = exp / f"{buf}-X8-H{H}"
            thr, _ = avg_metric_from_dir(d, "I")
            ys.append(thr)
        all_ys.extend(ys)

        s = {k: dict(v) for k, v in line_styles.items()}
        if buf in s:
            s[buf]["label"] = f"{s[buf].get('label','')}"
        ax.plot(x_positions, ys, **s.get(buf, {}))

    ax.set_xlabel("bucket count")
    ax.set_ylabel("insert throughput(ops)")
    if YLIM_INSERT:
        ax.set_ylim(*YLIM_INSERT)

    # --- Y-Axis Scientific Notation ---
    max_y = max([y for y in all_ys if not math.isnan(y)] + [1])
    y_power = math.floor(math.log10(max_y)) if max_y > 0 else 0
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.0f}")
    )
    ax.text(
        0.02, 0.98, r"$\times 10^{{{}}}$".format(y_power),
        transform=ax.transAxes, fontsize=plt.rcParams["font.size"],
        ha="left", va="top"
    )
    # --- End Y-Axis ---

    # --- MODIFIED: X-Axis Scientific Notation using 10^3 ---
    x_power = 3 # Use fixed power of 3 (kilo)
    # Create labels manually, dividing by power
    x_labels = []
    for b in buckets:
        scaled_value = b / (10**x_power)
        # Format nicely (avoid unnecessary decimals like .0)
        if scaled_value < 1: # Handle 100 -> 0.1
             label = f"{scaled_value:.1f}".rstrip('0').rstrip('.')
        elif scaled_value == int(scaled_value):
            label = f"{int(scaled_value)}"
        else:
            # Should not happen with current buckets, but handles cases like 1500 -> 1.5
            label = f"{scaled_value:.1f}".rstrip('0').rstrip('.')
        x_labels.append(label)
    ax.set_xticklabels(x_labels) # Set the formatted labels

    # Add x-axis multiplier text in lower-right
    ax.text(
        0.98, 0.02, r'$\times 10^{{{}}}$'.format(x_power),
        transform=ax.transAxes, fontsize=plt.rcParams['font.size'],
        ha='right', va='bottom'
    )
    # --- End X-Axis MODIFIED ---


    handles, labels = ax.get_legend_handles_labels()
    base_out = PLOTS_DIR / "insert_tput_vs_bucket_count_X8"
    caption_text = "Insert Throughput vs. Bucket Count (X=8)"

    for ext in ["pdf", "svg"]:
        output_path = base_out.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {output_path.name}")
    plt.close(fig)

    save_plot_legend(handles, labels, base_out)
    save_plot_caption(caption_text, base_out)


def plot_varH_pq():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_X8_varH_PQ-lowpri_false-")
    if not exp:
        print("Warning: Skipping 'plot_varH_pq', directory not found.")
        return
    buckets = BUCKET_COUNTS_TO_PLOT
    fig, ax = fig_ax()
    # <-- MODIFIED: Use categorical axis with numbers -->
    x_positions = categorical_axis(ax, buckets)

    all_ys = []
    for buf in BUFFERS_TO_PLOT:
        ys = []
        for H in buckets:
            d = exp / f"{buf}-X8-H{H}"
            thr, _ = avg_metric_from_dir(d, "Q")
            ys.append(thr)
        all_ys.extend(ys)

        s = {k: dict(v) for k, v in line_styles.items()}
        if buf in s:
            s[buf]["label"] = f"{s[buf].get('label','')}"
        ax.plot(x_positions, ys, **s.get(buf, {}))

    ax.set_xlabel("bucket count")
    ax.set_ylabel("PQ throughput(ops)")
    if YLIM_PQ:
        ax.set_ylim(*YLIM_PQ)

    # --- Y-Axis Scientific Notation ---
    max_y = max([y for y in all_ys if not math.isnan(y)] + [1])
    y_power = math.floor(math.log10(max_y)) if max_y > 0 else 0
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.0f}")
    )
    ax.text(
        0.02, 0.98, r"$\times 10^{{{}}}$".format(y_power),
        transform=ax.transAxes, fontsize=plt.rcParams["font.size"],
        ha="left", va="top"
    )
    # --- End Y-Axis ---

    # --- MODIFIED: X-Axis Scientific Notation using 10^3 ---
    x_power = 3 # Use fixed power of 3 (kilo)
    # Create labels manually, dividing by power
    x_labels = []
    for b in buckets:
        scaled_value = b / (10**x_power)
        if scaled_value < 1: # Handle 100 -> 0.1
             label = f"{scaled_value:.1f}".rstrip('0').rstrip('.')
        elif scaled_value == int(scaled_value):
            label = f"{int(scaled_value)}"
        else:
             label = f"{scaled_value:.1f}".rstrip('0').rstrip('.')
        x_labels.append(label)
    ax.set_xticklabels(x_labels) # Set the formatted labels

    # Add x-axis multiplier text in lower-right
    ax.text(
        0.98, 0.02, r'$\times 10^{{{}}}$'.format(x_power),
        transform=ax.transAxes, fontsize=plt.rcParams['font.size'],
        ha='right', va='bottom'
    )
    # --- End X-Axis MODIFIED ---


    handles, labels = ax.get_legend_handles_labels()
    base_out = PLOTS_DIR / "pq_tput_vs_bucket_count_X8"
    caption_text = "PQ Throughput vs. Bucket Count (X=8)"

    for ext in ["pdf", "svg"]:
        output_path = base_out.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {output_path.name}")
    plt.close(fig)

    save_plot_legend(handles, labels, base_out)
    save_plot_caption(caption_text, base_out)


def plot_varX_pq():
    # No changes needed for x-axis here
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_H1M_varX-lowpri_false-")
    if not exp:
        print("Warning: Skipping 'plot_varX_pq', directory not found.")
        return
    xs = list(range(1, 9))
    fig, ax = fig_ax()

    all_ys = []
    for buf in BUFFERS_TO_PLOT:
        ys = []
        for X in xs:
            d = exp / f"{buf}-X{X}-H1000000"
            thr, _ = avg_metric_from_dir(d, "Q")
            ys.append(thr)
        all_ys.extend(ys)

        s = {k: dict(v) for k, v in line_styles.items()}
        if buf in s:
            s[buf]["label"] = f"{s[buf].get('label','')}"
        ax.plot(xs, ys, **s.get(buf, {}))
    ax.set_xlabel("prefix length")
    ax.set_ylabel("PQ throughput(ops)")
    ax.set_xticks(xs)
    if YLIM_PQ_VARX:
        ax.set_ylim(*YLIM_PQ_VARX)

    # --- Y-Axis Scientific Notation ---
    max_y = max([y for y in all_ys if not math.isnan(y)] + [1])
    power = math.floor(math.log10(max_y)) if max_y > 0 else 0
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda y, p: f"{y / (10**power):.0f}")
    )
    ax.text(
        0.02, 0.98, r"$\times 10^{{{}}}$".format(power),
        transform=ax.transAxes, fontsize=plt.rcParams["font.size"],
        ha="left", va="top"
    )
    # --- End Y-Axis ---


    handles, labels = ax.get_legend_handles_labels()
    base_out = PLOTS_DIR / "pq_tput_vs_prefix_H1M"
    caption_text = "PQ Throughput vs. Prefix Length (H=1M)"

    for ext in ["pdf", "svg"]:
        output_path = base_out.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {output_path.name}")
    plt.close(fig)

    save_plot_legend(handles, labels, base_out)
    save_plot_caption(caption_text, base_out)


def plot_varC_rq():
    # No changes needed for x-axis here
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_X8H1M_varC-lowpri_false-")
    if not exp:
        print("Warning: Skipping 'plot_varC_rq', directory not found.")
        return
    xs = list(range(0, 9))
    fig, ax = fig_ax()

    all_ys = []
    for buf in BUFFERS_TO_PLOT:
        ys = []
        for C in xs:
            d = exp / f"{buf}-X8-H1000000-C{C}"
            thr, _ = avg_metric_from_dir(d, "S")
            ys.append(thr)
        all_ys.extend(ys)

        s = {k: dict(v) for k, v in line_styles.items()}
        if buf in s:
            s[buf]["label"] = f"{s[buf].get('label','')}"
        ax.plot(xs, ys, **s.get(buf, {}))
    ax.set_xlabel("common prefix length")
    ax.set_ylabel("RQ throughput(ops)")
    ax.set_xticks(xs)
    if YLIM_RQ:
        ax.set_ylim(*YLIM_RQ)

    # --- Y-Axis Scientific Notation ---
    max_y = max([y for y in all_ys if not math.isnan(y)] + [1])
    power = math.floor(math.log10(max_y)) if max_y > 0 else 0
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda y, p: f"{y / (10**power):.0f}")
    )
    ax.text(
        0.02, 0.98, r"$\times 10^{{{}}}$".format(power),
        transform=ax.transAxes, fontsize=plt.rcParams["font.size"],
        ha="left", va="top"
    )
    # --- End Y-Axis ---


    handles, labels = ax.get_legend_handles_labels()
    base_out = PLOTS_DIR / "rq_tput_vs_common_prefix_C_X8H1M"
    caption_text = "RQ Throughput vs. Common Prefix (X=8, H=1M)"

    for ext in ["pdf", "svg"]:
        output_path = base_out.with_suffix(f".{ext}")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {output_path.name}")
    plt.close(fig)

    save_plot_legend(handles, labels, base_out)
    save_plot_caption(caption_text, base_out)


def main():
    plot_varH_insert()
    plot_varH_pq()
    plot_varX_pq()
    plot_varC_rq()


if __name__ == "__main__":
    main()