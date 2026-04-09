from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.ticker as ticker
import math

# --- Import plotting setup ---
from plot import *

# --- Path Setup ---
CURR_DIR = Path(__file__).resolve().parent

# --- Data & Plot Directories ---
STATS_DIR = EXP_DIR / "medium_varied_bucketprefix"
PLOTS_DIR = (CURR_DIR / "paper_plot" / "medium_variedbucketplot").resolve()
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


# --- Constants ---
NS_TO_S = 1e-9
YLIM_INSERT = (0, 600000)
YLIM_PQ = (0, 400000)
YLIM_PQ_VARX = (0, 400000)
YLIM_RQ = (0, 78000)

BUCKET_COUNTS_TO_PLOT = [100, 1000, 10000, 100000, 250000, 500000, 1000000]
BUFFERS_TO_PLOT = ["hash_skip_list", "hash_linked_list"]
FIGSIZE = (4.5, 3.6)

TOKEN_PAT = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point": re.compile(r"\bQ(\d+)\b"),
    "range": re.compile(r"\bS(\d+)\b"),
}
TIME_RE = re.compile(
    r"^(Inserts|PointQuery|RangeQuery)\s*Execution Time:\s*(\d+)\s*$", re.IGNORECASE
)

# --- Helper Functions ---

def find_dir_ci(root: Path, regex: str):
    cands = [p for p in root.iterdir() if p.is_dir() and re.match(regex, p.name, re.IGNORECASE)]
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
            kind, ns = m.group(1), int(m.group(2))
            if kind.lower().startswith("insert"): out["Inserts"] = ns
            elif kind.lower().startswith("point"): out["PointQuery"] = ns
            elif kind.lower().startswith("range"): out["RangeQuery"] = ns
    return out

def safe_div(numer, denom):
    return numer / denom if numer and denom else 0

def avg_metric_from_dir(d: Path, phase: str):
    exp_dir = d.parent
    if exp_dir.name.startswith(("hash_", "hash", "skiplist", "Vector", "Unsorted", "Always")):
        exp_dir = d.parent.parent
    nI, nQ, nS = parse_operation_counts_from_exp_dir(exp_dir)
    thr_runs, lat_runs = [], []
    for r in (1, 2, 3):
        w = d / f"workload{r}.log"
        if not w.exists(): continue
        execs = parse_exec_times(w.read_text(errors="ignore"))
        tI, tQ, tS = execs["Inserts"] * NS_TO_S, execs["PointQuery"] * NS_TO_S, execs["RangeQuery"] * NS_TO_S
        if phase == "I":
            thr, lat = safe_div(nI, tI), safe_div(execs["Inserts"], nI)
        elif phase == "Q":
            thr, lat = safe_div(nQ, tQ), safe_div(execs["PointQuery"], nQ)
        else:
            thr, lat = safe_div(nS, tS), safe_div(execs["RangeQuery"], nS)
        thr_runs.append(thr)
        lat_runs.append(lat)
    return float(np.mean(thr_runs)) if thr_runs else 0.0, float(np.mean(lat_runs)) if lat_runs else 0.0

def fig_ax():
    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.35)
    return fig, ax

def categorical_axis(ax, numbers):
    x_positions = np.arange(len(numbers))
    ax.set_xlim(-0.5, len(numbers) - 0.5)
    ax.set_xticks(x_positions)
    ax.margins(x=0)
    return x_positions

def save_plot_legend(handles, labels, base_output_path):
    if not handles: return
    legend_fig = plt.figure(figsize=(3, 0.25))
    legend_fig.legend(handles, labels, loc="center", ncol=1, frameon=False, labelspacing=0.2, handletextpad=0.2, columnspacing=0.1)
    plt.axis("off")
    for ext in ["pdf", "svg"]:
        legend_out = base_output_path.with_name(f"{base_output_path.name}_legend.{ext}").resolve()
        legend_fig.savefig(legend_out, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {legend_out}")
    plt.close(legend_fig)

# --- Plotting Functions ---

def plot_varH_insert():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_X8_varH-lowpri_false-")
    if not exp: return
    buckets = BUCKET_COUNTS_TO_PLOT
    fig, ax = fig_ax()
    x_positions = categorical_axis(ax, buckets)
    all_ys = []
    for buf in BUFFERS_TO_PLOT:
        ys = [avg_metric_from_dir(exp / f"{buf}-X8-H{H}", "I")[0] for H in buckets]
        all_ys.extend(ys)
        ax.plot(x_positions, ys, **line_styles.get(buf, {}))
    
    ax.set_xlabel("bucket count")
    ax.set_ylabel("insert throughput(ops)")
    if YLIM_INSERT: ax.set_ylim(*YLIM_INSERT)

    max_y = max([y for y in all_ys if not math.isnan(y)] + [1])
    y_power = math.floor(math.log10(max_y)) if max_y > 0 else 0
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.0f}"))
    ax.text(0.02, 0.98, r"$\times 10^{{{}}}$".format(y_power), transform=ax.transAxes, ha="left", va="top")

    x_power = 3
    x_labels = [f"{int(b/10**x_power)}" if b/10**x_power >= 1 else f"{b/10**x_power:.1f}".rstrip('0').rstrip('.') for b in buckets]
    ax.set_xticklabels(x_labels)
    ax.text(0.98, 0.02, r'$\times 10^{{{}}}$'.format(x_power), transform=ax.transAxes, ha='right', va='bottom')

    handles, labels = ax.get_legend_handles_labels()
    base_out = PLOTS_DIR / "insert_tput_vs_bucket_count_X8"
    for ext in ["pdf", "svg"]:
        out_path = base_out.with_suffix(f".{ext}").resolve()
        fig.savefig(out_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {out_path}")
    plt.close(fig)
    save_plot_legend(handles, labels, base_out)

def plot_varH_pq():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_X8_varH_PQ-lowpri_false-")
    if not exp: return
    buckets = BUCKET_COUNTS_TO_PLOT
    fig, ax = fig_ax()
    x_positions = categorical_axis(ax, buckets)
    all_ys = []
    for buf in BUFFERS_TO_PLOT:
        ys = [avg_metric_from_dir(exp / f"{buf}-X8-H{H}", "Q")[0] for H in buckets]
        all_ys.extend(ys)
        ax.plot(x_positions, ys, **line_styles.get(buf, {}))
    
    ax.set_xlabel("bucket count")
    ax.set_ylabel("PQ throughput(ops)")
    if YLIM_PQ: ax.set_ylim(*YLIM_PQ)

    max_y = max([y for y in all_ys if not math.isnan(y)] + [1])
    y_power = math.floor(math.log10(max_y)) if max_y > 0 else 0
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.0f}"))
    ax.text(0.02, 0.98, r"$\times 10^{{{}}}$".format(y_power), transform=ax.transAxes, ha="left", va="top")

    x_power = 3
    x_labels = [f"{int(b/10**x_power)}" if b/10**x_power >= 1 else f"{b/10**x_power:.1f}".rstrip('0').rstrip('.') for b in buckets]
    ax.set_xticklabels(x_labels)
    ax.text(0.98, 0.02, r'$\times 10^{{{}}}$'.format(x_power), transform=ax.transAxes, ha='right', va='bottom')

    handles, labels = ax.get_legend_handles_labels()
    base_out = PLOTS_DIR / "pq_tput_vs_bucket_count_X8"
    for ext in ["pdf", "svg"]:
        out_path = base_out.with_suffix(f".{ext}").resolve()
        fig.savefig(out_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {out_path}")
    plt.close(fig)
    save_plot_legend(handles, labels, base_out)

def plot_varX_pq():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_H1M_varX-lowpri_false-")
    if not exp: return
    xs = list(range(1, 9))
    fig, ax = fig_ax()
    all_ys = []
    for buf in BUFFERS_TO_PLOT:
        ys = [avg_metric_from_dir(exp / f"{buf}-X{X}-H1000000", "Q")[0] for X in xs]
        all_ys.extend(ys)
        ax.plot(xs, ys, **line_styles.get(buf, {}))
    
    ax.set_xlabel("prefix length")
    ax.set_ylabel("PQ throughput(ops)")
    ax.set_xticks(xs)
    if YLIM_PQ_VARX: ax.set_ylim(*YLIM_PQ_VARX)

    max_y = max([y for y in all_ys if not math.isnan(y)] + [1])
    power = math.floor(math.log10(max_y)) if max_y > 0 else 0
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, p: f"{y / (10**power):.0f}"))
    ax.text(0.02, 0.98, r"$\times 10^{{{}}}$".format(power), transform=ax.transAxes, ha="left", va="top")

    handles, labels = ax.get_legend_handles_labels()
    base_out = PLOTS_DIR / "pq_tput_vs_prefix_H1M"
    for ext in ["pdf", "svg"]:
        out_path = base_out.with_suffix(f".{ext}").resolve()
        fig.savefig(out_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {out_path}")
    plt.close(fig)
    save_plot_legend(handles, labels, base_out)

def plot_varC_rq():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_X8H1M_varC-lowpri_false-")
    if not exp: return
    xs = list(range(0, 9))
    fig, ax = fig_ax()
    all_ys = []
    for buf in BUFFERS_TO_PLOT:
        ys = [avg_metric_from_dir(exp / f"{buf}-X8-H1000000-C{C}", "S")[0] for C in xs]
        all_ys.extend(ys)
        ax.plot(xs, ys, **line_styles.get(buf, {}))
    
    ax.set_xlabel("common prefix length")
    ax.set_ylabel("RQ throughput(ops)")
    ax.set_xticks(xs)
    if YLIM_RQ: ax.set_ylim(*YLIM_RQ)

    max_y = max([y for y in all_ys if not math.isnan(y)] + [1])
    power = math.floor(math.log10(max_y)) if max_y > 0 else 0
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, p: f"{y / (10**power):.0f}"))
    ax.text(0.02, 0.98, r"$\times 10^{{{}}}$".format(power), transform=ax.transAxes, ha="left", va="top")

    handles, labels = ax.get_legend_handles_labels()
    base_out = PLOTS_DIR / "rq_tput_vs_common_prefix_C_X8H1M"
    for ext in ["pdf", "svg"]:
        out_path = base_out.with_suffix(f".{ext}").resolve()
        fig.savefig(out_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {out_path}")
    plt.close(fig)
    save_plot_legend(handles, labels, base_out)

def main():
    plot_varH_insert()
    plot_varH_pq()
    plot_varX_pq()
    plot_varC_rq()

if __name__ == "__main__":
    main()