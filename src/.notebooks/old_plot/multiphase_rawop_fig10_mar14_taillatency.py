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
MICROS_TO_NS = 1000

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

def get_formatted_buffer_label(buf_name):
    """Returns concise abbreviations for buffer names to avoid x-axis crowding."""
    abbrev_map = {
        "vector-preallocated": "VP",
        "unsortedvector-preallocated": "UVP",
        "alwayssortedvector-preallocated": "ASVP",
        "skiplist": "SL",
        "simple_skiplist": "SSL",
    }
    
    match = re.search(r"(hash_[^-]+)-X\d+-H\d+", buf_name, re.IGNORECASE)
    if match:
        raw_base = match.group(1).lower()
        if "vector" in raw_base: return "HV"
        if "skip_list" in raw_base: return "HSL"
        if "linked_list" in raw_base: return "HLL"
        return raw_base
    
    return abbrev_map.get(buf_name.lower(), buf_name.lower())

def parse_workload_log(log_path):
    """Parses execution times and all RocksDB P-values from workload_run.log"""
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
    print(f"\n--- Collecting records from: {data_dir.resolve()} ---")
    if not data_dir.exists():
        return pd.DataFrame()

    for log_path in data_dir.rglob("workload_run.log"):
        buffer_name = log_path.parent.name
        m_buf = re.match(r"^buffer-\d+-(.*)", buffer_name, re.IGNORECASE)
        buffer_key = m_buf.group(1) if m_buf else buffer_name

        exec_ns, rocks_stats = parse_workload_log(log_path)
        t_ins = exec_ns["Inserts"] * NS_TO_S
        t_pq = exec_ns["PointQuery"] * NS_TO_S
        t_rq = exec_ns["RangeQuery"] * NS_TO_S

        record = {
            "buffer": buffer_key,
            "thr_insert": safe_div(WORKLOAD_INSERTS, t_ins),
            "thr_pq": safe_div(WORKLOAD_PQ, t_pq),
            "thr_rq": safe_div(WORKLOAD_RQ, t_rq),
            "lat_insert": safe_div(exec_ns["Inserts"], WORKLOAD_INSERTS),
            "lat_pq": safe_div(exec_ns["PointQuery"], WORKLOAD_PQ),
            "lat_rq": safe_div(exec_ns["RangeQuery"], WORKLOAD_RQ),
            "lat_workload": safe_div(exec_ns["Workload"], WORKLOAD_INSERTS + WORKLOAD_PQ + WORKLOAD_RQ),
            "time_insert": t_ins,
            "time_pq": t_pq,
            "time_rq": t_rq,
        }
        
        # Convert micros to ns for the RocksDB stats
        scaled_rocks = {k: v * MICROS_TO_NS for k, v in rocks_stats.items()}
        record.update(scaled_rocks)
        records.append(record)
    return pd.DataFrame(records)

def apply_axis_style(ax, y_limit_tuple, is_latency_plot):
    if USE_LOG_SCALE:
        if y_limit_tuple:
            ax.set_ylim(*y_limit_tuple)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

def plot_rocksdb_distribution_box(df, op, filename_base, buffers, y_limit):
    """Generates a concatenated box plot showing data in nanoseconds."""
    fig, ax = plt.subplots(1, 1, figsize=(7, 4))
    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)

    stats_list = []
    print(f"\n--- DEBUG DATA: db.{op} distribution (ns) ---")
    print(f"{'Buffer':<30} | {'P50':>8} | {'P95':>8} | {'P99':>8} | {'P100':>8}")
    print("-" * 75)

    for buf_name in buffers:
        try:
            row = df[df["buffer"] == buf_name].iloc[0]
            p50, p95, p99, p100 = row[f"{op}_p50"], row[f"{op}_p95"], row[f"{op}_p99"], row[f"{op}_p100"]
            print(f"{buf_name:<30} | {p50:8.2f} | {p95:8.2f} | {p99:8.2f} | {p100:8.2f}")

            stats = {
                "label": get_formatted_buffer_label(buf_name),
                "med": p50, "q1": p50, "q3": p95,
                "whislo": p50, "whishi": p99,
                "fliers": [p100]
            }
            stats_list.append(stats)
        except: continue

    if not stats_list:
        plt.close(fig)
        return

    bxp_output = ax.bxp(stats_list, showfliers=True, patch_artist=True)
    for patch, buf_name in zip(bxp_output['boxes'], buffers):
        style = bar_style(buf_name)
        patch.set_facecolor(style.get("facecolor", "none"))
        patch.set_edgecolor(style.get("edgecolor", "black"))
        patch.set_hatch(style.get("hatch", ""))

    ax.set_ylabel(f"db.{op} latency (ns)")
    ax.set_xticks(range(1, len(stats_list) + 1))
    ax.set_xticklabels([s["label"] for s in stats_list], rotation=0)
    
    # Using a 1e0 (1 ns) baseline since all your data is now > 500 ns
    apply_axis_style(ax, (1e0, 1e9), is_latency_plot=True)
    fig.tight_layout()
    fig.savefig(filename_base.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)

def process_and_plot(data_dir, file_suffix, y_limit_thr, y_limit_lat):
    raw_df = collect_records(data_dir)
    if raw_df.empty: return None, False
    grouped_df = raw_df.groupby("buffer", as_index=False).mean()
    if FILTER_BUFFERS:
        grouped_df = grouped_df[grouped_df["buffer"].isin(FILTER_BUFFERS)]
    if grouped_df.empty: return None, False
    bufs = [b for b in FILTER_BUFFERS if b in grouped_df["buffer"].values]

    for op in ["get", "write"]:
        boxplot_filename = PLOTS_DIR / f"rocksdb_{op}_distribution{file_suffix}"
        plot_rocksdb_distribution_box(grouped_df, op, boxplot_filename, bufs, (1e0, 1e9))
    return grouped_df, True

def main():
    process_and_plot(RAWOP_DIR, "_rawop", YLIM_LOG_THR_RAWOP, YLIM_LOG_LAT_RAWOP)
    process_and_plot(INTERLEAVE_DIR, "_interleave", YLIM_LOG_THR_INTERLEAVE, YLIM_LOG_LAT_INTERLEAVE)

if __name__ == "__main__":
    main()