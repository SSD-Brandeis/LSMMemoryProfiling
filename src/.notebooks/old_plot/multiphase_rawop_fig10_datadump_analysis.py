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

USE_LOG_SCALE = False
NS_TO_S = 1e-9

YLIM_LOG_THR_RAWOP = (1e0, 1e8)
YLIM_LOG_LAT_RAWOP = (1e0, 1e5)
YLIM_LOG_THR_INTERLEAVE = (1e0, 1e8)
YLIM_LOG_LAT_INTERLEAVE = (1e0, 1e9)
YLIM_LOG_LAT_WORKLOAD = (1e3, 1e9)

WORKLOAD_INSERTS = 740000
WORKLOAD_PQ = 100000 
WORKLOAD_RQ = 1000

EXP_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data")
RAWOP_DIR = EXP_DIR / "filter_result_mar15_multiphase_rawop"
INTERLEAVE_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_multiphase_interleave_inmemory_314")

PLOTS_DIR = Path(
    "/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/multi_throughput_multiphase_314"
)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FILTER_BUFFERS = [
    "Vector-dynamic",
    "vector-preallocated",
    "UnsortedVector-dynamic",
    "unsortedvector-preallocated",
    "AlwaysSortedVector-dynamic",
    "alwayssortedVector-preallocated",
    "hash_vector-X2-H1000",
    "hash_vector-X6-H100000",
    "skiplist",
    "simple_skiplist",
    "hash_skip_list-X2-H1000",
    "hash_skip_list-X6-H100000",
    "linkedlist",
    "hash_linked_list-X2-H1000",
    "hash_linked_list-X6-H100000",
]

TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")
GETTIME_RE = re.compile(r"^GetTime:\s*(\d+)")

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

def parse_exec_times(text):
    out = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "Workload": 0}
    for line in text.splitlines():
        m = TIME_RE.match(line.strip())
        if m:
            kind = m.group(1)
            ns = int(m.group(2))
            out[kind] = ns
        else:
            m_workload = WORKLOAD_TIME_RE.match(line.strip())
            if m_workload:
                out["Workload"] = int(m_workload.group(1))
    return out

def parse_stats_log(stats_path):
    if not stats_path.exists():
        return 0, 0, 0, 0
    lats = []
    with open(stats_path, 'r') as f:
        for line in f:
            m = GETTIME_RE.match(line.strip())
            if m: lats.append(int(m.group(1)))
    nonempty_lats = lats[:50000]
    empty_lats = lats[50000:100000]
    lat_nonempty = sum(nonempty_lats) / len(nonempty_lats) if nonempty_lats else 0
    lat_empty = sum(empty_lats) / len(empty_lats) if empty_lats else 0
    return lat_nonempty, 0, lat_empty, 0

def collect_records(data_dir):
    records = []
    if not data_dir.exists():
        return pd.DataFrame()

    norm_map = {b.lower(): b for b in FILTER_BUFFERS}

    for log_path in data_dir.rglob("workload*.log"):
        folder_name = log_path.parent.name
        m_buf = re.match(r"^buffer-\d+-(.*)", folder_name, re.IGNORECASE)
        raw_key = m_buf.group(1) if m_buf else folder_name
        buffer_key = norm_map.get(raw_key.lower(), raw_key)
        
        exec_ns = parse_exec_times(log_path.read_text())
        t_ins, t_pq, t_rq = exec_ns["Inserts"]*NS_TO_S, exec_ns["PointQuery"]*NS_TO_S, exec_ns["RangeQuery"]*NS_TO_S
        
        lat_nonempty_pq, _, lat_empty_pq, _ = parse_stats_log(log_path.parent / "stats.log")
        
        records.append({
            "buffer": buffer_key,
            "thr_insert": safe_div(WORKLOAD_INSERTS, t_ins),
            "thr_pq": safe_div(WORKLOAD_PQ, t_pq),
            "thr_rq": safe_div(WORKLOAD_RQ, t_rq),
            "lat_insert": safe_div(exec_ns["Inserts"], WORKLOAD_INSERTS),
            "lat_pq": safe_div(exec_ns["PointQuery"], WORKLOAD_PQ),
            "lat_rq": safe_div(exec_ns["RangeQuery"], WORKLOAD_RQ),
            "lat_workload": safe_div(exec_ns["Workload"], (WORKLOAD_INSERTS + WORKLOAD_PQ + WORKLOAD_RQ)),
            "lat_nonempty_pq": lat_nonempty_pq, 
            "lat_empty_pq": lat_empty_pq,
        })
    return pd.DataFrame(records)

def consolidate_and_export(df_rawop, df_interleave):
    output_log = PLOTS_DIR / "buffer_analysis_dump.log"
    
    # Re-attach experiment labels to grouped DataFrames before combining
    final_dfs = []
    if df_rawop is not None:
        df_rawop['experiment'] = 'sequential'
        final_dfs.append(df_rawop)
    if df_interleave is not None:
        df_interleave['experiment'] = 'interleaved'
        final_dfs.append(df_interleave)
        
    combined = pd.concat(final_dfs, ignore_index=True)
    metrics = {'thr_insert': 'Inserts', 'thr_pq': 'Point Query', 'thr_rq': 'Range Query'}
    
    log_content = "="*70 + "\n RELATIVE PERFORMANCE IMPROVEMENT ANALYSIS (BEST VS ALL)\n" + "="*70 + "\n"
    
    for exp_type in combined['experiment'].unique():
        log_content += f"\n>>> PHASE: {exp_type.upper()}\n"
        exp_df = combined[combined['experiment'] == exp_type]
        for metric, label in metrics.items():
            best_row = exp_df.loc[exp_df[metric].idxmax()]
            best_val, best_buf = best_row[metric], best_row['buffer']
            
            log_content += f"\n  {label} (Baseline Best: {best_buf})\n"
            log_content += f"  {'Target Buffer':<35} | {'Improvement % over Target':<25}\n"
            log_content += f"  {'-'*35}-|-{'-'*25}\n"
            
            # Sort current view by throughput descending
            for _, row in exp_df.sort_values(metric, ascending=False).iterrows():
                target_buf, target_val = row['buffer'], row[metric]
                if target_buf == best_buf:
                    imp_str = "--- (BEST) ---"
                elif target_val > 0:
                    imp = ((best_val - target_val) / target_val) * 100
                    imp_str = f"{imp:,.2f}%"
                else:
                    imp_str = "N/A"
                log_content += f"  {target_buf:<35} | {imp_str:<25}\n"
        log_content += "\n" + "-"*70 + "\n"

    print(log_content)
    with open(output_log, 'w') as f: f.write(log_content)

def process_and_plot(data_dir, suffix, y_limit_thr, y_limit_lat):
    df = collect_records(data_dir)
    if df.empty: return None, False
    
    # numeric_only=True prevents the mean() function from crashing on string columns
    grouped_df = df.groupby("buffer", as_index=False).mean(numeric_only=True)
    grouped_df = grouped_df[grouped_df["buffer"].isin(FILTER_BUFFERS)]
    
    return grouped_df, True

def main():
    df_rawop, _ = process_and_plot(RAWOP_DIR, "_rawop", YLIM_LOG_THR_RAWOP, YLIM_LOG_LAT_RAWOP)
    df_interleave, _ = process_and_plot(INTERLEAVE_DIR, "_interleave", YLIM_LOG_THR_INTERLEAVE, YLIM_LOG_LAT_INTERLEAVE)
    consolidate_and_export(df_rawop, df_interleave)

if __name__ == "__main__":
    main()