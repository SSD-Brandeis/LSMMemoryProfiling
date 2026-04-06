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
import matplotlib.font_manager as font_manager

# --- 1. SETUP & FONTS ---
try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

ROOT = CURR_DIR.parent

FONT_CANDIDATES = [
    CURR_DIR / "LinLibertine_Mah.ttf",
    ROOT / "LinLibertine_Mah.ttf",
    Path.home() / "Desktop/tectonic/LinLibertine_Mah.ttf",
]
for fp in FONT_CANDIDATES:
    if fp.exists():
        prop = font_manager.FontProperties(fname=str(fp))
        plt.rcParams["font.family"] = prop.get_name()
        break
plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "normal"
plt.rcParams["font.size"] = 24

# --- 2. LOCAL STYLES ---
try:
    from plot import *
except ImportError:
    print("⚠️  'plot.py' not found. Styles may be missing.")
    bar_styles = {}


PLOT_CONFIG = {
    "insert": {
        "scale": "linear",
        "ylim_thr":   (0, None),     # Throughput
        "ylim_mean":  (0, None),     # Mean Latency (ns/op)
        "ylim_total": (0, 20),       # Total Latency (s)
    },
    "point": {
        "scale": "log",              # Log Scale
        "ylim_thr":   (1e0, None),   # Throughput
        "ylim_mean":  (1e0, None),   # Mean Latency (ms/op)
        "ylim_total": (1e0, 1e5),    # Total Latency (ms)
    },
    "range": {
        "scale": "linear",
        "ylim_thr":   (0, None),     # Throughput
        "ylim_mean":  (0, None),     # Mean Latency (ns/op)
        "ylim_total": (0, 70),       # Total Latency (s)
    }
}

# Workload Comparison Limits
YLIM_LOG_LAT_WORKLOAD = (1e0, 1e7)

# --- CONSTANTS ---
NS_TO_S = 1e-9
NS_TO_MS = 1e-6  # For converting ns -> ms
S_TO_MS = 1e3    # For converting s -> ms

DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_ondisk_throughput")
RAWOP_DIR = DATA_ROOT / "fixed_rerun_ondisk_interleave_smallwl-lowpri_true-I450000-U0-Q10000-S1000-Y0.1-T2-P1024-B32-E128"
INTERLEAVE_DIR = DATA_ROOT / "fixed_rerun_ondisk_sequential_smallwl-lowpri_true-I450000-U0-Q10000-S1000-Y0.1-T2-P1024-B32-E128"
PLOTS_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/poster_ondisk")

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
TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery) Execution Time:\s*(\d+)")
WORKLOAD_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)")

# --- HELPER FUNCTIONS ---

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

def parse_operation_counts(folder_name):
    m_ins = TOKEN_PAT["insert"].search(folder_name)
    m_pq = TOKEN_PAT["point"].search(folder_name)
    m_rq = TOKEN_PAT["range"].search(folder_name)
    if m_ins and m_pq and m_rq:
        return int(m_ins.group(1)), int(m_pq.group(1)), int(m_rq.group(1))
    return None

def parse_exec_times(text):
    out = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0, "Workload": 0}
    for line in text.splitlines():
        m = TIME_RE.match(line.strip())
        if m:
            out[m.group(1)] = int(m.group(2))
        else:
            m_wk = WORKLOAD_TIME_RE.match(line.strip())
            if m_wk:
                out["Workload"] = int(m_wk.group(1))
    return out

def collect_records(data_dir):
    records = []
    print(f"\n--- Collecting records from: {data_dir} ---")
    
    if not data_dir.exists():
        print(f"Directory not found: {data_dir}")
        return pd.DataFrame()

    for log_path in data_dir.rglob("workload*.log"):
        buffer_name = log_path.parent.name
        if buffer_name.startswith("hash_skip_list"): buffer_key = "hash_skip_list"
        elif buffer_name.startswith("hash_linked_list"): buffer_key = "hash_linked_list"
        elif buffer_name.startswith("AlwayssortedVector"): buffer_key = "AlwaysSortedVector-dynamic"
        else: buffer_key = buffer_name

        counts = parse_operation_counts(data_dir.name)
        if counts is None: continue

        n_ins, n_pq, n_rq = counts
        n_total = n_ins + n_pq + n_rq
        exec_ns = parse_exec_times(log_path.read_text())
        
        # Calculate Times (Seconds)
        t_ins_s = exec_ns["Inserts"] * NS_TO_S
        t_pq_s = exec_ns["PointQuery"] * NS_TO_S
        t_rq_s = exec_ns["RangeQuery"] * NS_TO_S

        # --- CONVERSIONS ---
        mean_lat_ins_ns = safe_div(exec_ns["Inserts"], n_ins)
        mean_lat_pq_ms  = safe_div(exec_ns["PointQuery"], n_pq) * NS_TO_MS # ns to ms
        mean_lat_rq_ns  = safe_div(exec_ns["RangeQuery"], n_rq)

        total_lat_ins_s = t_ins_s
        total_lat_pq_ms = t_pq_s * S_TO_MS # s to ms
        total_lat_rq_s  = t_rq_s

        records.append({
            "buffer": buffer_key,
            # Throughput
            "thr_insert": safe_div(n_ins, t_ins_s),
            "thr_pq":     safe_div(n_pq, t_pq_s),
            "thr_rq":     safe_div(n_rq, t_rq_s),
            
            # Mean Latency 
            "lat_mean_insert": mean_lat_ins_ns, # ns/op
            "lat_mean_pq":     mean_lat_pq_ms,  # ms/op
            "lat_mean_rq":     mean_lat_rq_ns,  # ns/op
            
            # Total Latency
            "lat_total_insert": total_lat_ins_s, # s
            "lat_total_pq":     total_lat_pq_ms, # ms
            "lat_total_rq":     total_lat_rq_s,  # s

            "lat_workload": safe_div(exec_ns["Workload"], n_total),
        })
    return pd.DataFrame(records)

# --- PLOTTING HELPERS ---

def save_plot_caption(caption_text, base_output_path):
    if not caption_text: return
    title_fig = plt.figure(figsize=(0.1, 0.1))
    txt = title_fig.text(0, 0, caption_text, ha="left", va="bottom", fontsize=plt.rcParams["font.size"])
    plt.axis("off")
    title_fig.savefig(base_output_path.with_name(f"{base_output_path.name}_caption.pdf"),
                      bbox_inches="tight", pad_inches=0.01, bbox_extra_artists=[txt])
    plt.close(title_fig)

def save_individual_legend(handle, label, base_output_path):
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend([handle], [label], loc="center", ncol=1, frameon=False,
                            fontsize=plt.rcParams["font.size"], borderpad=0.1, handletextpad=0.5)
    plt.axis("off")
    legend_fig.savefig(base_output_path.with_suffix(".pdf"), bbox_inches="tight", pad_inches=0.01, bbox_extra_artists=[leg])
    plt.close(legend_fig)

def save_plot_accessories(fig, filename_base, caption=None):
    output_path = filename_base.with_suffix(".pdf")
    fig.savefig(output_path, bbox_inches="tight")
    print(f"[saved] {output_path.name}")

# --- MAIN PLOTTING FUNCTION ---

def plot_single_metric(df, metric_col, y_label, filename, buffers, scale_mode, y_limits=None, x_label_text=None):
    fig, ax = plt.subplots(figsize=(4, 3.4))
    
    # --- Strict Axis Config ---
    if scale_mode == "log":
        ax.set_yscale("log", base=10)
        bottom = y_limits[0] if y_limits and y_limits[0] is not None else 1e0
        top = y_limits[1] if y_limits and y_limits[1] is not None else None
        ax.set_ylim(bottom=bottom, top=top)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        # Linear
        ax.set_yscale("linear")
        bottom = y_limits[0] if y_limits and y_limits[0] is not None else 0
        top = y_limits[1] if y_limits and y_limits[1] is not None else None
        ax.set_ylim(bottom=bottom, top=top)
        # CHANGED: Use 'plain' style to prevent scientific notation (e.g., 60 instead of 6 x 10^1)
        ax.ticklabel_format(style='plain', axis='y')

    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

    bar_width = 0.6
    x_positions = np.arange(len(buffers))
    
    for i, buf_name in enumerate(buffers):
        try:
            val = df.loc[df["buffer"] == buf_name, metric_col].iat[0]
        except IndexError:
            val = 0
        
        if scale_mode == "log" and val <= 0:
            val = bottom if bottom > 0 else 1e-10
        
        ax.bar(x_positions[i], val, width=bar_width, **bar_style(buf_name))

    ax.set_xticks(x_positions)
    ax.set_xticklabels([]) 
    
    # --- Apply X-Label here ---
    if x_label_text:
        ax.set_xlabel(x_label_text)
    
    ax.set_ylabel(y_label)
    fig.tight_layout()
    
    save_plot_accessories(fig, filename)
    plt.close(fig)

def plot_workload_comparison(df_seq, df_interleave, buffers, filename):
    fig, ax = plt.subplots(figsize=(5, 3.6))
    
    ax.set_yscale("log", base=10)
    ax.set_ylim(YLIM_LOG_LAT_WORKLOAD)
    ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

    num_groups = 2
    group_width = 0.8
    width_per_bar = group_width / len(buffers)
    
    group_indices = np.arange(num_groups)
    x_labels = ["sequential", "interleaved"]
    dfs = [df_seq, df_interleave]
    
    for g_idx, df in enumerate(dfs):
        if df is None: continue
        
        center = group_indices[g_idx]
        start_x = center - (group_width/2) + (width_per_bar/2)
        
        for b_idx, buf in enumerate(buffers):
            x = start_x + b_idx * width_per_bar
            try:
                val = df.loc[df["buffer"] == buf, "lat_workload"].iat[0]
            except: 
                val = 1e0
            
            if val <= 0: val = 1e0
            ax.bar(x, val, width=width_per_bar * 0.9, **bar_style(buf))
            
    ax.set_xticks(group_indices)
    ax.set_xticklabels(x_labels)
    ax.set_ylabel("total wl latency (ns/op)")
    fig.tight_layout()
    
    save_plot_accessories(fig, filename)
    plt.close(fig)

def process_dataset(data_dir, suffix):
    raw_df = collect_records(data_dir)
    if raw_df.empty: return None, False
    
    grouped_df = raw_df.groupby("buffer", as_index=False).mean()
    if FILTER_BUFFERS:
        grouped_df = grouped_df[grouped_df["buffer"].isin(FILTER_BUFFERS)]
    
    if grouped_df.empty: return None, False

    csv_path = PLOTS_DIR / f"metrics{suffix}.csv"
    grouped_df.to_csv(csv_path, index=False)
    
    buffers = [b for b in FILTER_BUFFERS if b in grouped_df["buffer"].values]

    ops = ["insert", "point", "range"]
    op_map = {
        "insert": {"col_thr": "thr_insert", "col_mean": "lat_mean_insert", "col_total": "lat_total_insert"},
        "point":  {"col_thr": "thr_pq",     "col_mean": "lat_mean_pq",     "col_total": "lat_total_pq"},
        "range":  {"col_thr": "thr_rq",     "col_mean": "lat_mean_rq",     "col_total": "lat_total_rq"},
    }
    
    # --- LABELS MAPPING ---
    op_display_labels = {
        "insert": "Insert",
        "point": "Point Query",
        "range": "Range Query"
    }

    for op in ops:
        config = PLOT_CONFIG[op]
        mapping = op_map[op]
        
        # Dynamic Y-Labels
        if op == "point":
            mean_label = "mean latency (ms/op)"
            total_label = "total latency (ms)"
        else:
            mean_label = "mean latency (ns/op)"
            total_label = "total latency (s)"
            
        # Get X-Label
        x_lbl = op_display_labels.get(op, op)

        # 1. Throughput
        plot_single_metric(
            grouped_df, mapping["col_thr"], "throughput (ops/sec)",
            PLOTS_DIR / f"thr_{op}{suffix}", buffers, 
            config["scale"], config["ylim_thr"],
            x_label_text=x_lbl
        )
        
        # 2. Mean Latency
        plot_single_metric(
            grouped_df, mapping["col_mean"], mean_label,
            PLOTS_DIR / f"lat_mean_{op}{suffix}", buffers, 
            config["scale"], config["ylim_mean"],
            x_label_text=x_lbl
        )

        # 3. Total Latency
        plot_single_metric(
            grouped_df, mapping["col_total"], total_label,
            PLOTS_DIR / f"lat_total_{op}{suffix}", buffers, 
            config["scale"], config["ylim_total"],
            x_label_text=x_lbl
        )

    base_caption = "P=1024 B=32 E=128 T=2 I=450K PQ=10k RQ=1k S=0.1"
    cap = f"Sequential {base_caption}" if "rawop" in suffix else f"Interleaved {base_caption}"
    save_plot_caption(cap, PLOTS_DIR / f"caption{suffix}")

    return grouped_df, True

def save_legends(buffers):
    # Individual Legends
    for b in buffers:
        s = bar_style(b)
        patch = plt.Rectangle((0,0),1,1, facecolor=s.get("facecolor"), 
                              edgecolor=s.get("edgecolor"), hatch=s.get("hatch"))
        save_individual_legend(patch, b.lower(), PLOTS_DIR / f"legend_{b}")

    # Master Legend
    dummy_handles = []
    dummy_labels = []
    for b in buffers:
        s = bar_style(b)
        patch = plt.Rectangle((0,0),1,1, facecolor=s.get("facecolor"), 
                              edgecolor=s.get("edgecolor"), hatch=s.get("hatch"))
        dummy_handles.append(patch)
        dummy_labels.append(s.get("label", b).lower())
    
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    ncol = ceil(len(dummy_handles) / 2)
    leg = legend_fig.legend(dummy_handles, dummy_labels, loc="center", ncol=ncol, frameon=False,
                            fontsize=plt.rcParams["font.size"], borderpad=0.1, 
                            columnspacing=0.8, handletextpad=0.5)
    plt.axis("off")
    legend_fig.savefig(PLOTS_DIR / "master_legend.pdf", bbox_inches="tight", pad_inches=0.01, bbox_extra_artists=[leg])
    plt.close(legend_fig)

def main():
    print("Processing RawOp (Sequential)...")
    df_rawop, success_rawop = process_dataset(RAWOP_DIR, "_rawop")
    
    print("\nProcessing Interleave...")
    df_int, success_int = process_dataset(INTERLEAVE_DIR, "_interleave")
    
    if success_rawop and success_int:
        print("\nGenerating Workload Comparison...")
        buffers_rawop = set(df_rawop["buffer"])
        buffers_int = set(df_int["buffer"])
        common = [b for b in FILTER_BUFFERS if b in buffers_rawop and b in buffers_int]
        
        if common:
            plot_workload_comparison(
                df_rawop, df_int, common,
                PLOTS_DIR / "multi_latency_workload_comparison"
            )
    
    if success_rawop or success_int:
        print("\nGenerating Legends...")
        save_legends(FILTER_BUFFERS)

    print("\nDone.")

if __name__ == "__main__":
    main()