from pathlib import Path
import sys, re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager

CURR_DIR = Path(__file__).resolve().parent
ROOT = CURR_DIR.parent
sys.path.insert(0, str(ROOT))
from notebooks.style_old import line_styles 


DATA_ROOT = ROOT / "data"
STATS_DIR = DATA_ROOT / "filter_result_entrysize32"
PLOTS_DIR = DATA_ROOT / "plots" / "common_prefix_plots_entrysize_32"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

FONT_CANDIDATES = [
    CURR_DIR / "LinLibertine_Mah.ttf",
    ROOT / "LinLibertine_Mah.ttf",
    DATA_ROOT.parent / "LinLibertine_Mah.ttf",
    Path("/Users/cba/Desktop/tectonic/LinLibertine_Mah.ttf"),
]

for fp in FONT_CANDIDATES:
    if fp.exists():
        prop = font_manager.FontProperties(fname=str(fp))
        plt.rcParams["font.family"] = prop.get_name()
        print(f"✅ Using publication font found at: {fp}")
        break
else:
    print("---" * 20, file=sys.stderr)
    print("❌ CRITICAL ERROR: Publication font 'LinLibertine_Mah.ttf' not found.", file=sys.stderr)
    print("\nThe script searched in the following locations:", file=sys.stderr)
    for cand in FONT_CANDIDATES:
        print(f"  - {cand}", file=sys.stderr)
    print("\nTo fix this, please place the font file in the script's directory.", file=sys.stderr)
    print("---" * 20, file=sys.stderr)
    sys.exit(1)

plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "normal"
plt.rcParams["font.size"] = 22

NS_TO_S = 1e-9
SECONDS_PER_MIN = 60

YLIM_INSERT   = None
YLIM_PQ       = None
YLIM_PQ_VARX  = None
YLIM_RQ       = None

BUCKET_COUNTS_TO_PLOT = [100, 1000, 10000, 100000, 250000, 500000, 1000000, 2000000, 4000000]

BUFFERS_TO_PLOT = ["hash_skip_list", "hash_linked_list"]
FIGSIZE = (5, 3.6)

TOKEN_PAT = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point":  re.compile(r"\bQ(\d+)\b"),
    "range":  re.compile(r"\bS(\d+)\b"),
}
TIME_RE = re.compile(r"^(Inserts|PointQuery|RangeQuery)\s*Execution Time:\s*(\d+)\s*$", re.IGNORECASE)

def format_bucket_count_label(n: int) -> str:
    if n >= 1_000_000:
        if n % 1_000_000 == 0:
            return f"{n // 1_000_000}M"
        return f"{n / 1_000_000:.1f}".rstrip("0").rstrip(".") + "M"
    if n >= 1_000:
        if n % 1_000 == 0:
            return f"{n // 1_000}k"
        return f"{n / 1_000:.1f}".rstrip("0").rstrip(".") + "k"
    return str(n)

def find_dir_ci(root: Path, regex: str):
    cands = [p for p in root.iterdir() if p.is_dir() and re.match(regex, p.name, re.IGNORECASE)]
    return sorted(cands, key=lambda p: p.name)[:1][0] if cands else None

def parse_operation_counts_from_exp_dir(exp_dir: Path):
    name = exp_dir.name
    m_ins = TOKEN_PAT["insert"].search(name)
    m_pq  = TOKEN_PAT["point"].search(name)
    m_rq  = TOKEN_PAT["range"].search(name)
    if not (m_ins and m_pq and m_rq):
        if TOKEN_PAT["point"].search(name) is None:
             m_pq_val = 0
        else:
            m_pq_val = int(m_pq.group(1))

        if not (m_ins and m_rq):
             return None, None, None
        return int(m_ins.group(1)), m_pq_val, int(m_rq.group(1))
        
    return int(m_ins.group(1)), int(m_pq.group(1)), int(m_rq.group(1))


def parse_exec_times(text: str):
    out = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0}
    for line in text.splitlines():
        m = TIME_RE.match(line.strip())
        if m:
            kind = m.group(1)
            ns   = int(m.group(2))
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
    if exp_dir.name.startswith(("hash_", "hash", "skiplist", "Vector", "Unsorted", "Always")):
        exp_dir = d.parent.parent
    nI, nQ, nS = parse_operation_counts_from_exp_dir(exp_dir)
    thr_runs, lat_runs = [], []
    for r in (1, 2, 3):
        w = d / f"workload{r}.log"
        if not w.exists():
            continue
        execs = parse_exec_times(w.read_text(errors="ignore"))
        tI, tQ, tS = execs["Inserts"] * NS_TO_S, execs["PointQuery"] * NS_TO_S, execs["RangeQuery"] * NS_TO_S
        if phase == "I":
            thr = safe_div(nI, tI) * SECONDS_PER_MIN
            lat = safe_div(execs["Inserts"], nI)
        elif phase == "Q":
            thr = safe_div(nQ, tQ) * SECONDS_PER_MIN
            lat = safe_div(execs["PointQuery"], nQ)
        else:
            thr = safe_div(nS, tS) * SECONDS_PER_MIN
            lat = safe_div(execs["RangeQuery"], nS)
        thr_runs.append(thr); lat_runs.append(lat)
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
        ncol=2,
        bbox_to_anchor=(0.5, -0.02),
        frameon=False,
        labelspacing=0.3,
        handletextpad=0.6,
        columnspacing=1.0,
    )

def categorical_axis(ax, labels):
    x = np.arange(len(labels))
    ax.set_xlim(-0.5, len(labels) - 0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.margins(x=0)
    return x

def plot_varH_insert():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_X8_varH-lowpri_false-")
    if not exp: return
    buckets = BUCKET_COUNTS_TO_PLOT
    labels  = [format_bucket_count_label(b) for b in buckets]
    fig, ax = fig_ax()
    x = categorical_axis(ax, labels)
    for buf in BUFFERS_TO_PLOT:
        ys = []
        for H in buckets:
            d = exp / f"{buf}-X8-H{H}"
            thr, _ = avg_metric_from_dir(d, "I")
            ys.append(thr)
        s = {k: dict(v) for k, v in line_styles.items()}
        if buf in s:
            s[buf]["label"] = f"{s[buf].get('label','')} X=8"
        ax.plot(x, ys, **s.get(buf, {}))
    ax.set_xlabel("bucket count")
    ax.set_ylabel("insert throughput (ops/min)")
    if YLIM_INSERT: ax.set_ylim(*YLIM_INSERT)
    legend_bottom(fig)
    out = PLOTS_DIR / "insert_tput_vs_bucket_count_X8.pdf"
    fig.savefig(out, bbox_inches="tight", pad_inches=0.02)
    print(f"[saved] {out.name}")

def plot_varH_pq():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_X8_varH_PQ-lowpri_false-")
    if not exp: return
    buckets = BUCKET_COUNTS_TO_PLOT
    labels  = [format_bucket_count_label(b) for b in buckets]
    fig, ax = fig_ax()
    x = categorical_axis(ax, labels)
    for buf in BUFFERS_TO_PLOT:
        ys = []
        for H in buckets:
            d = exp / f"{buf}-X8-H{H}"
            thr, _ = avg_metric_from_dir(d, "Q")
            ys.append(thr)
        s = {k: dict(v) for k, v in line_styles.items()}
        if buf in s:
            s[buf]["label"] = f"{s[buf].get('label','')} X=8"
        ax.plot(x, ys, **s.get(buf, {}))
    ax.set_xlabel("bucket count")
    ax.set_ylabel("PQ throughput (ops/min)")
    if YLIM_PQ: ax.set_ylim(*YLIM_PQ)
    legend_bottom(fig)
    out = PLOTS_DIR / "pq_tput_vs_bucket_count_X8.pdf"
    fig.savefig(out, bbox_inches="tight", pad_inches=0.02)
    print(f"[saved] {out.name}")

def plot_varX_pq():
    exp = find_dir_ci(STATS_DIR, r"insertPQRS_H1M_varX-lowpri_false-")
    if not exp: return
    xs = list(range(1, 9))
    fig, ax = fig_ax()
    for buf in BUFFERS_TO_PLOT:
        ys = []
        for X in xs:
            d = exp / f"{buf}-X{X}-H1000000"
            thr, _ = avg_metric_from_dir(d, "Q")
            ys.append(thr)
        s = {k: dict(v) for k, v in line_styles.items()}
        if buf in s:
            s[buf]["label"] = f"{s[buf].get('label','')} H=1M"
        ax.plot(xs, ys, **s.get(buf, {}))
    ax.set_xlabel("prefix length")
    ax.set_ylabel("PQ throughput (ops/min)")
    ax.set_xticks(xs)
    if YLIM_PQ_VARX: ax.set_ylim(*YLIM_PQ_VARX)
    legend_bottom(fig)
    out = PLOTS_DIR / "pq_tput_vs_prefix_H1M.pdf"
    fig.savefig(out, bbox_inches="tight", pad_inches=0.02)
    print(f"[saved] {out.name}")

def plot_varC_rq():
    
    exp_pattern = r"common_prefix_keysize_8_value_24-insertPQRS_X8H1M_varC-lowpri_false-"
    exp = find_dir_ci(STATS_DIR, exp_pattern)
    if not exp: 
        print(f"Error: Could not find experiment directory in {STATS_DIR} matching pattern: {exp_pattern}")
        return
    
    all_C_values = list(range(0, 9))
    
    fig, ax = fig_ax()
    
    for buf in BUFFERS_TO_PLOT:
        
        available_C_values = []
        ys = []
        
        for C in all_C_values:
            d = exp / f"{buf}-X8-H1000000-C{C}"
            if not d.exists():
                print(f"Warning: Data directory not found, skipping: {d}")
                continue
            
            thr, _ = avg_metric_from_dir(d, "S")
            
            if thr > 0:
                ys.append(thr)
                available_C_values.append(C)
            else:
                 print(f"Warning: Throughput calculation resulted in 0.0, skipping: {d}")


        if not ys:
            continue

        s = {k: dict(v) for k, v in line_styles.items()}
        if buf in s:
            s[buf]["label"] = f"{s[buf].get('label','')} X=8 H=1M"
        
        ax.plot(available_C_values, ys, **s.get(buf, {}))
        
    ax.set_xlabel("common prefix C = x−y")
    ax.set_ylabel("RQ throughput (ops/min)")
    
    ax.set_xticks(all_C_values) 
    
    if YLIM_RQ: ax.set_ylim(*YLIM_RQ)
    legend_bottom(fig)
    out = PLOTS_DIR / "rq_tput_vs_common_prefix_C_X8H1M_rq100.pdf"
    fig.savefig(out, bbox_inches="tight", pad_inches=0.02)
    print(f"[saved] {out.name}")

def main():

    plot_varC_rq()

if __name__ == "__main__":
    main()