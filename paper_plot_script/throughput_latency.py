
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import re
import sys
from pathlib import Path
from math import ceil
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from style import bar_styles

prop = font_manager.FontProperties(fname="./LinLibertine_Mah.ttf")
plt.rcParams["font.family"] = prop.get_name()
plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "bold"
plt.rcParams["font.size"] = 22

USE_LOG_SCALE = True
YLIM_LOG      = (1e0, 1e9)
YLIM_LINEAR   = (1e0, 1e8)

NS_TO_S         = 1e-9
SECONDS_PER_MIN = 60

DATA_ROOT  = Path("/Users/cba/Desktop/LSMMemoryBuffer/data")
RAWOP_ROOT = DATA_ROOT / "filter_result_rawop"
PLOTS_ROOT = DATA_ROOT / "plots"
PLOTS_ROOT.mkdir(exist_ok=True, parents=True)

TOKEN_PAT = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point":  re.compile(r"\bQ(\d+)\b"),
    "range":  re.compile(r"\bS(\d+)\b"),
}

TIME_RE = re.compile(
    r"^(?P<kind>(Inserts|PointQuery|RangeQuery)) Execution Time:\s*(?P<val>\d+)"
)

def safe_div(a, b):
    if a and b:
        return a / b
    return 0

records = []

for log_path in RAWOP_ROOT.rglob("workload*.log"):
    buf_dir  = log_path.parent.name
    buf_type = buf_dir.split("-", 1)[0]
    params   = log_path.parent.parent.name

    try:
        n_ins = int(TOKEN_PAT["insert"].search(params).group(1))
        n_pq  = int(TOKEN_PAT["point"].search(params).group(1))
        n_rq  = int(TOKEN_PAT["range"].search(params).group(1))
    except AttributeError:
        continue

    exec_ns = {
        "Inserts":    0,
        "PointQuery": 0,
        "RangeQuery": 0,
    }

    text = log_path.read_text()
    for line in text.splitlines():
        match = TIME_RE.match(line.strip())
        if match:
            kind = match.group("kind")
            val  = int(match.group("val"))
            exec_ns[kind] = val

    total_s_ins = exec_ns["Inserts"]    * NS_TO_S
    total_s_pq  = exec_ns["PointQuery"] * NS_TO_S
    total_s_rq  = exec_ns["RangeQuery"] * NS_TO_S

    records.append({
        "buffer":     buf_type,
        "thr_insert": safe_div(n_ins, total_s_ins) * SECONDS_PER_MIN,
        "thr_pq":     safe_div(n_pq,  total_s_pq)  * SECONDS_PER_MIN,
        "thr_rq":     safe_div(n_rq,  total_s_rq)  * SECONDS_PER_MIN,
        "lat_insert": safe_div(exec_ns["Inserts"],    n_ins),
        "lat_pq":     safe_div(exec_ns["PointQuery"], n_pq),
        "lat_rq":     safe_div(exec_ns["RangeQuery"], n_rq),
    })

df = pd.DataFrame(records)
df = df.groupby("buffer", as_index=False).mean()
df.to_csv(PLOTS_ROOT / "rawop_metrics.csv", index=False)

def bar_plot(metric_cols, title, fname):
    categories = ["Insert", "Point queries", "Range queries"]
    buffers    = df["buffer"].tolist()
    bar_width  = 0.8 / len(buffers)

    fig, ax = plt.subplots(figsize=(9, 4.5))

    for i, buf in enumerate(buffers):
        values    = df.loc[df["buffer"] == buf, metric_cols].values.flatten()
        positions = [x + i * bar_width for x in range(len(categories))]
        style     = bar_styles.get(
            buf,
            {"color": "None", "edgecolor": "black", "hatch": ""}
        )
        ax.bar(positions, values, width=bar_width, lw=1, **style)

    center_positions = [
        x + (len(buffers) - 1) * bar_width / 2
        for x in range(len(categories))
    ]
    ax.set_xticks(center_positions)
    ax.set_xticklabels(categories, fontsize=12)
    ax.set_ylabel(title, fontsize=13)

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
        ax.set_ylim(*YLIM_LOG)
    else:
        ax.set_ylim(*YLIM_LINEAR)
        ax.ticklabel_format(style="plain", axis="y", useOffset=False)

    ax.grid(True, linestyle="--", alpha=0.3, axis="y")

    ncol = ceil(len(buffers) / 2)
    ax.legend(loc="upper left", frameon=False, fontsize=10, ncol=ncol)

    fig.tight_layout()
    fig.savefig(PLOTS_ROOT / fname, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)

bar_plot(
    ["thr_insert", "thr_pq", "thr_rq"],
    "Throughput (ops/min)",
    "throughput_combined.pdf"
)

bar_plot(
    ["lat_insert", "lat_pq", "lat_rq"],
    "Mean latency (ns/op)",
    "latency_combined.pdf"
)

print(f"[DONE] Plots saved in {PLOTS_ROOT}")
