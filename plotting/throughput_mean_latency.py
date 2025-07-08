#!/usr/bin/env python3
import re
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import LogFormatterMathtext
from math import ceil

# ─── USER CONFIG ─────────────────────────────────────────────────────────────
USE_LOG_SCALE = True             # switch between log/linear y-axis
YLIM_LOG     = (10**0, 1e9)      # 10⁰ → 10⁹ when in log-scale
ytl = 10**8
YLIM_LINEAR  = (10**0, 10**8)    # 10⁰ → 10⁸ when in linear-scale

# ─── TIME BASE CONSTANTS ────────────────────────────────────────────────────
TIME_BASE_NS    = 1e9   # number of nanoseconds in one second\nSECONDS_PER_MIN = 60    # seconds per minute

RAWOP_ROOT = Path("/home/cc/LSMMemoryProfiling/.result/7_5_rawop_low_pri_false_default_refill")
PARAM_RE   = re.compile(
    r"I(?P<insert>\d+)-Q(?P<point>\d+)-U\d+-S(?P<range>\d+)-"
    r"Y(?P<selectivity>[\d\.]+)-T(?P<threads>\d+)"
)
TIME_RE    = re.compile(
    r"^(?P<kind>(Inserts|PointQuery|RangeQuery)) Execution Time:\s*(?P<val>\d+)"
)

records = []
for log_path in RAWOP_ROOT.rglob("workload.log"):
    params_dir  = log_path.parent.parent
    buffer_kind = params_dir.parent.name

    m = PARAM_RE.search(params_dir.name)
    if not m:
        print(f"[Something Wrong] unparsable folder: {params_dir.name}")
        continue

    n_ins, n_pq, n_rq = map(int, (m["insert"], m["point"], m["range"]))
    exec_ns = {"Inserts": 0, "PointQuery": 0, "RangeQuery": 0}
    for line in log_path.read_text().splitlines():
        t = TIME_RE.match(line.strip())
        if t:
            exec_ns[t["kind"]] = int(t["val"])

    if not any(exec_ns.values()):
        print(f"[WARN] no exec time in {log_path}")
        continue

    # safe division helper
    safe = lambda a, b: a / b if (a and b) else 0

    # compute throughputs:
    #   first ops/sec, then convert to ops/min
    thr_ins_per_s = safe(n_ins * TIME_BASE_NS, exec_ns["Inserts"])
    thr_pq_per_s  = safe(n_pq  * TIME_BASE_NS, exec_ns["PointQuery"])
    thr_rq_per_s  = safe(n_rq  * TIME_BASE_NS, exec_ns["RangeQuery"])

    thr_ins = thr_ins_per_s * SECONDS_PER_MIN
    thr_pq  = thr_pq_per_s  * SECONDS_PER_MIN
    thr_rq  = thr_rq_per_s  * SECONDS_PER_MIN

    # compute mean latencies (ns/op)
    lat_ins = safe(exec_ns["Inserts"],     n_ins)
    lat_pq  = safe(exec_ns["PointQuery"],  n_pq)
    lat_rq  = safe(exec_ns["RangeQuery"],  n_rq)

    records.append({
        "buffer":     buffer_kind,
        "lat_insert": lat_ins,
        "lat_pq":     lat_pq,
        "lat_rq":     lat_rq,
        "thr_insert": thr_ins,
        "thr_pq":     thr_pq,
        "thr_rq":     thr_rq,
    })

if not records:
    sys.exit("No workload.log files parsed.")

df = pd.DataFrame(records)

out_dir = RAWOP_ROOT / "throughput plots"
out_dir.mkdir(exist_ok=True)
df.to_csv(out_dir / "rawop_metrics.csv", index=False)

def combined_bar(metric_cols, title, fname):
    cats = ["insert", "point queries", "range queries"]
    bufs = sorted(df["buffer"].unique())
    bar_w = 0.8 / len(bufs)

    hatch_map = {
        "AlwayssortedVector":              "",
        "preallocated AlwayssortedVector": "..",
        "UnsortedVector":                  "//",
        "preallocated UnsortedVector":     "\\\\",
        "Vector":                          "--",
        "preallocated Vector":             "++",
        "hash_linked_list":                "\\\\",
        "hash_skip_list":                  "xx",
        "skiplist":                        "++",
    }

    fig, ax = plt.subplots(figsize=(10, 4))
    for i, buf in enumerate(bufs):
        vals      = [df.loc[df["buffer"] == buf, m].mean() for m in metric_cols]
        xs        = [x + i * bar_w for x in range(len(cats))]
        hatch     = hatch_map.get(buf, "||")
        facecolor = "0.9" if buf.startswith("preallocated") else "white"

        ax.bar(xs, vals, width=bar_w, edgecolor="black",
               facecolor=facecolor, hatch=hatch, lw=1,
               label=buf.replace("_", " "))

    ax.set_xticks([x + (len(bufs) - 1) * bar_w / 2 for x in range(len(cats))])
    ax.set_xticklabels(cats, fontsize=11)
    ax.set_ylabel(title, fontsize=12)

    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
        ax.set_ylim(*YLIM_LOG)
    else:
        ax.set_ylim(*YLIM_LINEAR)
        ax.ticklabel_format(style='plain', axis='y', useOffset=False)

    ncol = ceil(len(bufs) / 2)
    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.18),
        ncol=ncol,
        frameon=False,
        fontsize=9
    )

    plt.tight_layout(rect=[0, 0.18, 1, 0.9])
    plt.savefig(out_dir / fname, dpi=300)
    plt.close(fig)

# draw your plots
combined_bar(
    ["lat_insert", "lat_pq", "lat_rq"],
    "Mean Operation Latency (ns/op)",
    "latency_combined.png"
)
combined_bar(
    ["thr_insert", "thr_pq", "thr_rq"],
    "Throughput (ops/min)",
    "throughput_combined.png"
)

print(f"[DONE] CSV + plots saved in {out_dir} (log scale = {USE_LOG_SCALE})")
