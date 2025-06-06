#!/usr/bin/env python3
import re
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import LogFormatterMathtext

RAWOP_ROOT = Path("/home/cc/LSMMemoryProfiling/.result/6_7_insert_only_rawop_low_pri_false_dynamic_vec_memtable_profile")
PARAM_RE = re.compile(r"I(?P<insert>\d+)-Q(?P<point>\d+)-U\d+-S(?P<range>\d+)-Y(?P<selectivity>[\d\.]+)-T(?P<threads>\d+)")
TIME_RE = re.compile(r"^(?P<kind>(Inserts|PointQuery|RangeQuery)) Execution Time:\s*(?P<val>\d+)")

records = []
for log_path in RAWOP_ROOT.rglob("workload.log"):
    struct_dir = log_path.parent.parent
    buffer_kind = struct_dir.name.split("-")[0]
    m = PARAM_RE.search(struct_dir.name)
    if not m:
        print(f"[SOmething Wrong] folder name unparsable: {struct_dir.name}")
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
    safe = lambda a, b: a / b if (a and b) else 0
    FACT_MIN = 60_000_000_000
    thr_ins = safe(n_ins * FACT_MIN, exec_ns["Inserts"])
    thr_pq = safe(n_pq * FACT_MIN, exec_ns["PointQuery"])
    thr_rq = safe(n_rq * FACT_MIN, exec_ns["RangeQuery"])
    lat_ins = safe(exec_ns["Inserts"], n_ins)
    lat_pq = safe(exec_ns["PointQuery"], n_pq)
    lat_rq = safe(exec_ns["RangeQuery"], n_rq)
    print(f"\n=== DEBUG =========================================================")
    print(log_path)
    print(f"buffer kind:   {buffer_kind}")
    print(f"Counts:        I={n_ins}  Q={n_pq}  S={n_rq}")
    print(f"Exec-time (ns): Inserts={exec_ns['Inserts']}  PointQuery={exec_ns['PointQuery']}  RangeQuery={exec_ns['RangeQuery']}")
    print(f"THR (ops/min): ins={thr_ins:.2f}  pq={thr_pq:.2f}  rq={thr_rq:.4f}")
    print(f"LAT (ns/op):   ins={lat_ins:.2f}  pq={lat_pq:.2f}  rq={lat_rq:.2f}")
    print("===================================================================\n")
    records.append({
        "buffer":      buffer_kind,
        "lat_insert":  lat_ins,
        "lat_pq":      lat_pq,
        "lat_rq":      lat_rq,
        "thr_insert":  thr_ins,
        "thr_pq":      thr_pq,
        "thr_rq":      thr_rq,
    })

if not records:
    sys.exit("No workload.log parsed.")

df = pd.DataFrame(records)

print("\n--- AGGREGATED VALUES USED FOR PLOTS --------------------------------")
print(df[["buffer", "lat_insert", "lat_pq", "lat_rq", "thr_insert", "thr_pq", "thr_rq"]].to_string(index=False))
print("---------------------------------------------------------------------\n")

out_dir = RAWOP_ROOT / "plots"
out_dir.mkdir(exist_ok=True)
df.to_csv(out_dir / "rawop_metrics.csv", index=False)

def combined_bar(metric_cols, title, _ylabel, fname, log=False):
    cats = ["insert", "point queries", "range queries"]
    bufs = sorted(df["buffer"].unique())
    bar_w = 0.8 / len(bufs)

    # Use a unique hatch for each buffer type
    hatch_map = {
        "AlwayssortedVector": "",        # empty
        "UnsortedVector": "//",
        "hash_linked_list": "\\\\",
        "hash_skip_list": "xx",
        "skiplist": "++",
        "vector": "--"                  # distinguish vector clearly
    }

    fig, ax = plt.subplots(figsize=(10, 4))
    for i, buf in enumerate(bufs):
        vals = [df.loc[df["buffer"] == buf, m].mean() for m in metric_cols]
        xs = [x + i * bar_w for x in range(len(cats))]
        hatch = hatch_map.get(buf, "||")
        # Set facecolor light gray for vector, white otherwise
        facecolor = "0.9" if buf == "vector" else "white"
        ax.bar(xs, vals, width=bar_w, edgecolor="black",
               facecolor=facecolor, hatch=hatch, lw=1,
               label=buf.replace("_", " "))

    ax.set_xticks([x + (len(bufs) - 1) * bar_w / 2 for x in range(len(cats))])
    ax.set_xticklabels(cats, fontsize=11)
    ax.set_ylabel(title, fontsize=12)
    ax.set_ylim(bottom=10e1)

    if log:
        ax.set_yscale("log", base=10)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
        ax.set_ylim(bottom=10)

    ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.15),
              ncol=len(bufs), frameon=False, fontsize=9)
    ax.grid(False)
    plt.tight_layout()
    plt.savefig(out_dir / fname, dpi=300)
    plt.close()

combined_bar(
    ["lat_insert", "lat_pq", "lat_rq"],
    "Mean Operation Latency (ns/op)",
    None,
    "latency_combined.png",
    log=True
)
combined_bar(
    ["thr_insert", "thr_pq", "thr_rq"],
    "Throughput (ops/min)",
    None,
    "throughput_combined.png",
    log=True
)

print(f"[DONE] CSV + plots saved in {out_dir}")
