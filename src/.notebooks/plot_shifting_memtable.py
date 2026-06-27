#!/usr/bin/env python3
"""
Rolling-window throughput comparison across all memtable implementations.

For each phase, instantaneous throughput (1e3 / latency_ns = MOPS) is smoothed
with a rolling average of WINDOW ops, then every DOWNSAMPLE-th point is plotted.
The window resets at each phase boundary.
"""

import json
import math
import re
import sys
import time
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from plot import style
_LINE_STYLES = style.line_styles

plt.rcParams["text.usetex"] = True
plt.rcParams["font.size"]   = 22

# ---------------------------------------------------------------------------
# *** Tune these before each run ***
# ---------------------------------------------------------------------------

WINDOW      = 10000  # rolling average width in ops — controls smoothness
PLOT_POINTS = 5    # number of points to plot per phase

RUN_DIR   = Path(__file__).resolve().parent.parent.parent / ".vstats" / "switch-memtable-exp2"
SPEC_FILE = RUN_DIR / "workload.specs.json"
OUT_FILE  = RUN_DIR / "throughput_rolling.pdf"
LEGEND_FILE = RUN_DIR / "throughput_rolling_legend.pdf"

PHASES = None

# 1-based phase indices to drop entirely.
SKIP_PHASES: set = set()

SKIP: set = {"vector-preallocated", "hashskiplist-H100000-X6"} # "hashvector-H100000-X6"

IMPL_ORDER = [
    "dynamic-run",
    "unsortedvector-preallocated",
    "skiplist",
    "hashlinkedlist-H100000-X6",
    "hashvector-H100000-X6",
]

STYLE_KEYS = {
    "dynamic-run": "dynamic",
    "skiplist":                    "skiplist",
    "vector-preallocated":         "vector",
    "hashskiplist-H100000-X6":     "hashskiplist",
    "hashlinkedlist-H100000-X6":   "hashlinkedlist",
    "unsortedvector-preallocated": "unsortedvector",
    "hashvector-H100000-X6":       "hashvector",
}
_DEFAULT_STYLE = {
    "label": "dynamic",
    "color": "black", "linestyle": "solid", "linewidth": 2,
    "marker": "x", "markersize": 12, "markerfacecolor": "none",
}

_OP_FIELDS = [
    "inserts", "unique_inserts",
    "updates", "merges",
    "point_queries", "empty_point_queries",
    "point_deletes", "empty_point_deletes",
    "range_queries", "range_deletes",
    "blind_point_queries", "blind_point_deletes", "blind_range_queries",
]

# ---------------------------------------------------------------------------
# Spec parsing
# ---------------------------------------------------------------------------

def _eval_number_expr(expr) -> float:
    if isinstance(expr, (int, float)):
        return float(expr)
    if not isinstance(expr, dict):
        return 0.0
    if "uniform"    in expr: d = expr["uniform"];    return (d["min"] + d["max"]) / 2.0
    if "normal"     in expr: return float(expr["normal"]["mean"])
    if "log_normal" in expr: d = expr["log_normal"]; return math.exp(d["mean"] + d["std_dev"]**2 / 2.0)
    if "exponential"in expr: lam = expr["exponential"]["lambda"]; return 1.0/lam if lam else 0.0
    if "poisson"    in expr: return float(expr["poisson"]["lambda"])
    if "beta"       in expr: d = expr["beta"];       return d["alpha"] / (d["alpha"] + d["beta"])
    if "weibull"    in expr: d = expr["weibull"];    return d["scale"] * math.gamma(1.0 + 1.0/d["shape"])
    if "pareto"     in expr:
        d = expr["pareto"]
        return d["scale"]*d["shape"]/(d["shape"]-1.0) if d["shape"] > 1.0 else float("inf")
    if "zipf"       in expr: n = expr["zipf"]["n"]; return (n + 1) / 2.0
    print(f"  [warn] unknown NumberExpr: {list(expr.keys())} — treating as 0", file=sys.stderr)
    return 0.0


def phases_from_spec(spec_path: Path) -> list:
    with open(spec_path) as f:
        spec = json.load(f)
    phases, cursor = [], 0
    for sec_idx, section in enumerate(spec.get("sections", [])):
        for grp_idx, group in enumerate(section.get("groups", [])):
            name = group.get("name") or f"Section {sec_idx+1}  Group {grp_idx+1}"
            ops  = sum(
                int(_eval_number_expr(group[f].get("op_count", 0)))
                for f in _OP_FIELDS if f in group
            )
            phases.append((name, cursor, cursor + ops))
            cursor += ops
    return phases

# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

_LATENCY_RE = re.compile(rb'^[A-Z]+:\s+(\d+)', re.MULTILINE)

def read_latencies(path: Path) -> np.ndarray:
    cache = path.with_suffix('.npy')
    if cache.exists() and cache.stat().st_mtime >= path.stat().st_mtime:
        return np.load(cache)
    arr = np.array(_LATENCY_RE.findall(path.read_bytes()), dtype=np.float64)
    np.save(cache, arr)
    return arr


def build_rolling_series(latencies: np.ndarray, phases: list,
                         window: int, plot_points: int):
    """
    Per phase:
      1. Instantaneous throughput = 1e3 / latency_ns  (MOPS)
      2. Rolling average over `window` ops via convolution (resets per phase)
      3. Downsample to exactly `plot_points` evenly spaced points per phase
    Returns (xs, ys) with xs as global op indices.
    """
    kernel = np.ones(window, dtype=np.float64) / window
    xs_all, ys_all = [], []
    for phase_idx, (_, start, end) in enumerate(phases):
        seg = latencies[start:end]
        if len(seg) < window:
            continue
        smoothed = np.convolve(1e3 / seg, kernel, mode='valid')
        idx      = np.linspace(0, len(smoothed) - 1, plot_points, dtype=int)
        sampled  = smoothed[idx]
        x_local  = np.linspace(phase_idx * plot_points,
                               (phase_idx + 1) * plot_points,
                               plot_points, endpoint=False)
        xs_all.append(x_local)
        ys_all.append(sampled)
    if not xs_all:
        return np.array([]), np.array([])
    return np.concatenate(xs_all), np.concatenate(ys_all)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    active_phases = PHASES
    # if active_phases is None:
    #     if not SPEC_FILE.exists():
    #         print(f"Spec file not found: {SPEC_FILE}", file=sys.stderr)
    #         sys.exit(1)
    #     active_phases = phases_from_spec(SPEC_FILE)

    # active_phases = [p for i, p in enumerate(active_phases, 1) if i not in SKIP_PHASES]

    # print("Phases:")
    # for name, start, end in active_phases:
    #     print(f"  [{start:>8,} – {end:>8,}]  {end-start:>7,} ops   {name}")
    # print()

    impl_dirs = sorted(
        d for d in RUN_DIR.iterdir()
        if d.is_dir() and (d / "stats.log").exists()
    )
    if not impl_dirs:
        print(f"No stats.log files found under {RUN_DIR}", file=sys.stderr)
        sys.exit(1)

    active_dirs = [d for d in impl_dirs if d.name not in SKIP]
    # active_dirs.sort(key=lambda d: IMPL_ORDER.index(d.name) if d.name in IMPL_ORDER else 999)
    # for d in impl_dirs:
    #     if d.name in SKIP:
    #         print(f"  skipping {d.name} (in SKIP)")

    # def _load(impl_dir):
    #     return impl_dir, read_latencies(impl_dir / "stats.log")

    # def _cache_exists(d):
    #     c, s = d / "stats.npy", d / "stats.log"
    #     return c.exists() and c.stat().st_mtime >= s.stat().st_mtime

    # cached = sum(1 for d in active_dirs if _cache_exists(d))
    # t0 = time.perf_counter()
    # results = {}
    # with ThreadPoolExecutor() as pool:
    #     for fut in [pool.submit(_load, d) for d in active_dirs]:
    #         impl_dir, lats = fut.result()
    #         results[impl_dir] = lats
    # print(f"  loaded {len(active_dirs)} impls in {time.perf_counter()-t0:.2f}s "
    #       f"({cached} from cache, {len(active_dirs)-cached} parsed)\n")

    # _, ax = plt.subplots(figsize=(10, 2.8))

    legend_handles = []
    for impl_dir in active_dirs:
        st  = {**_DEFAULT_STYLE, **_LINE_STYLES.get(STYLE_KEYS.get(impl_dir.name, ""), {})}
        # xs, ys = build_rolling_series(results[impl_dir], active_phases, WINDOW, PLOT_POINTS)
        # if xs.size == 0:
        #     print(f"  skipping {impl_dir.name}: no phase long enough for window={WINDOW}")
        #     continue

        # ax.plot(
        #     xs, ys,
        #     color=st["color"],
        #     linestyle="solid",
        #     linewidth=st["linewidth"],
        #     marker=st["marker"],
        #     markersize=st["markersize"]-2,
        #     markerfacecolor=st["markerfacecolor"],
        #     markevery=1,
        # )

        legend_handles.append(Line2D(
            [], [],
            color=st["color"],
            linestyle="solid",
            linewidth=st["linewidth"],
            marker=st["marker"],
            markersize=st["markersize"],
            markerfacecolor=st["markerfacecolor"],
            label=st["label"],
        ))

    #     valid = ys[~np.isnan(ys)]
    #     print(f"  {st['label']:30s}  median={np.median(valid):>8,.2f} MOPS  "
    #           f"p95={np.percentile(valid, 95):>8,.2f} MOPS")

    # for i in range(1, len(active_phases)):
    #     ax.axvline(i * PLOT_POINTS, color="grey", linewidth=0.8, linestyle="--", alpha=0.55)

    # phase_mids = [(i + 0.5) * PLOT_POINTS for i in range(len(active_phases))]
    # ax.set_xticks(phase_mids)
    # ax.set_xticklabels([f"P{i+1}" for i in range(len(active_phases))])
    # # ax.set_xlim(0, len(active_phases) * PLOT_POINTS)
    # ax.set_ylim(0, 2)
    # ax.set_yticks([0, 1, 2], ["0", "1", "2"])

    # ax.set_ylabel("throughput (MOpS)", labelpad=-0.5, y=0.42, fontsize=22)

    # plt.savefig(OUT_FILE, bbox_inches="tight", pad_inches=0.02)
    # print(f"\nSaved → {OUT_FILE}")

    # --- separate legend ---
    fig_legend = plt.figure(figsize=(6, 0.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(
        handles=legend_handles,
        loc="center",
        ncol=5,
        frameon=False,
        borderaxespad=0,
        labelspacing=0.1,
        borderpad=0,
        handlelength=1,
        columnspacing=0.5,
        handletextpad=0.1,
    )
    fig_legend.savefig(LEGEND_FILE, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved → {LEGEND_FILE}")


def dump_phase_stats():
    active_phases = PHASES
    if active_phases is None:
        active_phases = phases_from_spec(SPEC_FILE)
    active_phases = [p for i, p in enumerate(active_phases, 1) if i not in SKIP_PHASES]

    impl_dirs = sorted(
        d for d in RUN_DIR.iterdir()
        if d.is_dir() and (d / "stats.log").exists()
    )
    active_dirs = [d for d in impl_dirs if d.name not in SKIP]
    active_dirs.sort(key=lambda d: IMPL_ORDER.index(d.name) if d.name in IMPL_ORDER else 999)

    results = {}
    with ThreadPoolExecutor() as pool:
        for fut in [pool.submit(lambda d: (d, read_latencies(d / "stats.log")), d)
                    for d in active_dirs]:
            impl_dir, lats = fut.result()
            results[impl_dir] = lats

    # per-phase mean MOPS for each impl
    kernel = np.ones(WINDOW, dtype=np.float64) / WINDOW
    phase_stats: dict[int, dict[str, dict]] = {}

    for phase_idx, (phase_name, start, end) in enumerate(active_phases):
        phase_stats[phase_idx] = {}
        for impl_dir in active_dirs:
            lats = results[impl_dir]
            seg  = lats[start:end]
            if len(seg) < WINDOW:
                continue
            smoothed = np.convolve(1e3 / seg, kernel, mode="valid")
            st       = {**_DEFAULT_STYLE, **_LINE_STYLES.get(STYLE_KEYS.get(impl_dir.name, ""), {})}
            phase_stats[phase_idx][impl_dir.name] = {
                "label":  st["label"],
                "mean":   float(smoothed.mean()),
                "median": float(np.median(smoothed)),
                "p5":     float(np.percentile(smoothed, 5)),
                "p95":    float(np.percentile(smoothed, 95)),
            }

    col_w = 22
    for phase_idx, (phase_name, _, _) in enumerate(active_phases):
        stats = phase_stats[phase_idx]
        if not stats:
            continue

        ranked = sorted(stats.items(), key=lambda kv: kv[1]["mean"], reverse=True)
        _, best   = ranked[0]
        _, second = ranked[1] if len(ranked) > 1 else (None, None)

        print(f"\n{'='*70}")
        print(f"Phase {phase_idx+1}: {phase_name}")
        print(f"{'='*70}")
        print(f"  {'impl':<{col_w}}  {'mean (MOPS)':>12}  {'median':>10}  {'p5':>8}  {'p95':>8}")
        print(f"  {'-'*col_w}  {'-'*12}  {'-'*10}  {'-'*8}  {'-'*8}")
        for rank, (_, s) in enumerate(ranked, 1):
            marker = " ← best" if rank == 1 else ""
            print(f"  {s['label']:<{col_w}}  {s['mean']:>12.4f}  {s['median']:>10.4f}"
                  f"  {s['p5']:>8.4f}  {s['p95']:>8.4f}{marker}")

        print(f"\n  Winner: {best['label']}")
        print(f"    mean  = {best['mean']:.4f} MOPS")
        if second:
            abs_margin = best["mean"] - second["mean"]
            pct_margin = abs_margin / second["mean"] * 100
            print(f"    vs {second['label']}: +{abs_margin:.4f} MOPS ({pct_margin:+.1f}%)")

    print()


if __name__ == "__main__":
    main()
    # dump_phase_stats()
