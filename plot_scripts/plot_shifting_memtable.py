#!/usr/bin/env python3

import json
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D

sys.path.insert(0, str(Path(__file__).resolve().parent))
from style import configure_matplotlib_style, latex_escape, MARKER_SIZE, LINE_WIDTH
from memtable_style import (
    MEMTABLE_DISPLAY_NAMES,
    MEMTABLE_COLORS,
    MEMTABLE_LINESTYLES,
    MEMTABLE_MARKERS,
)

configure_matplotlib_style(plt, fm)

WINDOW = 10000
PLOT_POINTS = 5

RUN_DIR = Path(__file__).resolve().parent.parent / ".vstats" / "switch-memtable-exp2-revision"
SPEC_FILE = RUN_DIR / "workload.specs.json"

COMPARISON_TAG = "adaptive-no-hashlinklist_vs_skiplist"
PLOT_DIR = RUN_DIR / "plots" / COMPARISON_TAG
OUT_FILE = PLOT_DIR / "throughput_rolling.pdf"
LEGEND_FILE = PLOT_DIR / "throughput_rolling_legend.pdf"

PHASES = None
SKIP_PHASES: set = {2}

SKIP: set = {
    "adaptive", "simpleskiplist",
    "vector-preallocated", "hashskiplist-H100000-X6", "sortedvector-preallocated",
    "unsortedvector-preallocated", "hashvector-H100000-X6", "hashlinkedlist-H100000-X6",
}

IMPL_ORDER = [
    "adaptive-no-hashlinklist",
    "skiplist",
]

FOLDER_TO_CANONICAL = {
    "adaptive": "adaptive",
    "adaptive-no-hashlinklist": "adaptive",
    "simpleskiplist": "simple_skiplist",
    "skiplist": "skiplist",
    "vector-preallocated": "vector",
    "unsortedvector-preallocated": "unsorted_vector",
    "sortedvector-preallocated": "sorted_vector",
    "hashskiplist-H100000-X6": "hash_skiplist",
    "hashvector-H100000-X6": "hash_vector",
    "hashlinkedlist-H100000-X6": "hash_linklist",
}

_OP_FIELDS = [
    "inserts", "unique_inserts",
    "updates", "merges",
    "point_queries", "empty_point_queries",
    "point_deletes", "empty_point_deletes",
    "range_queries", "range_deletes",
    "blind_point_queries", "blind_point_deletes", "blind_range_queries",
]


def _eval_number_expr(expr) -> float:
    if isinstance(expr, (int, float)):
        return float(expr)
    if not isinstance(expr, dict):
        return 0.0
    if "uniform" in expr: d = expr["uniform"]; return (d["min"] + d["max"]) / 2.0
    if "normal" in expr: return float(expr["normal"]["mean"])
    if "log_normal" in expr: d = expr["log_normal"]; return math.exp(d["mean"] + d["std_dev"]**2 / 2.0)
    if "exponential" in expr: lam = expr["exponential"]["lambda"]; return 1.0 / lam if lam else 0.0
    if "poisson" in expr: return float(expr["poisson"]["lambda"])
    if "beta" in expr: d = expr["beta"]; return d["alpha"] / (d["alpha"] + d["beta"])
    if "weibull" in expr: d = expr["weibull"]; return d["scale"] * math.gamma(1.0 + 1.0 / d["shape"])
    if "pareto" in expr:
        d = expr["pareto"]
        return d["scale"] * d["shape"] / (d["shape"] - 1.0) if d["shape"] > 1.0 else float("inf")
    if "zipf" in expr: n = expr["zipf"]["n"]; return (n + 1) / 2.0
    print(f"unknown NumberExpr: {list(expr.keys())}", file=sys.stderr)
    return 0.0


def phases_from_spec(spec_path: Path) -> list:
    with open(spec_path) as f:
        spec = json.load(f)
    phases, cursor = [], 0
    for sec_idx, section in enumerate(spec.get("sections", [])):
        for grp_idx, group in enumerate(section.get("groups", [])):
            name = group.get("name") or f"section {sec_idx+1} group {grp_idx+1}"
            ops = sum(
                int(_eval_number_expr(group[f].get("op_count", 0)))
                for f in _OP_FIELDS if f in group
            )
            phases.append((name, cursor, cursor + ops))
            cursor += ops
    return phases


_LATENCY_RE = re.compile(rb'^[A-Z]+:\s+(\d+)', re.MULTILINE)


def read_latencies(path: Path) -> np.ndarray:
    cache = path.with_suffix(".npy")
    if cache.exists() and cache.stat().st_mtime >= path.stat().st_mtime:
        return np.load(cache)
    arr = np.array(_LATENCY_RE.findall(path.read_bytes()), dtype=np.float64)
    np.save(cache, arr)
    return arr


def build_rolling_series(latencies: np.ndarray, phases: list, window: int, plot_points: int):
    kernel = np.ones(window, dtype=np.float64) / window
    xs_all, ys_all = [], []
    for phase_idx, (_, start, end) in enumerate(phases):
        seg = latencies[start:end]
        if len(seg) < window:
            continue
        smoothed = np.convolve(1e3 / seg, kernel, mode="valid")
        stride = max(1, len(smoothed) // plot_points)
        sampled = smoothed[::stride]
        x_local = np.linspace(phase_idx * plot_points, (phase_idx + 1) * plot_points, len(sampled), endpoint=False)
        xs_all.append(x_local)
        ys_all.append(sampled)
    if not xs_all:
        return np.array([]), np.array([])
    return np.concatenate(xs_all), np.concatenate(ys_all)


def main():
    active_phases = PHASES
    if active_phases is None:
        if not SPEC_FILE.exists():
            print(f"spec file not found: {SPEC_FILE}", file=sys.stderr)
            sys.exit(1)
        active_phases = phases_from_spec(SPEC_FILE)

    active_phases = [p for i, p in enumerate(active_phases, 1) if i not in SKIP_PHASES]
    PLOT_DIR.mkdir(parents=True, exist_ok=True)

    print("phases:")
    for name, start, end in active_phases:
        print(f"  [{start:>8,} - {end:>8,}]  {end-start:>7,} ops   {name}")
    print()

    impl_dirs = sorted(
        d for d in RUN_DIR.iterdir()
        if d.is_dir() and (d / "stats.log").exists()
    )
    if not impl_dirs:
        print(f"no stats.log files found under {RUN_DIR}", file=sys.stderr)
        sys.exit(1)

    active_dirs = [d for d in impl_dirs if d.name not in SKIP]
    active_dirs.sort(key=lambda d: IMPL_ORDER.index(d.name) if d.name in IMPL_ORDER else 999)
    for d in impl_dirs:
        if d.name in SKIP:
            print(f"  skipping {d.name} (in SKIP)")

    def _load(impl_dir):
        return impl_dir, read_latencies(impl_dir / "stats.log")

    def _cache_exists(d):
        c, s = d / "stats.npy", d / "stats.log"
        return c.exists() and c.stat().st_mtime >= s.stat().st_mtime

    cached = sum(1 for d in active_dirs if _cache_exists(d))
    t0 = time.perf_counter()
    results = {}
    with ThreadPoolExecutor() as pool:
        for fut in [pool.submit(_load, d) for d in active_dirs]:
            impl_dir, lats = fut.result()
            results[impl_dir] = lats
    print(f"  loaded {len(active_dirs)} impls in {time.perf_counter()-t0:.2f}s "
          f"({cached} from cache, {len(active_dirs)-cached} parsed)\n")

    _, ax = plt.subplots(figsize=(8, 2.8))

    legend_handles = []
    for impl_dir in active_dirs:
        canonical = FOLDER_TO_CANONICAL.get(impl_dir.name, impl_dir.name)
        color = MEMTABLE_COLORS.get(canonical, "black")
        linestyle = MEMTABLE_LINESTYLES.get(canonical, "solid")
        marker = MEMTABLE_MARKERS.get(canonical, "x")
        label = latex_escape(MEMTABLE_DISPLAY_NAMES.get(canonical, canonical))

        xs, ys = build_rolling_series(results[impl_dir], active_phases, WINDOW, PLOT_POINTS)
        if xs.size == 0:
            print(f"  skipping {impl_dir.name}: no phase long enough for window={WINDOW}")
            continue

        ax.plot(
            xs, ys,
            color=color, linestyle=linestyle, linewidth=LINE_WIDTH,
            marker=marker, markersize=MARKER_SIZE, markerfacecolor="none",
            markevery=PLOT_POINTS // 4,
        )

        legend_handles.append(Line2D(
            [], [],
            color=color, linestyle=linestyle, linewidth=LINE_WIDTH,
            marker=marker, markersize=MARKER_SIZE, markerfacecolor="none",
            label=label,
        ))

        valid = ys[~np.isnan(ys)]
        print(f"  {label:20s}  median={np.median(valid):>8,.2f} Mops  "
              f"p95={np.percentile(valid, 95):>8,.2f} Mops")

    for i in range(1, len(active_phases)):
        ax.axvline(i * PLOT_POINTS, color="grey", linewidth=0.8, linestyle="--", alpha=0.55)

    phase_mids = [(i + 0.5) * PLOT_POINTS for i in range(len(active_phases))]
    ax.set_xticks(phase_mids)
    ax.set_xticklabels([f"P{i+1}" for i in range(len(active_phases))])
    ax.set_ylim(0, 2)
    ax.set_yticks([0, 1, 2], ["0", "1", "2"])
    ax.tick_params(direction="out")

    ax.set_ylabel("throughput (Mops)", labelpad=-0.5, y=0.42)

    plt.savefig(OUT_FILE, bbox_inches="tight", pad_inches=0.02)
    print(f"\nsaved -> {OUT_FILE}")

    fig_legend = plt.figure(figsize=(6, 0.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(
        handles=legend_handles,
        loc="center",
        ncol=5,
        frameon=True,
        edgecolor="none",
        borderaxespad=0,
        labelspacing=0.1,
        borderpad=0,
        handlelength=1,
        columnspacing=0.2,
        handletextpad=0.1,
    )
    fig_legend.savefig(LEGEND_FILE, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"saved -> {LEGEND_FILE}")


if __name__ == "__main__":
    main()
