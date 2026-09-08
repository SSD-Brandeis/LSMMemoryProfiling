#!/usr/bin/env python3

import csv
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from style import configure_matplotlib_style, latex_escape
from memtable_style import (
    MEMTABLE_DISPLAY_NAMES,
    MEMTABLE_COLORS,
    MEMTABLE_HATCHES,
)

configure_matplotlib_style(plt, fm)

RUN_DIR = Path(__file__).resolve().parent.parent / ".vstats" / "switch-memtable-exp2-revision"
COMPARISON_TAG = "adaptive-no-hashlinklist_vs_skiplist"
PLOT_DIR = RUN_DIR / "plots" / COMPARISON_TAG
CSV_PATH = PLOT_DIR / "write_latency_per_phase.csv"

OUT_FILE = PLOT_DIR / "write_latency_p95_per_phase_bar.pdf"
LEGEND_FILE = PLOT_DIR / "write_latency_p95_per_phase_bar_legend.pdf"


def main():
    rows = list(csv.DictReader(open(CSV_PATH)))
    phases = [r["phase"] for r in rows]
    adaptive_vals = np.array([float(r["adaptive_p95_us"]) for r in rows])
    skiplist_vals = np.array([float(r["skiplist_p95_us"]) for r in rows])

    a_color = MEMTABLE_COLORS["adaptive"]
    a_hatch = MEMTABLE_HATCHES["adaptive"]
    a_label = latex_escape(MEMTABLE_DISPLAY_NAMES["adaptive"])
    s_color = MEMTABLE_COLORS["skiplist"]
    s_hatch = MEMTABLE_HATCHES["skiplist"]
    s_label = latex_escape(MEMTABLE_DISPLAY_NAMES["skiplist"])

    x = np.arange(len(phases))
    width = 0.35

    _, ax = plt.subplots(1, 1, figsize=(9, 2.8))
    ax.bar(x - width / 2, adaptive_vals, width, color="none", edgecolor=a_color, hatch=a_hatch, linewidth=1.5)
    ax.bar(x + width / 2, skiplist_vals, width, color="none", edgecolor=s_color, hatch=s_hatch, linewidth=1.5)

    ax.set_xticks(x)
    ax.set_xticklabels(phases)
    ax.set_ylim(0, None)
    ax.tick_params(direction="out")
    ax.set_ylabel("write p95 latency ($\\mu$s)")

    plt.savefig(OUT_FILE, bbox_inches="tight", pad_inches=0.15)
    print(f"saved -> {OUT_FILE}")

    fig_legend = plt.figure(figsize=(4, 0.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(
        handles=[
            Patch(facecolor="none", edgecolor=a_color, hatch=a_hatch, linewidth=1.5, label=a_label),
            Patch(facecolor="none", edgecolor=s_color, hatch=s_hatch, linewidth=1.5, label=s_label),
        ],
        loc="center", ncol=2, frameon=True, edgecolor="none",
        borderaxespad=0, labelspacing=0.1, borderpad=0,
        handlelength=1.5, columnspacing=0.5, handletextpad=0.3,
    )
    fig_legend.savefig(LEGEND_FILE, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"saved -> {LEGEND_FILE}")

    max_pct_worse = max(
        (a - s) / s * 100 for a, s in zip(adaptive_vals, skiplist_vals)
    )
    print(f"\nmax pct by which adaptive p95 exceeds skiplist p95 across all phases: {max_pct_worse:+.2f}%")


if __name__ == "__main__":
    main()
