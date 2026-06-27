import os
from pathlib import Path

from plot import *

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm

TAG = "vector-get-timer-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG / "vector-preallocated"

WINDOW = 2

# Columns to plot and their display names (op_id is the index, skip it)
FUNCTIONS = [
    ("getimpl_ns",        "Get (total)"),
    ("memtable_get_ns",   "memtable Get"),
    ("get_from_table_ns", "get from table"),
    ("vectorrep_get_ns",  "VectorRep::Get"),
    ("lock_ns",           "lock"),
    ("bucket_copy_ns",    "bucket copy"),
    ("seek_ns",           "seek"),
    ("callback_ns",       "callback"),
    # bloom_ns is always 0 for vector — omitted
]


def load_fnbreakdown(log_file: Path) -> dict[str, np.ndarray]:
    cache_dir = log_file.parent
    cache_mtime = log_file.stat().st_mtime

    data = {}
    all_cached = True
    for col, _ in FUNCTIONS:
        cache = cache_dir / f"fnbd_{col}.npy"
        if not cache.exists() or cache.stat().st_mtime < cache_mtime:
            all_cached = False
            break

    if all_cached:
        for col, _ in FUNCTIONS:
            data[col] = np.load(cache_dir / f"fnbd_{col}.npy")
        print(f"Loaded {len(next(iter(data.values())))} rows from cache")
        return data

    print(f"Parsing {log_file} ...")
    cols = {col: [] for col, _ in FUNCTIONS}
    with open(log_file) as f:
        header = f.readline().strip().split(",")
        col_idx = {name: i for i, name in enumerate(header)}
        for line in f:
            parts = line.strip().split(",")
            for col, _ in FUNCTIONS:
                cols[col].append(int(parts[col_idx[col]]))

    for col, _ in FUNCTIONS:
        arr = np.array(cols[col])
        np.save(cache_dir / f"fnbd_{col}.npy", arr)
        data[col] = arr

    print(f"Parsed and cached {len(next(iter(data.values())))} rows")
    return data


def rolling_mean(arr: np.ndarray, window: int) -> tuple[np.ndarray, np.ndarray]:
    half = window // 2
    n = len(arr)
    x, mean = [], []
    for i in range(half, n - half):
        x.append(i + 1)
        mean.append(np.mean(arr[i - half: i + half]))
    return np.array(x), np.array(mean)


BREAKDOWN_SHOW = [
    "getimpl_ns",
    "memtable_get_ns",
    "get_from_table_ns",
    "vectorrep_get_ns",
    # "lock_ns",
    # "bucket_copy_ns",
    "seek_ns",
    # "callback_ns",
]


FN_BREAKDOWN_EXP = r"$\times10^{7}$"


def plot_fn_breakdown():
    log_file = EXP_DIR / "fnbreakdown.log"
    if not log_file.exists():
        print(f"Missing: {log_file}")
        return

    data = load_fnbreakdown(log_file)

    visible = [(col, label) for col, label in FUNCTIONS if col in BREAKDOWN_SHOW]
    colors = cm.tab10(np.linspace(0, 1, len(visible)))

    fig, ax = plt.subplots(figsize=(4, 2.8))

    for (col, label), color in zip(visible, colors):
        arr = data[col][:10000]
        x, mean = rolling_mean(arr, WINDOW)
        ax.plot(x, mean, linewidth=1.4, label=label, color=color, alpha=0.8)

    n = len(data[visible[0][0]])
    # ax.set_xlim(0, n)
    ax.set_ylim(bottom=0)
    step = n // 4
    ax.set_xticks([0, step, step * 2, step * 3, n],
                  ["0", f"{step//1000}", f"{step*2//1000}",
                   f"{step*3//1000}", f"{n//1000}"])
    ax.set_xlabel("point query (k)", labelpad=2)
    ax.set_ylabel("latency (ns)", labelpad=2)
    ax.yaxis.offsetText.set_visible(False)
    ax.text(0.02, 0.97, FN_BREAKDOWN_EXP, transform=ax.transAxes,
            fontsize=20, va="top", ha="left")
    ax.legend(frameon=False, loc="lower right", fontsize=19,
              labelspacing=0.2, handlelength=1.2)

    output_file = CURR_DIR / TAG / "vector-fn-breakdown.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def _plot_inner_panel(data, series, output_name, exp_text=None,
                      yticks=None, yticklabels=None):
    colors = cm.tab10(np.linspace(0, 0.9, len(series)))
    fig, ax = plt.subplots(figsize=(5, 2.8))
    n = len(data[series[0][0]])
    for (col, label), color in zip(series, colors):
        x, mean = rolling_mean(data[col], WINDOW)
        ax.plot(x, mean, linewidth=1.4, label=label, color=color, alpha=0.8)
    step = n // 4
    ax.set_xlim(0, n)
    ax.set_ylim(bottom=0)
    ax.set_xticks([0, step, step * 2, step * 3, n],
                  ["0", f"{step//1000}", f"{step*2//1000}",
                   f"{step*3//1000}", f"{n//1000}"])
    if yticks is not None:
        ax.set_yticks(yticks)
        ax.set_yticklabels(yticklabels if yticklabels is not None else [str(t) for t in yticks])
        ax.yaxis.offsetText.set_visible(False)
    ax.set_xlabel("point query (k)", labelpad=2)
    ax.set_ylabel("latency (ns)", labelpad=2)
    if exp_text:
        ax.text(0.02, 0.97, exp_text, transform=ax.transAxes,
                fontsize=20, va="top", ha="left")
    ax.legend(frameon=False, fontsize=7, loc="upper right",
              labelspacing=0.2, handlelength=1.2)
    output_file = CURR_DIR / TAG / output_name
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_fn_breakdown_lock_callback():
    log_file = EXP_DIR / "fnbreakdown.log"
    if not log_file.exists():
        print(f"Missing: {log_file}")
        return
    data = load_fnbreakdown(log_file)
    _plot_inner_panel(data, [
        ("lock_ns",     "lock"),
        ("callback_ns", "callback"),
    ], "vector-fn-lock-callback.pdf",
        exp_text=r"$\times10^{2}$",
        yticks=[0, 2500, 5000, 7500],
        yticklabels=["0", "25", "50", "75"],
    )


def plot_fn_breakdown_copy_seek():
    log_file = EXP_DIR / "fnbreakdown.log"
    if not log_file.exists():
        print(f"Missing: {log_file}")
        return
    data = load_fnbreakdown(log_file)
    _plot_inner_panel(data, [
        ("bucket_copy_ns", "bucket copy"),
        ("seek_ns",        "seek"),
    ], "vector-fn-copy-seek.pdf",
        exp_text=r"$\times10^{7}$",
    )


def plot_fn_time_percentage():
    log_file = EXP_DIR / "fnbreakdown.log"
    if not log_file.exists():
        print(f"Missing: {log_file}")
        return

    data = load_fnbreakdown(log_file)
    means = {col: np.mean(data[col]) for col, _ in FUNCTIONS}

    total = means["getimpl_ns"]

    print("\n--- Raw column means (ns) ---")
    for col, _ in FUNCTIONS:
        print(f"  {col:25s}: {means[col]:12.1f} ns")

    # Exclusive times: subtract nested children from each parent
    excl = {}
    excl["get_outer"]      = means["getimpl_ns"]      - means["memtable_get_ns"]
    excl["memtable_outer"] = means["memtable_get_ns"]  - means["get_from_table_ns"]
    excl["table_outer"]    = means["get_from_table_ns"] - means["vectorrep_get_ns"]
    excl["vrep_outer"]     = means["vectorrep_get_ns"]  - (
        means["lock_ns"] + means["bucket_copy_ns"] + means["seek_ns"] + means["callback_ns"]
    )
    excl["lock_ns"]        = means["lock_ns"]
    excl["bucket_copy_ns"] = means["bucket_copy_ns"]
    excl["seek_ns"]        = means["seek_ns"]
    excl["callback_ns"]    = means["callback_ns"]

    print("\n--- Exclusive times (ns) ---")
    for key, val in excl.items():
        print(f"  {key:25s}: {val:12.1f} ns  ({val/total*100:.2f}%)")

    all_labels = [
        ("get_outer",      "Get (excl. memtable)"),
        ("memtable_outer", "memtable Get (excl. table)"),
        ("table_outer",    "get_from_table (excl. vrep)"),
        ("vrep_outer",     "VectorRep::Get (excl. leaves)"),
        ("lock_ns",        "lock"),
        ("bucket_copy_ns", "bucket copy"),
        ("seek_ns",        "seek"),
        ("callback_ns",    "callback"),
    ]

    entries = [(key, label, max(excl[key] / total, 0.0)) for key, label in all_labels]
    colors  = {key: cm.tab10(i / len(all_labels)) for i, (key, _, _) in enumerate(entries)}

    seek_val  = next(v for key, _, v in entries if key == "seek_ns")
    other_val = sum(v for key, _, v in entries if key != "seek_ns")

    names = ["seek", "other"]
    vals  = [seek_val, other_val]
    clrs  = [colors["seek_ns"], "0.6"]

    fig, ax = plt.subplots(figsize=(1.4, 2.8))
    bars = ax.bar(names, vals, color=clrs, edgecolor="none", width=0.5)
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, v + 0.005,
                f"{v:.3f}", va="bottom", ha="center", fontsize=18, rotation=90)

    ax.set_ylabel("PQ latency split", labelpad=2)
    ax.set_ylim(0, 1.4)
    ax.set_yticks([0, 0.5, 1], ["0", "0.5", "1"])
    ax.set_xlabel("sequential", labelpad=2)
    ax.text(0.99, 0.98, "(B)", transform=ax.transAxes,
            va="top", ha="right", fontsize=20)

    output_file = CURR_DIR / TAG / "vector-fn-pct-breakdown.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    # plot_fn_breakdown()
    # plot_fn_breakdown_lock_callback()
    # plot_fn_breakdown_copy_seek()
    plot_fn_time_percentage()
