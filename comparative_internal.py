#!/usr/bin/env python3
"""
compare_bump_subplots.py
Generate for each buffer a PNG with three stacked latency plots
(one per experiment) and a single legend centred below them.

Run:
    python3 compare_bump_subplots.py
"""

from pathlib import Path
from collections import defaultdict
import re
import matplotlib.pyplot as plt
from matplotlib.ticker import LogFormatterMathtext


EXPERIMENT_ROOTS = [
    Path("/home/cc/LSMMemoryProfiling/.result/6_29_rawop_low_pri_false_larger_refill_I_PQ"),
    Path("/home/cc/LSMMemoryProfiling/.result/6_29_rawop_low_pri_true_default_refill_I_PQ"),
    Path("/home/cc/LSMMemoryProfiling/.result/6_29_rawop_low_pri_true_larger_refill_I_PQ"),
]
OUT_SUBDIR        = "comparative plots"
TARGET_ENTRY_SIZE = None                 

_PREAMBLE = ("Destroying database", "kBlockSize", "Clearing system cache")
PAIR_RE   = re.compile(r"([\w]+):\s*([0-9]+)")


def _clean(tok):
    t = tok.lower()
    if t.startswith("hashlinklist"):
        return "HashLinkList"
    if "_" in tok:
        return tok.split("_", 1)[1]
    return tok


def _size_tag():
    if TARGET_ENTRY_SIZE:
        m = re.match(r"(\d+)", str(TARGET_ENTRY_SIZE))
        return f"entry_{m.group(1)}b" if m else None
    return None


def collect(roots):
    tag  = _size_tag()
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for root in roots:
        label = root.name
        for log in root.rglob("temp.log"):
            exp_dir = log.parent.parent.name
            if tag and tag not in exp_dir:
                continue
            buf = log.parent.parent.parent.name
            with log.open() as f:
                for line in f:
                    ln = line.strip()
                    if not ln or ln.startswith(_PREAMBLE):
                        continue
                    for tok, val in PAIR_RE.findall(ln):
                        data[buf][label][_clean(tok)].append(int(val))
    return data


def plot(data, labels, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    ymax = max(
        v
        for buf in data.values()
        for exp in buf.values()
        for lst in exp.values()
        for v in lst
    )

    comp_order = ["HashLinkList", "InsertKey", "MemTable",
                  "PutCFImpl", "WriteBatchInternal", "DBImpl"]
    colours = (plt.rcParams["axes.prop_cycle"].by_key()["color"] * 10)
    suffix = f"_{TARGET_ENTRY_SIZE}B" if TARGET_ENTRY_SIZE else ""

    for buf, per_exp in data.items():
        fig, axes = plt.subplots(len(labels), 1,
                                 figsize=(8, 2.7 * len(labels)),
                                 sharex=True, sharey=True)
        axes = axes.flatten() if hasattr(axes, "flatten") else [axes]

        legend_handles = []
        legend_labels  = []


        for ax_index, (ax, exp_label) in enumerate(zip(axes, labels)):
            comp_d = per_exp[exp_label]
            comps  = sorted(
                comp_d,
                key=lambda c: comp_order.index(c) if c in comp_order else len(comp_order)
            )
            for colour, comp in zip(colours, comps):
                y = comp_d[comp]
                if not y:
                    continue
                x = range(1, len(y) + 1)
                pretty = "Insert" if comp == "HashLinkList" else comp
                line, = ax.plot(x, y, lw=1, color=colour, label=pretty)


                if ax_index == 0:
                    legend_handles.append(line)
                    legend_labels.append(pretty)

            ax.set_yscale("log", base=10)
            ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
            ax.set_ylim(10, ymax * 1.05)
            ax.set_title(exp_label, fontsize=8, pad=3)
            ax.tick_params(axis='y', labelsize=8)
            ax.tick_params(axis='x', labelsize=8)

        axes[-1].set_xlabel("Data-point #")
        fig.text(0.02, 0.5, "Latency (ns)",
                 rotation=90, va="center", ha="center", fontsize=10)


        fig.legend(
            legend_handles,
            legend_labels,
            ncol=3,
            frameon=False,
            loc="lower center",      
            bbox_to_anchor=(0.5, 0.03),
            fontsize=7
        )

   
        fig.subplots_adjust(left=0.14, top=0.88, bottom=0.28, hspace=0.35)

        out_path = out_dir / f"{buf}{suffix}.png"
        fig.savefig(out_path, dpi=250)
        plt.close(fig)
        print(f"[SAVED] {out_path}")


def main():
    for p in EXPERIMENT_ROOTS:
        if not p.is_dir():
            raise SystemExit(f"Missing experiment dir: {p}")

    out_dir = EXPERIMENT_ROOTS[0] / OUT_SUBDIR
    data    = collect(EXPERIMENT_ROOTS)
    plot(data, [p.name for p in EXPERIMENT_ROOTS], out_dir)
    print("[DONE]")


if __name__ == "__main__":
    main()
