from pathlib import Path
from collections import defaultdict
import re
import matplotlib.pyplot as plt
from matplotlib.ticker import LogFormatterMathtext
from glob import glob


EXPERIMENT_ROOTS = [Path(path) for path in glob(".result/") if Path(path).is_dir()]

OUT_SUBDIR = "comparative plots"
TARGET_ENTRY_SIZE = None


Y_LIM_BOTTOM = 0
Y_LIM_TOP = 1e6


_PREAMBLE = ("Destroying database", "kBlockSize", "Clearing system cache")
PAIR_RE = re.compile(r"([\w]+):\s*([0-9]+)")


def _clean(tok: str) -> str:
    t = tok.lower()
    if t.startswith("hashlinklist"):
        return "HashLinkList"
    if "_" in tok:
        return tok.split("_", 1)[1]
    return tok


def _size_tag() -> str | None:
    if TARGET_ENTRY_SIZE is None:
        return None
    m = re.match(r"(\d+)", str(TARGET_ENTRY_SIZE))
    return f"entry_{m.group(1)}b" if m else None


def collect(roots):
    tag = _size_tag()
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for root in roots:
        label = root.name
        for log in root.rglob("run1.log"):
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

    raw_order = [
        "HashLinkList",  # insert-variant A
        "InsertKey",  # separate op
        "MemTable",
        "PutCFImpl",
        "WriteBatchInternal",
        "DBImpl",
    ]

    def group_of(comp: str) -> str:
        if comp == "HashLinkList" or comp not in raw_order:
            return "Insert"
        return comp

    group_order = [
        "Insert",
        "InsertKey",
        "MemTable",
        "PutCFImpl",
        "WriteBatchInternal",
        "DBImpl",
    ]

    base_colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    group_color = {
        grp: base_colors[i % len(base_colors)] for i, grp in enumerate(group_order)
    }

    suffix = f"_{TARGET_ENTRY_SIZE}B" if TARGET_ENTRY_SIZE else ""

    for buf, per_exp in data.items():
        SCALE = 4
        fig, axes = plt.subplots(
            len(labels),
            1,
            figsize=(SCALE * 8, SCALE * 2.7 * len(labels)),
            sharex=True,
            sharey=False,
        )
        axes = axes.flatten() if hasattr(axes, "flatten") else [axes]

        handles_by_group = {}

        for ax_idx, (ax, exp_label) in enumerate(zip(axes, labels)):
            comp_d = per_exp[exp_label]

            raw_present = [c for c in raw_order if c in comp_d]
            extras = sorted(c for c in comp_d if c not in raw_order)
            plot_comps = raw_present + extras

            for comp in plot_comps:
                y = comp_d[comp]
                if not y:
                    continue
                x = range(1, len(y) + 1)
                grp = group_of(comp)
                (line,) = ax.plot(x, y, lw=1, color=group_color[grp], label=grp)

                if ax_idx == 0 and grp not in handles_by_group:
                    handles_by_group[grp] = line

            ax.set_yscale("log", base=10)
            ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))

            if Y_LIM_TOP is not None:
                ax.set_ylim(Y_LIM_BOTTOM, Y_LIM_TOP)
            else:
                ax.set_ylim(bottom=Y_LIM_BOTTOM)
                ax.margins(y=0.05)

            ax.set_title(exp_label, fontsize=8, pad=3)
            ax.tick_params(axis="y", labelsize=8)
            ax.tick_params(axis="x", labelsize=8)

        axes[-1].set_xlabel("Data-point #")
        fig.text(
            0.02,
            0.5,
            "Latency (ns)",
            rotation=90,
            va="center",
            ha="center",
            fontsize=10,
        )

        legend_handles = [
            handles_by_group[g] for g in group_order if g in handles_by_group
        ]
        legend_labels = [g for g in group_order if g in handles_by_group]
        fig.legend(
            legend_handles,
            legend_labels,
            ncol=3,
            frameon=False,
            loc="lower center",
            bbox_to_anchor=(0.5, 0.03),
            fontsize=7,
        )

        fig.subplots_adjust(left=0.14, top=0.90, bottom=0.28, hspace=0.35)

        out_path = out_dir / f"{buf}{suffix}.png"
        fig.savefig(out_path, dpi=250)
        plt.close(fig)
        print(f"[SAVED] {out_path}")


def main():
    for p in EXPERIMENT_ROOTS:
        if not p.is_dir():
            raise SystemExit(f"Missing experiment dir: {p}")

    out_dir = EXPERIMENT_ROOTS[0] / OUT_SUBDIR
    data = collect(EXPERIMENT_ROOTS)
    plot(data, [p.name for p in EXPERIMENT_ROOTS], out_dir)
    print("[DONE]")


if __name__ == "__main__":
    main()
