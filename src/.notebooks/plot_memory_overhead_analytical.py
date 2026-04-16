import copy
import os
from plot import *
import numpy as np
import matplotlib.pyplot as plt

from plot.style import line_styles

TAG = "analytical"
os.makedirs(TAG, exist_ok=True)
output_file_memory_overhead = f"{TAG}/memory_overhead.pdf"
output_file_flushes = f"{TAG}/flushes.pdf"

# -------------------------
# Parameters
# -------------------------
ptr_size = 8
E = 1024  # average entry size in bytes (for memory overhead plot)
N_total = 5e6  # total workload size (fixed)

K = np.linspace(1000, 60000, 200)  # logical entries per buffer

# -------------------------
# Hash-hybrid parameters
# -------------------------
H = 100000  # number of hash buckets

# -------------------------
# Memory overhead models (as a function of K entries)
# -------------------------
vector_mem = ptr_size * K
linked_list_mem = 2 * ptr_size * K

# skip list: 1 key pointer + avg E[l]=2 level pointers (capped at 4)
skip_list_mem = 3 * ptr_size * K

# inline skip list: no key pointer (inline), 1 level-0 ptr + E[l-1]=1 avg higher ptrs + splice (2*l ptrs)
# (K * (1 + E[l-1]) + 2*l) * ptr_size
l = 4
inline_skip_mem = (K * (1 + 1) + 2 * l) * ptr_size

# hash hybrids: H bucket pointers + per-entry overhead in buckets
hash_vec_mem = H * ptr_size + ptr_size * K
hash_list_mem = H * ptr_size * (1 + 2 * (K / H))  # H * ptr_size * (1 + 2*Ke)
hash_skip_mem = H * ptr_size + K * (
    ptr_size + ptr_size * 2
)  # H ptrs + K * (key_ptr + E[l] level ptrs)

# -------------------------
# Plot 1: Memory overhead vs K
# -------------------------
_, ax = plt.subplots(figsize=(3, 2.5))

ax.plot(K, vector_mem, **line_styles["vector"])
ax.plot(K, linked_list_mem, **line_styles["linkedlist"])
ax.plot(K, skip_list_mem, **line_styles["simpleskiplist"])
ax.plot(K, inline_skip_mem, **line_styles["skiplist"])

ax.plot(K, hash_vec_mem, **line_styles["hashvector"])
ax.plot(K, hash_list_mem, **line_styles["hashlinkedlist"])
ax.plot(K, hash_skip_mem, **line_styles["hashskiplist"])

ax.set_ylim(0)

ax.set_xlabel("buffer size")
ax.set_ylabel("metadata overhead", y=0.425)
ax.set_yticks([])
ax.set_yticklabels([])
ax.set_xticks([])
ax.set_xticklabels([])

# --- Equation annotations (color-coded, placed near right end of each line) ---
# Place each label at the right end of the curve, offset to avoid overlap
K_right = K[-1]  # rightmost K value
y_max = ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else 3 * ptr_size * K_right * 1.1

# Compute y at right edge for each curve, then annotate right-aligned outside axes
annotations = [
    (vector_mem[-1], r"$K \cdot p$", "#006d2c"),
    (inline_skip_mem[-1], r"$E[fwd] \cdot K \cdot p$", "#6a3d9a"),
    (hash_vec_mem[-1], r"$H{\cdot}p + K{\cdot}p$", "#ff7f0e"),
    (linked_list_mem[-1], r"$2K \cdot p$", "#222d8b"),
    (hash_list_mem[-1], r"$H{\cdot}p + 2K{\cdot}p$", "#b22222"),
    (skip_list_mem[-1], r"$(1{+}E[fwd]){\cdot}K{\cdot}p$", "#cf17a7"),
    (hash_skip_mem[-1], r"$H{\cdot}p{+}(1{+}E[fwd]){\cdot}K{\cdot}p$", "#1f78b4"),
]

# Sort by y-value so we can space them if they overlap
annotations.sort(key=lambda t: t[0])

# # Place labels just outside the right edge of the axes
# for y_val, label, color in annotations:
#     ax.annotate(label, xy=(K_right, y_val), xycoords="data",
#                 xytext=(6, 0), textcoords="offset points",
#                 fontsize=9, color=color, ha="left", va="center",
#                 annotation_clip=False)

# # Parameter + definitions box (top-left)
# param_text = (r"$p = \mathrm{ptr\_size}$" + "\n"
#               r"$E[fwd] = \frac{1 - \mathrm{prob}^l}{1 - \mathrm{prob}}$" + "\n"
#               f"$H = {H}$ (hash buckets)")
# ax.text(0.02, 0.98, param_text, transform=ax.transAxes,
#         fontsize=9, va="top", ha="left",
#         bbox=dict(boxstyle="round,pad=0.4", facecolor="white", edgecolor="gray", alpha=0.9))

plt.savefig(output_file_memory_overhead, bbox_inches="tight", pad_inches=0.06)

# -------------------------
# Plot 2: Flushes vs M (buffer size in bytes)
#
# For a buffer of M bytes, the effective capacity (entries) is:
#   C = (M - fixed_overhead) / (E_flush + per_entry_overhead)
# Flushes = N_total / C
#
# With large entries (e.g., 1KB), per-entry overhead is <3% of E
# and all curves overlap. Smaller entries amplify the differences.
# -------------------------
E_flush = 32  # smaller entry size to show capacity differences
M = np.linspace(32 * 1024, 4 * 1024 * 1024, 200)  # 32 KB to 4 MB

# Per-entry overhead for each structure (from the paper's memory models)
per_entry_overhead = {
    "vector": ptr_size,  # K * ptr_size
    "linkedlist": 2 * ptr_size,  # K * 2 * ptr_size (prev + next)
    "simpleskiplist": 3 * ptr_size,  # K * (ptr + E[l]*ptr), E[l]=2
    "skiplist": 2 * ptr_size,  # K * (1 + E[l-1]) * ptr, E[l-1]=1
    "hashvector": ptr_size,  # per-entry: 1 pointer in sorted vector bucket
    "hashlinkedlist": 2 * ptr_size,  # per-entry: prev + next in linked-list bucket
    "hashskiplist": 3 * ptr_size,  # per-entry: key ptr + level ptrs in skip-list bucket
}

# Fixed overhead (independent of number of entries)
fixed_overhead = {
    "vector": 0,
    "linkedlist": 0,
    "simpleskiplist": 0,
    "skiplist": 2 * l * ptr_size,  # splice: 2 pointers per level
    "hashvector": H * ptr_size,  # bucket pointer array
    "hashlinkedlist": H * ptr_size,
    "hashskiplist": H * ptr_size,
}


def effective_capacity(M_buf, per_entry, fixed):
    usable = M_buf - fixed
    usable = np.maximum(usable, 0)
    cap = usable / (E_flush + per_entry)
    return np.maximum(cap, 1)


def flushes(cap):
    return N_total / cap


_, ax = plt.subplots(figsize=(5, 3.5))

plot_order = [
    "vector",
    "linkedlist",
    "simpleskiplist",
    "skiplist",
    "hashvector",
    "hashlinkedlist",
    "hashskiplist",
]

for name in plot_order:
    cap = effective_capacity(M, per_entry_overhead[name], fixed_overhead[name])
    fl = flushes(cap)
    ax.plot(M / (1024 * 1024), fl, **line_styles[name])

ax.set_xlabel("M (buffer size in MB)")
ax.set_ylabel("flush count")
ax.set_yscale("log")
ax.set_ylim(1)

plt.savefig(output_file_flushes, bbox_inches="tight", pad_inches=0.06)

# -------------------------
# Legend (shared)
# -------------------------
_, legend_fig = plt.subplots(figsize=(2, 0.7))

edited_lines = copy.deepcopy(line_styles)
del edited_lines["unsortedvector"]
del edited_lines["alwayssortedvector"]

order = [
    "vector",
    "linkedlist",
    "skiplist",
    "simpleskiplist",
    "hashlinkedlist",
    "hashskiplist",
    "hashvector",
]
edited_lines = {name: edited_lines[name] for name in order}

for name, style in edited_lines.items():
    legend_fig.plot([], [], **style)
legend_fig.legend(
    loc="center",
    ncol=4,
    frameon=False,
    borderaxespad=0,
    labelspacing=0,
    borderpad=0,
    columnspacing=0.5,
    handletextpad=0.2,
)
legend_fig.axis("off")
plt.savefig(f"{TAG}/legend.pdf", bbox_inches="tight", pad_inches=0.06)
