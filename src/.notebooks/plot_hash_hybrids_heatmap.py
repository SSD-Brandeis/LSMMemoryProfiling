from plot import *

import os
import numpy as np

TAG = "hash_hybrids"
os.makedirs(TAG, exist_ok=True)

N = 1e9  # total write (PUT) operations in the workload
UPDATES_PER_KEY = 10  # avg times each distinct key is (re)written; tune to match the real workload
K = N / UPDATES_PER_KEY  # distinct keyspace size implied by the update rate

X_vals = np.arange(1, 9)          # prefix length: 1 to 8
H_vals = np.logspace(0, 12, 200)  # bucket count: 1 to 1e12 (log scale)


def generalized_harmonic(n, s):
    """H_n^(s) = sum_{k=1}^n k^-s, via Euler-Maclaurin approximation (exact at n=1)."""
    n = np.asarray(n, dtype=np.float64)
    if np.isclose(s, 1.0):
        return np.log(n) + np.euler_gamma
    return (np.power(n, 1 - s) - 1) / (1 - s) + 1


def plot_heatmap(Z, X_grid, H_grid, output_file, cbar_ticks, show_boundary=True, label=None, panel_label=None):
    fig, ax = plt.subplots(figsize=(7, 5))

    c = ax.pcolormesh(X_grid, H_grid, Z, shading="auto")
    ax.set_yscale("log")

    if label:
        ax.text(
            0.95, 0.95, label,
            transform=ax.transAxes,
            color="white",
            ha="right", va="top",
        )

    if panel_label:
        # outside the axes, level with the xlabel, left of the y-tick labels
        ax.text(
            -0.15, -0.11, panel_label,
            transform=ax.transAxes,
            ha="left", va="top",
            fontweight="bold",
        )

    ax.set_xlabel("prefix length (X)")
    ax.set_ylabel("bucket count (H)", labelpad=-2)

    cb = fig.colorbar(c, ax=ax)
    cb.set_ticks(cbar_ticks)

    if show_boundary:
        boundary_H = np.clip(np.array([128.0**x for x in X_vals]), H_vals.min(), H_vals.max())
        ax.plot(X_vals, boundary_H, linestyle="--", linewidth=2, label=r"$H = 128^X$")

    ax.set_xticks(X_vals)
    ax.set_yticks([1, 1e3, 1e6, 1e9, 1e12])
    ax.set_yticklabels([r"$10^0$", r"$10^3$", r"$10^6$", r"$10^9$", r"$10^{12}$"])

    plt.tight_layout()
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
    plt.close(fig)

    print(f"Saved heatmap to {output_file}")


X_grid, H_grid = np.meshgrid(X_vals, H_vals)
# bucketing only separates distinct keys, so it's capped by K, not by total writes N
effective = np.minimum(np.minimum(H_grid, 128.0**X_grid), K)

# --- Uniform workload: writes spread evenly across K keys, so a bucket's
# physical entry count is proportional to how many distinct keys collide there. ---
Z_uniform = np.log10(N / effective)
plot_heatmap(Z_uniform, X_grid, H_grid, f"{TAG}/heatmap.pdf", cbar_ticks=[0, 2, 4, 6, 8])

# --- Skewed workload: LSM never updates in place, so every repeat write to a
# hot key is a new physical entry - and since bucket placement depends only on
# the key's value, all of a hot key's duplicates always land in the same
# bucket no matter how fine H/X are. That's an irreducible floor
# (N / H_K^theta) on top of the usual "other distinct keys sharing the
# bucket" term, so cost can't be hashed away the way it can under uniform. ---
def plot_skewed(theta, output_file, panel_label):
    hot_key_floor = N / generalized_harmonic(K, theta)  # scalar: independent of H, X
    Z = np.log10(N / effective + hot_key_floor)
    ticks = np.round(np.linspace(Z.min(), Z.max(), 4), 1)
    plot_heatmap(Z, X_grid, H_grid, output_file, cbar_ticks=ticks, show_boundary=False,
                 label=rf"$\theta = {theta}$", panel_label=f"({panel_label})")
    print(f"  theta={theta}: hottest-key share={1/generalized_harmonic(K, theta)*100:.2f}% of writes, floor=10^{np.log10(hot_key_floor):.2f}")


for panel_label, theta in zip("AB", [0.5, 0.99]):
    plot_skewed(theta, f"{TAG}/heatmap_skewed_{theta}.pdf", panel_label)
