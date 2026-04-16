
# import os

# import numpy as np
# import matplotlib.pyplot as plt

from plot import *

# TAG = "hash_hybrids"
# if not os.path.exists(TAG):
#     os.makedirs(TAG)
# output_file = f"{TAG}/heatmap.pdf"

# N = 1e9

# X_vals = np.arange(1, 11)
# H_vals = np.logspace(0, 12, 50)

# Z = np.zeros((len(H_vals), len(X_vals)))

# for i, H in enumerate(H_vals):
#     for j, X in enumerate(X_vals):
#         effective = min(H, 128**X)
#         Z[i, j] = max(0, np.log(N / effective))

# # Plot heatmap
# plt.figure()
# plt.imshow(
#     Z,
#     aspect="auto",
#     origin="lower",
#     extent=[X_vals.min(), X_vals.max(), np.log10(H_vals.min()), np.log10(H_vals.max())],
# )
# plt.xlabel("prefix length (X)")
# # plt.ylabel("log10(Bucket Count H)")
# plt.ylabel("bucket count (H)")
# plt.colorbar(label="cost")

# plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)


import os
import numpy as np
import matplotlib.pyplot as plt

TAG = "hash_hybrids"
os.makedirs(TAG, exist_ok=True)
output_file = f"{TAG}/heatmap.pdf"

N = 1e9  # total number of keys

X_vals = np.arange(1, 9)                 # prefix length: 1 to 8
H_vals = np.logspace(0, 12, 200)          # bucket count: 1 to 1e12 (log scale)

Z = np.zeros((len(H_vals), len(X_vals)))

for i, H in enumerate(H_vals):
    for j, X in enumerate(X_vals):
        effective = min(H, 128**X, N)
        Z[i, j] = np.log10(N / effective)   # stable + interpretable

fig, ax = plt.subplots(figsize=(7, 5))

X_grid, H_grid = np.meshgrid(X_vals, H_vals)

c = ax.pcolormesh(
    X_grid,
    H_grid,
    Z,
    shading="auto"
)

ax.set_yscale("log")

ax.set_xlabel("prefix length (X)")
ax.set_ylabel("bucket count (H)", labelpad=-2)

cb = fig.colorbar(c, ax=ax)
cb.set_ticks([0, 2, 4, 6, 8])

boundary_H = np.array([128**x for x in X_vals])

# Clip boundary to plotting range
boundary_H = np.clip(boundary_H, H_vals.min(), H_vals.max())

ax.plot(
    X_vals,
    boundary_H,
    linestyle="--",
    linewidth=2,
    label=r"$H = 128^X$"
)
ax.set_xticks(X_vals)

# Clean log ticks
ax.set_yticks([1, 1e3, 1e6, 1e9, 1e12])
ax.set_yticklabels([r"$10^0$", r"$10^3$", r"$10^6$", r"$10^9$", r"$10^{12}$"])

plt.tight_layout()
plt.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
plt.close()

print(f"Saved heatmap to {output_file}")