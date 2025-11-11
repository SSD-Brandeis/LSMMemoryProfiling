# notebooks/plot/__init__.py
import os
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager

from .style import line_styles, bar_styles

matplotlib.use("Agg")

here = Path(__file__).resolve().parent        # .../LSMMemoryBuffer/notebooks/plot
repo_root = here.parents[1]                   # .../LSMMemoryBuffer

# Font (local copy next to this file; fallback if missing)
font_path = here / "LinLibertine_Mah.ttf"
if font_path.exists():
    prop = font_manager.FontProperties(fname=str(font_path))
    plt.rcParams["font.family"] = prop.get_name()
else:
    plt.rcParams["font.family"] = "DejaVu Sans"

plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "bold"
plt.rcParams["font.size"] = 20

# Data root: env → data → .result → .vstats (all relative to repo root)
env_dir = os.environ.get("LSMMB_STATS_DIR")
candidates = [
    Path(env_dir) if env_dir else None,
    repo_root / "data",
    repo_root / ".result",
    repo_root / ".vstats",
]
EXP_DIR = next((p for p in candidates if p and p.exists()), repo_root / "data")
