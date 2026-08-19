import os
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.figure
import matplotlib.font_manager as font_manager

from .style import line_styles, bar_styles
matplotlib.use('Agg')

# Revision figures must not overwrite the versions already placed in the paper,
# so every saved figure (plt.savefig / fig.savefig, across every plot_*.py
# script) gets a "-v2" suffix inserted before its extension.
_original_savefig = matplotlib.figure.Figure.savefig


def _savefig_with_v2_suffix(self, fname, *args, **kwargs):
    if isinstance(fname, (str, Path)):
        p = Path(fname)
        fname = p.with_name(p.stem + "-v2" + p.suffix)
    return _original_savefig(self, fname, *args, **kwargs)


matplotlib.figure.Figure.savefig = _savefig_with_v2_suffix

CURR_DIR = Path(__file__).parent

prop = font_manager.FontProperties(fname=CURR_DIR / "LinLibertine_Mah.ttf")
plt.rcParams["font.family"] = prop.get_name()
plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "bold"
plt.rcParams["font.size"] = 22

EXP_DIR = Path(__file__).parent.parent.parent.parent / ".vstats-old" / "data_new"

# "/Users/shubham/Dropbox/Apps/Overleaf/LSMBufferAnalysis/Figures"
DROPBOX_PATH = Path("/Users/shubham/Dropbox/Apps/Overleaf/LSMBufferAnalysis/Figures")