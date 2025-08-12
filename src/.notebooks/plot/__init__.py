import os
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager

from .style import line_styles, bar_styles


matplotlib.use('Agg')

prop = font_manager.FontProperties(fname="src/.notebooks/plot/LinLibertine_Mah.ttf")
plt.rcParams["font.family"] = prop.get_name()
plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "bold"
plt.rcParams["font.size"] = 20

EXP_DIR = Path(__file__).parent.parent.parent.parent / ".vstats"