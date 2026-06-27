bar_styles = {
    "vector":           {"label": "vector",           "color": "#006d2c", "edgecolor": "#006d2c", "hatch": ""},
    "unsortedvector":   {"label": "unsorted vector",  "color": "none", "edgecolor": "#4d4d4d", "hatch": "\\"},
    "alwayssortedvector":{"label": "sorted vector",   "color": "none", "edgecolor": "#8b4513", "hatch": "/"},
    "skiplist":         {"label": "inline skip-list", "color": "none", "edgecolor": "#6a3d9a", "hatch": "-"},
    "simpleskiplist":   {"label": "skip-list",        "color": "none", "edgecolor": "#cf17a7", "hatch": "---"},
    "linkedlist":       {"label": "linked-list",      "color": "none", "edgecolor": "#222d8b", "hatch": "\\."},
    "hashlinkedlist":   {"label": "hash linked-list", "color": "none", "edgecolor": "#b22222", "hatch": "////"},
    "hashskiplist":     {"label": "hash skip-list",   "color": "none", "edgecolor": "#1f78b4", "hatch": "\\\\\\\\"},
    "hashvector":       {"label": "hash vector",      "color": "none", "edgecolor": "#ff7f0e", "hatch": "/."},
}

line_styles_bold = {
    "vector": {
        "label": "\\textbf{vec}tor",
        "color": "#006d2c",  # Dark green (distinct and rich)
        "linestyle": 'solid', # (0, (1, 1)),  # Dotted
        # "marker": "x",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "unsortedvector": {
        "label": "\\textbf{u}nsorted \\textbf{vec}tor",
        "color": "#4d4d4d",  # Dark gray (visible and clean)
        "linestyle": 'dotted',  # Long dashes
        # "marker": "v",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "alwayssortedvector": {
        "label": "\\textbf{s}orted \\textbf{vec}tor",
        "color": "#8b4513",  # SaddleBrown (dark reddish-brown)
        "linestyle": 'dashed',  # Dash-dot-dot
        # "marker": "s",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "skiplist": {
        "label": "\\textbf{i}nline \\textbf{skip}-list",
        "color": "#6a3d9a",  # Deep purple
        "linestyle": (0, (3, 5, 1, 5, 1, 5)),  # Dotted (long gaps)
        # "marker": "o",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "simpleskiplist": {
        "label": "\\textbf{skip}-list",
        "color": "#cf17a7",  # Teal (distinct from purple & blue)
        "linestyle": (0, (1, 1)),    # Dotted (different from skiplist solid)
        # "marker": "P",       # Plus-filled marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "linkedlist": {
        "label": "linked-list",
        "color": "#222d8b",  # ForestGreen (distinct and vibrant)
        "linestyle": (5, (10, 3)),
        # "marker": "D",       # Diamond marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashlinkedlist": {
        "label": "\\textbf{h}ash \\textbf{link}ed-list",
        "color": "#b22222",  # Firebrick (dark red)
        "linestyle": "-.",
        # "marker": "D",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashskiplist": {
        "label": "\\textbf{h}ash \\textbf{skip}-list",
        "color": "#1f78b4",  # Darker blue (colorblind-friendly)
        "linestyle": "--",
        # "marker": "^",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashvector": {
        "label": "\\textbf{h}ash \\textbf{vec}tor",
        "color": "#ff7f0e",  # Orange (distinct from others)
        "linestyle": (0, (3, 1, 1, 1, 1, 1)),
        # "marker": "H",       # Hexagon marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
}

line_styles = {
    "dynamic": {
        "label": "\\texttt{Adaptive}",
        "color": "black", "linestyle": "solid", "linewidth": 2,
        "marker": "x", "markersize": 12, "markerfacecolor": "none",
    },
    "vector": {
        "label": "\\texttt{V-Qsort}",
        "color": "#006d2c",  # Dark green (distinct and rich)
        "linestyle": 'solid', # (0, (1, 1)),  # Dotted
        "marker": "x",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "unsortedvector": {
        "label": "\\texttt{V-Qscan}",
        "color": "#4d4d4d",  # Dark gray (visible and clean)
        "linestyle": 'dotted',  # Long dashes
        "marker": "v",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "alwayssortedvector": {
        "label": "\\texttt{V-Sorted}",
        "color": "#8b4513",  # SaddleBrown (dark reddish-brown)
        "linestyle": 'dashed',  # Dash-dot-dot
        "marker": "s",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "skiplist": {
        "label": "\\texttt{InSkip-L}",
        "color": "#6a3d9a",  # Deep purple
        "linestyle": (0, (3, 5, 1, 5, 1, 5)),  # Dotted (long gaps)
        "marker": "o",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "simpleskiplist": {
        "label": "\\texttt{Skip-L}",
        "color": "#cf17a7",  # Teal (distinct from purple & blue)
        "linestyle": (0, (1, 1)),    # Dotted (different from skiplist solid)
        "marker": "P",       # Plus-filled marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "linkedlist": {
        "label": "\\texttt{Link-L}",
        "color": "#222d8b",  # ForestGreen (distinct and vibrant)
        "linestyle": (5, (10, 3)),
        "marker": "D",       # Diamond marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashlinkedlist": {
        "label": "\\texttt{Hash-LL}",
        "color": "#b22222",  # Firebrick (dark red)
        "linestyle": "-.",
        "marker": "D",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashskiplist": {
        "label": "\\texttt{Hash-SL}",
        "color": "#1f78b4",  # Darker blue (colorblind-friendly)
        "linestyle": "--",
        "marker": "^",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashvector": {
        "label": "\\texttt{Hash-V}",
        "color": "#ff7f0e",  # Orange (distinct from others)
        "linestyle": (0, (3, 1, 1, 1, 1, 1)),
        "marker": "H",       # Hexagon marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
}


hatch_map = {
    "vector": "",
    "unsortedvector": "\\",
    "alwayssortedvector": "/",
    "skiplist": "-",
    "simpleskiplist": "---",
    "linkedlist": "\\.",
    "hashlinkedlist": "////",
    "hashskiplist": "\\\\\\\\",
    "hashvector": "/.",
}