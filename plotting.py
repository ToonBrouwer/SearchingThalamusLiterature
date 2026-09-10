"""
Comprehensive plotting utilities for thalamus literature project.

Re-exports everything from plot_style.py and adds convenience helpers
for panel labels, despining, stacked areas, bar charts, and heatmaps.

Usage:
    from plotting import apply_style, COLORS, WONG, save_fig, panel_label
    apply_style()
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.patches as mpatches

# ── Re-export everything from plot_style ──────────────────────────────────
from Analysis.plot_style import (          # noqa: F401
    COLORS,
    RC,
    apply_style as _apply_style_base,
    save_fig,
    FIG_1COL,
    FIG_15COL,
    FIG_2COL,
    FIG_FULL,
)

# ── Full Wong 2011 colourblind-safe palette (Nat Methods 8:441) ───────────
WONG = {
    "orange":  "#E69F00",
    "blue":    "#0072B2",
    "green":   "#009E73",
    "cyan":    "#56B4E9",
    "red":     "#D55E00",
    "pink":    "#CC79A7",
    "yellow":  "#F0E442",
    "purple":  "#7B4F9E",
    "grey":    "#999999",
    "ltgrey":  "#cccccc",
}

# ── Augmented RC overrides (slightly larger defaults) ─────────────────────
_RC_OVERRIDES = {
    "font.size":          10,
    "axes.labelsize":     11,
    "xtick.labelsize":    9,
    "ytick.labelsize":    9,
    "figure.facecolor":   "white",
    "axes.facecolor":     "white",
    "savefig.facecolor":  "white",
}


def apply_style():
    """Apply base RC params plus project-level overrides."""
    _apply_style_base()
    mpl.rcParams.update(_RC_OVERRIDES)


# ── Panel labels ──────────────────────────────────────────────────────────

def panel_label(ax, label, x=-0.08, y=1.06, fontsize=14):
    """Add a bold panel label (A, B, C ...) in the top-left corner of *ax*."""
    ax.text(
        x, y, label,
        transform=ax.transAxes,
        fontsize=fontsize,
        fontweight="bold",
        va="top",
        ha="left",
        family="sans-serif",
    )


# ── Spine removal ─────────────────────────────────────────────────────────

def despine(ax, top=True, right=True):
    """Remove selected spines from *ax*."""
    if top:
        ax.spines["top"].set_visible(False)
    if right:
        ax.spines["right"].set_visible(False)


# ── Stacked area chart ────────────────────────────────────────────────────

def _smooth(arr, window):
    """Simple centred moving average; returns original if window <= 1."""
    if window <= 1:
        return arr
    kernel = np.ones(window) / window
    return np.convolve(arr, kernel, mode="same")


def stacked_area(ax, years, data_dict, colors_dict, smooth_window=3):
    """
    Draw a stacked area chart on *ax*.

    Parameters
    ----------
    years        : array-like of x-values (e.g. publication years)
    data_dict    : {label: array-like of y-values} in desired stacking order
    colors_dict  : {label: colour} matching keys of *data_dict*
    smooth_window: int, moving-average kernel width (1 = no smoothing)
    """
    years = np.asarray(years)
    labels = list(data_dict.keys())
    stacked = np.row_stack(
        [_smooth(np.asarray(data_dict[k], dtype=float), smooth_window)
         for k in labels]
    )
    ax.stackplot(
        years, stacked,
        labels=labels,
        colors=[colors_dict.get(k, WONG["grey"]) for k in labels],
        alpha=0.85,
    )
    despine(ax)


# ── Horizontal bar chart of Mann-Kendall tau values ──────────────────────

def mk_bar(ax, items, sort=True):
    """
    Horizontal bar chart of Mann-Kendall tau values.

    Parameters
    ----------
    items : list of (label, tau) tuples
    sort  : bool, sort by absolute tau descending
    """
    if sort:
        items = sorted(items, key=lambda x: abs(x[1]), reverse=True)

    labels = [it[0] for it in items]
    taus   = [it[1] for it in items]

    colours = [WONG["red"] if t > 0 else WONG["blue"] for t in taus]
    y_pos = np.arange(len(labels))

    ax.barh(y_pos, taus, color=colours, edgecolor="none", height=0.7)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    ax.axvline(0, color="#444444", linewidth=0.6)
    ax.set_xlabel("Mann-Kendall tau")
    despine(ax)
    ax.invert_yaxis()


# ── Annotated heatmap ────────────────────────────────────────────────────

def heatmap(ax, matrix, row_labels, col_labels, cmap="YlOrRd",
            fmt=".2f", fontsize=7, vmin=None, vmax=None):
    """
    Draw an annotated heatmap on *ax*.

    Parameters
    ----------
    matrix     : 2-D array-like
    row_labels : list of str
    col_labels : list of str
    cmap       : str or Colormap
    fmt        : format string for cell annotations
    fontsize   : annotation font size
    vmin, vmax : colour-scale bounds (optional)
    """
    matrix = np.asarray(matrix, dtype=float)
    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=vmin, vmax=vmax)

    ax.set_xticks(np.arange(len(col_labels)))
    ax.set_yticks(np.arange(len(row_labels)))
    ax.set_xticklabels(col_labels, rotation=45, ha="right", fontsize=fontsize)
    ax.set_yticklabels(row_labels, fontsize=fontsize)

    # Annotate every cell
    thresh = matrix.max() / 2.0 if np.isfinite(matrix).any() else 0
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            val = matrix[i, j]
            if not np.isfinite(val):
                continue
            colour = "white" if val > thresh else "black"
            ax.text(j, i, format(val, fmt), ha="center", va="center",
                    color=colour, fontsize=fontsize)

    ax.set_xticks(np.arange(len(col_labels) + 1) - 0.5, minor=True)
    ax.set_yticks(np.arange(len(row_labels) + 1) - 0.5, minor=True)
    ax.grid(which="minor", color="white", linewidth=1)
    ax.tick_params(which="minor", bottom=False, left=False)

    return im
