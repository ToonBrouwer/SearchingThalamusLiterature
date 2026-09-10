"""
Shared plotting style for all publication figures.

Import this module and call apply_style() once at the top of any figure
script to get consistent typography, colours, and layout across all outputs.

Usage:
    from plot_style import apply_style, COLORS, save_fig
    apply_style()
    fig, ax = plt.subplots(...)
    save_fig(fig, GRAPH / "my_figure")   # saves .png (300 dpi) + .svg
"""

import matplotlib.pyplot as plt
import matplotlib as mpl
from pathlib import Path

# ── Colorblind-safe palette (Wong 2011, Nat Methods 8:441) ───────────────────
COLORS = {
    "relay":   "#D55E00",   # vermillion  — relay / gateway framing
    "hub":     "#0072B2",   # blue        — hub / integration framing
    "other":   "#999999",   # grey        — unclassified
    "green":   "#009E73",   # green
    "orange":  "#E69F00",   # orange
    "sky":     "#56B4E9",   # sky blue
    "yellow":  "#F0E442",   # yellow
    "pink":    "#CC79A7",   # pink
    "black":   "#000000",
    "white":   "#FFFFFF",
}

# ── rcParams for publication quality ─────────────────────────────────────────
RC = {
    # Typography
    "font.family":           "sans-serif",
    "font.sans-serif":       ["DejaVu Sans", "Arial", "Helvetica", "Liberation Sans"],
    "font.size":             11,
    "axes.titlesize":        13,
    "axes.titleweight":      "bold",
    "axes.labelsize":        11,
    "xtick.labelsize":       9,
    "ytick.labelsize":       9,
    "legend.fontsize":       9,
    "legend.title_fontsize": 9,
    "legend.framealpha":     0.85,
    "legend.edgecolor":      "#cccccc",
    # Axes
    "axes.spines.top":       False,
    "axes.spines.right":     False,
    "axes.linewidth":        0.8,
    "axes.grid":             False,
    "axes.axisbelow":        True,
    # Lines & patches
    "lines.linewidth":       2.0,
    "patch.linewidth":       0.5,
    # Output
    "figure.dpi":            150,    # screen preview (300 applied at save time)
    "savefig.dpi":           300,
    "savefig.bbox":          "tight",
    "savefig.pad_inches":    0.05,
}

# Standard figure widths (inches) matching common journal column widths
FIG_1COL  = 3.46   # 88 mm  — single column
FIG_15COL = 5.20   # 132 mm — 1.5 column
FIG_2COL  = 7.09   # 180 mm — double column (most main figures)
FIG_FULL  = 8.27   # 210 mm — full page width


def apply_style():
    """Apply RC params globally.  Call once before creating any figures."""
    mpl.rcParams.update(RC)


def save_fig(fig, path_stem, dpi=300, formats=("png", "svg")):
    """
    Save *fig* to both PNG (raster, print-ready) and SVG (vector, editable).

    Parameters
    ----------
    fig       : matplotlib Figure
    path_stem : str or Path — file path *without* extension
    dpi       : int — raster DPI (default 300)
    formats   : iterable of str — file formats to write
    """
    stem = Path(path_stem)
    stem.parent.mkdir(parents=True, exist_ok=True)
    for fmt in formats:
        out = stem.with_suffix(f".{fmt}")
        fig.savefig(out, dpi=dpi if fmt == "png" else None,
                    bbox_inches="tight", pad_inches=0.05)
        print(f"  Saved {out.name}")
