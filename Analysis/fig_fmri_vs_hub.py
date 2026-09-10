"""
fig_fmri_vs_hub.py — Standalone rebuild of the "rise of hub framing vs
rise of fMRI methodology" time-series plot.

Same prevalence definitions as method_precedes_framing.py:

  * Hub frame prevalence (year y) =
        100 * (# framing sentences in year y with frame_type == "hub")
            / (# framing sentences in year y)

  * fMRI prevalence (year y) =
        100 * (# papers in year y with has_fMRI == True)
            / (# papers in year y)

Two-panel figure:
    A) raw + 3-year-smoothed time series, dual y-axis (fMRI on left
       blue, hub on right red), 1985-2025
    B) cross-correlation at lags -5..+5 years (positive lag = fMRI
       leads hub) so the lead-lag is visible

Outputs:
    Graphs/pub/fig_fmri_vs_hub.png  (+ .svg)
    Data/processed/fmri_vs_hub_timeseries.csv  (raw + smoothed series)

Usage:
    python Code/analysis/fig_fmri_vs_hub.py
    python Code/analysis/fig_fmri_vs_hub.py --start-year 1990
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                    errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB = ROOT / "Graphs" / "pub"


# ─────────────────────────────────────────────────────────────────────────
# Prevalence builders (mirror method_precedes_framing.py)
# ─────────────────────────────────────────────────────────────────────────
def build_frame_prevalence(years: np.ndarray, frame_id: str = "hub"
                            ) -> pd.Series:
    """% of framing sentences per year that use `frame_id`."""
    df = pd.read_csv(PROC / "all_sentences_final.csv", low_memory=False,
                     usecols=["year", "frame_type"])
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    framing = df[df["frame_type"].notna() & (df["frame_type"] != "")]
    framing = framing[framing["year"].between(years[0], years[-1])]
    n_per_year = framing.groupby("year").size()
    n_frame = (framing[framing["frame_type"] == frame_id]
                .groupby("year").size())
    pct = (n_frame / n_per_year * 100).reindex(years, fill_value=0)
    return pct


def build_technique_prevalence(years: np.ndarray, has_col: str = "has_fMRI"
                                ) -> pd.Series:
    """% of papers per year that have the requested technique flag."""
    pt = pd.read_csv(PROC / "paper_techniques.csv", low_memory=False,
                     usecols=[has_col, "year"])
    pt["year"] = pd.to_numeric(pt["year"], errors="coerce")
    pt = pt[pt["year"].between(years[0], years[-1])]
    yearly = pt.groupby("year")[has_col].mean() * 100
    return yearly.reindex(years, fill_value=0)


# ─────────────────────────────────────────────────────────────────────────
# Cross-correlation at integer lags
# ─────────────────────────────────────────────────────────────────────────
def cross_correlate(x: np.ndarray, y: np.ndarray, max_lag: int = 5
                     ) -> dict[int, float]:
    """Pearson r between x[t] and y[t + lag]. Positive lag = x leads y."""
    x = np.nan_to_num(np.asarray(x, dtype=float))
    y = np.nan_to_num(np.asarray(y, dtype=float))
    if np.std(x) < 1e-6 or np.std(y) < 1e-6:
        return {lag: np.nan for lag in range(-max_lag, max_lag + 1)}
    x = (x - x.mean()) / (x.std() + 1e-10)
    y = (y - y.mean()) / (y.std() + 1e-10)
    n = len(x)
    out = {}
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            xs, ys = x[: n - lag], y[lag:]
        else:
            xs, ys = x[-lag:], y[: n + lag]
        out[lag] = (float(np.corrcoef(xs, ys)[0, 1])
                    if len(xs) > 5 else float("nan"))
    return out


# ─────────────────────────────────────────────────────────────────────────
# Figure
# ─────────────────────────────────────────────────────────────────────────
def make_figure(start_year: int = 1985, end_year: int = 2025,
                 smooth: int = 3, max_lag: int = 5) -> Path:
    years = np.arange(start_year, end_year + 1)
    fmri_raw = build_technique_prevalence(years, "has_fMRI")
    hub_raw = build_frame_prevalence(years, "hub")
    fmri_sm = fmri_raw.rolling(smooth, center=True, min_periods=1).mean()
    hub_sm = hub_raw.rolling(smooth, center=True, min_periods=1).mean()

    # Save the underlying data alongside the figure
    out_csv = PROC / "fmri_vs_hub_timeseries.csv"
    pd.DataFrame({
        "year": years,
        "fmri_pct_raw": fmri_raw.values,
        "fmri_pct_smooth3yr": fmri_sm.values,
        "hub_pct_raw": hub_raw.values,
        "hub_pct_smooth3yr": hub_sm.values,
    }).to_csv(out_csv, index=False)
    print(f"  saved {out_csv.relative_to(ROOT)}")

    fig = plt.figure(figsize=(13, 4.5))
    gs = fig.add_gridspec(1, 2, width_ratios=[2.2, 1.0], wspace=0.55)
    ax = fig.add_subplot(gs[0, 0])
    ax2 = ax.twinx()

    # Raw thin lines
    ax.plot(years, fmri_raw, color=WONG["blue"], linewidth=0.9, alpha=0.35,
             label="fMRI (raw)")
    ax2.plot(years, hub_raw, color=WONG["red"], linewidth=0.9, alpha=0.35,
              label="hub (raw)")
    # Smoothed thick lines
    ax.plot(years, fmri_sm, color=WONG["blue"], linewidth=2.4,
             label="fMRI prevalence (3-yr smooth)")
    ax2.plot(years, hub_sm, color=WONG["red"], linewidth=2.4,
              label="hub-frame prevalence (3-yr smooth)")

    ax.set_xlabel("year")
    ax.set_ylabel("% papers using fMRI", color=WONG["blue"], fontsize=10)
    ax.tick_params(axis="y", labelcolor=WONG["blue"])
    ax2.set_ylabel("% framing sentences using \"hub\"",
                    color=WONG["red"], fontsize=10)
    ax2.tick_params(axis="y", labelcolor=WONG["red"])

    # Combined legend
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8, frameon=False)

    ax.set_title("A. fMRI adoption vs hub framing, "
                 f"{start_year}–{end_year}", loc="left", fontsize=11)
    ax.spines["top"].set_visible(False)
    ax2.spines["top"].set_visible(False)
    panel_label(ax, "A")

    # Cross-correlation panel
    ax_b = fig.add_subplot(gs[0, 1])
    corrs = cross_correlate(fmri_sm.values, hub_sm.values, max_lag=max_lag)
    lags = sorted(corrs.keys())
    rs = [corrs[l] for l in lags]
    bar_colors = [WONG["red"] if l == max(corrs, key=lambda k: corrs[k] or -2)
                  else WONG["grey"] for l in lags]
    ax_b.bar(lags, rs, color=bar_colors, edgecolor="black", linewidth=0.5)
    ax_b.axhline(0, color="black", linewidth=0.6)
    ax_b.set_xlabel("lag in years (fMRI → hub)")
    ax_b.set_ylabel("Pearson r")
    best_lag = max(corrs, key=lambda k: corrs[k] or -2)
    best_r = corrs[best_lag]
    ax_b.set_title(f"B. Cross-correlation\n(best: lag={best_lag:+d}, "
                    f"r={best_r:.2f})", loc="left", fontsize=11)
    ax_b.set_xticks(lags)
    despine(ax_b)
    panel_label(ax_b, "B")

    plt.tight_layout()
    out = PUB / "fig_fmri_vs_hub"
    save_fig(fig, out)
    plt.close(fig)
    print(f"  saved {out.with_suffix('.png').relative_to(ROOT)}")

    # Print quick summary
    print("\nQuick summary:")
    print(f"  fMRI {start_year}: {fmri_raw.iloc[0]:.1f}%  → "
          f"{end_year}: {fmri_raw.iloc[-1]:.1f}%")
    print(f"  hub  {start_year}: {hub_raw.iloc[0]:.1f}%  → "
          f"{end_year}: {hub_raw.iloc[-1]:.1f}%")
    print(f"  best lag: fMRI leads hub by {best_lag:+d} years (r = {best_r:.2f})")
    return out.with_suffix(".png")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=1985)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--smooth", type=int, default=3,
                        help="Rolling window size in years")
    args = parser.parse_args()
    make_figure(args.start_year, args.end_year, smooth=args.smooth)


if __name__ == "__main__":
    main()
