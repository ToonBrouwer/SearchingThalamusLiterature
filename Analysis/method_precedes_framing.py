"""
method_precedes_framing.py — Does methodology change precede framing change?

Cross-correlation and Granger causality analysis:
  - Per-year technique and frame prevalence time series
  - Three smoothing levels (raw, 2yr, 3yr rolling) for robustness
  - Cross-correlation at lags -5 to +5 years
  - Granger causality tests
  - Subcategory analysis (resting-state fMRI, functional connectivity, etc.)

Output:
  Graphs/pub/fig_method_precedes_framing.png
  Data/processed/granger_results.csv
"""
import sys, io, os, re, time, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
from scipy import signal

from plotting import apply_style, save_fig, panel_label, despine, WONG
from technique_patterns import TECHNIQUES, TECHNIQUE_SUBCATEGORIES, tag_techniques, tag_technique_subcategories
from taxonomy import FRAMES_CLEAN

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

# ═══════════════════════════════════════════════════════════════════════════
# LOAD AND TAG DATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
df = pd.read_csv(PROC / "all_sentences_final.csv", low_memory=False)
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df = df[df["year"].between(1985, 2025)].copy()
print(f"  {len(df):,} sentences (1985-2025)")

# Use enriched paper-level technique data if available
paper_tech_path = PROC / "paper_techniques.csv"
if paper_tech_path.exists():
    print("Loading enriched paper-level technique data...")
    paper_tech = pd.read_csv(paper_tech_path, low_memory=False)
    paper_tech["year"] = pd.to_numeric(paper_tech["year"], errors="coerce")
    paper_tech = paper_tech[paper_tech["year"].between(1985, 2025)]
    print(f"  {len(paper_tech):,} papers with enriched technique tags")
else:
    print("Tagging techniques from sentences (no enriched data)...")
    tag_techniques(df)
    tag_technique_subcategories(df)
    paper_tech = df.groupby(["pmid", "year"]).first().reset_index()

# ═══════════════════════════════════════════════════════════════════════════
# BUILD ANNUAL TIME SERIES
# ═══════════════════════════════════════════════════════════════════════════
print("\nBuilding annual time series...")

years = np.arange(1985, 2026)

# Technique prevalence: % of papers per year that use technique X
n_papers_per_year = paper_tech.groupby("year").size()

tech_ids = list(TECHNIQUES.keys()) + list(TECHNIQUE_SUBCATEGORIES.keys())
tech_series = {}
for tid in tech_ids:
    col = f"has_{tid}"
    if col not in paper_tech.columns:
        continue
    yearly = paper_tech.groupby("year")[col].mean() * 100  # percent
    tech_series[tid] = yearly.reindex(years, fill_value=0)

# Frame prevalence: % of framing sentences per year that use frame X
framing = df[df["frame_type"].notna() & (df["frame_type"] != "")].copy()
n_framing_per_year = framing.groupby("year").size()

frame_ids = ["relay", "hub", "gate", "gateway", "clinical_target", "pacemaker",
             "modulator", "integrator"]
frame_series = {}
for fid in frame_ids:
    yearly = framing[framing["frame_type"] == fid].groupby("year").size()
    # Normalize by total framing sentences per year
    yearly_pct = (yearly / n_framing_per_year * 100).reindex(years, fill_value=0)
    frame_series[fid] = yearly_pct

print(f"  Technique series: {len(tech_series)}")
print(f"  Frame series: {len(frame_series)}")

# ═══════════════════════════════════════════════════════════════════════════
# CROSS-CORRELATION AT MULTIPLE SMOOTHING LEVELS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("CROSS-CORRELATION ANALYSIS")
print("=" * 70)

MAX_LAG = 5
SMOOTHING_LEVELS = [1, 2, 3]  # 1=raw, 2=2yr rolling, 3=3yr rolling

# Key technique-frame pairs to test
KEY_PAIRS = [
    ("fMRI", "hub"),
    ("fMRI", "gateway"),
    ("functional_connectivity", "hub"),
    ("resting_state_fMRI", "hub"),
    ("electrophysiology", "relay"),
    ("optogenetics", "gate"),
    ("DBS", "clinical_target"),
    ("tract_tracing", "relay"),
    ("calcium_imaging", "gate"),
    ("structural_MRI", "hub"),
    ("diffusion_MRI", "hub"),
    ("computational_model", "hub"),
    ("patch_clamp", "relay"),
    ("single_unit", "relay"),
]


def cross_correlate(x, y, max_lag=MAX_LAG):
    """Cross-correlation at lags -max_lag to +max_lag.
    Positive lag = x leads y (technique precedes framing).
    Only uses the overlapping period where both signals have variance."""
    x = np.array(x, dtype=float)
    y = np.array(y, dtype=float)

    # Find period where both have non-trivial values (>0.1)
    valid = (x > 0.1) | (y > 0.1)
    # Expand to contiguous region
    first_valid = np.argmax(valid) if valid.any() else 0
    x = x[first_valid:]
    y = y[first_valid:]

    if len(x) < 10 or np.std(x) < 1e-6 or np.std(y) < 1e-6:
        return {lag: np.nan for lag in range(-max_lag, max_lag + 1)}

    # Replace any remaining NaN/inf
    x = np.nan_to_num(x, nan=0.0)
    y = np.nan_to_num(y, nan=0.0)

    # Standardize
    x = (x - np.mean(x)) / (np.std(x) + 1e-10)
    y = (y - np.mean(y)) / (np.std(y) + 1e-10)
    n = len(x)
    corrs = {}
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            c = np.corrcoef(x[:n-lag], y[lag:])[0, 1] if n - lag > 5 else np.nan
        else:
            c = np.corrcoef(x[-lag:], y[:n+lag])[0, 1] if n + lag > 5 else np.nan
        corrs[lag] = c if np.isfinite(c) else np.nan
    return corrs


results_all = []

for smooth in SMOOTHING_LEVELS:
    smooth_label = f"raw" if smooth == 1 else f"{smooth}yr"
    print(f"\n  Smoothing: {smooth_label}")

    for tech_id, frame_id in KEY_PAIRS:
        if tech_id not in tech_series or frame_id not in frame_series:
            continue

        ts_tech = tech_series[tech_id].copy()
        ts_frame = frame_series[frame_id].copy()

        # Apply smoothing
        if smooth > 1:
            ts_tech = ts_tech.rolling(smooth, center=True, min_periods=1).mean()
            ts_frame = ts_frame.rolling(smooth, center=True, min_periods=1).mean()

        # Cross-correlation
        corrs = cross_correlate(ts_tech.values, ts_frame.values)

        # Best lag (positive = technique leads)
        best_lag = max(corrs, key=corrs.get)
        best_r = corrs[best_lag]

        results_all.append({
            "technique": tech_id,
            "frame": frame_id,
            "smoothing": smooth_label,
            "best_lag": best_lag,
            "best_r": round(best_r, 4),
            "r_at_lag0": round(corrs[0], 4),
            "r_at_lag_minus2": round(corrs.get(-2, 0), 4),
            "r_at_lag_plus2": round(corrs.get(2, 0), 4),
        })

        if smooth == 1:  # Only print for raw
            lag_str = f"lag={best_lag:+d}" if best_lag != 0 else "simultaneous"
            direction = "TECH LEADS" if best_lag > 0 else ("FRAME LEADS" if best_lag < 0 else "SIMULTANEOUS")
            print(f"    {tech_id:25s} → {frame_id:18s}: r={best_r:.3f} ({lag_str}, {direction})")

results_df = pd.DataFrame(results_all)

# ═══════════════════════════════════════════════════════════════════════════
# GRANGER CAUSALITY
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("GRANGER CAUSALITY TESTS")
print("=" * 70)

from statsmodels.tsa.stattools import grangercausalitytests

granger_results = []

for tech_id, frame_id in KEY_PAIRS:
    if tech_id not in tech_series or frame_id not in frame_series:
        continue

    ts_tech = tech_series[tech_id].values
    ts_frame = frame_series[frame_id].values

    # Trim to overlapping period where both have signal
    valid = (ts_tech > 0.1) | (ts_frame > 0.1)
    first = np.argmax(valid) if valid.any() else 0
    ts_tech = ts_tech[first:]
    ts_frame = ts_frame[first:]

    # Need both series with some variance and enough data points
    if np.std(ts_tech) < 0.01 or np.std(ts_frame) < 0.01 or len(ts_tech) < 10:
        continue

    ts_tech = np.nan_to_num(ts_tech, nan=0.0)
    ts_frame = np.nan_to_num(ts_frame, nan=0.0)

    data = np.column_stack([ts_frame, ts_tech])  # [y, x] format for grangercausalitytests

    try:
        gc = grangercausalitytests(data, maxlag=3, verbose=False)
        # Get best lag result
        best_p = 1.0
        best_f = 0.0
        best_gc_lag = 1
        for lag in [1, 2, 3]:
            f_stat = gc[lag][0]['ssr_ftest'][0]
            p_val = gc[lag][0]['ssr_ftest'][1]
            if p_val < best_p:
                best_p = p_val
                best_f = f_stat
                best_gc_lag = lag

        sig = "***" if best_p < 0.001 else "**" if best_p < 0.01 else "*" if best_p < 0.05 else "ns"
        print(f"  {tech_id:25s} → {frame_id:18s}: F={best_f:.2f}, p={best_p:.4f} "
              f"(lag={best_gc_lag}) {sig}")

        granger_results.append({
            "technique": tech_id, "frame": frame_id,
            "granger_F": round(best_f, 3), "granger_p": round(best_p, 5),
            "granger_lag": best_gc_lag,
            "significant": best_p < 0.05,
        })
    except Exception as e:
        print(f"  {tech_id:25s} → {frame_id:18s}: FAILED ({e})")

granger_df = pd.DataFrame(granger_results)

# Also test reverse: does framing Granger-cause technique?
print("\n  Reverse direction (framing → technique):")
for tech_id, frame_id in KEY_PAIRS[:6]:  # test top 6
    if tech_id not in tech_series or frame_id not in frame_series:
        continue
    ts_tech = tech_series[tech_id].values
    ts_frame = frame_series[frame_id].values
    if np.std(ts_tech) < 0.01 or np.std(ts_frame) < 0.01:
        continue
    data_rev = np.column_stack([ts_tech, ts_frame])
    try:
        gc_rev = grangercausalitytests(data_rev, maxlag=3, verbose=False)
        best_p_rev = min(gc_rev[lag][0]['ssr_ftest'][1] for lag in [1, 2, 3])
        sig_rev = "*" if best_p_rev < 0.05 else "ns"
        print(f"    {frame_id:18s} → {tech_id:25s}: p={best_p_rev:.4f} {sig_rev}")
    except:
        pass

# ═══════════════════════════════════════════════════════════════════════════
# ROBUSTNESS: SMOOTHING COMPARISON
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("ROBUSTNESS: LAG STABILITY ACROSS SMOOTHING")
print("=" * 70)

for tech_id, frame_id in KEY_PAIRS[:8]:
    rows = results_df[(results_df["technique"] == tech_id) & (results_df["frame"] == frame_id)]
    if len(rows) == 0:
        continue
    lags = rows.set_index("smoothing")["best_lag"].to_dict()
    rs = rows.set_index("smoothing")["best_r"].to_dict()
    stable = len(set(lags.values())) == 1
    print(f"  {tech_id:25s} → {frame_id:12s}: "
          f"raw={lags.get('raw','?'):+d}(r={rs.get('raw',0):.3f}), "
          f"2yr={lags.get('2yr','?'):+d}(r={rs.get('2yr',0):.3f}), "
          f"3yr={lags.get('3yr','?'):+d}(r={rs.get('3yr',0):.3f}) "
          f"{'STABLE' if stable else 'VARIES'}")

# ═══════════════════════════════════════════════════════════════════════════
# SAVE DATA
# ═══════════════════════════════════════════════════════════════════════════
results_df.to_csv(PROC / "cross_correlation_results.csv", index=False)
if len(granger_df) > 0:
    granger_df.to_csv(PROC / "granger_results.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE: 6-panel
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("GENERATING FIGURES")
print("=" * 70)

fig, axes = plt.subplots(3, 2, figsize=(16, 18))
fig.suptitle("Does Methodology Change Precede Framing Change?",
             fontsize=15, fontweight="bold", y=0.98)

# Panel A: fMRI vs hub time series (raw + smoothed)
ax = axes[0, 0]
for smooth, ls, alpha in [(1, "-", 0.3), (3, "-", 1.0)]:
    ts_fmri = tech_series["fMRI"].rolling(smooth, center=True, min_periods=1).mean()
    ts_hub = frame_series["hub"].rolling(smooth, center=True, min_periods=1).mean()
    ax.plot(years, ts_fmri, color=WONG["blue"], linestyle=ls, alpha=alpha,
            linewidth=2 if smooth > 1 else 1,
            label=f"fMRI prevalence {'(3yr smooth)' if smooth > 1 else '(raw)'}")
    ax2 = ax.twinx()
    ax2.plot(years, ts_hub, color=WONG["red"], linestyle=ls, alpha=alpha,
             linewidth=2 if smooth > 1 else 1,
             label=f"hub frame % {'(3yr smooth)' if smooth > 1 else '(raw)'}")
ax.set_xlabel("Year")
ax.set_ylabel("fMRI prevalence (%)", color=WONG["blue"])
ax2.set_ylabel("Hub frame (%)", color=WONG["red"])
ax.set_title("A. fMRI adoption vs hub framing")
ax.legend(loc="upper left", fontsize=7)
ax2.legend(loc="lower right", fontsize=7)
despine(ax)
panel_label(ax, "A")

# Panel B: functional_connectivity vs hub
ax = axes[0, 1]
if "functional_connectivity" in tech_series:
    ts_fc = tech_series["functional_connectivity"].rolling(3, center=True, min_periods=1).mean()
    ts_hub = frame_series["hub"].rolling(3, center=True, min_periods=1).mean()
    ax.plot(years, ts_fc, color=WONG["green"], linewidth=2, label="Functional connectivity (%)")
    ax2 = ax.twinx()
    ax2.plot(years, ts_hub, color=WONG["red"], linewidth=2, label="Hub frame (%)")
    ax.set_xlabel("Year")
    ax.set_ylabel("FC prevalence (%)", color=WONG["green"])
    ax2.set_ylabel("Hub frame (%)", color=WONG["red"])
    ax.legend(loc="upper left", fontsize=8)
    ax2.legend(loc="lower right", fontsize=8)
ax.set_title("B. Functional connectivity vs hub framing")
despine(ax)
panel_label(ax, "B")

# Panel C: Cross-correlogram for top 4 pairs
ax = axes[1, 0]
lags_range = np.arange(-MAX_LAG, MAX_LAG + 1)
top_pairs = [("fMRI", "hub"), ("functional_connectivity", "hub"),
             ("electrophysiology", "relay"), ("DBS", "clinical_target")]
colors_pairs = [WONG["blue"], WONG["green"], WONG["orange"], WONG["grey"]]

for (tech, frame), color in zip(top_pairs, colors_pairs):
    if tech not in tech_series or frame not in frame_series:
        continue
    ts_t = tech_series[tech].rolling(3, center=True, min_periods=1).mean().values
    ts_f = frame_series[frame].rolling(3, center=True, min_periods=1).mean().values
    corrs = cross_correlate(ts_t, ts_f)
    ax.plot(lags_range, [corrs[l] for l in lags_range], "o-",
            color=color, linewidth=2, markersize=5,
            label=f"{tech} → {frame}")

ax.axvline(0, color="grey", linestyle="--", alpha=0.5)
ax.axhline(0, color="grey", linestyle="-", alpha=0.3)
ax.set_xlabel("Lag (years; positive = technique leads)")
ax.set_ylabel("Cross-correlation")
ax.set_title("C. Cross-correlograms (3yr smoothed)")
ax.legend(fontsize=7)
despine(ax)
panel_label(ax, "C")

# Panel D: Lag heatmap (technique × frame, color = best lag at raw)
ax = axes[1, 1]
raw_results = results_df[results_df["smoothing"] == "raw"]
tech_list = ["fMRI", "functional_connectivity", "resting_state_fMRI",
             "electrophysiology", "single_unit", "optogenetics",
             "DBS", "tract_tracing", "diffusion_MRI"]
frame_list = ["hub", "relay", "gate", "gateway", "clinical_target", "pacemaker"]

lag_matrix = np.full((len(tech_list), len(frame_list)), np.nan)
for i, tech in enumerate(tech_list):
    for j, frame in enumerate(frame_list):
        row = raw_results[(raw_results["technique"] == tech) & (raw_results["frame"] == frame)]
        if len(row) > 0:
            lag_matrix[i, j] = row.iloc[0]["best_lag"]

im = ax.imshow(lag_matrix, aspect="auto", cmap="RdBu_r", vmin=-5, vmax=5)
ax.set_xticks(range(len(frame_list)))
ax.set_xticklabels(frame_list, fontsize=8, rotation=45, ha="right")
ax.set_yticks(range(len(tech_list)))
ax.set_yticklabels(tech_list, fontsize=8)
for i in range(lag_matrix.shape[0]):
    for j in range(lag_matrix.shape[1]):
        v = lag_matrix[i, j]
        if not np.isnan(v):
            ax.text(j, i, f"{int(v):+d}", ha="center", va="center", fontsize=7,
                    color="white" if abs(v) > 3 else "black")
plt.colorbar(im, ax=ax, label="Best lag (+ = tech leads)", shrink=0.8)
ax.set_title("D. Optimal lag (technique → frame)")
panel_label(ax, "D")

# Panel E: Granger causality summary
ax = axes[2, 0]
if len(granger_df) > 0:
    gc_sig = granger_df[granger_df["significant"]].sort_values("granger_F", ascending=True)
    gc_ns = granger_df[~granger_df["significant"]].sort_values("granger_F", ascending=True)

    all_gc = pd.concat([gc_ns, gc_sig])
    y_pos = np.arange(len(all_gc))
    colors_gc = [WONG["red"] if s else WONG["grey"] for s in all_gc["significant"]]
    labels_gc = [f"{r.technique} → {r.frame}" for _, r in all_gc.iterrows()]

    ax.barh(y_pos, all_gc["granger_F"], color=colors_gc, alpha=0.8)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels_gc, fontsize=7)
    ax.set_xlabel("Granger F-statistic")
    ax.axvline(3.0, color="grey", linestyle="--", alpha=0.5, label="~p=0.05 threshold")
    ax.legend(fontsize=8)

ax.set_title("E. Granger causality (red = significant)")
despine(ax)
panel_label(ax, "E")

# Panel F: Robustness — lag stability across smoothing
ax = axes[2, 1]
stability_data = []
for tech_id, frame_id in KEY_PAIRS[:8]:
    rows = results_df[(results_df["technique"] == tech_id) & (results_df["frame"] == frame_id)]
    if len(rows) < 3:
        continue
    for _, r in rows.iterrows():
        stability_data.append({
            "pair": f"{tech_id}→{frame_id}",
            "smoothing": r["smoothing"],
            "lag": r["best_lag"],
            "r": r["best_r"],
        })

if stability_data:
    stab_df = pd.DataFrame(stability_data)
    pairs = stab_df["pair"].unique()
    x_pos = {"raw": 0, "2yr": 1, "3yr": 2}
    for i, pair in enumerate(pairs):
        pdata = stab_df[stab_df["pair"] == pair]
        xs = [x_pos[s] for s in pdata["smoothing"]]
        ax.plot(xs, pdata["lag"], "o-", label=pair, markersize=5, linewidth=1.5)

    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels(["Raw annual", "2-year smooth", "3-year smooth"])
    ax.set_ylabel("Best lag (years)")
    ax.axhline(0, color="grey", linestyle="--", alpha=0.5)
    ax.legend(fontsize=6, ncol=2)

ax.set_title("F. Lag stability across smoothing levels")
despine(ax)
panel_label(ax, "F")

plt.tight_layout(rect=[0, 0, 1, 0.96])
save_fig(fig, PUB / "fig_method_precedes_framing.png")
print("  Saved fig_method_precedes_framing.png")

print("\nDone.")
