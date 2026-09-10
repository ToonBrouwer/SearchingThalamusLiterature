"""
Phase F — Publication-quality figures.

Remakes the key analysis figures at DPI 300 with consistent typography,
a colorblind-safe palette (Wong 2011), and saves each as both PNG and SVG.

Figures produced (Graphs/pub/):
  pub_fig1_temporal_trend.{png,svg}   — Normalised relay + hub % over time
                                         with Mann-Kendall annotations and
                                         Wilson 95% CI ribbons
  pub_fig2_umap.{png,svg}             — UMAP coloured by framing + by year
  pub_fig3_narrative.{png,svg}        — 3-panel relay→hub transition story
  pub_fig4_conclusion_heatmap.{png,svg} — framing × conclusion-cluster heatmap
  pub_fig5_knowledge_map.{png,svg}    — UMAP of conclusion sentences coloured
                                         by year (left) and framing (right)

Inputs (Data/processed/):
    framing_embedded_clustered.csv
    abstract_framing_classified.csv
    not_merely_relay_sentences.csv
    conclusion_sentences_clustered.csv
    conclusion_framing_matrix.csv

Run phase_e_conclusion_clustering.py before this script.
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.colors import Normalize, BoundaryNorm
from matplotlib import cm
from scipy.stats import kendalltau
from pathlib import Path

# ── Project paths ─────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT  = _ANALYSIS_DIR.parents[1]
PROC  = ROOT / "Data" / "processed"
GRAPH = ROOT / "Graphs"
PUB   = GRAPH / "pub"        # publication-quality output folder
PUB.mkdir(parents=True, exist_ok=True)

# ── Import shared style ───────────────────────────────────────────────────────
import sys as _sys
_sys.path.insert(0, str(_ANALYSIS_DIR))
from plot_style import apply_style, COLORS, save_fig, FIG_2COL
apply_style()

# ── Framing group definitions ─────────────────────────────────────────────────
RELAY_IDS = {55, 56, 46}
HUB_IDS   = {53, 54}

def broad_frame_cluster(cid):
    if cid in RELAY_IDS: return "relay"
    if cid in HUB_IDS:   return "hub"
    return "other"

# ── Wilson CI helper ──────────────────────────────────────────────────────────
def wilson_ci(k, n, z=1.96):
    if n == 0: return 0.0, 0.0
    p      = k / n
    denom  = 1 + z**2 / n
    center = (p + z**2 / (2*n)) / denom
    margin = z * np.sqrt(p*(1-p)/n + z**2/(4*n**2)) / denom
    return float(center - margin), float(center + margin)

# ── Load all datasets ─────────────────────────────────────────────────────────
print("Loading datasets …")
ft = pd.read_csv(PROC / "framing_embedded_clustered.csv", dtype={"pmid": str})
ft["year"]  = pd.to_numeric(ft["year"], errors="coerce")
ft = ft[ft["cluster_id"] != -1].copy()
ft["broad"] = ft["cluster_id"].apply(broad_frame_cluster)

ab = pd.read_csv(PROC / "abstract_framing_classified.csv", dtype={"pmid": str})
ab["year"] = pd.to_numeric(ab["year"], errors="coerce")

not_relay = pd.read_csv(PROC / "not_merely_relay_sentences.csv", dtype={"pmid": str})
not_relay["year"] = pd.to_numeric(not_relay["year"], errors="coerce")

concl = pd.read_csv(PROC / "conclusion_sentences_clustered.csv", dtype={"pmid": str})
concl["year"] = pd.to_numeric(concl["year"], errors="coerce")

matrix = pd.read_csv(PROC / "conclusion_framing_matrix.csv", index_col=0)


# ── Helper: yearly % trend with CI ───────────────────────────────────────────
def yearly_trend(df, framing, broad_col="broad", year_lo=1980, year_hi=2026,
                 min_n=5, roll=5):
    """Return (years, pct_raw, pct_smooth, lo_arr, hi_arr) for a given framing type.

    Mann-Kendall should always be computed on pct_raw (not pct_smooth) to
    avoid inflating significance through the rolling average.
    """
    sub = df[df["year"].between(year_lo, year_hi)]
    yr  = (sub.groupby(["year", broad_col]).size()
              .unstack(fill_value=0))
    if framing not in yr.columns:
        yr[framing] = 0
    yr["total"] = yr.sum(axis=1)
    yr = yr[yr["total"] >= min_n]
    pct    = yr[framing] / yr["total"] * 100
    smooth = pct.rolling(roll, min_periods=2, center=True).mean()
    lo = np.array([wilson_ci(int(yr.loc[y, framing]),
                             int(yr.loc[y, "total"]))[0]*100 for y in pct.index])
    hi = np.array([wilson_ci(int(yr.loc[y, framing]),
                             int(yr.loc[y, "total"]))[1]*100 for y in pct.index])
    return pct.index.values, pct.values, smooth.values, lo, hi


# ══════════════════════════════════════════════════════════════════════════════
# Figure 1 — Normalised temporal trend (relay + hub, full text + abstracts)
# ══════════════════════════════════════════════════════════════════════════════
print("\nFigure 1 — Temporal trend …")

fig, axes = plt.subplots(2, 1, figsize=(FIG_2COL, 6), sharex=True)
fig.subplots_adjust(hspace=0.08)

for ax, framing, color in [
    (axes[0], "relay", COLORS["relay"]),
    (axes[1], "hub",   COLORS["hub"]),
]:
    yrs_ft, raw_ft, sm_ft, lo_ft, hi_ft = yearly_trend(ft, framing)
    yrs_ab, raw_ab, sm_ab, lo_ab, hi_ab = yearly_trend(ab, framing, broad_col="broad")

    # Wilson CI ribbon for full text
    ax.fill_between(yrs_ft, lo_ft, hi_ft, alpha=0.15, color=color, lw=0)
    # Smoothed lines
    ax.plot(yrs_ft, sm_ft, color=color, lw=2.2, label="Full text", zorder=3)
    ax.plot(yrs_ab, sm_ab, color=color, lw=1.8, ls="--",
            alpha=0.8, label="Abstracts", zorder=3)

    # Mann-Kendall on RAW yearly proportions (not smoothed) — avoids
    # inflating significance through the rolling mean.
    raw_series = pd.Series(raw_ft, index=yrs_ft).dropna()
    if len(raw_series) >= 5:
        tau, pval = kendalltau(raw_series.index, raw_series.values)
        sig = "***" if pval < 0.001 else ("**" if pval < 0.01 else
              ("*" if pval < 0.05 else "ns"))
        ax.text(0.97, 0.90,
                f"Mann–Kendall τ = {tau:.2f}, {sig}",
                transform=ax.transAxes, ha="right", fontsize=9,
                color=color)

    ax.set_ylabel("% of framing sentences")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left")
    ax.spines["bottom"].set_visible(ax == axes[1])

    framing_label = "Relay / gateway" if framing == "relay" else "Hub / integration"
    ax.set_title(framing_label, color=color, loc="left", pad=4)

axes[1].set_xlabel("Year")
axes[1].set_xlim(1980, 2026)
fig.suptitle("Normalised thalamus framing over time (5-yr rolling mean, Wilson 95% CI)",
             y=1.01)
save_fig(fig, PUB / "pub_fig1_temporal_trend")
plt.close()


# ══════════════════════════════════════════════════════════════════════════════
# Figure 2 — UMAP semantic landscape
# ══════════════════════════════════════════════════════════════════════════════
print("Figure 2 — UMAP …")

umap_df = ft[ft["umap_x"].notna() & ft["umap_y"].notna()].copy()

FRAMING_PALETTE = {
    "relay":               COLORS["relay"],
    "gateway":             COLORS["orange"],
    "hub_node":            COLORS["hub"],
    "integrator":          COLORS["sky"],
    "network_component":   COLORS["green"],
    "modulator":           COLORS["pink"],
    "role_player":         COLORS["yellow"],
    "structure_description": "#AAAAAA",
    "clinical_target":     "#333333",
    "involved_in":         "#BBBBBB",
    "other":               "#E0E0E0",
}

fig, axes = plt.subplots(1, 2, figsize=(FIG_2COL, 4.5))

# Panel A: coloured by framing keyword
ax = axes[0]
# Plot "other" first (background)
for kw in ["other", "structure_description", "involved_in", "clinical_target",
           "role_player", "modulator", "network_component",
           "integrator", "gateway", "hub_node", "relay"]:
    sub   = umap_df[umap_df["framing_primary"] == kw]
    size  = 18 if kw in ("relay", "hub_node", "integrator", "gateway") else 6
    alpha = 0.75 if kw in ("relay", "hub_node", "integrator", "gateway") else 0.20
    ax.scatter(sub["umap_x"], sub["umap_y"],
               c=FRAMING_PALETTE.get(kw, "#E0E0E0"),
               s=size, alpha=alpha, linewidths=0,
               label=kw if kw in ("relay","hub_node","integrator","gateway") else "_nolegend_",
               rasterized=True)

ax.legend(title="Framing", fontsize=8, markerscale=2, loc="lower right",
          handletextpad=0.3, labelspacing=0.3)
ax.set_title("(A) Framing keyword", loc="left")
ax.axis("off")

# Panel B: coloured by year
ax = axes[1]
yr_valid = umap_df[umap_df["year"].between(1975, 2026)]
norm = Normalize(vmin=1980, vmax=2025)
sc   = ax.scatter(yr_valid["umap_x"], yr_valid["umap_y"],
                  c=yr_valid["year"], cmap="plasma", norm=norm,
                  s=6, alpha=0.4, linewidths=0, rasterized=True)
cbar = plt.colorbar(sc, ax=ax, shrink=0.75, pad=0.02)
cbar.set_label("Year", fontsize=9)
cbar.ax.tick_params(labelsize=8)
ax.set_title("(B) Publication year", loc="left")
ax.axis("off")

fig.suptitle("Semantic landscape of thalamus framing sentences (UMAP of sentence embeddings)")
save_fig(fig, PUB / "pub_fig2_umap")
plt.close()


# ══════════════════════════════════════════════════════════════════════════════
# Figure 3 — Narrative: the relay → hub transition
# ══════════════════════════════════════════════════════════════════════════════
print("Figure 3 — Narrative …")

fig = plt.figure(figsize=(FIG_2COL, 9))
gs  = gridspec.GridSpec(3, 1, hspace=0.08, figure=fig,
                        top=0.93, bottom=0.07, left=0.10, right=0.97)
axes = [fig.add_subplot(gs[i]) for i in range(3)]

panel_labels = ["A", "B", "C"]

for ax, framing, color, ab_col, panel in [
    (axes[0], "relay", COLORS["relay"], "broad", "A"),
    (axes[1], "hub",   COLORS["hub"],   "broad", "B"),
]:
    yrs_ft, raw_ft, sm_ft, lo_ft, hi_ft = yearly_trend(ft, framing)
    yrs_ab, raw_ab, sm_ab, lo_ab, hi_ab = yearly_trend(ab, framing, broad_col=ab_col)

    ax.fill_between(yrs_ft, lo_ft, hi_ft, alpha=0.15, color=color, lw=0)
    ax.plot(yrs_ft, sm_ft, color=color, lw=2.2, label="Full text", zorder=3)
    ax.plot(yrs_ab, sm_ab, color=color, lw=1.8, ls="--",
            alpha=0.8, label="Abstracts", zorder=3)

    # Mann-Kendall on RAW yearly proportions (not smoothed)
    raw_series = pd.Series(raw_ft, index=yrs_ft).dropna()
    if len(raw_series) >= 5:
        tau, pval = kendalltau(raw_series.index, raw_series.values)
        sig = "***" if pval<0.001 else ("**" if pval<0.01 else
              ("*" if pval<0.05 else "ns"))
        ax.text(0.97, 0.88, f"τ = {tau:.2f}, {sig}",
                transform=ax.transAxes, ha="right", fontsize=9, color=color)

    label = "Relay / gateway framing (%)" if framing=="relay" else "Hub / integration framing (%)"
    ax.set_ylabel(label, fontsize=10)
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=9, loc="upper left", frameon=True)
    ax.tick_params(labelbottom=False)
    # Panel letter
    ax.text(-0.08, 1.0, panel, transform=ax.transAxes,
            fontsize=13, fontweight="bold", va="top")

# Panel C: 'not merely a relay'
ax = axes[2]
ch = (not_relay[not_relay["year"].between(1980, 2026)]
      .groupby("year").size()
      .reindex(range(1980, 2027), fill_value=0))
ax.bar(ch.index, ch.values, color=COLORS["relay"], alpha=0.45,
       width=0.8, zorder=2, label="Challenge sentences (n)")
ax.plot(ch.index, ch.rolling(5, min_periods=2, center=True).mean(),
        color=COLORS["relay"], lw=2.2, zorder=3)
ax.set_ylabel("'Not merely a relay'\nsentences per year", fontsize=10)
ax.set_xlabel("Year", fontsize=11)
ax.tick_params(labelbottom=True)
ax.text(-0.08, 1.0, "C", transform=ax.transAxes,
        fontsize=13, fontweight="bold", va="top")

# Shared vertical marker
for ax in axes:
    ax.axvline(2010, color="#888888", ls=":", lw=1.0, zorder=1)
    ax.set_xlim(1980, 2026)

axes[0].text(2010.4, axes[0].get_ylim()[1]*0.92, "2010",
             color="#888888", fontsize=8)

fig.suptitle("The relay → hub transition in thalamus framing (1980–2026)",
             fontsize=13, fontweight="bold")
save_fig(fig, PUB / "pub_fig3_narrative")
plt.close()


# ══════════════════════════════════════════════════════════════════════════════
# Figure 4 — Framing × conclusion cluster heatmap
# ══════════════════════════════════════════════════════════════════════════════
print("Figure 4 — Framing × conclusion heatmap …")

# Normalise to % within each framing type (row-normalise)
mat = matrix.loc[matrix.index.isin(["relay","hub","other"])].copy()
mat_pct = mat.div(mat.sum(axis=1), axis=0) * 100

# Keep only the top 20 conclusion clusters by total mentions
top_cols = mat.sum(axis=0).nlargest(20).index
mat_pct  = mat_pct[top_cols]

# Get cluster labels (short descriptions from cluster_examples)
try:
    ex = pd.read_csv(PROC / "conclusion_cluster_examples.csv")
    # Use dominant framing + first few words of first example sentence as label
    def short_label(cid):
        sub = ex[ex["cluster_id"] == cid]
        if len(sub) == 0: return str(cid)
        sent = str(sub.iloc[0]["sentence"])[:60]
        return f"C{cid}: {sent}…"
    col_labels = [short_label(int(c)) for c in top_cols]
except Exception:
    col_labels = [str(c) for c in top_cols]

fig, ax = plt.subplots(figsize=(FIG_2COL, 3.5))
im = ax.imshow(mat_pct.values, aspect="auto", cmap="Blues",
               vmin=0, vmax=mat_pct.values.max())
cbar = plt.colorbar(im, ax=ax, shrink=0.8, pad=0.02)
cbar.set_label("% of papers with this framing", fontsize=9)
cbar.ax.tick_params(labelsize=8)

ax.set_xticks(range(len(top_cols)))
ax.set_xticklabels([f"C{c}" for c in top_cols], rotation=45, ha="right", fontsize=8)
ax.set_yticks(range(len(mat_pct)))
ax.set_yticklabels(mat_pct.index, fontsize=10)

# Annotate cells
for r in range(len(mat_pct)):
    for c in range(len(top_cols)):
        val = mat_pct.values[r, c]
        if val > 1:
            ax.text(c, r, f"{val:.0f}", ha="center", va="center",
                    fontsize=7,
                    color="white" if val > mat_pct.values.max()*0.6 else "black")

ax.set_title("Framing × conclusion cluster co-occurrence\n(% of papers per framing type)",
             loc="left")
ax.set_xlabel("Conclusion cluster ID")
ax.set_ylabel("Paper framing")
plt.tight_layout()
save_fig(fig, PUB / "pub_fig4_conclusion_heatmap")
plt.close()


# ══════════════════════════════════════════════════════════════════════════════
# Figure 5 — Temporal knowledge map (UMAP of conclusion sentences)
# ══════════════════════════════════════════════════════════════════════════════
print("Figure 5 — Knowledge map …")

concl_umap = concl[concl["umap_x"].notna() & concl["year"].between(1980, 2026)].copy()

fig, axes = plt.subplots(1, 2, figsize=(FIG_2COL, 4.5))

# Panel A: coloured by year
ax = axes[0]
norm = Normalize(vmin=1985, vmax=2025)
sc   = ax.scatter(concl_umap["umap_x"], concl_umap["umap_y"],
                  c=concl_umap["year"], cmap="plasma", norm=norm,
                  s=5, alpha=0.35, linewidths=0, rasterized=True)
cbar = plt.colorbar(sc, ax=ax, shrink=0.75, pad=0.02)
cbar.set_label("Year", fontsize=9)
cbar.ax.tick_params(labelsize=8)
ax.set_title("(A) Conclusion sentences by year", loc="left")
ax.axis("off")

# Panel B: coloured by framing of the paper
ax = axes[1]
framing_order = ["relay", "hub", "other"]
framing_colors = [COLORS["relay"], COLORS["hub"], COLORS["other"]]

for framing, color in zip(framing_order, framing_colors):
    sub = concl_umap[concl_umap["framing_broad"] == framing]
    size  = 8 if framing in ("relay","hub") else 4
    alpha = 0.5 if framing in ("relay","hub") else 0.15
    ax.scatter(sub["umap_x"], sub["umap_y"],
               c=color, s=size, alpha=alpha, linewidths=0,
               label=f"{framing} (n={len(sub):,})", rasterized=True)

ax.legend(title="Paper framing", fontsize=8, markerscale=2,
          loc="lower right", handletextpad=0.3)
ax.set_title("(B) Conclusion sentences by paper framing", loc="left")
ax.axis("off")

fig.suptitle("Semantic map of conclusion sentences — do relay and hub papers conclude differently?")
save_fig(fig, PUB / "pub_fig5_knowledge_map")
plt.close()

print("\nAll publication figures saved to Graphs/pub/")
