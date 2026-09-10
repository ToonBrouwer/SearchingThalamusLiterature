"""
Phase C — Showcase figures
===========================
Produces three publication-quality figures:

  fig_C1_appositives.png    — Appositive phrase analysis: what metaphors appear
                              in "the thalamus, [PHRASE], is…" constructions,
                              and how do they shift over time?

  fig_C2_umap_showcase.png  — UMAP embedding coloured by (a) framing keyword
                              and (b) publication year — shows how semantic
                              space and temporal change align.

  fig_C3_narrative.png      — 3-panel story figure:
                                Top   : relay framing % over time (abstract + full-text)
                                Middle: hub framing % over time
                                Bottom: 'not merely a relay' challenges per year
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D
from matplotlib.colors import Normalize
from matplotlib import cm
from collections import Counter
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT    = _ANALYSIS_DIR.parents[1]
PROC    = ROOT / "Data" / "processed"
INTERIM = ROOT / "Data" / "interim"
GRAPH   = ROOT / "Graphs"

# ── Load datasets ─────────────────────────────────────────────────────────────
ft = pd.read_csv(PROC / "framing_embedded_clustered.csv", dtype={"pmid": str})
ft["year"] = pd.to_numeric(ft["year"], errors="coerce")
ft = ft[ft["cluster_id"] != -1].copy()

ab = pd.read_csv(PROC / "abstract_framing_classified.csv", dtype={"pmid": str})
ab["year"] = pd.to_numeric(ab["year"], errors="coerce")

not_relay = pd.read_csv(PROC / "not_merely_relay_sentences.csv", dtype={"pmid": str})
not_relay["year"] = pd.to_numeric(not_relay["year"], errors="coerce")

# ── Wilson CI helper ──────────────────────────────────────────────────────────
def wilson_ci(k, n, z=1.96):
    if n == 0: return 0.0, 0.0
    p      = k / n
    denom  = 1 + z**2 / n
    center = (p + z**2 / (2*n)) / denom
    margin = z * np.sqrt(p*(1-p)/n + z**2/(4*n**2)) / denom
    return float(center - margin), float(center + margin)

# ══════════════════════════════════════════════════════════════════════════════
# C1 · Appositive phrase analysis
# ══════════════════════════════════════════════════════════════════════════════
# Combine appositives from full-text and abstract framing sentences
ft_app = ft[ft["appositive_phrase"].notna()][["year","appositive_phrase"]].copy()
ab_app = ab[ab["appositive_phrase"].notna()][["year","appositive_phrase"]].copy()
all_app = pd.concat([ft_app, ab_app], ignore_index=True)
all_app["year"] = pd.to_numeric(all_app["year"], errors="coerce")

print(f"Total appositive phrases: {len(all_app):,}")

# Semantic categories for appositive content
APP_CATS = {
    "relay station":  r"\brelay\s*(?:station|center|hub|nucleus|structure)?\b",
    "hub / node":     r"\b(hub|node|nexus|central\s+(?:node|hub))\b",
    "gateway / gate": r"\b(gateway|gate(?:keeper)?|portal)\b",
    "key structure":  r"\b(key|critical|crucial|essential|important|pivotal)\s+(?:sub)?(?:cortical\s+)?structure\b",
    "crossroad":      r"\b(crossroad|junction|intersection|interface|bridge)\b",
    "integrator":     r"\b(integrat(?:or|ion|ing))\b",
    "complex structure": r"\bcomplex\s+(?:brain\s+)?structure\b",
    "filter":         r"\bfilter(?:ing)?\b",
}

for cat, pat in APP_CATS.items():
    all_app[cat] = all_app["appositive_phrase"].str.contains(
        pat, case=False, na=False, regex=True)

# Top raw phrases (for the bar chart)
phrase_counts = Counter(
    p.lower().strip() for p in all_app["appositive_phrase"]
    if isinstance(p, str) and len(p) > 3
)
top_phrases = pd.Series(dict(phrase_counts.most_common(25)))

# Temporal: % of appositives falling into each category per decade
all_app["decade"] = pd.cut(all_app["year"],
                            bins=[1949,1979,1989,1999,2009,2019,2026],
                            labels=["pre-1980","1980s","1990s",
                                    "2000s","2010s","2020+"])
dec_cat = (all_app.groupby("decade")[list(APP_CATS.keys())]
                  .mean() * 100)

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Panel 1: top phrases
ax = axes[0]
top_phrases.sort_values().tail(20).plot(kind="barh", ax=ax, color="#59B069")
ax.set_xlabel("Count")
ax.set_title("Most frequent appositive phrases\n'the thalamus, [PHRASE], …'")

# Panel 2: category trend by decade
ax = axes[1]
dec_cat_plot = dec_cat[["relay station","hub / node","gateway / gate",
                          "integrator","crossroad","filter"]].T
dec_cat_plot.plot(kind="bar", ax=ax, colormap="tab10", width=0.7)
ax.set_xlabel("Appositive category")
ax.set_ylabel("% of appositive sentences")
ax.set_title("Appositive framing category by decade")
ax.legend(title="Decade", fontsize=8, loc="upper right")
ax.tick_params(axis="x", rotation=30)

plt.tight_layout()
fig.savefig(GRAPH / "fig_C1_appositives.png", dpi=150)
plt.close()
print("Saved fig_C1_appositives.png")

# ══════════════════════════════════════════════════════════════════════════════
# C2 · UMAP showcase: framing keyword + year gradient
# ══════════════════════════════════════════════════════════════════════════════
umap_df = ft[ft["umap_x"].notna() & ft["umap_y"].notna()].copy()

FRAMING_COLORS = {
    "relay":               "#E8342A",
    "gateway":             "#F97F51",
    "hub_node":            "#2E86AB",
    "integrator":          "#1A5276",
    "network_component":   "#48C9B0",
    "modulator":           "#9B59B6",
    "role_player":         "#F7DC6F",
    "structure_description":"#AAB7B8",
    "clinical_target":     "#922B21",
    "involved_in":         "#BDC3C7",
    "other":               "#D5D8DC",
}

fig, axes = plt.subplots(1, 2, figsize=(16, 7))

# Panel A: coloured by framing keyword
ax = axes[0]
for kw, color in FRAMING_COLORS.items():
    sub = umap_df[umap_df["framing_primary"] == kw]
    size = 18 if kw in ("relay","hub_node","gateway","integrator") else 8
    alpha = 0.7 if kw in ("relay","hub_node","gateway","integrator") else 0.3
    ax.scatter(sub["umap_x"], sub["umap_y"],
               c=color, s=size, alpha=alpha, label=kw, rasterized=True)
ax.set_title("UMAP coloured by framing keyword", fontsize=11)
ax.legend(fontsize=7, markerscale=1.5, loc="lower right",
          framealpha=0.9, ncol=2)
ax.set_xlabel("UMAP 1"); ax.set_ylabel("UMAP 2")
ax.axis("off")

# Panel B: coloured by year
ax = axes[1]
yr_valid = umap_df[umap_df["year"].between(1975, 2026)]
norm  = Normalize(vmin=1975, vmax=2026)
cmap  = cm.plasma
sc = ax.scatter(yr_valid["umap_x"], yr_valid["umap_y"],
                c=yr_valid["year"], cmap=cmap, norm=norm,
                s=8, alpha=0.4, rasterized=True)
cbar = plt.colorbar(sc, ax=ax, shrink=0.8)
cbar.set_label("Year", fontsize=9)
ax.set_title("UMAP coloured by publication year", fontsize=11)
ax.set_xlabel("UMAP 1"); ax.set_ylabel("UMAP 2")
ax.axis("off")

fig.suptitle("Semantic landscape of thalamus framing sentences", fontsize=13)
plt.tight_layout()
fig.savefig(GRAPH / "fig_C2_umap_showcase.png", dpi=150)
plt.close()
print("Saved fig_C2_umap_showcase.png")

# ══════════════════════════════════════════════════════════════════════════════
# C3 · Narrative figure: the relay → hub transition story
# ══════════════════════════════════════════════════════════════════════════════
RELAY_IDS = {55, 56, 46}
HUB_IDS   = {53, 54}
ft["broad"] = ft["cluster_id"].apply(
    lambda c: "relay" if c in RELAY_IDS else ("hub" if c in HUB_IDS else "other"))

def yearly_pct(df_in, framing, broad_col="broad",
               year_range=(1970,2026), min_n=5, roll=5):
    sub = df_in[df_in["year"].between(*year_range)]
    yr  = (sub.groupby(["year", broad_col]).size()
              .unstack(fill_value=0))
    if framing not in yr.columns:
        yr[framing] = 0
    yr["total"] = yr.sum(axis=1)
    pct = (yr[framing] / yr["total"].replace(0,1) * 100)
    pct = pct[yr["total"] >= min_n]
    lo_arr = [wilson_ci(int(yr.loc[y, framing]) if y in yr.index else 0,
                        int(yr.loc[y, "total"])  if y in yr.index else 1)[0]*100
              for y in pct.index]
    hi_arr = [wilson_ci(int(yr.loc[y, framing]) if y in yr.index else 0,
                        int(yr.loc[y, "total"])  if y in yr.index else 1)[1]*100
              for y in pct.index]
    smooth = pct.rolling(roll, min_periods=2, center=True).mean()
    return pct, smooth, np.array(lo_arr), np.array(hi_arr)

fig = plt.figure(figsize=(14, 10))
gs  = gridspec.GridSpec(3, 1, hspace=0.12, figure=fig)
axes = [fig.add_subplot(gs[i]) for i in range(3)]

# ── Panel 1: relay ────────────────────────────────────────────────────────────
ax = axes[0]
pct_ft,  sm_ft,  lo_ft,  hi_ft  = yearly_pct(ft,  "relay", year_range=(1980,2026))
pct_ab,  sm_ab,  lo_ab,  hi_ab  = yearly_pct(ab,  "relay", broad_col="broad",
                                               year_range=(1980,2026))
ax.fill_between(pct_ft.index, lo_ft, hi_ft, alpha=0.15, color="#E8342A")
ax.plot(sm_ft.index, sm_ft.values, color="#E8342A", lw=2.5, label="Full text")
ax.plot(sm_ab.index, sm_ab.values, color="#E8342A", lw=2.0, ls="--",
        label="Abstracts")
ax.set_ylabel("% of framing sentences", fontsize=10)
ax.set_title("Relay / gateway framing", fontsize=11, color="#E8342A", loc="left")
ax.legend(fontsize=9); ax.set_ylim(0); ax.set_xlim(1980, 2026)
ax.tick_params(labelbottom=False)

# ── Panel 2: hub ──────────────────────────────────────────────────────────────
ax = axes[1]
pct_fth, sm_fth, lo_fth, hi_fth = yearly_pct(ft,  "hub", year_range=(1980,2026))
pct_abh, sm_abh, lo_abh, hi_abh = yearly_pct(ab,  "hub", broad_col="broad",
                                               year_range=(1980,2026))
ax.fill_between(pct_fth.index, lo_fth, hi_fth, alpha=0.15, color="#2E86AB")
ax.plot(sm_fth.index, sm_fth.values, color="#2E86AB", lw=2.5, label="Full text")
ax.plot(sm_abh.index, sm_abh.values, color="#2E86AB", lw=2.0, ls="--",
        label="Abstracts")
ax.set_ylabel("% of framing sentences", fontsize=10)
ax.set_title("Hub / integration framing", fontsize=11, color="#2E86AB", loc="left")
ax.legend(fontsize=9); ax.set_ylim(0); ax.set_xlim(1980, 2026)
ax.tick_params(labelbottom=False)

# ── Panel 3: 'not merely a relay' challenges ─────────────────────────────────
ax = axes[2]
ch = (not_relay[not_relay["year"].between(1980, 2026)]
      .groupby("year").size())
ch = ch.reindex(range(1980, 2027), fill_value=0)
ax.bar(ch.index, ch.values, color="#E8342A", alpha=0.55, width=0.8,
       label="Challenge sentences")
smooth_ch = ch.rolling(5, min_periods=2, center=True).mean()
ax.plot(smooth_ch.index, smooth_ch.values, color="#922B21", lw=2)
ax.set_ylabel("n sentences", fontsize=10)
ax.set_xlabel("Year", fontsize=10)
ax.set_title("'Not merely a relay' — explicit challenges to relay framing",
             fontsize=11, color="#922B21", loc="left")
ax.set_xlim(1980, 2026)

# Shared vertical marker at 2010
for ax in axes:
    ax.axvline(2010, color="grey", ls=":", lw=1, alpha=0.6)
    ax.text(2010.3, ax.get_ylim()[1] * 0.85, "2010", color="grey", fontsize=7)

fig.suptitle("The relay → hub transition in thalamus framing (1980–2026)",
             fontsize=13, y=1.01)
fig.savefig(GRAPH / "fig_C3_narrative.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved fig_C3_narrative.png")
