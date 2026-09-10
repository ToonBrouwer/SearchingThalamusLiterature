"""
Phase B — Step 2: Analyse abstract framing sentences
======================================================
Applies the keyword-based framing classifier to the abstract framing sentences
extracted in phase_b_extract_abstract_framing.py, then:

  1. Plots the extended temporal trend (1950–2026) from abstracts.
  2. Replication check — does the abstract trend match the full-text trend?
  3. Compares framing keyword distributions between abstracts and full text.

Outputs:
  Data/processed/abstract_framing_classified.csv
  Graphs/fig_B1_abstract_temporal_extended.png
  Graphs/fig_B2_abstract_vs_fulltext_replication.png
  Graphs/fig_B3_abstract_keyword_distribution.png
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT    = _ANALYSIS_DIR.parents[1]
INTERIM = ROOT / "Data" / "interim" / "sentence_extraction"
PROC    = ROOT / "Data" / "processed"
GRAPH   = ROOT / "Graphs"

# ── Load abstract framing sentences ──────────────────────────────────────────
ab = pd.read_csv(INTERIM / "thalamus_abstract_framing_sentences.csv", dtype={"pmid": str})
ab["year"] = pd.to_numeric(ab["year"], errors="coerce")
print(f"Abstract framing sentences: {len(ab):,}")

# ── Keyword classifier (mirrors full-text classification) ─────────────────────
FRAMING_KEYWORDS = {
    "relay":               r"\b(relay\s*(?:station|center|hub|nucleus)?|relays?|relaying)\b",
    "hub_node":            r"\b(hub|nexus|node\b|central\s+node)\b",
    "gateway":             r"\b(gateway|gate(?:keeper)?|portal|entrance\s+to\s+(?:the\s+)?cortex)\b",
    "modulator":           r"\b(modulat|regulat(?:es?|ing|or)|controls?\s+(?:information|signal|flow))\b",
    "integrator":          r"\b(integrat(?:es?|ing|ion|or)|integration\s+(?:center|hub|node))\b",
    "role_player":         r"\bplays?\s+a\s+(?:key|central|critical|crucial|pivotal|important)?\s*role\b",
    "network_component":   r"\b(network\s+(?:node|hub)|(?:brain|cortical|thalamo.cortical)\s+network)\b",
    "structure_description": r"\b(complex\s+structure|heterogeneous|divided\s+into\s+nuclei|anatomical)\b",
    "clinical_target":     r"\b(target(?:ed)?\s+(?:for|by|in)\s+(?:dbs|stimulation|surgery|therapy)|treatment\s+target)\b",
    "involved_in":         r"\b(involved?\s+in|implicated\s+in|participates?\s+in)\b",
}

def classify_framing(sentence):
    s = str(sentence)
    for label, pat in FRAMING_KEYWORDS.items():
        if re.search(pat, s, re.IGNORECASE):
            return label
    return "other"

ab["framing_primary"] = ab["sentence"].apply(classify_framing)

# Broad relay / hub grouping
RELAY_KW = {"relay", "gateway"}
HUB_KW   = {"hub_node", "integrator", "network_component"}

def broad_frame_kw(kw):
    if kw in RELAY_KW: return "relay"
    if kw in HUB_KW:   return "hub"
    return "other"

ab["broad"] = ab["framing_primary"].apply(broad_frame_kw)
ab.to_csv(PROC / "abstract_framing_classified.csv", index=False)
print("Saved abstract_framing_classified.csv")

# ── Wilson CI helper ──────────────────────────────────────────────────────────
def wilson_ci(k, n, z=1.96):
    if n == 0: return 0.0, 0.0
    p      = k / n
    denom  = 1 + z**2 / n
    center = (p + z**2 / (2*n)) / denom
    margin = z * np.sqrt(p*(1-p)/n + z**2/(4*n**2)) / denom
    return float(center - margin), float(center + margin)

# ── 1. Extended temporal trend (1950–2026) ────────────────────────────────────
yr_ab = (ab[ab["year"].between(1950, 2026)]
         .groupby(["year","broad"]).size()
         .unstack(fill_value=0)
         .reindex(columns=["relay","hub","other"], fill_value=0))
yr_ab["total"] = yr_ab.sum(axis=1)

# Only plot years with ≥ 3 framing sentences
yr_ab = yr_ab[yr_ab["total"] >= 3]

fig, axes = plt.subplots(2, 1, figsize=(14, 7), sharex=True)
colors = {"relay": "#E8342A", "hub": "#2E86AB"}
for ax, framing in zip(axes, ["relay","hub"]):
    pct    = yr_ab[framing] / yr_ab["total"] * 100
    smooth = pct.rolling(7, min_periods=3, center=True).mean()
    # CI ribbon
    cis = [wilson_ci(yr_ab.loc[yr, framing], yr_ab.loc[yr, "total"])
           for yr in yr_ab.index]
    lo  = np.array([c[0]*100 for c in cis])
    hi  = np.array([c[1]*100 for c in cis])
    ax.fill_between(yr_ab.index, lo, hi,
                    alpha=0.15, color=colors[framing])
    ax.plot(yr_ab.index, smooth, color=colors[framing], lw=2.5,
            label=f"{framing} (7-yr rolling mean)")
    ax.scatter(yr_ab.index, pct, color=colors[framing], alpha=0.2, s=12)
    ax.set_ylabel("% of abstract framing sentences")
    ax.legend(fontsize=10); ax.set_ylim(0)
    ax.axvline(2010, color="grey", ls=":", lw=1, alpha=0.5)

axes[1].set_xlabel("Year")
fig.suptitle("Thalamus framing in abstracts — extended to 1950 (Wilson 95% CI)",
             fontsize=12)
plt.tight_layout()
fig.savefig(GRAPH / "fig_B1_abstract_temporal_extended.png", dpi=150)
plt.close()
print("Saved fig_B1_abstract_temporal_extended.png")

# ── 2. Replication check: abstracts vs full-text ──────────────────────────────
ft = pd.read_csv(PROC / "framing_embedded_clustered.csv", dtype={"pmid": str})
ft = ft[ft["cluster_id"] != -1].copy()
ft["year"] = pd.to_numeric(ft["year"], errors="coerce")

RELAY_IDS = {55, 56, 46}
HUB_IDS   = {53, 54}
ft["broad"] = ft["cluster_id"].apply(
    lambda c: "relay" if c in RELAY_IDS else ("hub" if c in HUB_IDS else "other"))

def make_trend(df_in, framing, year_range=(1990,2026), roll=5):
    sub = df_in[df_in["year"].between(*year_range)]
    yr  = (sub.groupby(["year","broad"]).size()
              .unstack(fill_value=0)
              .reindex(columns=["relay","hub","other"], fill_value=0))
    yr["total"] = yr.sum(axis=1)
    pct = (yr[framing] / yr["total"].replace(0,1) * 100)
    return pct.rolling(roll, min_periods=2, center=True).mean()

fig, axes = plt.subplots(1, 2, figsize=(13, 5))
for ax, framing, color in [(axes[0],"relay","#E8342A"), (axes[1],"hub","#2E86AB")]:
    ft_trend = make_trend(ft, framing)
    ab_trend = make_trend(ab, framing)
    ax.plot(ft_trend.index, ft_trend.values, color=color, lw=2.5,
            label="Full text (cluster-based)")
    ax.plot(ab_trend.index, ab_trend.values, color=color, lw=2.5,
            ls="--", label="Abstracts (keyword-based)")
    ax.set_title(f"{framing.capitalize()} framing")
    ax.set_xlabel("Year"); ax.set_ylabel("% of framing sentences (5-yr rolling)")
    ax.legend(fontsize=9); ax.set_ylim(0)
    # Pearson r on overlapping years
    common = ft_trend.index.intersection(ab_trend.index)
    if len(common) > 5:
        r = np.corrcoef(ft_trend[common].fillna(0),
                        ab_trend[common].fillna(0))[0,1]
        ax.text(0.02, 0.93, f"r = {r:.2f}", transform=ax.transAxes, fontsize=9)

fig.suptitle("Replication check: abstract framing vs full-text framing (1990–2026)",
             fontsize=11)
plt.tight_layout()
fig.savefig(GRAPH / "fig_B2_abstract_vs_fulltext_replication.png", dpi=150)
plt.close()
print("Saved fig_B2_abstract_vs_fulltext_replication.png")

# ── 3. Keyword distribution comparison ───────────────────────────────────────
ab_kw = ab["framing_primary"].value_counts(normalize=True) * 100

# For full-text use framing_primary from the clustered file
ft_kw = ft["framing_primary"].value_counts(normalize=True) * 100

kw_df = pd.DataFrame({"abstracts": ab_kw, "full_text": ft_kw}).fillna(0)
kw_df = kw_df.sort_values("abstracts", ascending=True)

fig, ax = plt.subplots(figsize=(10, 5))
y = range(len(kw_df))
ax.barh([yi + 0.22 for yi in y], kw_df["abstracts"], height=0.4,
        color="#59B069", alpha=0.8, label="Abstracts")
ax.barh([yi - 0.22 for yi in y], kw_df["full_text"], height=0.4,
        color="#9B59B6", alpha=0.8, label="Full text")
ax.set_yticks(list(y)); ax.set_yticklabels(kw_df.index, fontsize=9)
ax.set_xlabel("% of framing sentences")
ax.set_title("Framing keyword distribution: abstracts vs full text")
ax.legend()
plt.tight_layout()
fig.savefig(GRAPH / "fig_B3_abstract_keyword_distribution.png", dpi=150)
plt.close()
print("Saved fig_B3_abstract_keyword_distribution.png")

print(f"\nAbstract relay %: {(ab['broad']=='relay').mean()*100:.1f}")
print(f"Abstract hub %  : {(ab['broad']=='hub').mean()*100:.1f}")
