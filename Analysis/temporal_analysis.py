"""
Temporal analysis of thalamus framing clusters.

Reads the cluster-labelled framing sentences from embed_and_cluster.py /
apply_cluster_labels.py and produces five figures that describe how the
volume, composition, and framing of thalamus sentences changes over time.

Inputs (Data/processed/):
    framing_embedded_clustered.csv   — must include cluster_id, cluster_label,
                                       year, framing_primary columns

Outputs (Graphs/):
    fig1_volume_per_year.png             — total framing sentences per year
    fig2_framing_clusters_over_time.png  — relay/hub/oscillation clusters,
                                           3-yr rolling mean
    fig3_cluster_share_per_decade.png    — stacked bar of cluster share per
                                           decade (clusters >= 80 sentences)
    fig4_relay_vs_hub_over_time.png      — relay% vs hub% per year, 5-yr
                                           rolling mean
    fig5_keyword_framing_over_time.png   — keyword-taxonomy category trends

Note on paths:
    Paths use PROC/GRAPH constants resolved from __file__; Code/config.py and
    Code/paths.py are not wired in here.
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT  = _ANALYSIS_DIR.parents[1]
PROC  = ROOT / "Data" / "processed"
GRAPH = ROOT / "Graphs"
GRAPH.mkdir(exist_ok=True)

# ── Load ──────────────────────────────────────────────────────────────────────
df = pd.read_csv(PROC / "framing_embedded_clustered.csv")
df = df[df["year"].notna()].copy()
df["year"] = df["year"].astype(int)
df = df[(df["year"] >= 1975) & (df["year"] <= 2026)]

# Drop NOISE for most plots (keep flag for optional inclusion)
df_clean = df[df["cluster_id"] != -1].copy()

print(f"Total sentences (excl. noise): {len(df_clean)}")
print(f"Year range: {df_clean['year'].min()} – {df_clean['year'].max()}")
print(f"Unique clusters: {df_clean['cluster_id'].nunique()}")

# ── Decade bins ───────────────────────────────────────────────────────────────
bins   = [1975, 1985, 1995, 2005, 2015, 2027]
labels = ["1975–84","1985–94","1995–04","2005–14","2015–26"]
df_clean["decade"] = pd.cut(df_clean["year"], bins=bins, labels=labels, right=False)
df["decade"]       = pd.cut(df["year"],       bins=bins, labels=labels, right=False)

# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 1 — Overall volume of framing sentences per year
# ═══════════════════════════════════════════════════════════════════════════════
yearly = df_clean.groupby("year").size()

fig, ax = plt.subplots(figsize=(12, 4))
ax.bar(yearly.index, yearly.values, color="#4C72B0", width=0.8, alpha=0.8)
ax.set_xlabel("Year"); ax.set_ylabel("Framing sentences (n)")
ax.set_title("Volume of thalamus framing sentences per year")
ax.set_xlim(1974, 2027)
plt.tight_layout()
fig.savefig(GRAPH / "fig1_volume_per_year.png", dpi=150)
plt.close()
print("Saved fig1_volume_per_year.png")

# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 2 — The framing clusters only: relay vs hub over time (line)
# ═══════════════════════════════════════════════════════════════════════════════
FRAMING_CLUSTERS = {
    55: "relay / gateway",
    56: "sensory filtering / HO relay",
    53: "hub / integration",
    54: "information flow / mediator",
    21: "burst/tonic firing modes",
    46: "first- vs higher-order relay",
}

framing_df = df_clean[df_clean["cluster_id"].isin(FRAMING_CLUSTERS)].copy()
framing_df["label_short"] = framing_df["cluster_id"].map(FRAMING_CLUSTERS)

# Smooth with 3-year rolling
pivot = (framing_df.groupby(["year","label_short"])
                   .size()
                   .unstack(fill_value=0)
                   .rolling(3, min_periods=1, center=True)
                   .mean())

colors = ["#E8342A","#F7A541","#2E86AB","#59B069","#9B59B6","#555555"]
fig, ax = plt.subplots(figsize=(13, 5))
for col, color in zip(pivot.columns, colors):
    ax.plot(pivot.index, pivot[col], label=col, color=color, lw=2)
ax.set_xlabel("Year"); ax.set_ylabel("Sentences (3-yr rolling mean)")
ax.set_title("Framing-specific clusters over time")
ax.set_xlim(1975, 2027)
ax.legend(fontsize=8, loc="upper left")
plt.tight_layout()
fig.savefig(GRAPH / "fig2_framing_clusters_over_time.png", dpi=150)
plt.close()
print("Saved fig2_framing_clusters_over_time.png")

# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 3 — All clusters as % share per decade (stacked bar)
# Only show clusters with ≥ 80 sentences total to keep it readable
# ═══════════════════════════════════════════════════════════════════════════════
MIN_N = 80
big_clusters = (df_clean.groupby("cluster_label")
                        .size()
                        .loc[lambda s: s >= MIN_N]
                        .index.tolist())

sub = df_clean[df_clean["cluster_label"].isin(big_clusters)].copy()
ct  = (sub.groupby(["decade","cluster_label"])
          .size()
          .unstack(fill_value=0))
ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100

# Sort columns by first appearance
order = (df_clean[df_clean["cluster_label"].isin(big_clusters)]
            .groupby("cluster_label")["year"]
            .min()
            .sort_values()
            .index.tolist())
ct_pct = ct_pct[[c for c in order if c in ct_pct.columns]]

cmap = plt.get_cmap("tab20")
colors20 = [cmap(i / len(ct_pct.columns)) for i in range(len(ct_pct.columns))]

fig, ax = plt.subplots(figsize=(14, 6))
bottom = np.zeros(len(ct_pct))
for i, col in enumerate(ct_pct.columns):
    ax.bar(ct_pct.index, ct_pct[col], bottom=bottom,
           label=col, color=colors20[i], width=0.6)
    bottom += ct_pct[col].values

ax.set_xlabel("Decade"); ax.set_ylabel("% of framing sentences")
ax.set_title("Cluster share per decade (clusters ≥ 80 sentences)")
ax.set_ylim(0, 100)
ax.legend(fontsize=6.5, bbox_to_anchor=(1.01, 1), loc="upper left", ncol=1)
plt.tight_layout()
fig.savefig(GRAPH / "fig3_cluster_share_per_decade.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved fig3_cluster_share_per_decade.png")

# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 4 — Relay vs Hub framing: head-to-head normalised % per year
# ═══════════════════════════════════════════════════════════════════════════════
# "relay-type": clusters 55, 56, 46
# "hub-type":   clusters 53, 54
relay_ids = [55, 56, 46]
hub_ids   = [53, 54]

yearly_total = df_clean.groupby("year").size().rename("total")
relay_y = (df_clean[df_clean["cluster_id"].isin(relay_ids)]
           .groupby("year").size().rename("relay"))
hub_y   = (df_clean[df_clean["cluster_id"].isin(hub_ids)]
           .groupby("year").size().rename("hub"))

rh = pd.concat([relay_y, hub_y, yearly_total], axis=1).fillna(0)
rh = rh[(rh["total"] >= 5)]  # only years with ≥5 framing sentences
rh["relay_pct"] = rh["relay"] / rh["total"] * 100
rh["hub_pct"]   = rh["hub"]   / rh["total"] * 100

# 5-year rolling
rh_smooth = rh[["relay_pct","hub_pct"]].rolling(5, min_periods=2, center=True).mean()

fig, ax = plt.subplots(figsize=(13, 5))
ax.plot(rh_smooth.index, rh_smooth["relay_pct"], color="#E8342A", lw=2.5,
        label="Relay / gateway framing (clusters 55, 56, 46)")
ax.plot(rh_smooth.index, rh_smooth["hub_pct"],   color="#2E86AB", lw=2.5,
        label="Hub / integration framing (clusters 53, 54)")
ax.fill_between(rh_smooth.index, rh_smooth["relay_pct"], alpha=0.15, color="#E8342A")
ax.fill_between(rh_smooth.index, rh_smooth["hub_pct"],   alpha=0.15, color="#2E86AB")
ax.set_xlabel("Year"); ax.set_ylabel("% of framing sentences (5-yr rolling mean)")
ax.set_title("Relay vs Hub framing of the thalamus over time")
ax.set_xlim(1975, 2027)
ax.legend(fontsize=9)
plt.tight_layout()
fig.savefig(GRAPH / "fig4_relay_vs_hub_over_time.png", dpi=150)
plt.close()
print("Saved fig4_relay_vs_hub_over_time.png")

# ═══════════════════════════════════════════════════════════════════════════════
# FIGURE 5 — keyword-based framing categories over time (from framing_primary)
# ═══════════════════════════════════════════════════════════════════════════════
kw_yearly = (df_clean.groupby(["year","framing_primary"])
                     .size()
                     .unstack(fill_value=0))
kw_total  = df_clean.groupby("year").size()
kw_pct    = kw_yearly.div(kw_total, axis=0) * 100
kw_smooth = kw_pct.rolling(5, min_periods=2, center=True).mean()

# Pick meaningful keyword categories
KEY_CATS = ["relay","hub_node","gateway","modulator","integrator",
            "network_component","structure_description","clinical_target",
            "role_player","involved_in"]
kw_smooth = kw_smooth[[c for c in KEY_CATS if c in kw_smooth.columns]]

kw_colors = {
    "relay":               "#E8342A",
    "hub_node":            "#2E86AB",
    "gateway":             "#F7A541",
    "modulator":           "#59B069",
    "integrator":          "#9B59B6",
    "network_component":   "#16A085",
    "structure_description":"#7F8C8D",
    "clinical_target":     "#C0392B",
    "role_player":         "#E67E22",
    "involved_in":         "#AAB7B8",
}

fig, ax = plt.subplots(figsize=(13, 5))
for cat in kw_smooth.columns:
    ax.plot(kw_smooth.index, kw_smooth[cat],
            color=kw_colors.get(cat,"#333333"), lw=2, label=cat)
ax.set_xlabel("Year"); ax.set_ylabel("% of framing sentences (5-yr rolling mean)")
ax.set_title("Keyword-based framing categories over time")
ax.set_xlim(1975, 2027)
ax.legend(fontsize=8, loc="upper left")
plt.tight_layout()
fig.savefig(GRAPH / "fig5_keyword_framing_over_time.png", dpi=150)
plt.close()
print("Saved fig5_keyword_framing_over_time.png")

# ═══════════════════════════════════════════════════════════════════════════════
# Print summary stats
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── Relay vs Hub by decade ──────────────────────────────")
rh_dec = (df_clean.assign(
    rtype=lambda x: x["cluster_id"].apply(
        lambda c: "relay" if c in relay_ids else ("hub" if c in hub_ids else "other")))
    .groupby(["decade","rtype"]).size().unstack(fill_value=0))
rh_dec["relay_%"] = (rh_dec.get("relay",0) /
                     rh_dec.sum(axis=1) * 100).round(1)
rh_dec["hub_%"]   = (rh_dec.get("hub",0)   /
                     rh_dec.sum(axis=1) * 100).round(1)
print(rh_dec[["relay_%","hub_%"]])

print("\n── Top 5 clusters per decade ───────────────────────────")
for dec in labels:
    sub = df_clean[df_clean["decade"] == dec]
    top = sub.groupby("cluster_label").size().sort_values(ascending=False).head(5)
    print(f"\n  {dec}:")
    for lbl, n in top.items():
        print(f"    {n:4d}  {lbl}")
