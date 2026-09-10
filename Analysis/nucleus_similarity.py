import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

"""
Figure 5 -- Nucleus similarity analysis with multi-method clustering.

Clusters thalamic nuclei using three feature profile types:
  - Framing profile   (21-dim: proportion of each frame_type)
  - Mega-theme profile (n-dim: proportion of each mega_theme category)
  - Technique profile  (11-dim: proportion of each technique)

Uses 6 clustering methods (2 distances x 3 linkages) with cross-validation
via cophenetic correlation and Adjusted Rand Index.

Outputs:
    Graphs/pub/fig5_nucleus_similarity.png

Layout (3x2, panels A-F):
  A: Best dendrogram (highest cophenetic correlation)
  B: Consensus co-clustering heatmap (fraction of 6 methods agreeing)
  C: MDS 2D projection (cosine distance, coloured by dominant frame)
  D: Framing profile heatmap (nucleus x frame_type, row-normalised %)
  E: Technique profile heatmap (nucleus x technique, row-normalised %)
  F: Method comparison (cophenetic correlations or ARI heatmap)
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
# seaborn replaced with matplotlib imshow for environment compatibility
from pathlib import Path
from itertools import combinations
from scipy.spatial.distance import pdist, squareform, cosine
from scipy.cluster.hierarchy import linkage, dendrogram, fcluster, cophenet
from sklearn.manifold import MDS
from sklearn.metrics import adjusted_rand_score

from plotting import apply_style, save_fig, panel_label, despine, WONG
from nucleus_patterns import NUCLEI, tag_nuclei
from technique_patterns import TECHNIQUES, tag_techniques

apply_style()

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

MIN_SENTENCES = 50   # skip nuclei with fewer than this many sentence mentions
N_CLUST       = 4    # number of clusters for cross-validation

# ── Load data ────────────────────────────────────────────────────────────────
print("Loading all_sentences_final.csv ...")
df = pd.read_csv(DATA / "all_sentences_final.csv")
print(f"  {len(df):,} sentences loaded")

# ── Tag nuclei and techniques ────────────────────────────────────────────────
print("Tagging nuclei ...")
df = tag_nuclei(df, text_col="sentence")

print("Tagging techniques ...")
df = tag_techniques(df, text_col="sentence")

# ── Vocabulary ───────────────────────────────────────────────────────────────
frame_types  = sorted(df["frame_type"].dropna().unique())
mega_themes  = sorted(df["mega_theme"].dropna().unique())
tech_ids     = list(TECHNIQUES.keys())
tech_labels  = [TECHNIQUES[t]["label"] for t in tech_ids]

# Nucleus IDs and labels -- exclude Habenula if it somehow appears
nuc_ids    = [k for k in NUCLEI.keys() if k.lower() != "habenula"]
nuc_labels = [NUCLEI[n]["label"] for n in nuc_ids]

print(f"  {len(frame_types)} frame types, {len(mega_themes)} mega-themes, "
      f"{len(tech_ids)} techniques, {len(nuc_ids)} nuclei")

# ── Build feature vectors per nucleus ────────────────────────────────────────
n_nuc = len(nuc_ids)

# Total mention counts per nucleus
total_mentions = np.array([df[f"has_{nid}"].sum() for nid in nuc_ids])

# Filter: keep nuclei with >= MIN_SENTENCES mentions
keep = total_mentions >= MIN_SENTENCES
n_kept = keep.sum()
print(f"  Keeping {n_kept}/{n_nuc} nuclei with >= {MIN_SENTENCES} sentences")

nuc_ids_f    = [nuc_ids[i] for i in range(n_nuc) if keep[i]]
nuc_labels_f = [nuc_labels[i] for i in range(n_nuc) if keep[i]]
mentions_f   = total_mentions[keep]

# -- Framing profile (proportion of each of 21 frame types) --
print("Building framing vectors ...")
framing_raw = np.zeros((n_kept, len(frame_types)))
for i, nid in enumerate(nuc_ids_f):
    mask = df[f"has_{nid}"]
    sub = df.loc[mask, "frame_type"].dropna()
    for j, ft in enumerate(frame_types):
        framing_raw[i, j] = (sub == ft).sum()

# Row-normalise to proportions
framing_totals = framing_raw.sum(axis=1, keepdims=True)
framing_totals[framing_totals == 0] = 1.0
framing_prop = framing_raw / framing_totals

# -- Mega-theme profile (proportion of sentences in each mega_theme) --
print("Building mega-theme vectors ...")
mega_raw = np.zeros((n_kept, len(mega_themes)))
for i, nid in enumerate(nuc_ids_f):
    mask = df[f"has_{nid}"]
    sub = df.loc[mask, "mega_theme"].dropna()
    for j, mt in enumerate(mega_themes):
        mega_raw[i, j] = (sub == mt).sum()

mega_totals = mega_raw.sum(axis=1, keepdims=True)
mega_totals[mega_totals == 0] = 1.0
mega_prop = mega_raw / mega_totals

# -- Technique profile (proportion of sentences tagged with each technique) --
print("Building technique vectors ...")
technique_raw = np.zeros((n_kept, len(tech_ids)))
for i, nid in enumerate(nuc_ids_f):
    mask = df[f"has_{nid}"]
    for j, tid in enumerate(tech_ids):
        technique_raw[i, j] = df.loc[mask, f"has_{tid}"].sum()

tech_totals = technique_raw.sum(axis=1, keepdims=True)
tech_totals[tech_totals == 0] = 1.0
technique_prop = technique_raw / tech_totals

# ── Combined feature matrix ─────────────────────────────────────────────────
# Concatenate all three proportion vectors (framing + mega-theme + technique)
combined = np.hstack([framing_prop, mega_prop, technique_prop])
print(f"  Combined feature matrix: {combined.shape[0]} nuclei x {combined.shape[1]} features")

# ── Short labels for display ────────────────────────────────────────────────
short_labels = []
for lbl in nuc_labels_f:
    if "(" in lbl:
        short_labels.append(lbl.split("(")[0].strip())
    else:
        short_labels.append(lbl)

# Labels with sample size for dendrogram
dendro_labels = [f"{sl} (n={int(n)})" for sl, n in zip(short_labels, mentions_f)]

# ═══════════════════════════════════════════════════════════════════════════
# MULTI-METHOD CLUSTERING
#
# 6 combinations: {cosine, correlation} x {ward, complete, average}
#
# Note: Ward linkage requires Euclidean-like input. When using cosine or
# correlation distance, we convert the condensed distance vector to a
# full square matrix via squareform(), then back to condensed form
# suitable for Ward. This is a standard workaround documented in the
# scipy hierarchy docs.
# ═══════════════════════════════════════════════════════════════════════════

METRICS   = ["cosine", "correlation"]
LINKAGES  = ["ward", "complete", "average"]

# Compute condensed distance vectors for both metrics
dist_condensed = {}
for metric in METRICS:
    dist_condensed[metric] = pdist(combined, metric=metric)
    # Clip to avoid tiny negatives from float precision
    dist_condensed[metric] = np.clip(dist_condensed[metric], 0, None)

# Run all 6 clustering methods
methods = {}  # key: (metric, linkage_name), value: dict with Z, coph, clusters
print("\nMulti-method clustering (6 combinations):")
for metric in METRICS:
    for link in LINKAGES:
        key = (metric, link)
        dv = dist_condensed[metric]

        if link == "ward":
            # Ward requires squared Euclidean-like distances;
            # convert via squareform for cosine/correlation distances
            sq = squareform(dv)
            Z = linkage(squareform(sq, checks=False), method="ward")
        else:
            Z = linkage(dv, method=link)

        # Cophenetic correlation: measures how well the dendrogram
        # preserves the pairwise distances
        coph_corr, coph_dist = cophenet(Z, dv)

        # Cluster assignments at k=4
        clusters = fcluster(Z, t=N_CLUST, criterion="maxclust")

        methods[key] = {
            "Z": Z,
            "coph_corr": coph_corr,
            "clusters": clusters,
        }
        print(f"  {metric:12s} + {link:8s}:  coph_r = {coph_corr:.4f}")

# ── Cross-validation: ARI between all pairs of clusterings ──────────────────
method_keys = list(methods.keys())
n_methods = len(method_keys)

ari_matrix = np.zeros((n_methods, n_methods))
for i in range(n_methods):
    for j in range(n_methods):
        if i == j:
            ari_matrix[i, j] = 1.0
        else:
            ari_matrix[i, j] = adjusted_rand_score(
                methods[method_keys[i]]["clusters"],
                methods[method_keys[j]]["clusters"],
            )

method_labels_short = [f"{m[:3]}+{l[:3]}" for m, l in method_keys]
method_labels_full  = [f"{m} + {l}" for m, l in method_keys]

print("\nARI matrix (Adjusted Rand Index between clusterings):")
for i, lbl in enumerate(method_labels_full):
    vals = "  ".join(f"{ari_matrix[i, j]:.2f}" for j in range(n_methods))
    print(f"  {lbl:25s}:  {vals}")

# ── Identify best method (highest cophenetic correlation) ────────────────────
best_key = max(methods.keys(), key=lambda k: methods[k]["coph_corr"])
best_Z = methods[best_key]["Z"]
best_clusters = methods[best_key]["clusters"]
best_coph = methods[best_key]["coph_corr"]
print(f"\nBest method: {best_key[0]} + {best_key[1]}  (coph_r = {best_coph:.4f})")

# ── Consensus co-clustering matrix ──────────────────────────────────────────
# Cell (i,j) = fraction of 6 methods where nuclei i and j are assigned
# to the same cluster
cocluster = np.zeros((n_kept, n_kept))
for key in method_keys:
    cl = methods[key]["clusters"]
    for i in range(n_kept):
        for j in range(n_kept):
            if cl[i] == cl[j]:
                cocluster[i, j] += 1
cocluster /= n_methods  # normalise to fraction

# ── Robust pairs: nuclei that co-cluster in >= 5/6 methods ──────────────────
robust_threshold = 5 / 6
robust_pairs = []
for i in range(n_kept):
    for j in range(i + 1, n_kept):
        if cocluster[i, j] >= robust_threshold:
            robust_pairs.append((short_labels[i], short_labels[j],
                                 cocluster[i, j]))

print(f"\nRobust pairs (co-cluster in >= {int(robust_threshold * n_methods)}/{n_methods} methods):")
for a, b, frac in sorted(robust_pairs, key=lambda x: -x[2]):
    print(f"  {a:12s} -- {b:12s}  ({frac:.0%})")

# ── Unstable nuclei: never robustly paired with anyone ──────────────────────
robustly_paired = set()
for a, b, _ in robust_pairs:
    robustly_paired.add(a)
    robustly_paired.add(b)
unstable = [sl for sl in short_labels if sl not in robustly_paired]
print(f"\nUnstable nuclei (no robust pairing): {', '.join(unstable) if unstable else 'none'}")

# ── MDS 2D projection (combined features, cosine distance) ──────────────────
print("\nMDS projection (combined, cosine distance) ...")
dist_square = squareform(dist_condensed["cosine"])
mds = MDS(n_components=2, dissimilarity="precomputed", random_state=42,
          n_init=10, normalized_stress="auto")
coords = mds.fit_transform(dist_square)

# Dominant frame type per nucleus (for MDS colouring)
frame_cmap = {
    "relay":           WONG["blue"],
    "hub":             WONG["orange"],
    "gateway":         WONG["green"],
    "gate":            WONG["cyan"],
    "modulator":       WONG["red"],
    "integrator":      WONG["pink"],
    "filter":          WONG["purple"],
    "regulator":       WONG["yellow"],
    "pacemaker":       WONG["grey"],
    "clinical_target": "#A0522D",
    "anti_relay":      "#2E8B57",
}

most_common_frame = []
for i in range(n_kept):
    fv = framing_raw[i]
    if fv.sum() == 0:
        most_common_frame.append("none")
    else:
        most_common_frame.append(frame_types[np.argmax(fv)])

point_colors = [frame_cmap.get(f, "#888888") for f in most_common_frame]

# ── Filter frame types with combined N >= 20 for heatmap D ───────────────────
frame_col_totals = framing_raw.sum(axis=0)
frame_mask = frame_col_totals >= 20
frame_types_filt = [frame_types[j] for j in range(len(frame_types)) if frame_mask[j]]
framing_pct_filt = (framing_raw[:, frame_mask] /
                    framing_raw[:, frame_mask].sum(axis=1, keepdims=True) * 100)
# Handle NaN from zero-row divisions
framing_pct_filt = np.nan_to_num(framing_pct_filt, 0.0)

# ── Technique profile percentages for heatmap E ─────────────────────────────
technique_pct = technique_raw / technique_raw.sum(axis=1, keepdims=True) * 100
technique_pct = np.nan_to_num(technique_pct, 0.0)

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE (3x2, 6 panels, 16x14)
# ═══════════════════════════════════════════════════════════════════════════
print("\nPlotting Figure 5 ...")
fig, axes = plt.subplots(3, 2, figsize=(16, 14))
fig.subplots_adjust(hspace=0.45, wspace=0.35)
total_N = int(mentions_f.sum())

# ── Panel A: Best dendrogram ────────────────────────────────────────────────
ax = axes[0, 0]

# Colour palette for dendrogram clusters
from scipy.cluster.hierarchy import set_link_color_palette
cluster_palette = [WONG["blue"], WONG["orange"], WONG["green"],
                   WONG["red"], WONG["purple"], WONG["cyan"]]
set_link_color_palette([c for c in cluster_palette])

# Threshold for colouring: midpoint between the (n-k)th and (n-k+1)th merge
def cluster_color_threshold(Z, n_clust=N_CLUST):
    """Return a distance threshold that yields roughly n_clust coloured subtrees."""
    n = len(Z) + 1
    if n <= n_clust:
        return 0
    return (Z[-(n_clust), 2] + Z[-(n_clust - 1), 2]) / 2

thresh = cluster_color_threshold(best_Z)
dendrogram(best_Z, labels=dendro_labels, ax=ax, leaf_rotation=45,
           leaf_font_size=8, color_threshold=thresh,
           above_threshold_color="#888888")
ax.set_ylabel("Distance", fontsize=11)
ax.set_title(f"Best dendrogram: {best_key[0]}+{best_key[1]}  "
             f"(coph r={best_coph:.3f})", fontsize=11, fontweight="bold")
despine(ax)
panel_label(ax, "A")
ax.tick_params(axis="x", labelsize=7)

# ── Panel B: Consensus co-clustering heatmap ────────────────────────────────
ax = axes[0, 1]

im_b = ax.imshow(cocluster.values if hasattr(cocluster, 'values') else cocluster,
                  cmap="YlOrRd", vmin=0, vmax=1, aspect="auto")
n_nuc_kept = cocluster.shape[0] if hasattr(cocluster, 'shape') else len(short_labels)
for i in range(n_nuc_kept):
    for j in range(n_nuc_kept):
        val = cocluster.iloc[i, j] if hasattr(cocluster, 'iloc') else cocluster[i, j]
        ax.text(j, i, f"{val:.0%}", ha="center", va="center", fontsize=5,
                color="white" if val > 0.6 else "black")
ax.set_xticks(range(n_nuc_kept)); ax.set_xticklabels(short_labels, fontsize=7, rotation=45, ha="right")
ax.set_yticks(range(n_nuc_kept)); ax.set_yticklabels(short_labels, fontsize=7, rotation=0)
cb_b = plt.colorbar(im_b, ax=ax, fraction=0.046, pad=0.04)
cb_b.set_label("Co-cluster fraction", fontsize=8)
ax.set_title(f"Consensus co-clustering (N={total_N:,})", fontsize=11, fontweight="bold")
panel_label(ax, "B")

# ── Panel C: MDS 2D projection ─────────────────────────────────────────────
ax = axes[1, 0]

# Size by log(mention count)
sizes = np.log1p(mentions_f) * 40
sizes = np.clip(sizes, 40, 600)

ax.scatter(coords[:, 0], coords[:, 1], s=sizes, c=point_colors,
           alpha=0.85, edgecolors="black", linewidth=0.6, zorder=3)

# Add nucleus labels with offset arrows
for i, lbl in enumerate(short_labels):
    angle = np.arctan2(coords[i, 1] - coords[:, 1].mean(),
                       coords[i, 0] - coords[:, 0].mean())
    dx = 10 * np.cos(angle)
    dy = 10 * np.sin(angle) + 6
    ax.annotate(lbl, (coords[i, 0], coords[i, 1]),
                fontsize=7, fontweight="bold", ha="center", va="bottom",
                xytext=(dx, dy), textcoords="offset points",
                arrowprops=dict(arrowstyle="-", color="#aaaaaa", lw=0.5))

ax.set_xlabel("MDS dimension 1", fontsize=11)
ax.set_ylabel("MDS dimension 2", fontsize=11)
ax.set_title(f"MDS projection, cosine distance (N={total_N:,})",
             fontsize=11, fontweight="bold")
despine(ax)
panel_label(ax, "C")

# Legend for dominant frame colours
unique_frames = sorted(set(most_common_frame) - {"none"})
legend_patches = [mpatches.Patch(color=frame_cmap.get(f, "#888888"),
                                 label=f.replace("_", " ").title())
                  for f in unique_frames if f in frame_cmap]
if legend_patches:
    ax.legend(handles=legend_patches, fontsize=7, loc="lower right",
              title="Dominant frame", title_fontsize=8, frameon=False)

# ── Panel D: Framing profile heatmap (row-normalised %) ─────────────────────
ax = axes[1, 1]

# Pretty-print frame type labels
frame_labels_pretty = [ft.replace("_", " ").title() for ft in frame_types_filt]

# Row labels with marginal N
row_labels_d = [f"{sl} (n={int(n)})" for sl, n in zip(short_labels, mentions_f)]

_data_d = framing_pct_filt.values if hasattr(framing_pct_filt, 'values') else framing_pct_filt
im_d = ax.imshow(_data_d, cmap="YlOrRd", aspect="auto")
for i in range(_data_d.shape[0]):
    for j in range(_data_d.shape[1]):
        ax.text(j, i, f"{_data_d[i,j]:.0f}", ha="center", va="center", fontsize=5,
                color="white" if _data_d[i,j] > 50 else "black")
ax.set_xticks(range(len(frame_labels_pretty))); ax.set_xticklabels(frame_labels_pretty, fontsize=6, rotation=45, ha="right")
ax.set_yticks(range(len(row_labels_d))); ax.set_yticklabels(row_labels_d, fontsize=7, rotation=0)
cb_d = plt.colorbar(im_d, ax=ax, fraction=0.046, pad=0.04)
cb_d.set_label("% of sentences", fontsize=8)
ax.set_title("Framing profile (% by frame type, N>=20)", fontsize=11, fontweight="bold")
ax.tick_params(axis="x", labelsize=6, rotation=45)
ax.tick_params(axis="y", labelsize=7, rotation=0)
panel_label(ax, "D")

# ── Panel E: Technique profile heatmap (row-normalised %) ───────────────────
ax = axes[2, 0]

row_labels_e = [f"{sl} (n={int(n)})" for sl, n in zip(short_labels, mentions_f)]

_data_e = technique_pct.values if hasattr(technique_pct, 'values') else technique_pct
im_e = ax.imshow(_data_e, cmap="YlGnBu", aspect="auto")
for i in range(_data_e.shape[0]):
    for j in range(_data_e.shape[1]):
        ax.text(j, i, f"{_data_e[i,j]:.0f}", ha="center", va="center", fontsize=5,
                color="white" if _data_e[i,j] > 50 else "black")
ax.set_xticks(range(len(tech_labels))); ax.set_xticklabels(tech_labels, fontsize=6, rotation=45, ha="right")
ax.set_yticks(range(len(row_labels_e))); ax.set_yticklabels(row_labels_e, fontsize=7, rotation=0)
cb_e = plt.colorbar(im_e, ax=ax, fraction=0.046, pad=0.04)
cb_e.set_label("% of sentences", fontsize=8)
ax.set_title("Technique profile (% by technique)", fontsize=11, fontweight="bold")
ax.tick_params(axis="x", labelsize=6, rotation=45)
ax.tick_params(axis="y", labelsize=7, rotation=0)
panel_label(ax, "E")

# ── Panel F: Method comparison ──────────────────────────────────────────────
# Subpanel layout: top half = cophenetic correlation bar chart,
# bottom half = ARI heatmap (small)
ax = axes[2, 1]

# Option: bar chart of cophenetic correlations
coph_vals = [methods[k]["coph_corr"] for k in method_keys]
bar_labels = [f"{m}+{l}" for m, l in method_keys]
bar_colors = [WONG["blue"] if m == "cosine" else WONG["orange"] for m, l in method_keys]

y_pos = np.arange(n_methods)
ax.barh(y_pos, coph_vals, color=bar_colors, edgecolor="none", height=0.65)
ax.set_yticks(y_pos)
ax.set_yticklabels(bar_labels, fontsize=8)
ax.set_xlabel("Cophenetic correlation", fontsize=10)
ax.set_title(f"Method comparison (N={total_N:,})", fontsize=11, fontweight="bold")
ax.invert_yaxis()
despine(ax)
panel_label(ax, "F")

# Annotate bars with values
for i, v in enumerate(coph_vals):
    ax.text(v + 0.005, i, f"{v:.3f}", va="center", fontsize=8)

# Highlight best method
best_idx = method_keys.index(best_key)
ax.barh(best_idx, coph_vals[best_idx], color=WONG["green"],
        edgecolor="black", linewidth=1.0, height=0.65)

# Legend for metric colours
ax.legend(handles=[
    mpatches.Patch(color=WONG["blue"], label="cosine"),
    mpatches.Patch(color=WONG["orange"], label="correlation"),
    mpatches.Patch(color=WONG["green"], label="best"),
], fontsize=7, loc="lower right", frameon=False)

# ── Save ─────────────────────────────────────────────────────────────────────
out_png = PUB / "fig5_nucleus_similarity.png"
plt.tight_layout(rect=[0, 0, 1, 0.97])
save_fig(fig, out_png, dpi=150)
plt.close(fig)

# ═══════════════════════════════════════════════════════════════════════════
# PRINT STATISTICS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("NUCLEUS SIMILARITY SUMMARY (Figure 5)")
print("=" * 70)
print(f"  Nuclei analysed:     {n_kept}")
print(f"  Total sentence N:    {total_N:,}")
print(f"  Frame types:         {len(frame_types)}")
print(f"  Mega-themes:         {len(mega_themes)}")
print(f"  Techniques:          {len(tech_ids)}")
print(f"  Feature dimensions:  {combined.shape[1]}")
print()

# Per-nucleus summary
print("Per-nucleus breakdown:")
for i, nid in enumerate(nuc_ids_f):
    print(f"  {nuc_labels_f[i]:35s}  mentions={int(mentions_f[i]):5d}  "
          f"dominant_frame={most_common_frame[i]}")

# Cophenetic correlations
print(f"\nCophenetic correlations (6 methods):")
for key in method_keys:
    marker = " <-- BEST" if key == best_key else ""
    print(f"  {key[0]:12s} + {key[1]:8s}:  {methods[key]['coph_corr']:.4f}{marker}")

# Cluster assignments for best method
print(f"\nCluster assignments (k={N_CLUST}, best method: {best_key[0]}+{best_key[1]}):")
for cl in range(1, N_CLUST + 1):
    members = [short_labels[i] for i in range(n_kept) if best_clusters[i] == cl]
    print(f"  Cluster {cl}: {', '.join(members)}")

# Robust groupings
print(f"\nRobust pairs (co-cluster in >= {int(robust_threshold * n_methods)}/{n_methods} methods):")
if robust_pairs:
    for a, b, frac in sorted(robust_pairs, key=lambda x: -x[2]):
        print(f"  {a:12s} -- {b:12s}  ({frac:.0%})")
else:
    print("  No robust pairs found.")

# Unstable nuclei
print(f"\nUnstable nuclei (not robustly paired with any other nucleus):")
print(f"  {', '.join(unstable) if unstable else 'none'}")

# ARI summary
mean_ari = ari_matrix[np.triu_indices(n_methods, k=1)].mean()
print(f"\nMean pairwise ARI across 6 methods: {mean_ari:.3f}")

print(f"\nFigure saved to {out_png}")
print("Done.")
