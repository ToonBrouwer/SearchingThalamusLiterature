"""
cluster_stability.py — How robust are clusters across models and resolutions?

Computes:
  1. Per-cluster Jaccard similarity: for each reference cluster, how well does its
     membership match the best-matching cluster in each other configuration?
  2. Consensus matrix: for each pair of papers, the fraction of configurations where
     they are assigned to the same cluster.
  3. Core-vs-peripheral identification: papers that always cluster together (stable core)
     vs papers that jump between clusters (peripheral).
  4. Cluster-level stability score: mean Jaccard across all configurations.

Reference configuration: PubMedBERT @ res=0.008 (best tradeoff found in sweep).
Alternative configurations tested: SPECTER2 / PubMedBERT / MPNet at res=0.005, 0.008, 0.012, 0.020.

Output figures:
  fig_cluster_stability_heatmap.png  — ref clusters × configs, Jaccard
  fig_cluster_stability_summary.png  — stability per cluster, TF-IDF of most/least stable
  fig_consensus_cluster_core.png     — paper-pair consensus, core papers per cluster
  fig_cluster_evolution.png          — cluster splits/merges across resolutions
"""
import sys, io, os, time, re
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
from collections import Counter, defaultdict

import faiss
import igraph as ig
import leidenalg
from sklearn.feature_extraction.text import TfidfVectorizer

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
# Reference: PubMedBERT @ res=0.008
REFERENCE = ("PubMedBERT", 0.008)

# Configurations to compare against
CONFIGS = [
    ("SPECTER2", 0.005), ("SPECTER2", 0.008), ("SPECTER2", 0.012), ("SPECTER2", 0.020),
    ("PubMedBERT", 0.005), ("PubMedBERT", 0.008), ("PubMedBERT", 0.012), ("PubMedBERT", 0.020),
    ("MPNet", 0.005), ("MPNet", 0.008), ("MPNet", 0.012), ("MPNet", 0.020),
]

MIN_CLUSTER_SIZE = 50

MODELS = {
    "SPECTER2": PROC / "specter2_embeddings.npy",
    "PubMedBERT": PROC / "pubmedbert_embeddings.npy",
    "MPNet": PROC / "mpnet_embeddings.npy",
}

# ═══════════════════════════════════════════════════════════════════════════
# LOAD + CLUSTER ALL CONFIGS
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
papers = pd.read_csv(PROC / "paper_abstracts.csv", low_memory=False)
pmids = np.load(PROC / "specter2_pmids.npy")
N = len(papers)
print(f"  {N:,} papers")


def build_graph(embs, k=50):
    n, d = embs.shape
    index = faiss.IndexFlatIP(d)
    index.add(embs)
    D, I = index.search(embs, k + 1)
    edges, weights = [], []
    for i in range(n):
        for j_idx in range(1, k + 1):
            j = int(I[i, j_idx])
            if j != i:
                edges.append((i, j))
                weights.append(float(D[i, j_idx]))
    g = ig.Graph(n=n, edges=edges, directed=True)
    g.es["weight"] = weights
    return g.as_undirected(combine_edges="max")


print("Building graphs and clustering all configs...")
graphs = {}
memberships = {}
for model_name in set(m for m, _ in CONFIGS):
    embs = np.load(MODELS[model_name]).astype(np.float32)
    norms = np.linalg.norm(embs, axis=1, keepdims=True)
    embs = embs / (norms + 1e-10)
    graphs[model_name] = build_graph(embs)
    print(f"  {model_name}: graph built")

for model_name, res in CONFIGS:
    part = leidenalg.find_partition(
        graphs[model_name], leidenalg.CPMVertexPartition,
        resolution_parameter=res, weights="weight", seed=42,
    )
    memberships[(model_name, res)] = np.array(part.membership)

print(f"  {len(memberships)} clusterings computed")

# Reference
ref_mem = memberships[REFERENCE]
ref_sizes = Counter(ref_mem)
ref_clusters = sorted([c for c, n in ref_sizes.items() if n >= MIN_CLUSTER_SIZE],
                       key=lambda c: -ref_sizes[c])
print(f"  Reference ({REFERENCE}): {len(ref_clusters)} clusters ≥{MIN_CLUSTER_SIZE} papers")

# ═══════════════════════════════════════════════════════════════════════════
# PART 1: PER-CLUSTER JACCARD STABILITY
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("PART 1: PER-CLUSTER JACCARD STABILITY")
print("=" * 70)


def best_jaccard(ref_members, other_mem):
    """For a set of reference members, find the cluster in other_mem that has
    the highest Jaccard overlap with them."""
    ref_set = set(ref_members)
    other_clusters = Counter(other_mem[list(ref_set)])
    best_j = 0.0
    best_cluster = None
    best_overlap = 0
    # Only check clusters that contain ref members
    for c, overlap in other_clusters.items():
        other_members = np.where(other_mem == c)[0]
        union = len(ref_set | set(other_members))
        j = overlap / union if union > 0 else 0
        if j > best_j:
            best_j = j
            best_cluster = c
            best_overlap = overlap
    return best_j, best_cluster, best_overlap


# Jaccard matrix: ref_clusters × configs
jaccard_matrix = np.zeros((len(ref_clusters), len(CONFIGS)))
best_matches = {}  # (ref_cluster, config_idx) -> (jaccard, matched_cluster, overlap)

for i, rc in enumerate(ref_clusters):
    ref_members = np.where(ref_mem == rc)[0]
    for j, (model_name, res) in enumerate(CONFIGS):
        other = memberships[(model_name, res)]
        jacc, matched, overlap = best_jaccard(ref_members, other)
        jaccard_matrix[i, j] = jacc
        best_matches[(i, j)] = (jacc, matched, overlap)

# Stability score per reference cluster (mean Jaccard across all configs)
stability_per_ref = jaccard_matrix.mean(axis=1)
print(f"\n  Mean stability per reference cluster:")
for i, rc in enumerate(ref_clusters):
    n = ref_sizes[rc]
    mean_j = stability_per_ref[i]
    min_j = jaccard_matrix[i].min()
    max_j = jaccard_matrix[i].max()
    print(f"    C{rc:>3}: n={n:>4}, mean_J={mean_j:.3f}, "
          f"range=[{min_j:.3f}, {max_j:.3f}]")

# ═══════════════════════════════════════════════════════════════════════════
# CHARACTERIZE REF CLUSTERS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("CHARACTERIZING REFERENCE CLUSTERS")
print("=" * 70)

cluster_texts = []
for rc in ref_clusters:
    mask = ref_mem == rc
    cluster_texts.append(" ".join(papers.loc[mask, "abstract"].fillna("").tolist()))

vec = TfidfVectorizer(max_features=3000, stop_words="english",
                       ngram_range=(1, 2), min_df=2, max_df=0.5)
X = vec.fit_transform(cluster_texts)
feats = np.array(vec.get_feature_names_out())

ref_labels = []
for i, rc in enumerate(ref_clusters):
    top_idx = X[i].toarray()[0].argsort()[-5:][::-1]
    terms = feats[top_idx].tolist()
    ref_labels.append(f"C{rc}: " + ", ".join(terms[:3]))

# ═══════════════════════════════════════════════════════════════════════════
# PART 2: CONSENSUS MATRIX (paper-pair agreement)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("PART 2: CONSENSUS — per-paper stability")
print("=" * 70)

# For each paper, compute how often its "reference cluster neighbors" stay with it
# across all configurations. This is a per-paper stability score.

print("  Computing per-paper stability score...")
# For each paper i: fraction of reference-cluster-mates who remain in same cluster
#                    as i across all configurations (averaged over configs)
per_paper_stability = np.zeros(N)

for i in range(N):
    ref_cluster = ref_mem[i]
    ref_peers = np.where(ref_mem == ref_cluster)[0]
    ref_peers = ref_peers[ref_peers != i]  # exclude self
    if len(ref_peers) == 0:
        continue

    agreements = []
    for (m, r) in CONFIGS:
        other = memberships[(m, r)]
        other_cluster_i = other[i]
        n_agree = (other[ref_peers] == other_cluster_i).sum()
        agreements.append(n_agree / len(ref_peers))
    per_paper_stability[i] = np.mean(agreements)

print(f"    Mean per-paper stability: {per_paper_stability.mean():.3f}")
print(f"    Median: {np.median(per_paper_stability):.3f}")
print(f"    Papers with stability >0.7: {(per_paper_stability > 0.7).sum():,} "
      f"({(per_paper_stability > 0.7).mean()*100:.1f}%)")
print(f"    Papers with stability <0.3: {(per_paper_stability < 0.3).sum():,} "
      f"({(per_paper_stability < 0.3).mean()*100:.1f}%)")

# Per-cluster core/periphery
cluster_stability_profile = []
for rc in ref_clusters:
    mask = ref_mem == rc
    scores = per_paper_stability[mask]
    cluster_stability_profile.append({
        "cluster": rc,
        "n": mask.sum(),
        "mean_stability": scores.mean(),
        "n_core": (scores > 0.7).sum(),
        "n_stable": (scores > 0.5).sum(),
        "n_peripheral": (scores < 0.3).sum(),
    })
csp_df = pd.DataFrame(cluster_stability_profile)

# ═══════════════════════════════════════════════════════════════════════════
# PART 3: UNIVERSAL CLUSTERS (present in all configs)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("PART 3: UNIVERSAL CLUSTERS (Jaccard ≥0.5 in all configs)")
print("=" * 70)

THRESHOLD = 0.5
universal = []
for i, rc in enumerate(ref_clusters):
    if (jaccard_matrix[i] >= THRESHOLD).all():
        universal.append((rc, stability_per_ref[i]))

print(f"\n  Reference clusters found in ALL {len(CONFIGS)} configs with Jaccard ≥{THRESHOLD}:")
for rc, mean_j in sorted(universal, key=lambda x: -x[1]):
    idx = ref_clusters.index(rc)
    print(f"    {ref_labels[idx]:<60s} mean_J={mean_j:.3f}, n={ref_sizes[rc]}")

print(f"\n  Total universal clusters: {len(universal)} / {len(ref_clusters)}")

# Save
sum_df = pd.DataFrame({
    "cluster": ref_clusters,
    "label": ref_labels,
    "n_papers": [ref_sizes[c] for c in ref_clusters],
    "mean_jaccard": stability_per_ref,
    "min_jaccard": jaccard_matrix.min(axis=1),
    "max_jaccard": jaccard_matrix.max(axis=1),
    "universal": [c in {u[0] for u in universal} for c in ref_clusters],
})
sum_df = sum_df.sort_values("mean_jaccard", ascending=False)
sum_df.to_csv(PROC / "cluster_stability_summary.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE 1: Stability heatmap (ref clusters × configs)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("GENERATING FIGURES")
print("=" * 70)

# Sort ref clusters by mean stability (most stable at top)
sort_idx = np.argsort(-stability_per_ref)
sorted_matrix = jaccard_matrix[sort_idx]
sorted_labels = [ref_labels[i] for i in sort_idx]

fig, ax = plt.subplots(figsize=(12, max(10, len(ref_clusters) * 0.3)))
im = ax.imshow(sorted_matrix, aspect="auto", cmap="RdYlGn", vmin=0, vmax=1)

# X-axis: configs
x_labels = [f"{m}\nr={r}" for m, r in CONFIGS]
ax.set_xticks(range(len(CONFIGS)))
ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=7)

# Y-axis: clusters
ax.set_yticks(range(len(ref_clusters)))
ax.set_yticklabels(sorted_labels, fontsize=7)

# Cell text: Jaccard value
for i in range(sorted_matrix.shape[0]):
    for j in range(sorted_matrix.shape[1]):
        v = sorted_matrix[i, j]
        color = "white" if v > 0.6 or v < 0.2 else "black"
        ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                fontsize=5, color=color)

# Highlight reference column (PubMedBERT r=0.008)
ref_col = CONFIGS.index(REFERENCE)
for spine in ["top", "bottom", "left", "right"]:
    pass
ax.axvline(ref_col - 0.5, color="blue", linewidth=0.5)
ax.axvline(ref_col + 0.5, color="blue", linewidth=0.5)

# Model separators
for sep in [3.5, 7.5]:
    ax.axvline(sep, color="black", linewidth=1.5)

plt.colorbar(im, ax=ax, label="Jaccard (cluster overlap)", shrink=0.5)
ax.set_title(f"Cluster Stability: Reference = {REFERENCE[0]} @ res={REFERENCE[1]}\n"
             f"(rows = reference clusters, cols = other configurations)")
plt.tight_layout()
save_fig(fig, PUB / "fig_cluster_stability_heatmap.png")
print("  Saved fig_cluster_stability_heatmap.png")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE 2: Stability summary + TF-IDF of most/least stable
# ═══════════════════════════════════════════════════════════════════════════
fig, axes = plt.subplots(2, 2, figsize=(16, 10))

# A: Bar chart of stability per cluster
ax = axes[0, 0]
sort_idx2 = np.argsort(stability_per_ref)  # least → most stable
bars = ax.barh(range(len(ref_clusters)),
                stability_per_ref[sort_idx2],
                color=["#d62728" if v < 0.4 else "#2ca02c" if v > 0.6 else "#ff7f0e"
                       for v in stability_per_ref[sort_idx2]])
ax.axvline(THRESHOLD, color="black", linestyle="--", label=f"Universal threshold={THRESHOLD}")
ax.set_yticks(range(len(ref_clusters)))
ax.set_yticklabels([ref_labels[i] for i in sort_idx2], fontsize=6)
ax.set_xlabel("Mean Jaccard across configurations")
ax.set_title("A. Reference cluster stability ranking")
ax.legend(fontsize=8)
ax.set_xlim(0, 1)
despine(ax)
panel_label(ax, "A")

# B: Distribution of per-paper stability
ax = axes[0, 1]
ax.hist(per_paper_stability, bins=50, color=WONG["blue"], alpha=0.7, edgecolor="black")
ax.axvline(per_paper_stability.mean(), color="red", linestyle="--",
            label=f"mean={per_paper_stability.mean():.2f}")
ax.axvline(0.7, color="green", linestyle="--", label="stable threshold")
ax.axvline(0.3, color="orange", linestyle="--", label="peripheral threshold")
ax.set_xlabel("Per-paper stability score")
ax.set_ylabel("Number of papers")
ax.set_title(f"B. Per-paper stability (N={N:,})")
ax.legend(fontsize=8)
despine(ax)
panel_label(ax, "B")

# C: Universal vs specific clusters
ax = axes[1, 0]
n_univ = (sum_df["universal"]).sum()
n_spec = len(sum_df) - n_univ
labels = ["Universal\n(all configs\nJaccard ≥0.5)",
          "Configuration-\nspecific"]
ax.pie([n_univ, n_spec], labels=labels, autopct="%1.0f%%",
       colors=["#2ca02c", "#d62728"], startangle=90)
ax.set_title(f"C. Universal vs specific clusters\n({len(ref_clusters)} reference clusters total)")
panel_label(ax, "C")

# D: Stability vs cluster size
ax = axes[1, 1]
for _, row in sum_df.iterrows():
    color = "#2ca02c" if row["universal"] else "#d62728"
    ax.scatter(row["n_papers"], row["mean_jaccard"], s=40, c=color, alpha=0.7)
    # Label most stable and least stable
    if row["mean_jaccard"] > 0.7 or row["mean_jaccard"] < 0.35:
        short_lbl = row["label"][:30]
        ax.annotate(short_lbl, (row["n_papers"], row["mean_jaccard"]),
                    fontsize=5, textcoords="offset points", xytext=(3, 3))

ax.set_xscale("log")
ax.axhline(THRESHOLD, color="black", linestyle="--", alpha=0.5)
ax.set_xlabel("Cluster size (papers, log)")
ax.set_ylabel("Mean Jaccard across configs")
ax.set_title("D. Cluster size vs stability")
ax.set_ylim(0, 1)
patches = [mpatches.Patch(color="#2ca02c", label="Universal"),
           mpatches.Patch(color="#d62728", label="Specific")]
ax.legend(handles=patches, fontsize=8)
despine(ax)
panel_label(ax, "D")

plt.tight_layout()
save_fig(fig, PUB / "fig_cluster_stability_summary.png")
print("  Saved fig_cluster_stability_summary.png")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE 3: Core vs peripheral per cluster
# ═══════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(12, max(10, len(ref_clusters) * 0.3)))

# Sort clusters by universal status + mean stability
csp_df["label"] = [ref_labels[ref_clusters.index(c)] for c in csp_df["cluster"]]
csp_df = csp_df.sort_values("mean_stability")

y_pos = np.arange(len(csp_df))
total = csp_df["n"]
core = csp_df["n_core"]
stable = csp_df["n_stable"] - csp_df["n_core"]
peripheral = csp_df["n_peripheral"]
mid = total - core - stable - peripheral

ax.barh(y_pos, core, color="#2ca02c", label="Core (stability>0.7)", alpha=0.9)
ax.barh(y_pos, stable, left=core, color="#aec7e8",
         label="Stable (0.5-0.7)", alpha=0.8)
ax.barh(y_pos, mid, left=core + stable, color="#ff7f0e",
         label="Mid (0.3-0.5)", alpha=0.7)
ax.barh(y_pos, peripheral, left=core + stable + mid, color="#d62728",
         label="Peripheral (<0.3)", alpha=0.8)

ax.set_yticks(y_pos)
ax.set_yticklabels(csp_df["label"], fontsize=7)
ax.set_xlabel("Number of papers")
ax.set_title("Core vs Peripheral Membership per Reference Cluster")
ax.legend(fontsize=9, loc="lower right")
despine(ax)
plt.tight_layout()
save_fig(fig, PUB / "fig_consensus_cluster_core.png")
print("  Saved fig_consensus_cluster_core.png")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE 4: Cluster evolution across resolutions (Sankey-style)
# ═══════════════════════════════════════════════════════════════════════════
print("\n  Generating cluster evolution diagram...")

# Show how PubMedBERT clusters at res=0.005 → 0.008 → 0.012 → 0.020
# For each paper: which cluster does it belong to at each resolution?
# Track splits/merges.

resolutions_to_show = [0.005, 0.008, 0.012, 0.020]
fig, ax = plt.subplots(figsize=(16, 12))

# Compute cluster positions at each resolution
# X position = resolution index
# Y position = cluster rank by size (within that resolution)
x_positions = range(len(resolutions_to_show))

all_cluster_data = {}  # (res_idx, cluster_id) -> (x, y, size, papers_set)
for res_idx, res in enumerate(resolutions_to_show):
    mem = memberships[("PubMedBERT", res)]
    sizes = Counter(mem)
    big = [c for c, n in sizes.items() if n >= 50]
    big.sort(key=lambda c: -sizes[c])

    for y_rank, c in enumerate(big):
        papers_set = frozenset(np.where(mem == c)[0].tolist())
        all_cluster_data[(res_idx, c)] = (res_idx, y_rank, sizes[c], papers_set)

# Draw nodes
max_size_all = max(d[2] for d in all_cluster_data.values())
for (res_idx, c), (x, y, size, _) in all_cluster_data.items():
    radius = 5 + 50 * (size / max_size_all)
    ax.scatter(x, -y, s=radius, c=plt.get_cmap("tab20")(c % 20),
                alpha=0.7, edgecolors="black", linewidth=0.3, zorder=3)

# Draw connections: for each pair of adjacent resolutions, link parent→child
for res_idx in range(len(resolutions_to_show) - 1):
    clusters_left = [k for k in all_cluster_data if k[0] == res_idx]
    clusters_right = [k for k in all_cluster_data if k[0] == res_idx + 1]

    for kl in clusters_left:
        _, yl, size_l, papers_l = all_cluster_data[kl]
        for kr in clusters_right:
            _, yr, size_r, papers_r = all_cluster_data[kr]
            overlap = len(papers_l & papers_r)
            if overlap > 0:
                # Jaccard
                jacc = overlap / len(papers_l | papers_r)
                if jacc > 0.1 or overlap > 50:
                    alpha = min(jacc * 1.5, 0.5)
                    lw = max(0.2, overlap / 200)
                    ax.plot([res_idx, res_idx + 1], [-yl, -yr],
                             color="grey", alpha=alpha, linewidth=lw, zorder=1)

# Labels
ax.set_xticks(x_positions)
ax.set_xticklabels([f"res={r}\n({sum(1 for k in all_cluster_data if k[0]==ri)} clusters)"
                    for ri, r in enumerate(resolutions_to_show)])
ax.set_yticks([])
ax.set_title("Cluster Evolution Across Resolutions (PubMedBERT)\n"
             "Node size = cluster size | Edge width = papers shared between resolutions")
ax.axis("off")
for ri, r in enumerate(resolutions_to_show):
    ax.text(ri, 2, f"res={r}", ha="center", va="bottom", fontsize=11,
             fontweight="bold")

plt.tight_layout()
save_fig(fig, PUB / "fig_cluster_evolution.png")
print("  Saved fig_cluster_evolution.png")

# Save universal cluster list
universal_pmids = {}
for rc, _ in universal:
    mask = ref_mem == rc
    universal_pmids[rc] = pmids[mask].tolist()

# Save paper-level stability
paper_stability_df = pd.DataFrame({
    "pmid": pmids,
    "ref_cluster": ref_mem,
    "stability_score": per_paper_stability,
})
paper_stability_df.to_csv(PROC / "paper_stability.csv", index=False)

print(f"\nDone.")
