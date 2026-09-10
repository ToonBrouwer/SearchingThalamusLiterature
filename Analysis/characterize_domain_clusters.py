"""
characterize_domain_clusters.py — Characterize Leiden clusters from domain_clustering_pipeline.

For each cluster:
  - Top TF-IDF terms from abstracts
  - Most central papers (highest in-degree within cluster)
  - Temporal profile
  - Dominant frame (from our regex-based framing)
  - Dominant nucleus / technique tags

Produces figures:
  fig_domain_clusters_umap.png
  fig_domain_clusters_temporal.png
  fig_domain_clusters_framing.png
  fig_raw_vs_refined_comparison.png
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import Counter

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import adjusted_rand_score
from umap import UMAP

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
papers = pd.read_csv(PROC / "paper_abstracts.csv", low_memory=False)
papers["year"] = pd.to_numeric(papers["year"], errors="coerce")
print(f"  Papers: {len(papers):,}")

refined_emb = np.load(PROC / "specter2_refined_embeddings.npy")
raw_emb = np.load(PROC / "specter2_embeddings.npy")
pmids = np.load(PROC / "specter2_pmids.npy")

cluster_raw = pd.read_csv(PROC / "domain_clusters_raw.csv")
cluster_refined = pd.read_csv(PROC / "domain_clusters_refined.csv")

# Merge
df = papers.merge(cluster_raw, on="pmid", how="left")
df = df.merge(cluster_refined, on="pmid", how="left")
df = df.dropna(subset=["cluster_raw", "cluster_refined"])
df["cluster_raw"] = df["cluster_raw"].astype(int)
df["cluster_refined"] = df["cluster_refined"].astype(int)
print(f"  Papers with clusters: {len(df):,}")

# Load framing, nucleus, technique data
all_sents = pd.read_csv(PROC / "all_sentences_final.csv",
                         usecols=["pmid", "frame_type"] + [c for c in pd.read_csv(PROC / "all_sentences_final.csv", nrows=1).columns if c.startswith("has_")],
                         low_memory=False)

# Dominant frame per paper
framing = all_sents[all_sents["frame_type"].notna() & (all_sents["frame_type"] != "")]
dom_frame = (framing.groupby("pmid")["frame_type"]
             .agg(lambda x: Counter(x).most_common(1)[0][0])
             .rename("dominant_frame"))
df = df.merge(dom_frame, on="pmid", how="left")

# Load techniques
if (PROC / "paper_techniques.csv").exists():
    pt = pd.read_csv(PROC / "paper_techniques.csv", low_memory=False)
    tech_cols = [c for c in pt.columns if c.startswith("has_")]
    df = df.merge(pt[["pmid"] + tech_cols], on="pmid", how="left")
else:
    tech_cols = []

# ═══════════════════════════════════════════════════════════════════════════
# CLUSTER CHARACTERIZATION (for each method)
# ═══════════════════════════════════════════════════════════════════════════

def characterize(cluster_col, label, min_size=50):
    print(f"\n{'='*70}")
    print(f"CLUSTER CHARACTERIZATION: {label}")
    print("=" * 70)

    clusters = df[cluster_col].value_counts()
    big_clusters = clusters[clusters >= min_size].index.tolist()
    print(f"  Total clusters: {len(clusters)}")
    print(f"  Clusters with >={min_size} papers: {len(big_clusters)}")

    # TF-IDF for cluster labeling (use abstracts)
    print(f"\n  Computing TF-IDF terms per cluster...")
    # Build cluster-level concatenated abstracts
    cluster_texts = []
    for c in big_clusters:
        txt = " ".join(df[df[cluster_col] == c]["abstract"].fillna("").tolist())
        cluster_texts.append(txt)

    vec = TfidfVectorizer(
        max_features=5000, stop_words="english", ngram_range=(1, 2),
        min_df=2, max_df=0.5,
    )
    X = vec.fit_transform(cluster_texts)
    feature_names = np.array(vec.get_feature_names_out())

    cluster_info = []
    for i, c in enumerate(big_clusters):
        sub = df[df[cluster_col] == c]
        top_idx = X[i].toarray()[0].argsort()[-10:][::-1]
        top_terms = feature_names[top_idx].tolist()

        # Frame distribution
        if "dominant_frame" in sub.columns:
            frame_counts = sub["dominant_frame"].value_counts().head(2).to_dict()
        else:
            frame_counts = {}

        # Technique dominance
        tech_fracs = {}
        for tc in tech_cols:
            if tc in sub.columns:
                tech_fracs[tc.replace("has_", "")] = sub[tc].mean()
        top_tech = sorted(tech_fracs.items(), key=lambda x: x[1], reverse=True)[:3]

        year_mean = sub["year"].mean()
        year_std = sub["year"].std()

        cluster_info.append({
            "cluster": int(c),
            "n_papers": len(sub),
            "mean_year": round(year_mean, 1),
            "year_range": f"{int(year_mean - year_std)}-{int(year_mean + year_std)}",
            "top_terms": ", ".join(top_terms[:6]),
            "dominant_frames": str(frame_counts),
            "top_techniques": ", ".join(f"{t}({v:.2f})" for t, v in top_tech),
        })

    info_df = pd.DataFrame(cluster_info).sort_values("n_papers", ascending=False)

    # Print top clusters
    print(f"\n  Top 15 clusters:")
    for _, row in info_df.head(15).iterrows():
        print(f"\n  CLUSTER {row['cluster']} (n={row['n_papers']}, year~{row['mean_year']})")
        print(f"    Terms: {row['top_terms']}")
        print(f"    Frames: {row['dominant_frames']}")
        print(f"    Techniques: {row['top_techniques']}")

    info_df.to_csv(PROC / f"cluster_info_{label}.csv", index=False)
    return info_df, big_clusters


info_raw, big_raw = characterize("cluster_raw", "raw")
info_ref, big_ref = characterize("cluster_refined", "refined")

# ═══════════════════════════════════════════════════════════════════════════
# RAW vs REFINED COMPARISON
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("RAW vs REFINED COMPARISON")
print("=" * 70)

ari = adjusted_rand_score(df["cluster_raw"], df["cluster_refined"])
print(f"  Adjusted Rand Index: {ari:.3f}")
print(f"  Raw n_clusters: {df['cluster_raw'].nunique()}")
print(f"  Refined n_clusters: {df['cluster_refined'].nunique()}")

# ═══════════════════════════════════════════════════════════════════════════
# UMAP VISUALIZATION
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("UMAP VISUALIZATION")
print("=" * 70)

# Subsample for plotting (speed)
n_plot = min(5000, len(df))
sample_idx = np.random.RandomState(42).choice(len(df), n_plot, replace=False)

# Map pmid back to embedding index
pmid_to_idx = {p: i for i, p in enumerate(pmids)}
sample_pmids = df.iloc[sample_idx]["pmid"].values
emb_idx = np.array([pmid_to_idx[p] for p in sample_pmids if p in pmid_to_idx])
sample_df = df.iloc[sample_idx].reset_index(drop=True).iloc[:len(emb_idx)]

print(f"  Computing UMAP on refined ({len(emb_idx)} points)...")
reducer = UMAP(n_components=2, random_state=42, n_neighbors=30, min_dist=0.1)
umap_refined = reducer.fit_transform(refined_emb[emb_idx])

print(f"  Computing UMAP on raw...")
reducer = UMAP(n_components=2, random_state=42, n_neighbors=30, min_dist=0.1)
umap_raw = reducer.fit_transform(raw_emb[emb_idx])

# Figure 1: UMAP colored by cluster
fig, axes = plt.subplots(1, 2, figsize=(16, 8))
for ax, coords, cluster_col, label in [
    (axes[0], umap_raw, "cluster_raw", "Raw SPECTER2 (768-d)"),
    (axes[1], umap_refined, "cluster_refined", "Contrastive-refined (64-d)"),
]:
    clusters = sample_df[cluster_col].values
    n_clusters = len(set(clusters))
    # Use a color cycle
    cmap = plt.get_cmap("tab20")
    for i, c in enumerate(sorted(set(clusters))):
        mask = clusters == c
        if mask.sum() < 20:
            ax.scatter(coords[mask, 0], coords[mask, 1], s=4, c="grey", alpha=0.2)
        else:
            ax.scatter(coords[mask, 0], coords[mask, 1], s=8,
                        c=[cmap(i % 20)], alpha=0.6)
    ax.set_title(f"{label}\n{n_clusters} Leiden clusters")
    ax.set_xticks([])
    ax.set_yticks([])

plt.suptitle("Domain Clusters: Raw SPECTER2 vs Contrastive-Refined",
             fontsize=14, fontweight="bold")
plt.tight_layout()
save_fig(fig, PUB / "fig_domain_clusters_umap.png")
print("  Saved fig_domain_clusters_umap.png")

# ═══════════════════════════════════════════════════════════════════════════
# TEMPORAL PROFILES
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TEMPORAL PROFILES")
print("=" * 70)

# Show top 12 clusters' temporal trajectories
TOP_N = 12

def temporal_plot(cluster_col, label, top_clusters, ax):
    years = np.arange(1985, 2026)
    # Total papers per year
    total_per_year = df.groupby("year").size().reindex(years, fill_value=0)

    for i, c in enumerate(top_clusters[:TOP_N]):
        sub = df[df[cluster_col] == c]
        yearly = sub.groupby("year").size().reindex(years, fill_value=0)
        pct = (yearly / total_per_year.replace(0, np.nan) * 100).rolling(3, center=True, min_periods=1).mean()
        # Get top term for legend
        info_row = info_raw if label == "Raw" else info_ref
        info_match = info_row[info_row["cluster"] == c]
        if len(info_match) > 0:
            terms = info_match.iloc[0]["top_terms"].split(",")[0].strip()
        else:
            terms = f"C{c}"
        ax.plot(years, pct, label=f"C{c}: {terms}", linewidth=1.5,
                color=plt.get_cmap("tab20")(i % 20))
    ax.set_xlabel("Year")
    ax.set_ylabel("% of papers")
    ax.set_title(f"{label} clusters ({len(top_clusters)} total)")
    ax.legend(fontsize=6, ncol=2, loc="upper left")
    despine(ax)

fig, axes = plt.subplots(1, 2, figsize=(16, 8))
temporal_plot("cluster_raw", "Raw",
              info_raw.sort_values("n_papers", ascending=False)["cluster"].tolist(),
              axes[0])
temporal_plot("cluster_refined", "Refined",
              info_ref.sort_values("n_papers", ascending=False)["cluster"].tolist(),
              axes[1])
plt.suptitle("Top Domain Clusters Over Time (3-year smoothed)",
             fontsize=14, fontweight="bold")
plt.tight_layout()
save_fig(fig, PUB / "fig_domain_clusters_temporal.png")
print("  Saved fig_domain_clusters_temporal.png")

# ═══════════════════════════════════════════════════════════════════════════
# FRAMING COMPOSITION PER CLUSTER
# ═══════════════════════════════════════════════════════════════════════════
if "dominant_frame" in df.columns:
    print(f"\n{'='*70}")
    print("FRAMING COMPOSITION PER CLUSTER")
    print("=" * 70)

    fig, ax = plt.subplots(figsize=(14, 10))
    # Top 20 clusters
    top20 = info_ref.sort_values("n_papers", ascending=False).head(20)["cluster"].tolist()
    # Top 6 frames
    top_frames = df["dominant_frame"].value_counts().head(6).index.tolist()

    heatmap = np.zeros((len(top20), len(top_frames)))
    for i, c in enumerate(top20):
        sub = df[df["cluster_refined"] == c]
        total = len(sub)
        for j, f in enumerate(top_frames):
            heatmap[i, j] = (sub["dominant_frame"] == f).sum() / max(total, 1) * 100

    im = ax.imshow(heatmap, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(top_frames)))
    ax.set_xticklabels(top_frames, rotation=45, ha="right")
    ax.set_yticks(range(len(top20)))
    yticklabels = []
    for c in top20:
        info_match = info_ref[info_ref["cluster"] == c]
        if len(info_match) > 0:
            terms = info_match.iloc[0]["top_terms"].split(",")[:2]
            yticklabels.append(f"C{c}: {','.join(t.strip() for t in terms)}")
        else:
            yticklabels.append(f"C{c}")
    ax.set_yticklabels(yticklabels, fontsize=8)
    plt.colorbar(im, label="% papers with frame")
    ax.set_title("Frame Composition per Top 20 Refined Clusters")
    for i in range(heatmap.shape[0]):
        for j in range(heatmap.shape[1]):
            if heatmap[i, j] > 5:
                ax.text(j, i, f"{heatmap[i,j]:.0f}", ha="center", va="center",
                        fontsize=7, color="white" if heatmap[i,j] > 30 else "black")
    plt.tight_layout()
    save_fig(fig, PUB / "fig_domain_clusters_framing.png")
    print("  Saved fig_domain_clusters_framing.png")

# ═══════════════════════════════════════════════════════════════════════════
# COMPARISON TABLE
# ═══════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(10, 8))
# Contingency heatmap: rows = raw clusters (top 15), cols = refined clusters (top 15)
top_r = info_raw.head(15)["cluster"].tolist()
top_f = info_ref.head(15)["cluster"].tolist()

cont = np.zeros((len(top_r), len(top_f)))
for i, r in enumerate(top_r):
    for j, f in enumerate(top_f):
        cont[i, j] = ((df["cluster_raw"] == r) & (df["cluster_refined"] == f)).sum()

im = ax.imshow(cont, aspect="auto", cmap="Blues")
ax.set_xticks(range(len(top_f)))
ax.set_xticklabels([f"R{c}" for c in top_f], rotation=45)
ax.set_yticks(range(len(top_r)))
ax.set_yticklabels([f"Raw{c}" for c in top_r])
ax.set_xlabel("Refined cluster")
ax.set_ylabel("Raw cluster")
ax.set_title(f"Raw vs Refined Contingency\n(ARI = {ari:.3f})")
plt.colorbar(im, label="N papers")
plt.tight_layout()
save_fig(fig, PUB / "fig_raw_vs_refined_comparison.png")
print("  Saved fig_raw_vs_refined_comparison.png")

print("\nDone.")
