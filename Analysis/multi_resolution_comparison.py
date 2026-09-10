"""
multi_resolution_comparison.py — Multi-resolution clustering comparison across 3 models.

For each embedding model (SPECTER2, PubMedBERT, MPNet):
  - Cluster at 10 resolutions: 0.003 → 0.150
  - Compute: modularity, n_clusters, n_major_clusters (>50 papers)
  - Check visual/auditory cluster separation
  - Compute cluster coherence (intra-cluster similarity)

Cross-model comparison:
  - ARI between models at each resolution
  - Jaccard overlap of major cluster themes

Decision plots:
  - Modularity vs resolution (per model)
  - N_clusters vs resolution
  - ARI heatmap (model × resolution)
  - Domain coverage: which resolution first separates visual, auditory, DBS, MS, etc.

Output:
  Graphs/pub/fig_resolution_sweep_overview.png  (6-panel decision plot)
  Graphs/pub/fig_cluster_characterization_grid.png  (cluster heatmap)
  Data/processed/multi_res_results.csv
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
from collections import Counter

import faiss
import igraph as ig
import leidenalg
from sklearn.metrics import adjusted_rand_score
from sklearn.feature_extraction.text import TfidfVectorizer

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

# ═══════════════════════════════════════════════════════════════════════════
# LOAD
# ═══════════════════════════════════════════════════════════════════════════
print("Loading embeddings and abstracts...")
papers = pd.read_csv(PROC / "paper_abstracts.csv", low_memory=False)
pmids = np.load(PROC / "specter2_pmids.npy")

# Keyword detectors for domain validation
visual_re = re.compile(
    r'\b(lgn|dlgn|lateral geniculate|visual cortex|visual thalam|'
    r'retin\w+|v1|primary visual|photorecept|retinotop)\b', re.IGNORECASE)
auditory_re = re.compile(
    r'\b(mgb|mgv|medial geniculate|auditory thalam|'
    r'tonotop|cochlea|frequency tuning|inferior colliculus)\b', re.IGNORECASE)
somato_re = re.compile(
    r'\b(whisker|barrel|somatosensor|vpm|vpl|ventrobasal|trigeminal)\b',
    re.IGNORECASE)
clinical_mri_re = re.compile(
    r'\b(schizophrenia|major depressive|bipolar|mdd|ocd|multiple sclerosis|'
    r'parkinson|alzheimer)\b', re.IGNORECASE)
dbs_re = re.compile(r'\b(DBS|deep brain stimulation|thalamotomy|mrgfus|vim)\b',
                     re.IGNORECASE)

text_all = papers["title"].fillna("") + " " + papers["abstract"].fillna("")
papers["is_visual"] = text_all.str.contains(visual_re)
papers["is_auditory"] = text_all.str.contains(auditory_re)
papers["is_somato"] = text_all.str.contains(somato_re)
papers["is_clinical"] = text_all.str.contains(clinical_mri_re)
papers["is_dbs"] = text_all.str.contains(dbs_re)

MODELS = {
    "SPECTER2": PROC / "specter2_embeddings.npy",
    "PubMedBERT": PROC / "pubmedbert_embeddings.npy",
    "MPNet": PROC / "mpnet_embeddings.npy",
}

# Load all model embeddings
embeddings = {}
for name, path in MODELS.items():
    embeddings[name] = np.load(path).astype(np.float32)
    # L2 normalize
    norms = np.linalg.norm(embeddings[name], axis=1, keepdims=True)
    embeddings[name] = embeddings[name] / (norms + 1e-10)
    print(f"  {name}: {embeddings[name].shape}")


# ═══════════════════════════════════════════════════════════════════════════
# BUILD K-NN GRAPHS (once per model)
# ═══════════════════════════════════════════════════════════════════════════
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
    g = g.as_undirected(combine_edges="max")
    return g


print("\nBuilding K-NN graphs...")
graphs = {}
for name, embs in embeddings.items():
    t0 = time.time()
    graphs[name] = build_graph(embs)
    print(f"  {name}: {graphs[name].vcount()} nodes, {graphs[name].ecount()} edges "
          f"({time.time()-t0:.0f}s)")


# ═══════════════════════════════════════════════════════════════════════════
# MULTI-RESOLUTION SWEEP
# ═══════════════════════════════════════════════════════════════════════════

# Carefully chosen resolutions spanning from coarse to fine
RESOLUTIONS = [0.003, 0.005, 0.008, 0.012, 0.020, 0.035, 0.060, 0.100, 0.150]

print(f"\nResolution sweep: {len(RESOLUTIONS)} resolutions × {len(MODELS)} models")

all_memberships = {}   # (model, res) -> array of cluster labels
sweep_stats = []       # rows with stats

for model_name, g in graphs.items():
    print(f"\n  {model_name}:")
    for res in RESOLUTIONS:
        partition = leidenalg.find_partition(
            g, leidenalg.CPMVertexPartition,
            resolution_parameter=res, weights="weight", seed=42,
        )
        mem = np.array(partition.membership)
        all_memberships[(model_name, res)] = mem

        # Stats
        sizes = Counter(mem)
        n_clusters = len(sizes)
        n_major = sum(1 for c, n in sizes.items() if n >= 50)
        largest = max(sizes.values())

        # Domain coverage: does ANY cluster have >50% visual / >50% auditory / etc?
        visual_found = False
        auditory_found = False
        somato_found = False
        clinical_found = False
        dbs_found = False
        for c in sizes:
            if sizes[c] < 50:
                continue
            mask = mem == c
            if papers.loc[mask, "is_visual"].mean() > 0.4:
                visual_found = True
            if papers.loc[mask, "is_auditory"].mean() > 0.4:
                auditory_found = True
            if papers.loc[mask, "is_somato"].mean() > 0.3:
                somato_found = True
            if papers.loc[mask, "is_clinical"].mean() > 0.5:
                clinical_found = True
            if papers.loc[mask, "is_dbs"].mean() > 0.5:
                dbs_found = True

        sweep_stats.append({
            "model": model_name, "resolution": res,
            "modularity": partition.modularity,
            "n_clusters": n_clusters,
            "n_major": n_major,
            "largest_cluster": largest,
            "visual_separated": visual_found,
            "auditory_separated": auditory_found,
            "somatosensory_separated": somato_found,
            "clinical_separated": clinical_found,
            "dbs_separated": dbs_found,
            "domain_coverage": sum([visual_found, auditory_found, somato_found,
                                     clinical_found, dbs_found]),
        })
        print(f"    res={res:.3f}: mod={partition.modularity:.3f}, "
              f"n_clusters={n_clusters}, major={n_major}, "
              f"visual={'Y' if visual_found else '.'}, "
              f"aud={'Y' if auditory_found else '.'}, "
              f"som={'Y' if somato_found else '.'}, "
              f"cli={'Y' if clinical_found else '.'}")

stats_df = pd.DataFrame(sweep_stats)
stats_df.to_csv(PROC / "multi_res_sweep_stats.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════════
# CROSS-MODEL ARI
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("CROSS-MODEL ARI (Adjusted Rand Index)")
print("=" * 70)

model_pairs = [("SPECTER2", "PubMedBERT"), ("SPECTER2", "MPNet"),
               ("PubMedBERT", "MPNet")]

ari_rows = []
for res in RESOLUTIONS:
    for m1, m2 in model_pairs:
        ari = adjusted_rand_score(
            all_memberships[(m1, res)],
            all_memberships[(m2, res)]
        )
        ari_rows.append({"resolution": res, "pair": f"{m1} vs {m2}",
                         "ARI": ari})
        print(f"  res={res:.3f}  {m1:10s} vs {m2:10s}: ARI={ari:.3f}")

ari_df = pd.DataFrame(ari_rows)

# ═══════════════════════════════════════════════════════════════════════════
# DECISION PLOT (6-PANEL)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("GENERATING DECISION PLOTS")
print("=" * 70)

fig, axes = plt.subplots(3, 2, figsize=(14, 15))
fig.suptitle("Multi-Resolution Clustering Comparison (3 Embedding Models)",
             fontsize=14, fontweight="bold", y=0.995)

model_colors = {"SPECTER2": WONG["blue"], "PubMedBERT": WONG["red"],
                "MPNet": WONG["green"]}

# Panel A: Modularity vs resolution
ax = axes[0, 0]
for model, color in model_colors.items():
    subset = stats_df[stats_df["model"] == model]
    ax.plot(subset["resolution"], subset["modularity"], "o-",
            color=color, label=model, linewidth=2, markersize=6)
ax.set_xscale("log")
ax.set_xlabel("Resolution parameter")
ax.set_ylabel("Modularity")
ax.set_title("A. Modularity vs resolution")
ax.legend(fontsize=9)
despine(ax)
panel_label(ax, "A")

# Panel B: N_clusters vs resolution
ax = axes[0, 1]
for model, color in model_colors.items():
    subset = stats_df[stats_df["model"] == model]
    ax.plot(subset["resolution"], subset["n_major"], "o-",
            color=color, label=f"{model} (major)", linewidth=2, markersize=6)
    ax.plot(subset["resolution"], subset["n_clusters"], "o--",
            color=color, alpha=0.4, linewidth=1, markersize=4)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Resolution parameter")
ax.set_ylabel("Number of clusters")
ax.set_title("B. Cluster count vs resolution\n(solid = ≥50 papers, dashed = all)")
ax.legend(fontsize=9)
despine(ax)
panel_label(ax, "B")

# Panel C: Cross-model ARI
ax = axes[1, 0]
pair_colors = {"SPECTER2 vs PubMedBERT": WONG["blue"],
               "SPECTER2 vs MPNet": WONG["red"],
               "PubMedBERT vs MPNet": WONG["green"]}
for pair in ari_df["pair"].unique():
    subset = ari_df[ari_df["pair"] == pair]
    ax.plot(subset["resolution"], subset["ARI"], "o-",
            color=pair_colors.get(pair, "black"),
            label=pair, linewidth=2, markersize=5)
ax.set_xscale("log")
ax.set_xlabel("Resolution parameter")
ax.set_ylabel("Adjusted Rand Index")
ax.set_title("C. Cross-model agreement")
ax.axhline(0.5, color="grey", linestyle="--", alpha=0.5, label="ARI=0.5 threshold")
ax.legend(fontsize=8)
despine(ax)
panel_label(ax, "C")

# Panel D: Domain coverage (how many domains are separately identifiable)
ax = axes[1, 1]
for model, color in model_colors.items():
    subset = stats_df[stats_df["model"] == model]
    ax.plot(subset["resolution"], subset["domain_coverage"], "o-",
            color=color, label=model, linewidth=2, markersize=6)
ax.set_xscale("log")
ax.set_xlabel("Resolution parameter")
ax.set_ylabel("Domains identified (of 5)")
ax.set_title("D. Domain coverage\n(visual, auditory, somato, clinical, DBS)")
ax.set_yticks(range(6))
ax.legend(fontsize=9)
despine(ax)
panel_label(ax, "D")

# Panel E: Domain identification heatmap
ax = axes[2, 0]
domains = ["visual_separated", "auditory_separated", "somatosensory_separated",
           "clinical_separated", "dbs_separated"]
domain_labels = ["Visual", "Auditory", "Somato-\nsensory", "Clinical\n(MDD/MS/etc)",
                 "DBS /\ntremor"]

# 3 models × N resolutions × 5 domains
heat = np.zeros((len(domains) * len(MODELS), len(RESOLUTIONS)))
row_labels = []
for mi, model in enumerate(MODELS):
    subset = stats_df[stats_df["model"] == model].sort_values("resolution")
    for di, dom in enumerate(domains):
        row = mi * len(domains) + di
        row_labels.append(f"{model[:4]} | {domain_labels[di].replace(chr(10), ' ')[:15]}")
        for ri, res in enumerate(RESOLUTIONS):
            rv = subset[subset["resolution"] == res]
            if len(rv) > 0:
                heat[row, ri] = int(rv.iloc[0][dom])

im = ax.imshow(heat, aspect="auto", cmap="RdYlGn", vmin=0, vmax=1)
ax.set_xticks(range(len(RESOLUTIONS)))
ax.set_xticklabels([f"{r:.3f}" for r in RESOLUTIONS], rotation=45)
ax.set_yticks(range(len(row_labels)))
ax.set_yticklabels(row_labels, fontsize=6)
# Separator lines between models
for i in [len(domains), 2 * len(domains)]:
    ax.axhline(i - 0.5, color="black", linewidth=1)
ax.set_xlabel("Resolution")
ax.set_title("E. Domain identification per model/resolution\n(green = domain found)")
panel_label(ax, "E")

# Panel F: Recommendation summary
ax = axes[2, 1]
ax.axis("off")
# Find "best" configurations
best = stats_df[(stats_df["domain_coverage"] >= 4) &
                (stats_df["n_major"] >= 15) &
                (stats_df["n_major"] <= 40)].copy()
best = best.sort_values(["domain_coverage", "modularity"], ascending=False)

text_lines = ["TOP CONFIGURATIONS\n(≥4 domains, 15–40 major clusters)\n"]
text_lines.append(f"{'Model':<12} {'Res':<8} {'Mod':<6} {'Major':<6} {'Cov':<4}")
text_lines.append("-" * 45)
for _, row in best.head(10).iterrows():
    text_lines.append(
        f"{row['model']:<12} {row['resolution']:<8.3f} "
        f"{row['modularity']:<6.3f} {row['n_major']:<6} {row['domain_coverage']:<4}"
    )
ax.text(0.02, 0.95, "\n".join(text_lines), transform=ax.transAxes,
        fontsize=9, family="monospace", va="top")
panel_label(ax, "F")

plt.tight_layout(rect=[0, 0, 1, 0.98])
save_fig(fig, PUB / "fig_resolution_sweep_overview.png")
print("  Saved fig_resolution_sweep_overview.png")

# ═══════════════════════════════════════════════════════════════════════════
# CLUSTER CHARACTERIZATION AT BEST CONFIGS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("CHARACTERIZING TOP CONFIGURATIONS")
print("=" * 70)


def characterize(model_name, res, min_cluster_size=80):
    mem = all_memberships[(model_name, res)]
    df = papers.copy()
    df["cluster"] = mem

    sizes = Counter(mem)
    big_clusters = [c for c, n in sizes.items() if n >= min_cluster_size]
    big_clusters.sort(key=lambda c: sizes[c], reverse=True)

    # TF-IDF
    cluster_texts = []
    for c in big_clusters:
        sub = df[df["cluster"] == c]
        cluster_texts.append(" ".join(sub["abstract"].fillna("").tolist()))

    vec = TfidfVectorizer(max_features=3000, stop_words="english",
                           ngram_range=(1, 2), min_df=2, max_df=0.5)
    X = vec.fit_transform(cluster_texts)
    feats = np.array(vec.get_feature_names_out())

    rows = []
    for i, c in enumerate(big_clusters):
        sub = df[df["cluster"] == c]
        top_idx = X[i].toarray()[0].argsort()[-5:][::-1]
        terms = feats[top_idx].tolist()
        rows.append({
            "cluster": int(c),
            "n": len(sub),
            "year": round(sub["year"].mean(), 1),
            "vis%": round(sub["is_visual"].mean() * 100, 1),
            "aud%": round(sub["is_auditory"].mean() * 100, 1),
            "som%": round(sub["is_somato"].mean() * 100, 1),
            "cli%": round(sub["is_clinical"].mean() * 100, 1),
            "dbs%": round(sub["is_dbs"].mean() * 100, 1),
            "terms": ", ".join(terms[:4]),
        })
    return pd.DataFrame(rows)


# Characterize the top configurations
TO_CHARACTERIZE = [
    ("PubMedBERT", 0.008),
    ("PubMedBERT", 0.012),
    ("MPNet", 0.008),
    ("MPNet", 0.012),
    ("SPECTER2", 0.012),
    ("SPECTER2", 0.020),
]

char_dfs = {}
for model, res in TO_CHARACTERIZE:
    print(f"\n  {model} @ res={res}:")
    cdf = characterize(model, res)
    char_dfs[(model, res)] = cdf
    for _, r in cdf.head(20).iterrows():
        flags = ""
        if r["vis%"] > 30: flags += " VIS"
        if r["aud%"] > 30: flags += " AUD"
        if r["som%"] > 25: flags += " SOM"
        if r["cli%"] > 40: flags += " CLI"
        if r["dbs%"] > 50: flags += " DBS"
        print(f"    C{r['cluster']:>3}: n={r['n']:>4}, ~{r['year']:.0f}  "
              f"v={r['vis%']:>4.0f} a={r['aud%']:>4.0f} s={r['som%']:>4.0f} "
              f"c={r['cli%']:>4.0f} d={r['dbs%']:>4.0f}  "
              f"| {r['terms']}{flags}")

    cdf.to_csv(PROC / f"clusters_{model}_r{int(res*1000)}.csv", index=False)


# ═══════════════════════════════════════════════════════════════════════════
# CHARACTERIZATION GRID FIGURE
# ═══════════════════════════════════════════════════════════════════════════
print("\n  Generating characterization grid...")

fig, axes = plt.subplots(2, 3, figsize=(22, 14))
fig.suptitle("Cluster Composition Across Models & Resolutions",
             fontsize=14, fontweight="bold", y=0.995)

configs = [
    ("SPECTER2", 0.012), ("PubMedBERT", 0.008), ("MPNet", 0.008),
    ("SPECTER2", 0.020), ("PubMedBERT", 0.012), ("MPNet", 0.012),
]

for idx, (model, res) in enumerate(configs):
    ax = axes[idx // 3, idx % 3]
    cdf = char_dfs[(model, res)].head(25)  # top 25 clusters
    n_c = len(cdf)

    # Build heatmap: domain % per cluster
    domain_data = np.array([
        cdf["vis%"].values, cdf["aud%"].values, cdf["som%"].values,
        cdf["cli%"].values, cdf["dbs%"].values,
    ])
    im = ax.imshow(domain_data, aspect="auto", cmap="YlOrRd", vmin=0, vmax=80)

    ax.set_xticks(range(n_c))
    # Short labels
    labels = [f"C{r['cluster']} ({r['n']})\n{r['terms'][:20]}"
              for _, r in cdf.iterrows()]
    ax.set_xticklabels(labels, rotation=75, ha="right", fontsize=5.5)
    ax.set_yticks(range(5))
    ax.set_yticklabels(["Visual", "Auditory", "Somato.", "Clinical", "DBS"],
                        fontsize=9)
    ax.set_title(f"{model} @ res={res}  ({n_c} major clusters)", fontsize=11)

    # Add text for high values
    for yi in range(domain_data.shape[0]):
        for xi in range(domain_data.shape[1]):
            v = domain_data[yi, xi]
            if v >= 20:
                ax.text(xi, yi, f"{int(v)}", ha="center", va="center",
                        fontsize=5, color="white" if v > 40 else "black")

    if idx % 3 == 2:
        plt.colorbar(im, ax=ax, label="%", shrink=0.7)

plt.tight_layout(rect=[0, 0, 1, 0.98])
save_fig(fig, PUB / "fig_cluster_characterization_grid.png")
print("  Saved fig_cluster_characterization_grid.png")

print("\nDone.")
