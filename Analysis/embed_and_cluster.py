"""
Embed framing sentences with sentence-transformers, reduce with UMAP, cluster
with HDBSCAN, and save results for inspection and downstream analysis.

Reads the classified framing sentence CSVs produced by classify_framing.py,
merges full-text and abstract sources, embeds every sentence using the
all-MiniLM-L6-v2 sentence-transformer, reduces to 2D with UMAP, and clusters
the 2D embedding with HDBSCAN.  Cluster examples (10 closest to centroid per
cluster) are saved for manual labelling.

Inputs (Data/processed/):
    framing_sentences_classified.csv          — from classify_framing.py
    abstract_framing_sentences_classified.csv — from classify_framing.py

Outputs (Data/processed/):
    framing_embedded_clustered.csv   — one row per sentence: sid, cluster_id,
                                       umap_x/y, framing_primary, metadata
    framing_embeddings.npy           — raw 384-d embeddings (for reuse)
    cluster_examples.csv             — top 10 sentences per cluster

Outputs (Graphs/):
    05_umap_clusters.png             — UMAP scatter coloured by HDBSCAN cluster
    06_umap_keyword_category.png     — UMAP coloured by keyword taxonomy label
    07_umap_source.png               — UMAP coloured by data source
    08_umap_decade.png               — UMAP coloured by decade

Note on paths:
    Paths use ROOT resolved from __file__; Code/config.py and Code/paths.py
    are not wired in here.  A future refactor could replace ROOT with
    load_settings() + build_data_dirs() for full consistency.

Runtime:
    Embedding ~10,000 sentences takes ~2 min on CPU.  Run in the
    thalamus-embed conda environment (sentence-transformers, umap-learn,
    hdbscan required).
"""

import sys, io, re, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
warnings.filterwarnings("ignore")

from pathlib import Path
import pandas as pd
import numpy as np

_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT    = _ANALYSIS_DIR.parents[1]   # project root
PROC    = ROOT / "Data" / "processed"
GRAPHS  = ROOT / "Graphs"

# ── 1. Load & merge framing datasets ──────────────────────────────────────
print("Loading data...")
ft  = pd.read_csv(PROC / "framing_sentences_classified.csv", dtype={"pmid": str, "pmcid": str})
ab  = pd.read_csv(PROC / "abstract_framing_sentences_classified.csv", dtype={"pmid": str})

ft["source"] = "fulltext"
ab["source"] = "abstract"

# Ensure pmcid exists in abstract data
if "pmcid" not in ab.columns:
    ab["pmcid"] = pd.NA

combined = pd.concat([ft, ab], ignore_index=True)

# ── 2. Add sentence_id — unique key linking sentence back to paper ─────────
# Format: {pmid}_{source_initial}_{sentence_index}
combined["sentence_index"] = combined["sentence_index"].fillna(-1).astype(int)
combined["sid"] = (
    combined["pmid"].astype(str) + "_" +
    combined["source"].str[0] +           # 'f' or 'a'
    combined["sentence_index"].astype(str)
)

# Deduplicate (same pmid+sentence may appear in both datasets)
combined = combined.drop_duplicates(subset=["pmid", "sentence_index", "source"]).reset_index(drop=True)
print(f"  Total sentences after dedup: {len(combined)}")
print(f"  Source breakdown:\n{combined['source'].value_counts().to_string()}")

# ── 3. Embed sentences ─────────────────────────────────────────────────────
print("\nLoading sentence-transformer model (all-MiniLM-L6-v2)...")
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
sentences = combined["sentence"].fillna("").tolist()

print(f"Embedding {len(sentences)} sentences...")
embeddings = model.encode(sentences, batch_size=64, show_progress_bar=True,
                          convert_to_numpy=True)
print(f"  Embedding shape: {embeddings.shape}")

# Save embeddings (for later reuse without re-encoding)
np.save(PROC / "framing_embeddings.npy", embeddings)
print(f"  Saved embeddings -> {PROC / 'framing_embeddings.npy'}")

# ── 4. UMAP dimensionality reduction ──────────────────────────────────────
print("\nRunning UMAP (2D)...")
import umap

reducer = umap.UMAP(n_components=2, n_neighbors=20, min_dist=0.1,
                    metric="cosine", random_state=42, verbose=False)
embedding_2d = reducer.fit_transform(embeddings)
combined["umap_x"] = embedding_2d[:, 0]
combined["umap_y"] = embedding_2d[:, 1]
print("  UMAP done.")

# ── 5. HDBSCAN clustering ──────────────────────────────────────────────────
print("\nRunning HDBSCAN clustering...")
import hdbscan

clusterer = hdbscan.HDBSCAN(
    min_cluster_size=40,
    min_samples=5,
    metric="euclidean",       # on 2D UMAP space
    cluster_selection_method="eom",
)
cluster_labels = clusterer.fit_predict(embedding_2d)
combined["cluster_id"] = cluster_labels

n_clusters   = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
n_noise      = (cluster_labels == -1).sum()
print(f"  Clusters found: {n_clusters}")
print(f"  Noise points (cluster=-1): {n_noise} ({n_noise/len(cluster_labels)*100:.1f}%)")
print("\n  Cluster sizes:")
cluster_counts = pd.Series(cluster_labels).value_counts().sort_index()
print(cluster_counts.to_string())

# ── 6. Save main output ────────────────────────────────────────────────────
keep_cols = [
    "sid", "pmid", "pmcid", "source", "year", "journal", "title",
    "publication_types", "sentence_index", "sentence",
    "verb_trigger", "appositive_phrase",
    "framing_primary", "framing_categories",
    "cluster_id", "umap_x", "umap_y",
]
keep_cols = [c for c in keep_cols if c in combined.columns]
combined[keep_cols].to_csv(PROC / "framing_embedded_clustered.csv", index=False)
print(f"\n  Saved -> {PROC / 'framing_embedded_clustered.csv'}")

# ── 7. Save cluster examples (top 10 per cluster, closest to centroid) ─────
print("\nSaving cluster examples...")
example_rows = []

for cid in sorted(combined["cluster_id"].unique()):
    cdf = combined[combined["cluster_id"] == cid].copy()

    if cid == -1:
        label = "NOISE"
        # For noise, just take random sample
        sample = cdf.sample(min(10, len(cdf)), random_state=42)
    else:
        label = f"cluster_{cid}"
        # Find centroid and pick sentences closest to it
        cx = cdf["umap_x"].mean()
        cy = cdf["umap_y"].mean()
        cdf["dist_to_centroid"] = np.sqrt((cdf["umap_x"] - cx)**2 + (cdf["umap_y"] - cy)**2)
        sample = cdf.nsmallest(10, "dist_to_centroid")

    for _, row in sample.iterrows():
        example_rows.append({
            "cluster_id":      cid,
            "cluster_label":   label,
            "cluster_size":    len(cdf),
            "sid":             row["sid"],
            "pmid":            row.get("pmid"),
            "year":            row.get("year"),
            "source":          row.get("source"),
            "verb_trigger":    row.get("verb_trigger"),
            "framing_primary": row.get("framing_primary"),
            "sentence":        row.get("sentence"),
        })

examples_df = pd.DataFrame(example_rows)
examples_df.to_csv(PROC / "cluster_examples.csv", index=False)
print(f"  Saved -> {PROC / 'cluster_examples.csv'}")

# Print to console for quick review
print("\n=== CLUSTER EXAMPLES (top 5 per cluster) ===")
for cid in sorted(examples_df["cluster_id"].unique()):
    sub = examples_df[examples_df["cluster_id"] == cid]
    size = sub["cluster_size"].iloc[0]
    print(f"\n--- Cluster {cid}  (n={size}) ---")
    for _, row in sub.head(5).iterrows():
        kw = row.get("framing_primary", "")
        print(f"  [{int(row['year']) if pd.notna(row['year']) else '?'}][{kw}] {str(row['sentence'])[:200]}")

# ── 8. Plots ───────────────────────────────────────────────────────────────
print("\nGenerating plots...")
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.cm as cm

def scatter_plot(df, color_col, title, filename, palette=None, size=3, alpha=0.5):
    fig, ax = plt.subplots(figsize=(14, 10))
    labels = sorted(df[color_col].unique())

    if palette:
        colors = [palette.get(str(l), "#cccccc") for l in labels]
    else:
        cmap = cm.get_cmap("tab20", len(labels))
        colors = [cmap(i) for i in range(len(labels))]

    color_map = dict(zip(labels, colors))

    for label, color in color_map.items():
        sub = df[df[color_col] == label]
        ax.scatter(sub["umap_x"], sub["umap_y"], c=[color], s=size,
                   alpha=alpha, label=f"{label} (n={len(sub)})", rasterized=True)

    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.set_xlabel("UMAP 1"); ax.set_ylabel("UMAP 2")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1),
              frameon=False, fontsize=8, markerscale=4)
    plt.tight_layout()
    plt.savefig(GRAPHS / filename, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved -> {GRAPHS / filename}")

# Plot 1: coloured by HDBSCAN cluster
combined["cluster_str"] = combined["cluster_id"].astype(str)
scatter_plot(combined, "cluster_str",
             "UMAP of framing sentences — HDBSCAN clusters (data-driven)",
             "05_umap_clusters.png", alpha=0.4)

# Plot 2: coloured by keyword taxonomy category
KW_COLORS = {
    "relay":                "#E07B54",
    "hub_node":             "#5B9BD5",
    "gateway":              "#70AD47",
    "integrator":           "#9B59B6",
    "modulator":            "#F4C430",
    "network_component":    "#1ABC9C",
    "role_player":          "#95A5A6",
    "involved_in":          "#BDC3C7",
    "structure_description":"#AAB7B8",
    "clinical_target":      "#E74C3C",
    "other":                "#E8E8E8",
}
scatter_plot(combined, "framing_primary",
             "UMAP of framing sentences — keyword taxonomy category",
             "06_umap_keyword_category.png",
             palette={k: v for k, v in KW_COLORS.items()},
             alpha=0.5)

# Plot 3: coloured by source (abstract vs fulltext)
SOURCE_COLORS = {"fulltext": "#5B9BD5", "abstract": "#E07B54"}
scatter_plot(combined, "source",
             "UMAP of framing sentences — data source (full-text vs abstract)",
             "07_umap_source.png",
             palette=SOURCE_COLORS, alpha=0.4)

# Plot 4: coloured by decade
combined["decade"] = ((combined["year"] // 10) * 10).astype("Int64").astype(str)
DECADE_PALETTE = {
    "1970": "#f7fcb9", "1980": "#d9f0a3", "1990": "#addd8e",
    "2000": "#78c679", "2010": "#31a354", "2020": "#006837",
}
scatter_plot(combined, "decade",
             "UMAP of framing sentences — decade",
             "08_umap_decade.png",
             palette=DECADE_PALETTE, alpha=0.4)

print("\n=== Done ===")
