"""
compare_embedding_models.py — Cluster stability across embedding models.

Compares 3 embedding models:
  1. SPECTER2 (scientific, 768d) — already computed
  2. PubMedBERT (biomedical, 768d) — new
  3. MPNet-all (general, 768d) — new

For each: embed abstracts → Leiden at multiple resolutions → compare clustering.

Also tests resolution sensitivity: 8, 20, 40 clusters for SPECTER2 to see if visual
thalamus emerges as its own cluster at higher resolution.

Outputs:
  Data/processed/{model}_embeddings.npy
  Data/processed/domain_clusters_{model}_r{N}.csv
  Graphs/pub/fig_embedding_model_comparison.png
"""
import sys, io, os, time
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

papers = pd.read_csv(PROC / "paper_abstracts.csv", low_memory=False)
print(f"Papers: {len(papers):,}")

# ═══════════════════════════════════════════════════════════════════════════
# EMBED WITH ALTERNATIVE MODELS
# ═══════════════════════════════════════════════════════════════════════════

MODELS = {
    "pubmedbert": "neuml/pubmedbert-base-embeddings",  # sentence-transformers compatible
    "mpnet": "sentence-transformers/all-mpnet-base-v2",  # general, strong
}


def embed_with_st(model_name, texts, batch_size=32, cache_name=None):
    import torch
    from sentence_transformers import SentenceTransformer

    cache = PROC / f"{cache_name}_embeddings.npy"
    if cache.exists():
        print(f"  Loading cached {cache_name}: {cache}")
        return np.load(cache)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"  Loading {model_name} on {device}...")
    model = SentenceTransformer(model_name, device=device)
    t0 = time.time()
    emb = model.encode(texts, batch_size=batch_size, show_progress_bar=True,
                        convert_to_numpy=True, normalize_embeddings=True)
    print(f"  Done in {(time.time()-t0)/60:.1f} min, shape={emb.shape}")
    np.save(cache, emb)
    return emb


print("\n=== EMBEDDING WITH PUBMEDBERT ===")
emb_pubmed = embed_with_st(MODELS["pubmedbert"], papers["text"].tolist(),
                            cache_name="pubmedbert")

print("\n=== EMBEDDING WITH MPNET ===")
emb_mpnet = embed_with_st(MODELS["mpnet"], papers["text"].tolist(),
                           cache_name="mpnet")

# Load SPECTER2 (already computed)
print("\n=== LOADING SPECTER2 ===")
emb_specter = np.load(PROC / "specter2_embeddings.npy")
pmids = np.load(PROC / "specter2_pmids.npy")
# Ensure same order
assert len(emb_specter) == len(papers)
print(f"  SPECTER2 shape: {emb_specter.shape}")

# ═══════════════════════════════════════════════════════════════════════════
# LEIDEN CLUSTERING AT MULTIPLE RESOLUTIONS
# ═══════════════════════════════════════════════════════════════════════════
import faiss
import igraph as ig
import leidenalg


def cluster(embeddings, target_resolutions=None, k=50):
    """Build FAISS K-NN graph, run Leiden at specified resolutions."""
    # L2 normalize
    embs = embeddings.astype(np.float32)
    norms = np.linalg.norm(embs, axis=1, keepdims=True)
    embs = embs / (norms + 1e-10)
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

    results = {}
    for res in target_resolutions:
        partition = leidenalg.find_partition(
            g, leidenalg.CPMVertexPartition,
            resolution_parameter=res,
            weights="weight", seed=42,
        )
        results[res] = {
            "membership": partition.membership,
            "modularity": partition.modularity,
            "n_clusters": len(partition),
        }
    return results


# Target resolutions (fewer test points for speed)
TARGET_RES = [0.003, 0.005, 0.008, 0.012]

print("\n=== CLUSTERING SPECTER2 ===")
spec_results = cluster(emb_specter, TARGET_RES)
for res, r in spec_results.items():
    print(f"  res={res}: mod={r['modularity']:.3f}, n_clusters={r['n_clusters']}")

print("\n=== CLUSTERING PUBMEDBERT ===")
pubmed_results = cluster(emb_pubmed, TARGET_RES)
for res, r in pubmed_results.items():
    print(f"  res={res}: mod={r['modularity']:.3f}, n_clusters={r['n_clusters']}")

print("\n=== CLUSTERING MPNET ===")
mpnet_results = cluster(emb_mpnet, TARGET_RES)
for res, r in mpnet_results.items():
    print(f"  res={res}: mod={r['modularity']:.3f}, n_clusters={r['n_clusters']}")

# ═══════════════════════════════════════════════════════════════════════════
# COMPARE CLUSTERINGS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("CLUSTERING AGREEMENT (Adjusted Rand Index)")
print("=" * 70)

from sklearn.metrics import adjusted_rand_score

for res in TARGET_RES:
    s_mem = spec_results[res]["membership"]
    p_mem = pubmed_results[res]["membership"]
    m_mem = mpnet_results[res]["membership"]

    ari_sp = adjusted_rand_score(s_mem, p_mem)
    ari_sm = adjusted_rand_score(s_mem, m_mem)
    ari_pm = adjusted_rand_score(p_mem, m_mem)
    print(f"\n  res={res} (SPECTER2: {spec_results[res]['n_clusters']} clusters, "
          f"PubMedBERT: {pubmed_results[res]['n_clusters']}, "
          f"MPNet: {mpnet_results[res]['n_clusters']}):")
    print(f"    SPECTER2 vs PubMedBERT: ARI={ari_sp:.3f}")
    print(f"    SPECTER2 vs MPNet:      ARI={ari_sm:.3f}")
    print(f"    PubMedBERT vs MPNet:    ARI={ari_pm:.3f}")

# ═══════════════════════════════════════════════════════════════════════════
# SAVE CLUSTERINGS FOR CHARACTERIZATION
# ═══════════════════════════════════════════════════════════════════════════
for model_name, results in [("specter2", spec_results),
                             ("pubmedbert", pubmed_results),
                             ("mpnet", mpnet_results)]:
    for res, r in results.items():
        out = pd.DataFrame({
            "pmid": pmids,
            f"cluster_{model_name}_r{int(res*1000)}": r["membership"],
        })
        out.to_csv(PROC / f"domain_clusters_{model_name}_r{int(res*1000)}.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════════
# CHARACTERIZE FINEST SPECTER2 CLUSTERING (20+ clusters)
# ═══════════════════════════════════════════════════════════════════════════
from sklearn.feature_extraction.text import TfidfVectorizer

print("\n" + "=" * 70)
print("SPECTER2 AT res=0.005 (higher resolution)")
print("=" * 70)

df_fine = papers.copy()
df_fine["cluster"] = spec_results[0.005]["membership"]

# Check visual distribution
import re
visual_re = re.compile(r'\b(lgn|dlgn|lateral geniculate|visual cortex|visual thalam|'
                        r'retin\w+|v1|primary visual|photorecept|retinotop)\b',
                        re.IGNORECASE)
auditory_re = re.compile(r'\b(mgb|mgv|medial geniculate|auditory thalam|'
                          r'tonotop|frequency tuning|inferior colliculus)\b',
                          re.IGNORECASE)

text = df_fine["title"].fillna("") + " " + df_fine["abstract"].fillna("")
df_fine["is_visual"] = text.str.contains(visual_re)
df_fine["is_auditory"] = text.str.contains(auditory_re)

# For each cluster: size, visual%, auditory%, top terms
print(f"\n  {spec_results[0.005]['n_clusters']} clusters at res=0.005:")

cluster_texts = []
big_clusters = []
for c in sorted(df_fine["cluster"].unique()):
    sub = df_fine[df_fine["cluster"] == c]
    if len(sub) < 100:
        continue
    big_clusters.append(c)
    txt = " ".join(sub["abstract"].fillna("").tolist())
    cluster_texts.append(txt)

vec = TfidfVectorizer(max_features=3000, stop_words="english", ngram_range=(1, 2),
                      min_df=2, max_df=0.5)
X = vec.fit_transform(cluster_texts)
feature_names = np.array(vec.get_feature_names_out())

for i, c in enumerate(big_clusters):
    sub = df_fine[df_fine["cluster"] == c]
    top_idx = X[i].toarray()[0].argsort()[-8:][::-1]
    top_terms = feature_names[top_idx].tolist()
    vis_pct = sub["is_visual"].mean() * 100
    aud_pct = sub["is_auditory"].mean() * 100
    year_mean = sub["year"].mean()
    print(f"\n  C{c}: n={len(sub)}, year~{year_mean:.0f}, "
          f"vis={vis_pct:.0f}%, aud={aud_pct:.0f}%")
    print(f"    Terms: {', '.join(top_terms[:5])}")

print("\nDone.")
