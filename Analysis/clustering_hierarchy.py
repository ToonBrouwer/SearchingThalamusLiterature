"""
clustering_hierarchy.py — Hierarchical clustering of Leiden clusters.

Given a chosen clustering config (per-model, typically at the argmax-modularity
resolution), compute cluster centroids and run agglomerative clustering on
them. This reveals *umbrella domains* that group subclusters together —
analogous to hierarchical topic modelling.

For each requested config:
  1. Compute L2-normalized cluster centroids (only clusters ≥ MIN_CLUSTER_SIZE)
  2. Agglomerative clustering (average linkage, cosine distance)
  3. Save dendrogram linkage + flat cut assignments at multiple `n_umbrella` values
  4. TF-IDF for umbrella-level terms (aggregating within-umbrella papers)

Outputs:
    Data/processed/clustering_stability/hierarchy/
        linkage_{config}.npy              scipy linkage matrix
        cluster_to_umbrella_{config}.csv  cluster → umbrella at each cut
        umbrella_labels_{config}.csv      umbrella → top TF-IDF terms + child clusters

Usage:
    python analysis/clustering_hierarchy.py                          # all 6 configs
    python analysis/clustering_hierarchy.py --configs specter2__refined__r008
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
from collections import Counter

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import pdist
from sklearn.feature_extraction.text import TfidfVectorizer

from _clustering_utils import (  # noqa: E402
    PROC, MODEL_TAGS, CONDITIONS,
    load_embeddings, load_papers,
    cluster_centroids, MIN_CLUSTER_SIZE,
)

STAB_DIR = PROC / "clustering_stability"
HIER_DIR = STAB_DIR / "hierarchy"
HIER_DIR.mkdir(parents=True, exist_ok=True)


def parse_config(cfg: str) -> tuple[str, str, float]:
    """'specter2__refined__r008' → ('specter2', 'refined', 0.008)"""
    parts = cfg.split("__")
    return parts[0], parts[1], int(parts[2].lstrip("r")) / 1000.0


def umbrella_terms(papers, memberships, umbrella_map, top_k=8):
    """TF-IDF on papers grouped by umbrella cluster."""
    mapping = pd.Series(umbrella_map)
    paper_umbrella = mapping.reindex(memberships).values
    texts, ids = [], []
    for u in sorted(set(paper_umbrella[~pd.isna(paper_umbrella)])):
        mask = paper_umbrella == u
        if mask.sum() < 20:
            continue
        ids.append(int(u))
        texts.append(" ".join(papers.loc[mask, "abstract"].fillna("").tolist()))
    vec = TfidfVectorizer(max_features=3000, stop_words="english",
                          ngram_range=(1, 2), min_df=2, max_df=0.5)
    X = vec.fit_transform(texts)
    feats = np.array(vec.get_feature_names_out())
    return {u: feats[X[i].toarray()[0].argsort()[-top_k:][::-1]].tolist()
            for i, u in enumerate(ids)}


def run_one_config(cfg_key: str, cuts=(4, 6, 8, 10, 12)):
    model, cond, res = parse_config(cfg_key)
    mem_path = STAB_DIR / f"memberships_{cfg_key}.npy"
    if not mem_path.exists():
        print(f"  [SKIP {cfg_key}] memberships file not found — "
              f"run clustering_stability_full.py first")
        return
    memberships = np.load(mem_path)

    embs = load_embeddings(model, cond)
    papers = load_papers()
    centroids, cluster_ids = cluster_centroids(embs, memberships)
    if len(cluster_ids) < 3:
        print(f"  [SKIP {cfg_key}] only {len(cluster_ids)} big clusters")
        return

    print(f"\n=== {cfg_key}: {len(cluster_ids)} centroids (dim {centroids.shape[1]}) ===")
    # cosine distance on centroids
    dist = pdist(centroids, metric="cosine")
    Z = linkage(dist, method="average")
    np.save(HIER_DIR / f"linkage_{cfg_key}.npy", Z)
    np.save(HIER_DIR / f"cluster_ids_{cfg_key}.npy", cluster_ids)

    # flat cuts at multiple umbrella counts
    rows = {int(c): {} for c in cluster_ids}
    umb_summaries = []
    for k in cuts:
        if k >= len(cluster_ids):
            continue
        labels = fcluster(Z, t=k, criterion="maxclust")
        umap = dict(zip(cluster_ids.tolist(), labels.tolist()))
        for c in cluster_ids:
            rows[int(c)][f"umbrella_k{k}"] = int(umap[int(c)])
        # Summarize umbrellas
        paper_to_umb = np.array([umap.get(int(c), -1) for c in memberships])
        terms = umbrella_terms(papers, memberships, umap, top_k=8)
        sizes = Counter(paper_to_umb)
        for u, size in sizes.items():
            if u == -1:
                continue
            child_clusters = sorted([int(c) for c, lbl in umap.items() if lbl == u])
            umb_summaries.append({
                "config": cfg_key, "n_umbrella": k,
                "umbrella": int(u),
                "n_child_clusters": len(child_clusters),
                "child_clusters": ",".join(map(str, child_clusters)),
                "n_papers": int(size),
                "top_terms": " | ".join(terms.get(u, [])),
            })

    df = pd.DataFrame.from_dict(rows, orient="index").reset_index().rename(
        columns={"index": "cluster"})
    df["config"] = cfg_key
    df.to_csv(HIER_DIR / f"cluster_to_umbrella_{cfg_key}.csv", index=False)

    pd.DataFrame(umb_summaries).to_csv(
        HIER_DIR / f"umbrella_labels_{cfg_key}.csv", index=False)
    print(f"  saved: linkage, cluster→umbrella, umbrella_labels")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--configs", nargs="*", default=None,
                        help="Configs to process (default: all 6 at res=0.008)")
    parser.add_argument("--resolution", type=float, default=0.008,
                        help="Default resolution if --configs omitted")
    parser.add_argument("--cuts", nargs="+", type=int,
                        default=[4, 6, 8, 10, 12])
    args = parser.parse_args()

    if args.configs is None:
        rtag = f"r{int(args.resolution*1000):03d}"
        args.configs = [f"{m}__{c}__{rtag}" for m in MODEL_TAGS for c in CONDITIONS]
    print(f"Configs to process: {args.configs}")

    for cfg in args.configs:
        run_one_config(cfg, cuts=args.cuts)

    print(f"\nAll hierarchy outputs in {HIER_DIR}")


if __name__ == "__main__":
    main()
