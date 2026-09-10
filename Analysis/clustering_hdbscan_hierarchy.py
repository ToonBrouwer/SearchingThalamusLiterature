"""
clustering_hdbscan_hierarchy.py — Agglomerative cluster-of-clusters on
HDBSCAN cluster centroids. Mirror of `clustering_hierarchy.py`.

Only non-noise clusters of size ≥ MIN_CLUSTER_SIZE are used. Centroids
are L2-normalised mean embeddings in the model's *original* embedding
space (not the 5-D UMAP) so umbrella distances remain comparable across
configs.

Outputs (under `Data/processed/clustering_hdbscan/hierarchy/`):
    linkage_{config}.npy              scipy linkage matrix
    cluster_ids_{config}.npy          parallel array of child cluster ids
    cluster_to_umbrella_{config}.csv  cluster → umbrella@k for k ∈ cuts
    umbrella_labels_{config}.csv      TF-IDF label + child list per umbrella

Usage
-----
    python analysis/clustering_hdbscan_hierarchy.py
    python analysis/clustering_hdbscan_hierarchy.py --configs specter2__refined__mcs050
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import pdist
from sklearn.feature_extraction.text import TfidfVectorizer

from _clustering_utils import (  # noqa: E402
    load_embeddings, load_papers, cluster_centroids, MIN_CLUSTER_SIZE,
)
from _hdbscan_utils import HDB_DIR, MODEL_TAGS, CONDITIONS, DEFAULT_MIN_CLUSTER_SIZES


HIER_DIR = HDB_DIR / "hierarchy"
HIER_DIR.mkdir(parents=True, exist_ok=True)


def parse_config(cfg: str) -> tuple[str, str, int]:
    """`specter2__raw__mcs050` → ('specter2', 'raw', 50)"""
    parts = cfg.split("__")
    return parts[0], parts[1], int(parts[2].lstrip("mcs"))


def umbrella_terms(papers, memberships, umbrella_map, top_k=8):
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
    model, cond, mcs = parse_config(cfg_key)
    mem_path = HDB_DIR / f"memberships_{cfg_key}.npy"
    if not mem_path.exists():
        print(f"  [SKIP {cfg_key}] memberships missing — run clustering_hdbscan_full first")
        return
    memberships = np.load(mem_path)

    embs = load_embeddings(model, cond)   # original dim for centroids
    papers = load_papers()

    # Only non-noise clusters of sufficient size
    # cluster_centroids handles the size filter but NOT the -1 label
    non_noise = memberships.copy()
    non_noise = np.where(non_noise == -1, -999, non_noise)  # exclude noise from centroid groupings
    centroids, cluster_ids = cluster_centroids(embs, non_noise)
    # filter out the dummy -999 if present
    keep = cluster_ids >= 0
    centroids = centroids[keep]; cluster_ids = cluster_ids[keep]
    if len(cluster_ids) < 3:
        print(f"  [SKIP {cfg_key}] only {len(cluster_ids)} big clusters")
        return

    print(f"\n=== {cfg_key}: {len(cluster_ids)} centroids (dim {centroids.shape[1]}) ===")
    dist = pdist(centroids, metric="cosine")
    Z = linkage(dist, method="average")
    np.save(HIER_DIR / f"linkage_{cfg_key}.npy", Z)
    np.save(HIER_DIR / f"cluster_ids_{cfg_key}.npy", cluster_ids)

    rows = {int(c): {} for c in cluster_ids}
    umb_summaries = []
    for k in cuts:
        if k >= len(cluster_ids):
            continue
        labels = fcluster(Z, t=k, criterion="maxclust")
        umap = dict(zip(cluster_ids.tolist(), labels.tolist()))
        for c in cluster_ids:
            rows[int(c)][f"umbrella_k{k}"] = int(umap[int(c)])
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
                        help="Configs to process (default: all 6 at the default mcs)")
    parser.add_argument("--min-cluster-size", type=int, default=50,
                        help="Default mcs if --configs omitted")
    parser.add_argument("--cuts", nargs="+", type=int,
                        default=[4, 6, 8, 10, 12])
    args = parser.parse_args()

    if args.configs is None:
        mtag = f"mcs{args.min_cluster_size:03d}"
        args.configs = [f"{m}__{c}__{mtag}" for m in MODEL_TAGS for c in CONDITIONS]
    print(f"Configs to process: {args.configs}")

    for cfg in args.configs:
        run_one_config(cfg, cuts=args.cuts)

    print(f"\nAll hierarchy outputs in {HIER_DIR}")


if __name__ == "__main__":
    main()
