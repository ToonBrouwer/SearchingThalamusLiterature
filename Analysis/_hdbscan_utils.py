"""
_hdbscan_utils.py — shared helpers for the HDBSCAN branch of the
paper-level domain-clustering analysis.

Mirrors `_clustering_utils.py` (Leiden) but:
  * reduces embeddings to 5-D via UMAP before clustering (standard
    density-based practice; BERTopic uses the same trick)
  * replaces resolution with HDBSCAN's min_cluster_size as the
    granularity parameter
  * replaces Leiden modularity with DBCV (density-based cluster
    validation) as the internal-quality metric

Output directory is `Data/processed/clustering_hdbscan/` (parallel to
`Data/processed/clustering_stability/` used by the Leiden pipeline).
"""
from __future__ import annotations
import os
from pathlib import Path
from collections import Counter
from typing import Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

# Reuse the Leiden utilities' paths, model registry, and domain regexes
# so stability / Jaccard / TF-IDF / figure-style code stays identical.
from _clustering_utils import (  # noqa: E402
    PROC, PUB, MODEL_TAGS, MODEL_LABELS, CONDITIONS,
    MIN_CLUSTER_SIZE,
    load_embeddings, load_papers, load_pmids,
    best_jaccard, compute_domain_flags, top_tfidf_terms,
    cluster_centroids,
    DOMAIN_RE, DOMAIN_COLORS,
)

# ─────────────────────────────────────────────────────────────────────────
# HDBSCAN-branch paths & defaults
# ─────────────────────────────────────────────────────────────────────────
HDB_DIR = PROC / "clustering_hdbscan"
HDB_UMAP_DIR = HDB_DIR / "umap_5d"
HDB_DIR.mkdir(parents=True, exist_ok=True)
HDB_UMAP_DIR.mkdir(parents=True, exist_ok=True)

# Granularity parameter = HDBSCAN's min_cluster_size. The five values
# give cluster counts comparable to the four Leiden resolutions we used:
# ~12–20 clusters at mcs=200 ↔ Leiden r=0.005, etc.
DEFAULT_MIN_CLUSTER_SIZES = (30, 50, 80, 120, 200)

# UMAP preprocessing — 5-D, cosine metric (BERTopic convention)
UMAP_COMPONENTS = 5
UMAP_NEIGHBORS = 30
UMAP_MIN_DIST = 0.0        # 0.0 makes HDBSCAN happier (tight clumps)
UMAP_SEED = 42


# ─────────────────────────────────────────────────────────────────────────
# UMAP-5D cache
# ─────────────────────────────────────────────────────────────────────────
def umap5d_path(model_tag: str, condition: str) -> Path:
    return HDB_UMAP_DIR / f"umap5d_{model_tag}__{condition}.npy"


def load_or_make_umap5d(model_tag: str, condition: str,
                         verbose: bool = True) -> np.ndarray:
    p = umap5d_path(model_tag, condition)
    if p.exists():
        return np.load(p)
    from umap import UMAP
    if verbose:
        print(f"  UMAP 5-D ({model_tag}/{condition}) …", flush=True)
    embs = load_embeddings(model_tag, condition)
    reducer = UMAP(
        n_components=UMAP_COMPONENTS,
        n_neighbors=UMAP_NEIGHBORS,
        min_dist=UMAP_MIN_DIST,
        metric="cosine",
        random_state=UMAP_SEED,
        verbose=False,
    )
    coords = reducer.fit_transform(embs).astype(np.float32)
    np.save(p, coords)
    if verbose:
        print(f"    saved {p.name}  shape={coords.shape}", flush=True)
    return coords


# ─────────────────────────────────────────────────────────────────────────
# HDBSCAN wrapper
# ─────────────────────────────────────────────────────────────────────────
def run_hdbscan(umap5d: np.ndarray,
                min_cluster_size: int,
                min_samples: int | None = None,
                metric: str = "euclidean",
                cluster_selection_method: str = "eom",
                seed: int = 42) -> np.ndarray:
    """
    Run HDBSCAN on 5-D UMAP coordinates. Returns an integer membership
    vector where -1 denotes noise points.
    """
    import hdbscan
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples if min_samples is not None else min_cluster_size // 2,
        metric=metric,
        cluster_selection_method=cluster_selection_method,
    )
    return clusterer.fit_predict(umap5d.astype(np.float64)).astype(np.int32)


# ─────────────────────────────────────────────────────────────────────────
# DBCV — density-based cluster validity
# ─────────────────────────────────────────────────────────────────────────
def dbcv_score(umap5d: np.ndarray, memberships: np.ndarray,
               sample_cap: int = 6000, seed: int = 42) -> float:
    """
    DBCV via hdbscan.validity.validity_index. Sub-samples large corpora
    to keep runtime tractable (DBCV is O(n² log n) in the worst case).
    Returns NaN if validity is undefined (e.g. all noise).
    """
    from hdbscan import validity
    rng = np.random.RandomState(seed)
    if len(umap5d) > sample_cap:
        idx = rng.choice(len(umap5d), size=sample_cap, replace=False)
        X = umap5d[idx].astype(np.float64)
        y = memberships[idx].astype(np.int32)
    else:
        X = umap5d.astype(np.float64)
        y = memberships.astype(np.int32)
    # Need ≥ 2 real clusters
    if len(np.unique(y[y >= 0])) < 2:
        return float("nan")
    try:
        return float(validity.validity_index(X, y))
    except Exception as exc:  # pragma: no cover – logged, not fatal
        return float("nan")


# ─────────────────────────────────────────────────────────────────────────
# Config key + stability helpers
# ─────────────────────────────────────────────────────────────────────────
def hdb_config_key(model_tag: str, condition: str, min_cluster_size: int) -> str:
    """`specter2__raw__mcs050`."""
    return f"{model_tag}__{condition}__mcs{int(min_cluster_size):03d}"


def big_clusters(memberships: np.ndarray,
                 min_report_size: int = MIN_CLUSTER_SIZE) -> List[int]:
    sizes = Counter(memberships[memberships >= 0])
    return sorted([c for c, n in sizes.items() if n >= min_report_size],
                  key=lambda c: -sizes[c])


def noise_fraction(memberships: np.ndarray) -> float:
    return float((memberships == -1).mean())


def stability_for_config(
    cfg_key: str,
    memberships: Dict[str, np.ndarray],
    other_keys: Sequence[str] | None = None,
    min_size: int = MIN_CLUSTER_SIZE,
) -> pd.DataFrame:
    """
    Same Jaccard-stability routine as the Leiden side but written to
    cope with HDBSCAN's noise label (-1): noise is never considered a
    cluster when computing best-Jaccard overlaps.
    """
    mem = memberships[cfg_key]
    big = big_clusters(mem, min_size)
    if other_keys is None:
        other_keys = [k for k in memberships if k != cfg_key]

    rows = []
    for c in big:
        members_set = set(np.where(mem == c)[0].tolist())
        jaccs = {}
        for k in other_keys:
            other_mem = memberships[k]
            # mask out noise in the comparator so an all-(-1) cluster
            # never wins the overlap contest
            valid = other_mem >= 0
            if not valid.any():
                jaccs[k] = 0.0
                continue
            jaccs[k] = best_jaccard(members_set, other_mem)
        sizes = Counter(mem)
        rows.append({
            "config": cfg_key,
            "cluster": int(c),
            "n_papers": int(sizes[c]),
            "stability_mean": float(np.mean(list(jaccs.values()))),
            "stability_min": float(np.min(list(jaccs.values()))),
            "stability_max": float(np.max(list(jaccs.values()))),
            **{f"jacc_{k}": float(v) for k, v in jaccs.items()},
        })
    return pd.DataFrame(rows)
