"""
_clustering_utils.py — shared helpers for paper-level domain clustering.

Used by:
  - clustering_stability_analysis.py    (24-config stability)
  - clustering_modularity_sweep.py      (resolution selection)
  - clustering_space_viz.py             (UMAP panels)
  - clustering_hierarchy.py             (cluster-of-clusters)
  - clustering_model_selection.py       (scorecard)

Centralizes: loading embeddings, building FAISS k-NN graph, running Leiden,
computing per-cluster stability, and domain-keyword regexes.
"""
from __future__ import annotations
import re
from pathlib import Path
from collections import Counter
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Paths / model registry
# ─────────────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB = ROOT / "Graphs" / "pub"

MODEL_TAGS = ("specter2", "pubmedbert", "mpnet")
MODEL_LABELS = {"specter2": "SPECTER2", "pubmedbert": "PubMedBERT", "mpnet": "MPNet"}
CONDITIONS = ("raw", "refined")   # 768-dim vs 64-dim contrastive

DEFAULT_RESOLUTIONS = (0.005, 0.008, 0.012, 0.020)
MIN_CLUSTER_SIZE = 50


def embedding_path(model_tag: str, condition: str = "raw") -> Path:
    """
    Returns the .npy path for a (model, condition) pair.

        (specter2, raw)     → specter2_embeddings.npy            (768-dim)
        (pubmedbert, refined) → pubmedbert_refined_embeddings.npy (64-dim)
    """
    if condition == "raw":
        return PROC / f"{model_tag}_embeddings.npy"
    elif condition == "refined":
        return PROC / f"{model_tag}_refined_embeddings.npy"
    raise ValueError(f"unknown condition: {condition}")


def load_embeddings(model_tag: str, condition: str = "raw") -> np.ndarray:
    """Load + L2-normalize embeddings for one (model, condition)."""
    emb = np.load(embedding_path(model_tag, condition)).astype(np.float32)
    norms = np.linalg.norm(emb, axis=1, keepdims=True)
    return emb / (norms + 1e-10)


def load_papers() -> pd.DataFrame:
    return pd.read_csv(PROC / "paper_abstracts.csv", low_memory=False)


def load_pmids() -> np.ndarray:
    return np.load(PROC / "specter2_pmids.npy")


# ─────────────────────────────────────────────────────────────────────────────
# Graph + Leiden
# ─────────────────────────────────────────────────────────────────────────────
def build_knn_graph(embs: np.ndarray, k: int = 50):
    """
    FAISS IndexFlatIP (cosine on L2-normalized) → symmetrized igraph with
    `combine_edges="max"`. Matches the reference methodology.
    """
    import faiss
    import igraph as ig

    n, d = embs.shape
    index = faiss.IndexFlatIP(d)
    index.add(embs)
    D, I = index.search(embs, k + 1)  # +1 since top-1 is self

    edges: List[Tuple[int, int]] = []
    weights: List[float] = []
    for i in range(n):
        for j_idx in range(1, k + 1):
            j = int(I[i, j_idx])
            if j != i:
                edges.append((i, j))
                weights.append(float(D[i, j_idx]))
    g = ig.Graph(n=n, edges=edges, directed=True)
    g.es["weight"] = weights
    return g.as_undirected(combine_edges="max")


def run_leiden(graph, resolution: float, seed: int = 42):
    """Leiden with CPM at a single resolution. Returns the partition object."""
    import leidenalg
    return leidenalg.find_partition(
        graph, leidenalg.CPMVertexPartition,
        resolution_parameter=resolution, weights="weight", seed=seed,
    )


def leiden_sweep(graph, resolutions: Sequence[float],
                 seed: int = 42, verbose: bool = False
                 ) -> pd.DataFrame:
    """
    Run Leiden at many resolutions. Returns a DataFrame with columns:
      resolution, modularity, n_clusters, n_big (≥ MIN_CLUSTER_SIZE).
    """
    rows = []
    for r in resolutions:
        part = run_leiden(graph, r, seed=seed)
        mem = np.array(part.membership)
        sizes = Counter(mem)
        n_big = sum(1 for s in sizes.values() if s >= MIN_CLUSTER_SIZE)
        rows.append({
            "resolution": float(r),
            "modularity": float(part.modularity),
            "n_clusters": int(len(sizes)),
            "n_big": int(n_big),
        })
        if verbose:
            print(f"    res={r:.4f}: mod={part.modularity:.4f}, "
                  f"n={len(sizes)} ({n_big} big)")
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Cluster stability
# ─────────────────────────────────────────────────────────────────────────────
def best_jaccard(members_set: set, other_mem: np.ndarray) -> float:
    """Best Jaccard of `members_set` against any cluster in `other_mem`."""
    if len(members_set) == 0:
        return 0.0
    members_idx = list(members_set)
    other_counts = Counter(other_mem[members_idx])
    best = 0.0
    for c, overlap in other_counts.items():
        other_members = np.where(other_mem == c)[0]
        union = len(members_set | set(other_members.tolist()))
        j = overlap / union if union > 0 else 0.0
        if j > best:
            best = j
    return best


def stability_for_config(
    cfg_key: str,
    memberships: Dict[str, np.ndarray],
    other_keys: Optional[Iterable[str]] = None,
    min_size: int = MIN_CLUSTER_SIZE,
) -> pd.DataFrame:
    """
    For each cluster in config `cfg_key` (size ≥ min_size), compute best-Jaccard
    vs every *other* config in `memberships`, and their mean/min.

    Returns per-cluster DataFrame with stability columns.
    """
    mem = memberships[cfg_key]
    if other_keys is None:
        other_keys = [k for k in memberships if k != cfg_key]
    sizes = Counter(mem)
    clusters = sorted([c for c, n in sizes.items() if n >= min_size],
                     key=lambda c: -sizes[c])

    rows = []
    for c in clusters:
        members_set = set(np.where(mem == c)[0].tolist())
        jaccards = {k: best_jaccard(members_set, memberships[k]) for k in other_keys}
        rows.append({
            "config": cfg_key,
            "cluster": int(c),
            "n_papers": int(sizes[c]),
            "stability_mean": float(np.mean(list(jaccards.values()))),
            "stability_min": float(np.min(list(jaccards.values()))),
            "stability_max": float(np.max(list(jaccards.values()))),
            **{f"jacc_{k}": float(v) for k, v in jaccards.items()},
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Cluster characterization
# ─────────────────────────────────────────────────────────────────────────────
# Domain-keyword regexes (copied from cluster_tables.py for consistency)
DOMAIN_RE = {
    "VIS":  re.compile(r"\b(lgn|dlgn|lateral geniculate|visual cortex|visual thalam|retin\w+|v1|primary visual|photorecept|retinotop)\b", re.IGNORECASE),
    "AUD":  re.compile(r"\b(mgb|mgv|medial geniculate|auditory thalam|tonotop|cochlea|frequency tuning|inferior colliculus)\b", re.IGNORECASE),
    "SOM":  re.compile(r"\b(whisker|barrel|somatosensor|vpm|vpl|ventrobasal|trigeminal)\b", re.IGNORECASE),
    "PAIN": re.compile(r"\b(nociceptive|morphine|pain|allodynia|neuropathic|hyperalgesia)\b", re.IGNORECASE),
    "CLIN": re.compile(r"\b(schizophrenia|major depressive|bipolar|mdd|ocd|multiple sclerosis|parkinson|alzheimer)\b", re.IGNORECASE),
    "DBS":  re.compile(r"\b(DBS|deep brain stimulation|thalamotomy|mrgfus|vim)\b", re.IGNORECASE),
    "SLP":  re.compile(r"\b(sleep|nrem|rem sleep|slow wave|insomnia|wakefulness|arousal|vigilance)\b", re.IGNORECASE),
    "DEV":  re.compile(r"\b(embryonic|subplate|thalamocortical axon|developmental|preterm|progenitor)\b", re.IGNORECASE),
    "MEM":  re.compile(r"\b(memory|amnesia|fear conditioning|spatial memory|maze|extinction)\b", re.IGNORECASE),
    "EPI":  re.compile(r"\b(epilepsy|seizure|absence|spike.wave|gaers|swd|ictal)\b", re.IGNORECASE),
    "OSC":  re.compile(r"\b(oscillat|burst firing|pacemaker|spindle|rhythm|tonic|t-type|i\bt\b)\b", re.IGNORECASE),
    "TRAC": re.compile(r"\b(hrp|tract trac\w+|anterograde|retrograde|wga|fluorogold|cholera toxin|aav tracer)\b", re.IGNORECASE),
    "STRK": re.compile(r"\b(stroke|infarct|hemorrhage|hematoma|paramedian|aphasia)\b", re.IGNORECASE),
    "CIRC": re.compile(r"\b(corticothalamic|thalamocortical loop|reciprocal|circuit|connectivity|functional connectivity)\b", re.IGNORECASE),
}

DOMAIN_COLORS = {
    "VIS": "#1f77b4", "AUD": "#ff7f0e", "SOM": "#2ca02c",
    "PAIN": "#d62728", "CLIN": "#9467bd", "DBS": "#8c564b",
    "SLP": "#e377c2", "DEV": "#7f7f7f", "MEM": "#bcbd22",
    "EPI": "#17becf", "OSC": "#aec7e8", "TRAC": "#ffbb78",
    "STRK": "#98df8a", "CIRC": "#ff9896",
}


def compute_domain_flags(papers: pd.DataFrame) -> pd.DataFrame:
    """Add boolean dom_{KEY} columns for each DOMAIN_RE pattern."""
    text = papers["title"].fillna("") + " " + papers["abstract"].fillna("")
    out = papers.copy()
    for k, pat in DOMAIN_RE.items():
        out[f"dom_{k}"] = text.str.contains(pat)
    return out


def cluster_domain_fractions(
    papers_with_flags: pd.DataFrame, memberships: np.ndarray,
    min_size: int = MIN_CLUSTER_SIZE,
) -> pd.DataFrame:
    """Fraction of each cluster's papers matching each domain regex."""
    sizes = Counter(memberships)
    rows = []
    for c, n in sizes.items():
        if n < min_size:
            continue
        mask = memberships == c
        sub = papers_with_flags.loc[mask]
        rows.append({
            "cluster": int(c), "n_papers": int(n),
            **{d: float(sub[f"dom_{d}"].mean()) for d in DOMAIN_RE.keys()},
        })
    return pd.DataFrame(rows)


def top_tfidf_terms(papers: pd.DataFrame, memberships: np.ndarray,
                    top_k: int = 6, min_size: int = MIN_CLUSTER_SIZE
                    ) -> Dict[int, List[str]]:
    """TF-IDF (1-2grams) on cluster-concatenated abstracts. Returns {cluster: [terms]}."""
    from sklearn.feature_extraction.text import TfidfVectorizer

    sizes = Counter(memberships)
    clusters = sorted([c for c, n in sizes.items() if n >= min_size])
    texts = []
    for c in clusters:
        mask = memberships == c
        texts.append(" ".join(papers.loc[mask, "abstract"].fillna("").tolist()))

    vec = TfidfVectorizer(max_features=3000, stop_words="english",
                          ngram_range=(1, 2), min_df=2, max_df=0.5)
    X = vec.fit_transform(texts)
    feats = np.array(vec.get_feature_names_out())
    out = {}
    for i, c in enumerate(clusters):
        top_idx = X[i].toarray()[0].argsort()[-top_k:][::-1]
        out[int(c)] = feats[top_idx].tolist()
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: cluster by centroid
# ─────────────────────────────────────────────────────────────────────────────
def cluster_centroids(embs: np.ndarray, memberships: np.ndarray,
                      min_size: int = MIN_CLUSTER_SIZE
                      ) -> Tuple[np.ndarray, np.ndarray]:
    """
    Mean embedding per cluster (L2-normalized). Returns (centroids, cluster_ids).
    Only includes clusters with ≥ min_size members.
    """
    sizes = Counter(memberships)
    ids = sorted([c for c, n in sizes.items() if n >= min_size])
    cents = np.vstack([embs[memberships == c].mean(axis=0) for c in ids])
    norms = np.linalg.norm(cents, axis=1, keepdims=True)
    cents = cents / (norms + 1e-10)
    return cents.astype(np.float32), np.array(ids)


def config_key(model_tag: str, condition: str, resolution: float) -> str:
    """Canonical config string, e.g. 'specter2__raw__r008'."""
    return f"{model_tag}__{condition}__r{int(round(resolution * 1000)):03d}"
