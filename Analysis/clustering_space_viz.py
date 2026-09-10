"""
clustering_space_viz.py — 2-D projections of each embedding space, colored by
Leiden cluster. Serves as the visual analogue of "show me the graph with
clusters filled in."

Strategy: for each (model, condition) pair, run UMAP (2-D) on the same
embeddings used for clustering. This preserves neighborhood structure that
the FAISS k-NN graph relies on, so the Leiden clusters appear as spatially
coherent blobs. We save the 2-D coordinates so downstream figure scripts
can reuse them without re-computing.

Outputs:
    Data/processed/clustering_stability/umap/
        umap_{model}__{condition}.npy     shape (n_papers, 2)

Usage:
    python analysis/clustering_space_viz.py
    python analysis/clustering_space_viz.py --seed 0
"""
from __future__ import annotations
import argparse, os, sys, io, time
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
import numpy as np

from _clustering_utils import (  # noqa: E402
    PROC, MODEL_TAGS, CONDITIONS, load_embeddings,
)

UMAP_DIR = PROC / "clustering_stability" / "umap"
UMAP_DIR.mkdir(parents=True, exist_ok=True)


def project_one(model: str, condition: str, n_neighbors: int = 30,
                min_dist: float = 0.05, seed: int = 42) -> np.ndarray:
    out_path = UMAP_DIR / f"umap_{model}__{condition}.npy"
    if out_path.exists():
        print(f"  [CACHED {model} {condition}] {out_path.name}")
        return np.load(out_path)
    from umap import UMAP
    embs = load_embeddings(model, condition)
    print(f"  ({model}, {condition}) UMAP on {embs.shape} "
          f"(n_neighbors={n_neighbors}, min_dist={min_dist})")
    t0 = time.time()
    reducer = UMAP(n_components=2, n_neighbors=n_neighbors, min_dist=min_dist,
                   metric="cosine", random_state=seed, verbose=False)
    coords = reducer.fit_transform(embs).astype(np.float32)
    np.save(out_path, coords)
    print(f"    → {out_path.name} ({time.time()-t0:.1f}s)")
    return coords


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=list(MODEL_TAGS))
    parser.add_argument("--conditions", nargs="+", default=list(CONDITIONS))
    parser.add_argument("--n-neighbors", type=int, default=30)
    parser.add_argument("--min-dist", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    print(f"Projecting {len(args.models)} × {len(args.conditions)} = "
          f"{len(args.models)*len(args.conditions)} configurations")
    for m in args.models:
        for c in args.conditions:
            try:
                project_one(m, c, n_neighbors=args.n_neighbors,
                            min_dist=args.min_dist, seed=args.seed)
            except FileNotFoundError:
                print(f"  [SKIP {m} {c}] embedding file missing")
    print(f"\nAll UMAP coordinates in {UMAP_DIR}")


if __name__ == "__main__":
    main()
