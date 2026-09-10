"""
clustering_modularity_sweep.py — Modularity-based resolution selection.

For each (model, condition) pair, sweeps Leiden-CPM across many resolutions
and records (modularity, n_clusters, n_big). Produces the diagnostic needed
to *justify* a chosen working resolution.

Reference methodology (Valerio et al.): 1000 resolutions in [0.001, 1.0],
early-stop after N non-improving modularity steps. Here we default to a
coarser sweep (100 points, log-spaced in [5e-4, 0.1]) since the meaningful
range for this corpus is [~0.003, 0.03]; the full 1000-point sweep is
available via --dense.

Outputs:
    Data/processed/clustering_stability/modularity_sweep.csv
        (model, condition, resolution, modularity, n_clusters, n_big)

Usage:
    python analysis/clustering_modularity_sweep.py
    python analysis/clustering_modularity_sweep.py --dense           # 1000 pts
    python analysis/clustering_modularity_sweep.py --models specter2 # subset
"""
from __future__ import annotations
import argparse, os, sys, io, time
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd

from _clustering_utils import (  # noqa: E402
    PROC, MODEL_TAGS, CONDITIONS,
    load_embeddings, build_knn_graph, leiden_sweep,
)

OUT_DIR = PROC / "clustering_stability"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=list(MODEL_TAGS))
    parser.add_argument("--conditions", nargs="+", default=list(CONDITIONS))
    parser.add_argument("--dense", action="store_true",
                        help="Full 1000-resolution sweep in [0.001, 1.0] "
                             "(takes ~2 min per config)")
    parser.add_argument("--k", type=int, default=50)
    args = parser.parse_args()

    if args.dense:
        resolutions = np.linspace(0.001, 1.0, 1000)
    else:
        # Log-spaced sweep in the meaningful range for this corpus
        resolutions = np.round(
            np.concatenate([
                np.linspace(0.0005, 0.005, 20),   # very coarse → few big clusters
                np.linspace(0.005, 0.03, 40),     # publication-useful range
                np.linspace(0.03, 0.1, 20),       # fine → many small clusters
            ]), 5)
        resolutions = np.unique(resolutions)
    print(f"Resolutions to sweep: {len(resolutions)} points "
          f"[{resolutions.min():.4f} → {resolutions.max():.4f}]")

    all_rows = []
    for m in args.models:
        for c in args.conditions:
            try:
                embs = load_embeddings(m, c)
            except FileNotFoundError:
                print(f"  [SKIP {m} {c}] embedding file missing")
                continue
            print(f"\n=== {m} / {c} ({embs.shape}) ===")
            t0 = time.time()
            g = build_knn_graph(embs, k=args.k)
            print(f"  graph: {g.vcount()} / {g.ecount()} "
                  f"({time.time()-t0:.1f}s)")
            t0 = time.time()
            df = leiden_sweep(g, resolutions)
            df["model"] = m
            df["condition"] = c
            all_rows.append(df)
            print(f"  sweep: {len(df)} resolutions in {time.time()-t0:.1f}s")
            # Locate argmax
            idx = df["modularity"].idxmax()
            print(f"  argmax modularity: res={df.loc[idx, 'resolution']:.4f} "
                  f"mod={df.loc[idx, 'modularity']:.4f} "
                  f"n={int(df.loc[idx, 'n_clusters'])} "
                  f"big={int(df.loc[idx, 'n_big'])}")

    out = pd.concat(all_rows, ignore_index=True)
    out_path = OUT_DIR / ("modularity_sweep_dense.csv" if args.dense
                          else "modularity_sweep.csv")
    out.to_csv(out_path, index=False)
    print(f"\nSaved: {out_path}")

    # Picked-resolution summary
    print("\n" + "=" * 70)
    print("ARGMAX-MODULARITY RESOLUTION PER (MODEL, CONDITION)")
    print("=" * 70)
    pick = out.loc[out.groupby(["model", "condition"])["modularity"].idxmax()]
    print(pick[["model", "condition", "resolution", "modularity",
                "n_clusters", "n_big"]].to_string(index=False))
    pick.to_csv(OUT_DIR / "modularity_picked_resolution.csv", index=False)


if __name__ == "__main__":
    main()
