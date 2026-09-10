"""
clustering_hdbscan_sweep.py — DBCV vs min_cluster_size sweep per
(model, condition). Mirror of `clustering_modularity_sweep.py` on the
Leiden side; DBCV replaces modularity as the internal-quality score.

Outputs
-------
    Data/processed/clustering_hdbscan/hdbscan_sweep.csv
        (model, condition, min_cluster_size, n_clusters, n_big, n_noise,
         noise_frac, dbcv)
    Data/processed/clustering_hdbscan/hdbscan_picked_min_cluster_size.csv
        argmax-DBCV pick per (model, condition)

Usage
-----
    python analysis/clustering_hdbscan_sweep.py
    python analysis/clustering_hdbscan_sweep.py \
        --min-cluster-sizes 20 30 50 80 120 200 300
"""
from __future__ import annotations
import argparse, os, sys, io, time
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from collections import Counter
import numpy as np
import pandas as pd

from _hdbscan_utils import (  # noqa: E402
    HDB_DIR, MODEL_TAGS, CONDITIONS,
    load_or_make_umap5d, run_hdbscan, dbcv_score, big_clusters, noise_fraction,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=list(MODEL_TAGS))
    parser.add_argument("--conditions", nargs="+", default=list(CONDITIONS))
    parser.add_argument("--min-cluster-sizes", nargs="+", type=int,
                        default=[20, 30, 50, 80, 120, 200, 300, 500])
    args = parser.parse_args()

    rows = []
    for m in args.models:
        for c in args.conditions:
            try:
                coords = load_or_make_umap5d(m, c)
            except FileNotFoundError:
                continue
            print(f"\n=== {m}/{c}  (5-D UMAP {coords.shape}) ===")
            for mcs in args.min_cluster_sizes:
                t0 = time.time()
                mem = run_hdbscan(coords, min_cluster_size=mcs)
                sizes = Counter(mem)
                n_clusters = len(sizes) - (1 if -1 in sizes else 0)
                dbcv = dbcv_score(coords, mem)
                row = {
                    "model": m, "condition": c,
                    "min_cluster_size": int(mcs),
                    "n_clusters": int(n_clusters),
                    "n_big": int(len(big_clusters(mem))),
                    "n_noise": int(sizes.get(-1, 0)),
                    "noise_frac": round(noise_fraction(mem), 4),
                    "dbcv": float(dbcv) if not np.isnan(dbcv) else None,
                }
                rows.append(row)
                print(f"  mcs={mcs:>4}: n={n_clusters}, big={row['n_big']}, "
                      f"noise={row['noise_frac']:.2%}, dbcv={dbcv:.3f} "
                      f"({time.time()-t0:.1f}s)", flush=True)

    df = pd.DataFrame(rows)
    df.to_csv(HDB_DIR / "hdbscan_sweep.csv", index=False)

    # Pick argmax-DBCV per (model, condition) among configs that produce ≥ 5 big clusters
    viable = df[df["n_big"] >= 5].copy()
    if len(viable) == 0:
        print("No config produced ≥5 big clusters — falling back to all rows")
        viable = df.copy()
    pick = viable.loc[viable.groupby(["model", "condition"])["dbcv"].idxmax()]
    pick.to_csv(HDB_DIR / "hdbscan_picked_min_cluster_size.csv", index=False)
    print("\nARGMAX-DBCV PICK PER (MODEL, CONDITION):")
    print(pick[["model", "condition", "min_cluster_size", "n_big",
                 "noise_frac", "dbcv"]].to_string(index=False))


if __name__ == "__main__":
    main()
