"""
clustering_hdbscan_modelselection.py — model-selection scorecard for the
HDBSCAN branch. Mirror of `clustering_model_selection.py`, with DBCV
replacing modularity.

Scores every (model × condition × min_cluster_size) config on:
    1. STABILITY   — mean per-cluster best-Jaccard across all other configs
                     (from clustering_hdbscan/stability_full.csv)
    2. SILHOUETTE  — mean silhouette on the 2-D UMAP (same coords used by
                     the Leiden pipeline; noise points excluded)
    3. DBCV        — density-based cluster validity on the 5-D UMAP
                     (from clustering_hdbscan/config_index.csv)

Each metric is normalised to [0, 1] across all scored configs; the
composite is the equally-weighted mean. A lower noise fraction is
considered "better" and is reported as a separate diagnostic (not
folded into the composite by default).

Outputs:
    Data/processed/clustering_hdbscan/model_selection_scores.csv

Usage:
    python analysis/clustering_hdbscan_modelselection.py
    python analysis/clustering_hdbscan_modelselection.py \
        --min-cluster-sizes 50 80
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
from sklearn.metrics import silhouette_score

from _clustering_utils import PROC
from _hdbscan_utils import (  # noqa: E402
    HDB_DIR, MODEL_TAGS, CONDITIONS, DEFAULT_MIN_CLUSTER_SIZES,
    hdb_config_key,
)

# Re-use the 2-D UMAP cached by the Leiden side for silhouette
LEIDEN_UMAP_DIR = PROC / "clustering_stability" / "umap"


def silhouette_on_2d(memberships: np.ndarray, model: str, condition: str,
                     sample_size: int = 5000) -> float:
    p = LEIDEN_UMAP_DIR / f"umap_{model}__{condition}.npy"
    if not p.exists():
        print(f"    [no 2-D UMAP for {model}/{condition} — "
              f"run clustering_space_viz.py first]")
        return float("nan")
    coords = np.load(p)
    rng = np.random.RandomState(42)
    # Drop noise so silhouette reflects actual cluster compactness
    valid = memberships >= 0
    if valid.sum() < 50:
        return float("nan")
    coords = coords[valid]; memberships = memberships[valid]
    if len(coords) > sample_size:
        idx = rng.choice(len(coords), size=sample_size, replace=False)
        coords = coords[idx]; memberships = memberships[idx]
    if len(np.unique(memberships)) < 2:
        return float("nan")
    try:
        return float(silhouette_score(coords, memberships))
    except Exception as e:
        return float("nan")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-cluster-sizes", nargs="+", type=int,
                        default=list(DEFAULT_MIN_CLUSTER_SIZES),
                        help="which mcs values to score")
    parser.add_argument("--metrics", nargs="+",
                        default=["stability", "silhouette", "dbcv"],
                        choices=["stability", "silhouette", "dbcv", "noise_inv"])
    parser.add_argument("--weights", nargs="+", type=float, default=None)
    args = parser.parse_args()

    if args.weights is None:
        args.weights = [1.0 / len(args.metrics)] * len(args.metrics)
    if len(args.weights) != len(args.metrics):
        raise SystemExit(f"need {len(args.metrics)} weights, got {len(args.weights)}")

    print(f"Scoring {len(MODEL_TAGS)} × {len(CONDITIONS)} × "
          f"{len(args.min_cluster_sizes)} = "
          f"{len(MODEL_TAGS)*len(CONDITIONS)*len(args.min_cluster_sizes)} configs")
    print(f"Metrics: {args.metrics}  weights: {list(np.round(args.weights, 3))}")

    stab = pd.read_csv(HDB_DIR / "stability_full.csv")
    cfg_idx = pd.read_csv(HDB_DIR / "config_index.csv")

    rows = []
    for mcs in args.min_cluster_sizes:
        for m in MODEL_TAGS:
            for c in CONDITIONS:
                ckey = hdb_config_key(m, c, mcs)
                sub = stab[stab["config"] == ckey]
                if len(sub) == 0:
                    continue
                stability = float(sub["stability_mean"].mean())

                cfg_row = cfg_idx[cfg_idx["config"] == ckey]
                if len(cfg_row) == 0:
                    continue
                dbcv = float(cfg_row["dbcv"].iloc[0]) \
                    if pd.notna(cfg_row["dbcv"].iloc[0]) else float("nan")
                noise = float(cfg_row["noise_frac"].iloc[0])
                n_big = int(cfg_row["n_big"].iloc[0])

                mem_path = HDB_DIR / f"memberships_{ckey}.npy"
                if not mem_path.exists():
                    continue
                mem = np.load(mem_path)
                sil = silhouette_on_2d(mem, m, c)

                rows.append({
                    "config": ckey, "model": m, "condition": c,
                    "min_cluster_size": int(mcs),
                    "n_big": n_big, "noise_frac": round(noise, 4),
                    "stability": stability, "silhouette": sil,
                    "dbcv": dbcv,
                    "noise_inv": 1.0 - noise,
                })
                print(f"  {ckey}: stab={stability:.3f} sil={sil:.3f} "
                      f"dbcv={dbcv:.3f} noise={noise:.2%} n_big={n_big}")

    df = pd.DataFrame(rows)
    if len(df) == 0:
        raise SystemExit("No rows to score.")

    for col in ["stability", "silhouette", "dbcv", "noise_inv"]:
        if col not in df.columns:
            continue
        lo, hi = df[col].min(), df[col].max()
        df[f"{col}_norm"] = (df[col] - lo) / (hi - lo) if hi > lo else 0.5

    w = np.array(args.weights)
    w = w / w.sum()
    df["composite"] = sum(w[i] * df[f"{m}_norm"]
                          for i, m in enumerate(args.metrics))
    df["metrics_used"] = "+".join(args.metrics)
    df = df.sort_values("composite", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1

    out = HDB_DIR / "model_selection_scores.csv"
    df.to_csv(out, index=False)

    print("\n" + "=" * 96)
    print(f"RANKED RESULTS (metrics = {args.metrics}, weights = {list(w.round(3))})")
    print("=" * 96)
    display_cols = ["rank", "config", "n_big", "noise_frac",
                    "stability", "silhouette", "dbcv", "composite"]
    print(df[display_cols].to_string(index=False, float_format="%.3f"))

    print("\nBest config per min_cluster_size:")
    for mcs in sorted(df["min_cluster_size"].unique()):
        best = df[df["min_cluster_size"] == mcs].iloc[0]
        print(f"  mcs={mcs:>4}: {best['config']}  "
              f"composite={best['composite']:.3f}")

    print("\nBest config per model:")
    for m in MODEL_TAGS:
        sub = df[df["model"] == m]
        if len(sub) == 0:
            continue
        best = sub.iloc[0]
        print(f"  {m:10s}: {best['config']}  composite={best['composite']:.3f}")

    print(f"\nOverall winner: {df.iloc[0]['config']} "
          f"(composite={df.iloc[0]['composite']:.3f})")
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
