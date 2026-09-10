"""
clustering_stability_full.py — Unified 24-config Leiden clustering + stability.

Clusters the corpus under every combination of:
    3 models    × 2 conditions      × 4 resolutions
    {SPECTER2,PubMedBERT,MPNet} × {raw-768d,refined-64d} × {0.005,0.008,0.012,0.020}
  = 24 configurations.

For each config, runs Leiden-CPM on the FAISS k=50 k-NN graph, then computes
best-Jaccard stability for each cluster against every *other* config. Also
records per-config modularity, cluster counts, and TF-IDF terms / domain flags.

Outputs:
    Data/processed/clustering_stability/
        memberships_{config}.npy         (n_papers,)  — one per config
        cluster_summary_{config}.csv     per-cluster n / year / TF-IDF / domains
        stability_full.csv               all 24 configs × clusters × stability
        stability_matrix.csv             config × config mean-Jaccard matrix
        config_index.csv                 (config, model, condition, res, n_clusters, mod)

Usage:
    python analysis/clustering_stability_full.py
    python analysis/clustering_stability_full.py --resolutions 0.005 0.010 0.020
"""
from __future__ import annotations
import argparse, os, sys, io, time
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
from collections import Counter

import numpy as np
import pandas as pd

from _clustering_utils import (  # noqa: E402
    PROC, MODEL_TAGS, CONDITIONS, DEFAULT_RESOLUTIONS, MIN_CLUSTER_SIZE,
    DOMAIN_RE,
    load_embeddings, load_papers,
    build_knn_graph, run_leiden,
    best_jaccard, compute_domain_flags, top_tfidf_terms,
    config_key,
)


OUT_DIR = PROC / "clustering_stability"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=list(MODEL_TAGS),
                        choices=list(MODEL_TAGS))
    parser.add_argument("--conditions", nargs="+", default=list(CONDITIONS),
                        choices=list(CONDITIONS))
    parser.add_argument("--resolutions", nargs="+", type=float,
                        default=list(DEFAULT_RESOLUTIONS))
    parser.add_argument("--k", type=int, default=50)
    args = parser.parse_args()

    configs = [(m, c, r) for m in args.models for c in args.conditions
               for r in args.resolutions]
    print(f"Running {len(configs)} configs: "
          f"{len(args.models)} models × {len(args.conditions)} conditions "
          f"× {len(args.resolutions)} resolutions")

    papers = load_papers()
    papers = compute_domain_flags(papers)
    n_papers = len(papers)

    # ── Build k-NN graph once per (model, condition); reuse across resolutions
    print("\nBuilding k-NN graphs (one per model × condition)...")
    graphs = {}
    for m in args.models:
        for c in args.conditions:
            key = (m, c)
            try:
                embs = load_embeddings(m, c)
            except FileNotFoundError:
                print(f"  [SKIP {m} {c}] embedding file missing")
                continue
            t0 = time.time()
            graphs[key] = build_knn_graph(embs, k=args.k)
            print(f"  ({m}, {c}) {embs.shape} → graph "
                  f"{graphs[key].vcount()} nodes / {graphs[key].ecount()} edges "
                  f"({time.time()-t0:.1f}s)")

    # ── Leiden at each resolution × config
    print("\nRunning Leiden across configs...")
    memberships: dict[str, np.ndarray] = {}
    config_rows = []

    for m, c, r in configs:
        if (m, c) not in graphs:
            continue
        ckey = config_key(m, c, r)
        g = graphs[(m, c)]
        t0 = time.time()
        part = run_leiden(g, resolution=r)
        mem = np.array(part.membership)
        memberships[ckey] = mem
        sizes = Counter(mem)
        n_clusters = len(sizes)
        n_big = sum(1 for s in sizes.values() if s >= MIN_CLUSTER_SIZE)
        np.save(OUT_DIR / f"memberships_{ckey}.npy", mem)
        config_rows.append({
            "config": ckey, "model": m, "condition": c, "resolution": r,
            "n_clusters": int(n_clusters), "n_big": int(n_big),
            "modularity": float(part.modularity),
            "elapsed_s": round(time.time() - t0, 2),
        })
        print(f"  {ckey}: mod={part.modularity:.4f}, "
              f"n={n_clusters} ({n_big} ≥{MIN_CLUSTER_SIZE}), "
              f"{time.time()-t0:.1f}s")

    config_df = pd.DataFrame(config_rows)
    config_df.to_csv(OUT_DIR / "config_index.csv", index=False)

    # ── Pairwise Jaccard stability (cluster-level)
    print("\nComputing cluster stability (per-cluster best-Jaccard vs all others)...")
    rows = []
    tfidf_cache = {}
    config_keys = list(memberships.keys())
    for ckey in config_keys:
        mem = memberships[ckey]
        sizes = Counter(mem)
        big = sorted([c for c, n in sizes.items() if n >= MIN_CLUSTER_SIZE],
                     key=lambda c: -sizes[c])
        if ckey not in tfidf_cache:
            tfidf_cache[ckey] = top_tfidf_terms(papers, mem, top_k=6)

        for clu in big:
            members_set = set(np.where(mem == clu)[0].tolist())
            jaccs = {}
            for other in config_keys:
                if other == ckey:
                    continue
                jaccs[other] = best_jaccard(members_set, memberships[other])
            sub = papers.iloc[list(members_set)]
            dom_fracs = {d: float(sub[f"dom_{d}"].mean()) for d in DOMAIN_RE}

            row = {
                "config": ckey, "cluster": int(clu),
                "n_papers": int(sizes[clu]),
                "year_mean": float(sub["year"].mean()),
                "year_std": float(sub["year"].std()),
                "top_terms": " | ".join(tfidf_cache[ckey].get(int(clu), [])),
                "stability_mean": float(np.mean(list(jaccs.values())))
                    if jaccs else float("nan"),
                "stability_min": float(np.min(list(jaccs.values())))
                    if jaccs else float("nan"),
                "stability_max": float(np.max(list(jaccs.values())))
                    if jaccs else float("nan"),
            }
            # Domain fractions (percentage)
            row.update({f"pct_{d}": round(v * 100, 2) for d, v in dom_fracs.items()})
            # Mean jaccard against same-model / same-condition cohorts
            same_model = [k for k in jaccs
                         if k.startswith(row["config"].split("__")[0])]
            same_cond = [k for k in jaccs
                        if k.split("__")[1] == ckey.split("__")[1]]
            row["stab_vs_same_model"] = (
                float(np.mean([jaccs[k] for k in same_model])) if same_model else np.nan)
            row["stab_vs_same_condition"] = (
                float(np.mean([jaccs[k] for k in same_cond])) if same_cond else np.nan)
            rows.append(row)

    stab_df = pd.DataFrame(rows)
    stab_df.to_csv(OUT_DIR / "stability_full.csv", index=False)
    print(f"  Saved stability_full.csv ({len(stab_df)} cluster rows)")

    # ── Config × config mean-Jaccard matrix (aggregate stability)
    print("\nComputing config × config mean-stability matrix...")
    mat = np.zeros((len(config_keys), len(config_keys)))
    for i, a in enumerate(config_keys):
        for j, b in enumerate(config_keys):
            if a == b:
                mat[i, j] = 1.0
                continue
            mem_a = memberships[a]
            sizes_a = Counter(mem_a)
            big_a = [c for c, n in sizes_a.items() if n >= MIN_CLUSTER_SIZE]
            jaccs = [best_jaccard(set(np.where(mem_a == c)[0].tolist()),
                                   memberships[b]) for c in big_a]
            mat[i, j] = float(np.mean(jaccs)) if jaccs else 0.0
    mat_df = pd.DataFrame(mat, index=config_keys, columns=config_keys)
    mat_df.to_csv(OUT_DIR / "stability_matrix.csv")
    print(f"  Saved stability_matrix.csv ({len(config_keys)}×{len(config_keys)})")

    # ── Summary printout
    print("\n" + "=" * 70)
    print("PER-CONFIG SUMMARY")
    print("=" * 70)
    summary = (stab_df.groupby("config")["stability_mean"]
               .agg(["mean", "median", "count"])
               .sort_values("mean", ascending=False))
    print(summary.to_string())
    summary.to_csv(OUT_DIR / "config_stability_summary.csv")
    print(f"\nAll outputs in {OUT_DIR}")


if __name__ == "__main__":
    main()
