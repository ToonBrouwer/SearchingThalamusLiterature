"""
clustering_hdbscan_full.py — HDBSCAN parallel to `clustering_stability_full.py`.

For every combination of:
    3 models × 2 conditions × 5 min_cluster_sizes  = 30 configs
we:
    1) UMAP-reduce the input embedding to 5-D (cached in clustering_hdbscan/umap_5d/)
    2) Run HDBSCAN
    3) Record memberships, DBCV, noise fraction, #clusters
    4) Compute per-cluster best-Jaccard stability vs every other config
    5) Compute the config × config aggregate-stability matrix

All outputs live under `Data/processed/clustering_hdbscan/`.

Usage
-----
    python analysis/clustering_hdbscan_full.py
    python analysis/clustering_hdbscan_full.py \
        --min-cluster-sizes 50 100 200 --models mpnet
"""
from __future__ import annotations
import argparse, os, sys, io, time
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from _clustering_utils import (  # noqa: E402
    MIN_CLUSTER_SIZE, DOMAIN_RE, load_papers, compute_domain_flags,
    top_tfidf_terms, best_jaccard,
)
from _hdbscan_utils import (  # noqa: E402
    HDB_DIR, MODEL_TAGS, CONDITIONS, DEFAULT_MIN_CLUSTER_SIZES,
    load_or_make_umap5d, run_hdbscan, dbcv_score, hdb_config_key,
    big_clusters, noise_fraction,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=list(MODEL_TAGS))
    parser.add_argument("--conditions", nargs="+", default=list(CONDITIONS))
    parser.add_argument("--min-cluster-sizes", nargs="+", type=int,
                        default=list(DEFAULT_MIN_CLUSTER_SIZES))
    args = parser.parse_args()

    configs = [(m, c, mcs) for m in args.models for c in args.conditions
               for mcs in args.min_cluster_sizes]
    print(f"Running {len(configs)} HDBSCAN configs "
          f"({len(args.models)} × {len(args.conditions)} × "
          f"{len(args.min_cluster_sizes)})")

    papers = compute_domain_flags(load_papers())

    # ── Pre-compute / load 5-D UMAP per (model, condition)
    umap_cache = {}
    for m in args.models:
        for c in args.conditions:
            try:
                umap_cache[(m, c)] = load_or_make_umap5d(m, c)
            except FileNotFoundError:
                print(f"  [SKIP {m}/{c}] raw embedding missing")

    # ── Run HDBSCAN for each config
    memberships: dict[str, np.ndarray] = {}
    cfg_rows = []
    for m, c, mcs in configs:
        if (m, c) not in umap_cache:
            continue
        ckey = hdb_config_key(m, c, mcs)
        t0 = time.time()
        mem = run_hdbscan(umap_cache[(m, c)], min_cluster_size=mcs)
        memberships[ckey] = mem
        np.save(HDB_DIR / f"memberships_{ckey}.npy", mem)

        big = big_clusters(mem)
        sizes = Counter(mem)
        n_clusters = len(sizes) - (1 if -1 in sizes else 0)
        dbcv = dbcv_score(umap_cache[(m, c)], mem)
        cfg_rows.append({
            "config": ckey, "model": m, "condition": c,
            "min_cluster_size": int(mcs),
            "n_clusters": int(n_clusters),
            "n_big": int(len(big)),
            "n_noise": int(sizes.get(-1, 0)),
            "noise_frac": round(noise_fraction(mem), 4),
            "dbcv": float(dbcv) if not np.isnan(dbcv) else None,
            "elapsed_s": round(time.time() - t0, 2),
        })
        print(f"  {ckey}: n={n_clusters} ({len(big)} big, "
              f"noise={noise_fraction(mem):.2%}, dbcv={dbcv:.3f} "
              f"in {time.time()-t0:.1f}s)", flush=True)

    cfg_df = pd.DataFrame(cfg_rows)
    cfg_df.to_csv(HDB_DIR / "config_index.csv", index=False)

    # ── Per-cluster stability vs every other config
    print("\nComputing per-cluster stability (vs every other config)…", flush=True)
    cfg_keys = list(memberships.keys())
    tfidf_cache = {}
    rows = []
    for ckey in cfg_keys:
        mem = memberships[ckey]
        big = big_clusters(mem)
        if ckey not in tfidf_cache:
            # TF-IDF is undefined with < 3 big clusters (max_df vs min_df); skip
            if len(big) >= 3:
                mask_tfidf = mem.copy()
                mask_tfidf[mask_tfidf == -1] = -999
                try:
                    tfidf_cache[ckey] = top_tfidf_terms(
                        papers, mask_tfidf, top_k=6)
                except ValueError as e:
                    print(f"  [TF-IDF fallback {ckey}]: {e}")
                    tfidf_cache[ckey] = {}
            else:
                tfidf_cache[ckey] = {}
        for c in big:
            members_set = set(np.where(mem == c)[0].tolist())
            sub = papers.iloc[list(members_set)]
            jaccs = {}
            for ok in cfg_keys:
                if ok == ckey:
                    continue
                jaccs[ok] = best_jaccard(members_set, memberships[ok])
            dom_fracs = {d: float(sub[f"dom_{d}"].mean()) for d in DOMAIN_RE}
            rows.append({
                "config": ckey, "cluster": int(c),
                "n_papers": int(len(members_set)),
                "year_mean": round(float(sub["year"].mean()), 1),
                "year_std": round(float(sub["year"].std()), 1),
                "top_terms": " | ".join(tfidf_cache[ckey].get(int(c), [])),
                "stability_mean": float(np.mean(list(jaccs.values())))
                    if jaccs else float("nan"),
                "stability_min":  float(np.min(list(jaccs.values())))
                    if jaccs else float("nan"),
                "stability_max":  float(np.max(list(jaccs.values())))
                    if jaccs else float("nan"),
                **{f"pct_{d}": round(v * 100, 2) for d, v in dom_fracs.items()},
            })
    stab_df = pd.DataFrame(rows)
    stab_df.to_csv(HDB_DIR / "stability_full.csv", index=False)
    print(f"  saved stability_full.csv ({len(stab_df)} rows)")

    # ── Config × config aggregate matrix
    print("Computing config × config mean-Jaccard matrix…", flush=True)
    n = len(cfg_keys)
    mat = np.zeros((n, n))
    for i, a in enumerate(cfg_keys):
        for j, b in enumerate(cfg_keys):
            if a == b:
                mat[i, j] = 1.0
                continue
            big_a = big_clusters(memberships[a])
            jaccs = [best_jaccard(set(np.where(memberships[a] == c)[0].tolist()),
                                   memberships[b]) for c in big_a]
            mat[i, j] = float(np.mean(jaccs)) if jaccs else 0.0
    mat_df = pd.DataFrame(mat, index=cfg_keys, columns=cfg_keys)
    mat_df.to_csv(HDB_DIR / "stability_matrix.csv")

    # ── Summary
    print("\nPER-CONFIG SUMMARY (ordered by mean stability)")
    summary = (stab_df.groupby("config")["stability_mean"]
               .agg(["mean", "median", "count"])
               .rename(columns={"mean": "stab_mean", "median": "stab_median",
                                 "count": "n_big"})
               .sort_values("stab_mean", ascending=False))
    summary = summary.join(
        cfg_df.set_index("config")[["dbcv", "noise_frac", "n_clusters"]])
    print(summary.to_string())
    summary.to_csv(HDB_DIR / "config_stability_summary.csv")

    print(f"\nAll outputs in {HDB_DIR}")


if __name__ == "__main__":
    main()
