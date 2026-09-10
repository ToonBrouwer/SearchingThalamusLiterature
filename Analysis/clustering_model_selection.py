"""
clustering_model_selection.py — Score every (model × condition × resolution)
configuration on a small panel of metrics and pick the canonical one.

Metrics per config:
  1. STABILITY   — mean per-cluster best-Jaccard against every other config
                   (all 23 others, regardless of resolution). From
                   `stability_full.csv` produced by clustering_stability_full.py.
  2. SILHOUETTE  — mean silhouette (on the 2-D UMAP projection, 5,000-paper
                   subsample). Global measure of how well-separated the
                   clusters are.
  3. DOMAIN-COV  — fraction of known thalamic subfields (14 regexes) that
                   have a "dominant-cluster" (≥50% of one cluster is that
                   domain). Optional; off by default because it biases toward
                   models that happen to align with our hand-picked regexes.
  4. MODULARITY  — Leiden modularity.

Each selected metric is normalized to [0, 1] across the scored configs; the
composite is the mean (equal-weighted by default).

Outputs:
    Data/processed/clustering_stability/model_selection_scores.csv
    (one row per config, ranked by composite)

Usage:
    # default: all 3 models × 2 conditions × 4 resolutions, no domain_cov
    python analysis/clustering_model_selection.py

    # single resolution (back-compat)
    python analysis/clustering_model_selection.py --resolutions 0.008

    # custom metric panel
    python analysis/clustering_model_selection.py --metrics stability modularity
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.metrics import silhouette_score

from _clustering_utils import (  # noqa: E402
    PROC, MODEL_TAGS, CONDITIONS, DOMAIN_RE, MIN_CLUSTER_SIZE,
    DEFAULT_RESOLUTIONS,
    load_embeddings, load_papers, compute_domain_flags, config_key,
)

STAB_DIR = PROC / "clustering_stability"
UMAP_DIR = STAB_DIR / "umap"


def silhouette_on_umap(memberships: np.ndarray, model: str, condition: str,
                        sample_size: int = 5000) -> float:
    """Silhouette on 2-D UMAP (faster + more interpretable for viz)."""
    coord_path = UMAP_DIR / f"umap_{model}__{condition}.npy"
    if not coord_path.exists():
        print(f"    [no UMAP for {model} {condition} — run clustering_space_viz.py]")
        return float("nan")
    coords = np.load(coord_path)
    # Subsample to speed up
    rng = np.random.RandomState(42)
    if len(coords) > sample_size:
        idx = rng.choice(len(coords), size=sample_size, replace=False)
        coords_s = coords[idx]
        mem_s = memberships[idx]
    else:
        coords_s = coords
        mem_s = memberships
    # Silhouette requires ≥2 clusters, each with ≥2 members
    unique = np.unique(mem_s)
    if len(unique) < 2:
        return float("nan")
    try:
        return float(silhouette_score(coords_s, mem_s))
    except Exception as e:
        print(f"    silhouette failed: {e}")
        return float("nan")


def domain_coverage(memberships: np.ndarray, papers_flagged: pd.DataFrame,
                     dominance_thresh: float = 0.5) -> tuple[float, dict]:
    """Fraction of 14 domains that have a dominant-cluster.

    A cluster is 'dominant' for domain D if ≥dominance_thresh of its papers
    match D's regex. Returns (fraction, per-domain flag dict).
    """
    from collections import Counter
    covered = {}
    sizes = Counter(memberships)
    big = [c for c, n in sizes.items() if n >= MIN_CLUSTER_SIZE]
    for dom in DOMAIN_RE:
        has_dom = False
        for c in big:
            mask = memberships == c
            frac = papers_flagged.loc[mask, f"dom_{dom}"].mean()
            if frac >= dominance_thresh:
                has_dom = True
                break
        covered[dom] = has_dom
    return float(np.mean(list(covered.values()))), covered


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolutions", nargs="+", type=float,
                        default=list(DEFAULT_RESOLUTIONS),
                        help="resolutions to score (default: all 4 from stability_full)")
    parser.add_argument("--metrics", nargs="+",
                        default=["stability", "silhouette", "modularity"],
                        choices=["stability", "silhouette", "domain_cov", "modularity"],
                        help="which metrics to include in the composite score")
    parser.add_argument("--weights", nargs="+", type=float, default=None,
                        help="weights for the listed --metrics (default: equal)")
    args = parser.parse_args()

    if args.weights is None:
        args.weights = [1.0 / len(args.metrics)] * len(args.metrics)
    if len(args.weights) != len(args.metrics):
        raise SystemExit(f"need {len(args.metrics)} weights, got {len(args.weights)}")

    print(f"Scoring {len(MODEL_TAGS)} models × {len(CONDITIONS)} conditions "
          f"× {len(args.resolutions)} resolutions = "
          f"{len(MODEL_TAGS)*len(CONDITIONS)*len(args.resolutions)} configs")
    print(f"Metrics: {args.metrics}  weights: {list(np.round(args.weights, 3))}")

    stab_path = STAB_DIR / "stability_full.csv"
    cfg_path = STAB_DIR / "config_index.csv"
    if not stab_path.exists() or not cfg_path.exists():
        raise SystemExit(
            f"Missing {stab_path} or {cfg_path} — run clustering_stability_full.py first."
        )
    stab = pd.read_csv(stab_path)
    cfg_idx = pd.read_csv(cfg_path)
    papers = compute_domain_flags(load_papers())

    rows = []
    for res in args.resolutions:
        for m in MODEL_TAGS:
            for c in CONDITIONS:
                ckey = config_key(m, c, res)
                sub = stab[stab["config"] == ckey]
                if len(sub) == 0:
                    print(f"  [SKIP {ckey}] no stability rows found")
                    continue
                stability = float(sub["stability_mean"].mean())

                cfg_row = cfg_idx[cfg_idx["config"] == ckey]
                mod = float(cfg_row["modularity"].iloc[0]) \
                    if len(cfg_row) else float("nan")

                mem_path = STAB_DIR / f"memberships_{ckey}.npy"
                if not mem_path.exists():
                    print(f"  [SKIP {ckey}] no memberships file")
                    continue
                mem = np.load(mem_path)

                sil = silhouette_on_umap(mem, m, c)
                dom_cov, dom_detail = domain_coverage(mem, papers)

                rows.append({
                    "config": ckey, "model": m, "condition": c,
                    "resolution": float(res),
                    "n_clusters": int(len(np.unique(mem))),
                    "n_big": int(sub["n_papers"].count()),  # already size-filtered
                    "stability": stability, "silhouette": sil,
                    "domain_cov": dom_cov, "modularity": mod,
                    **{f"dom_{d}": bool(v) for d, v in dom_detail.items()},
                })
                print(f"  {ckey}: stab={stability:.3f} sil={sil:.3f} "
                      f"dom={dom_cov:.3f} mod={mod:.4f} n_big={rows[-1]['n_big']}")

    df = pd.DataFrame(rows)
    if len(df) == 0:
        raise SystemExit("No rows to score.")

    # Normalize every metric globally across all scored configs
    for col in ["stability", "silhouette", "domain_cov", "modularity"]:
        lo, hi = df[col].min(), df[col].max()
        df[f"{col}_norm"] = (df[col] - lo) / (hi - lo) if hi > lo else 0.5

    w = np.array(args.weights)
    w = w / w.sum()
    df["composite"] = sum(w[i] * df[f"{m}_norm"]
                          for i, m in enumerate(args.metrics))
    df["metrics_used"] = "+".join(args.metrics)
    df = df.sort_values("composite", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1

    out_path = STAB_DIR / "model_selection_scores.csv"
    df.to_csv(out_path, index=False)

    # ── Print ranked table + per-resolution winner + per-model winner
    print("\n" + "=" * 88)
    print(f"RANKED RESULTS (metrics = {args.metrics}, weights = {list(w.round(3))})")
    print("=" * 88)
    display_cols = ["rank", "config", "n_big", "stability", "silhouette",
                    "domain_cov", "modularity", "composite"]
    print(df[display_cols].to_string(index=False, float_format="%.3f"))

    print("\nBest config per resolution:")
    for res in sorted(df["resolution"].unique()):
        best = df[df["resolution"] == res].iloc[0]
        print(f"  res={res:.3f}: {best['config']}  composite={best['composite']:.3f}")

    print("\nBest config per model:")
    for m in MODEL_TAGS:
        sub = df[df["model"] == m]
        if len(sub) == 0:
            continue
        best = sub.iloc[0]
        print(f"  {m:10s}: {best['config']}  composite={best['composite']:.3f}")

    print(f"\nOverall winner: {df.iloc[0]['config']} "
          f"(composite={df.iloc[0]['composite']:.3f})")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
