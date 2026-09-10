"""
clustering_overview_text.py — Human-readable summary of every method's
clusters + the umbrellas they roll up into.

Produces two Markdown files:
    Paper/clustering_overview_flat.md
        Per (model, condition) config: every cluster ≥ MIN_CLUSTER_SIZE with
        its size, year, top TF-IDF terms, and stability.

    Paper/clustering_overview_hierarchy.md
        Per config: each umbrella (at k=6) with its own TF-IDF label AND the
        child clusters nested inside (their labels, sizes, stability). So
        you can read "Umbrella 2 = clinical psychiatry (MDD, OCD, schizo,
        autism) combining cluster 9 [MDD/OCD, n=855] and cluster 21 [MS
        patients, n=312]".

Both files also print to stdout so you can copy-paste quickly.

Usage:
    python analysis/clustering_overview_text.py
    python analysis/clustering_overview_text.py --resolution 0.008 --k-umbrella 6
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
from collections import Counter, defaultdict

import numpy as np
import pandas as pd

from _clustering_utils import (  # noqa: E402
    PROC, MODEL_TAGS, MODEL_LABELS, CONDITIONS, MIN_CLUSTER_SIZE,
    load_papers, compute_domain_flags, top_tfidf_terms, config_key,
)

ROOT = PROC.parents[1]
STAB_DIR = PROC / "clustering_stability"
HIER_DIR = STAB_DIR / "hierarchy"
OUT_FLAT = ROOT / "Paper" / "clustering_overview_flat.md"
OUT_HIER = ROOT / "Paper" / "clustering_overview_hierarchy.md"


def join_terms(terms, n=6):
    """Join top-n TF-IDF terms with commas."""
    return ", ".join(terms[:n])


def summarize_flat(resolution: float, out_lines: list, stab: pd.DataFrame,
                   papers: pd.DataFrame):
    rtag = f"r{int(resolution*1000):03d}"
    out_lines.append(f"# Flat cluster overview (resolution = {resolution})\n")
    out_lines.append(
        "Per-configuration list of clusters ≥ "
        f"{MIN_CLUSTER_SIZE} papers. Each line: "
        "`Cluster N (n=..., stab=...): top-TF-IDF terms`.\n"
    )

    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            ckey = config_key(m, cond, resolution)
            mem_path = STAB_DIR / f"memberships_{ckey}.npy"
            if not mem_path.exists():
                continue
            mem = np.load(mem_path)
            sizes = Counter(mem)
            big = sorted([c for c, n in sizes.items() if n >= MIN_CLUSTER_SIZE],
                         key=lambda c: -sizes[c])

            tfidf = top_tfidf_terms(papers, mem, top_k=8,
                                     min_size=MIN_CLUSTER_SIZE)
            stab_cfg = stab[stab["config"] == ckey].set_index("cluster")

            out_lines.append(
                f"\n## {MODEL_LABELS[m]} / {cond} "
                f"({len(big)} clusters, n={len(mem):,} papers)\n"
            )
            # Sort by stability if available, else by size
            if ckey in stab["config"].values:
                ordered = stab_cfg.sort_values("stability_mean",
                                                ascending=False).index.tolist()
                ordered = [c for c in ordered if c in big]
            else:
                ordered = big

            for c in ordered:
                n = sizes[c]
                st = stab_cfg.loc[c, "stability_mean"] \
                    if c in stab_cfg.index else float("nan")
                year = papers.loc[np.where(mem == c)[0], "year"].mean()
                terms = join_terms(tfidf.get(int(c), []))
                out_lines.append(
                    f"- **C{int(c)}** (n={n:,}, year={year:.0f}, "
                    f"stab={st:.2f}): {terms}"
                )


def summarize_hierarchy(resolution: float, k_umb: int, out_lines: list,
                         stab: pd.DataFrame, papers: pd.DataFrame):
    rtag = f"r{int(resolution*1000):03d}"
    out_lines.append(f"# Hierarchical (umbrella) overview — k={k_umb} cuts\n")
    out_lines.append(
        f"Each umbrella was formed by agglomerative average-linkage "
        f"clustering of Leiden cluster centroids (cosine distance) at "
        f"resolution {resolution}. Under each umbrella we list its child "
        f"Leiden clusters — so you can read the umbrella's own TF-IDF label "
        f"together with what research topics actually roll up into it.\n"
    )

    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            ckey = config_key(m, cond, resolution)
            umb_path = HIER_DIR / f"umbrella_labels_{ckey}.csv"
            c2u_path = HIER_DIR / f"cluster_to_umbrella_{ckey}.csv"
            mem_path = STAB_DIR / f"memberships_{ckey}.npy"
            if not (umb_path.exists() and c2u_path.exists()
                    and mem_path.exists()):
                continue
            umb = pd.read_csv(umb_path)
            umb = umb[umb["n_umbrella"] == k_umb]
            if len(umb) == 0:
                continue
            c2u_df = pd.read_csv(c2u_path)
            mem = np.load(mem_path)
            sizes = Counter(mem)

            # Per-cluster TF-IDF
            tfidf = top_tfidf_terms(papers, mem, top_k=6,
                                     min_size=MIN_CLUSTER_SIZE)
            stab_cfg = stab[stab["config"] == ckey].set_index("cluster")

            # Build umbrella → [child cluster ids]
            u_col = f"umbrella_k{k_umb}"
            if u_col not in c2u_df.columns:
                continue
            children = defaultdict(list)
            for _, r in c2u_df.iterrows():
                children[int(r[u_col])].append(int(r["cluster"]))

            total_children = sum(len(v) for v in children.values())
            out_lines.append(
                f"\n## {MODEL_LABELS[m]} / {cond} "
                f"(k={k_umb} umbrellas over {total_children} clusters)\n"
            )

            # Sort umbrellas by total paper count (largest first)
            umb_sorted = umb.sort_values("n_papers", ascending=False)
            for _, urow in umb_sorted.iterrows():
                u_id = int(urow["umbrella"])
                u_label = urow["top_terms"]
                u_n = int(urow["n_papers"])
                kids = children.get(u_id, [])
                out_lines.append(
                    f"\n### Umbrella {u_id} — {u_label} "
                    f"(n={u_n:,}, {len(kids)} child clusters)"
                )
                # Sort kids by size
                kids = sorted(kids, key=lambda c: -sizes.get(c, 0))
                for c in kids:
                    n = sizes.get(c, 0)
                    if n < MIN_CLUSTER_SIZE:
                        continue
                    st = stab_cfg.loc[c, "stability_mean"] \
                        if c in stab_cfg.index else float("nan")
                    terms = join_terms(tfidf.get(int(c), []))
                    out_lines.append(
                        f"  - **C{int(c)}** (n={n:,}, stab={st:.2f}): {terms}"
                    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolution", type=float, default=0.008)
    parser.add_argument("--k-umbrella", type=int, default=6)
    args = parser.parse_args()

    print(f"Reading stability_full.csv + computing TF-IDF...")
    stab = pd.read_csv(STAB_DIR / "stability_full.csv")
    papers = load_papers()

    # ── Flat overview
    flat_lines: list[str] = []
    summarize_flat(args.resolution, flat_lines, stab, papers)
    OUT_FLAT.write_text("\n".join(flat_lines), encoding="utf-8")
    print(f"Saved {OUT_FLAT}")

    # ── Hierarchical overview
    hier_lines: list[str] = []
    summarize_hierarchy(args.resolution, args.k_umbrella, hier_lines,
                         stab, papers)
    OUT_HIER.write_text("\n".join(hier_lines), encoding="utf-8")
    print(f"Saved {OUT_HIER}")

    # ── Also echo to stdout (just the flat one — hierarchy is long)
    print("\n" + "=" * 80)
    print("FLAT OVERVIEW (also saved to", OUT_FLAT.name + ")")
    print("=" * 80)
    print("\n".join(flat_lines))


if __name__ == "__main__":
    main()
