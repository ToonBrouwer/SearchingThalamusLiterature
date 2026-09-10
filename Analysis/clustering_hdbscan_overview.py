"""
clustering_hdbscan_overview.py — readable summary markdown for the
HDBSCAN branch. Mirror of `clustering_overview_text.py`.

Outputs:
    Paper/clustering_hdbscan_overview_flat.md
    Paper/clustering_hdbscan_overview_hierarchy.md
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
    load_papers, top_tfidf_terms,
)
from _hdbscan_utils import HDB_DIR, hdb_config_key


ROOT = PROC.parents[1]
HIER_DIR = HDB_DIR / "hierarchy"
OUT_FLAT = ROOT / "Paper" / "clustering_hdbscan_overview_flat.md"
OUT_HIER = ROOT / "Paper" / "clustering_hdbscan_overview_hierarchy.md"


def join_terms(terms, n=6):
    return ", ".join(terms[:n])


def summarize_flat(mcs: int, lines: list, stab, papers):
    lines.append(f"# HDBSCAN flat cluster overview  (min_cluster_size = {mcs})\n")
    lines.append(
        f"Per-configuration list of clusters ≥ {MIN_CLUSTER_SIZE} papers "
        f"(non-noise only). Ordered by stability within each config. "
        f"Each line: `CN (n=..., year=..., stab=...): top-6 TF-IDF terms`. "
        f"Noise fraction reported in the header.\n"
    )
    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            ckey = hdb_config_key(m, cond, mcs)
            mem_path = HDB_DIR / f"memberships_{ckey}.npy"
            if not mem_path.exists():
                continue
            mem = np.load(mem_path)
            sizes = Counter(mem)
            big = sorted([c for c, n in sizes.items()
                          if c >= 0 and n >= MIN_CLUSTER_SIZE],
                         key=lambda c: -sizes[c])
            noise_frac = float((mem == -1).mean())
            if len(big) < 3:
                lines.append(
                    f"\n## {MODEL_LABELS[m]} / {cond}   "
                    f"({len(big)} clusters, noise = {noise_frac:.1%})  "
                    f"— degenerate, TF-IDF undefined\n"
                )
                for c in big:
                    lines.append(f"- **C{int(c)}** (n={sizes[c]:,})")
                continue
            try:
                tfidf = top_tfidf_terms(papers, mem, top_k=8,
                                         min_size=MIN_CLUSTER_SIZE)
            except ValueError:
                tfidf = {}
            stab_cfg = stab[stab["config"] == ckey].set_index("cluster")

            lines.append(
                f"\n## {MODEL_LABELS[m]} / {cond}   "
                f"({len(big)} clusters, noise = {noise_frac:.1%})\n"
            )
            ordered = stab_cfg.sort_values("stability_mean",
                                            ascending=False).index.tolist()
            ordered = [c for c in ordered if c in big]
            if not ordered:
                ordered = big
            for c in ordered:
                st = stab_cfg.loc[c, "stability_mean"] \
                    if c in stab_cfg.index else float("nan")
                year = papers.loc[np.where(mem == c)[0], "year"].mean()
                lines.append(
                    f"- **C{int(c)}** (n={sizes[c]:,}, year={year:.0f}, "
                    f"stab={st:.2f}): {join_terms(tfidf.get(int(c), []))}"
                )


def summarize_hierarchy(mcs: int, k_umb: int, lines: list, stab, papers):
    lines.append(f"# HDBSCAN hierarchical overview — k={k_umb} umbrellas  "
                 f"(mcs = {mcs})\n")
    lines.append(
        f"Average-linkage agglomerative clustering on L2-normalised cluster "
        f"centroids (cosine distance). Each umbrella lists its own TF-IDF "
        f"terms AND its constituent HDBSCAN clusters.\n"
    )
    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            ckey = hdb_config_key(m, cond, mcs)
            umb_path = HIER_DIR / f"umbrella_labels_{ckey}.csv"
            c2u_path = HIER_DIR / f"cluster_to_umbrella_{ckey}.csv"
            mem_path = HDB_DIR / f"memberships_{ckey}.npy"
            if not (umb_path.exists() and c2u_path.exists() and mem_path.exists()):
                continue
            umb = pd.read_csv(umb_path)
            umb = umb[umb["n_umbrella"] == k_umb]
            if len(umb) == 0:
                continue
            c2u_df = pd.read_csv(c2u_path)
            mem = np.load(mem_path)
            sizes = Counter(mem)
            try:
                tfidf = top_tfidf_terms(papers, mem, top_k=6,
                                         min_size=MIN_CLUSTER_SIZE)
            except ValueError:
                tfidf = {}
            stab_cfg = stab[stab["config"] == ckey].set_index("cluster")

            u_col = f"umbrella_k{k_umb}"
            if u_col not in c2u_df.columns:
                continue
            children = defaultdict(list)
            for _, r in c2u_df.iterrows():
                children[int(r[u_col])].append(int(r["cluster"]))

            total = sum(len(v) for v in children.values())
            lines.append(
                f"\n## {MODEL_LABELS[m]} / {cond}   "
                f"(k={k_umb} umbrellas over {total} clusters)\n"
            )
            for _, urow in umb.sort_values("n_papers", ascending=False).iterrows():
                u_id = int(urow["umbrella"])
                lines.append(
                    f"\n### Umbrella {u_id} — {urow['top_terms']} "
                    f"(n={int(urow['n_papers']):,}, "
                    f"{len(children.get(u_id, []))} child clusters)"
                )
                kids = sorted(children.get(u_id, []),
                               key=lambda c: -sizes.get(c, 0))
                for c in kids:
                    if sizes.get(c, 0) < MIN_CLUSTER_SIZE:
                        continue
                    st = stab_cfg.loc[c, "stability_mean"] \
                        if c in stab_cfg.index else float("nan")
                    lines.append(
                        f"  - **C{int(c)}** (n={sizes[c]:,}, stab={st:.2f}): "
                        f"{join_terms(tfidf.get(int(c), []))}"
                    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-cluster-size", type=int, default=50)
    parser.add_argument("--k-umbrella", type=int, default=6)
    args = parser.parse_args()

    stab = pd.read_csv(HDB_DIR / "stability_full.csv")
    papers = load_papers()

    flat = []
    summarize_flat(args.min_cluster_size, flat, stab, papers)
    OUT_FLAT.write_text("\n".join(flat), encoding="utf-8")
    print(f"Saved {OUT_FLAT}")

    hier = []
    summarize_hierarchy(args.min_cluster_size, args.k_umbrella, hier, stab, papers)
    OUT_HIER.write_text("\n".join(hier), encoding="utf-8")
    print(f"Saved {OUT_HIER}")


if __name__ == "__main__":
    main()
