"""
clustering_hdbscan_figures.py — figures for the HDBSCAN branch, parallel
to `clustering_figures.py`. All artefacts go to `Graphs/pub/` with the
`fig_hdbscan_*` prefix so the Leiden figures remain untouched.

Figures produced:
    fig_hdbscan_cluster_stability      Config × config matrix + per-cluster boxplots
    fig_hdbscan_dbcv_sweep             DBCV + n_big vs min_cluster_size
    fig_hdbscan_cluster_space_umap     2-D UMAP coloured by HDBSCAN cluster
                                        (noise points greyed out)
    fig_hdbscan_cluster_hierarchy      6 dendrograms at default mcs
    fig_hdbscan_model_selection_scorecard
                                        Multi-config scorecard (stab, sil, DBCV)

Usage:
    python analysis/clustering_hdbscan_figures.py --figures all
    python analysis/clustering_hdbscan_figures.py --figures scorecard
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
from collections import Counter
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from scipy.cluster.hierarchy import dendrogram

from plotting import apply_style, save_fig, panel_label, despine, WONG
from _clustering_utils import PROC, PUB, MODEL_TAGS, MODEL_LABELS, CONDITIONS
from _hdbscan_utils import HDB_DIR, DEFAULT_MIN_CLUSTER_SIZES, hdb_config_key

apply_style()

HIER_DIR = HDB_DIR / "hierarchy"
LEIDEN_UMAP_DIR = PROC / "clustering_stability" / "umap"

MODEL_COLOR = {
    "specter2":   WONG["blue"],
    "pubmedbert": WONG["orange"],
    "mpnet":      WONG["green"],
}

FIG_ALL = ["stability", "sweep", "space", "hierarchy", "scorecard"]


# ─── 1. stability ─────────────────────────────────────────────────────────
def fig_stability(outdir: Path):
    mat_path = HDB_DIR / "stability_matrix.csv"
    stab_path = HDB_DIR / "stability_full.csv"
    if not (mat_path.exists() and stab_path.exists()):
        print("  [skip stability] prereq CSVs missing")
        return
    mat = pd.read_csv(mat_path, index_col=0)
    stab = pd.read_csv(stab_path)

    def sort_key(c):
        m, cond, r = c.split("__")
        return (list(MODEL_TAGS).index(m), list(CONDITIONS).index(cond), r)
    order = sorted(mat.index.tolist(), key=sort_key)
    mat = mat.loc[order, order]

    fig = plt.figure(figsize=(15, 8))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.3, 1], wspace=0.3)

    # Panel A: heatmap
    ax_a = fig.add_subplot(gs[0, 0])
    im = ax_a.imshow(mat.values, cmap="YlGnBu", vmin=0, vmax=1, aspect="auto")
    n = len(mat)
    ax_a.set_xticks(range(n)); ax_a.set_yticks(range(n))
    short = [c.replace("__", "/") for c in mat.index]
    ax_a.set_xticklabels(short, rotation=60, ha="right", fontsize=6)
    ax_a.set_yticklabels(short, fontsize=6)
    ax_a.set_title("A. HDBSCAN  —  mean-Jaccard matrix across all configs",
                   loc="left", fontsize=10)
    for i in range(1, 3):
        ax_a.axhline(i * (n/3) - 0.5, color="black", linewidth=0.8, alpha=0.4)
        ax_a.axvline(i * (n/3) - 0.5, color="black", linewidth=0.8, alpha=0.4)
    fig.colorbar(im, ax=ax_a, shrink=0.85, pad=0.02,
                  label="mean best-Jaccard")
    panel_label(ax_a, "A", x=-0.10, y=1.04)

    # Panel B: boxplot of per-cluster stability per config
    ax_b = fig.add_subplot(gs[0, 1])
    stab["model"] = stab["config"].str.split("__").str[0]
    stab["condition"] = stab["config"].str.split("__").str[1]
    cfg_order = sorted(stab["config"].unique(), key=sort_key)
    positions = np.arange(len(cfg_order))
    data = [stab.loc[stab["config"] == c, "stability_mean"].dropna().values
            for c in cfg_order]
    bp = ax_b.boxplot(data, positions=positions, widths=0.55,
                      patch_artist=True, showfliers=False)
    for i, patch in enumerate(bp["boxes"]):
        m = cfg_order[i].split("__")[0]
        cond = cfg_order[i].split("__")[1]
        patch.set_facecolor(MODEL_COLOR[m])
        patch.set_alpha(0.35 if cond == "raw" else 0.75)
        patch.set_edgecolor("black")
    for element in ["whiskers", "caps", "medians"]:
        for line in bp[element]:
            line.set_color("black")
            line.set_linewidth(0.8)
    for i, c in enumerate(cfg_order):
        vals = stab.loc[stab["config"] == c, "stability_mean"].dropna().values
        if len(vals) == 0:
            continue
        jitter = np.random.RandomState(42 + i).uniform(-0.15, 0.15, size=len(vals))
        ax_b.scatter(np.full(len(vals), i) + jitter, vals, s=6,
                      color="black", alpha=0.3, edgecolor="none")
    ax_b.set_xticks(positions)
    ax_b.set_xticklabels([c.replace("__", "/") for c in cfg_order],
                         rotation=60, ha="right", fontsize=6)
    ax_b.set_ylabel("per-cluster stability", fontsize=9)
    ax_b.set_title("B. Cluster-level stability per config", loc="left", fontsize=10)
    ax_b.set_ylim(0, 1)
    ax_b.axhline(0.5, color="grey", linestyle="--", linewidth=0.7, alpha=0.6)
    despine(ax_b); panel_label(ax_b, "B", x=-0.06, y=1.05)

    model_handles = [Patch(facecolor=MODEL_COLOR[m], edgecolor="black",
                            label=MODEL_LABELS[m]) for m in MODEL_TAGS]
    cond_handles = [Patch(facecolor=WONG["grey"], alpha=0.35, edgecolor="black",
                           label="raw (768d)"),
                    Patch(facecolor=WONG["grey"], alpha=0.75, edgecolor="black",
                           label="refined (64d)")]
    fig.legend(handles=model_handles + cond_handles, loc="upper center",
               ncol=5, fontsize=8, bbox_to_anchor=(0.5, 1.01), frameon=False)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    save_fig(fig, outdir / "fig_hdbscan_cluster_stability")
    plt.close(fig)
    print("  saved fig_hdbscan_cluster_stability.png")


# ─── 2. DBCV + big-cluster-count sweep ───────────────────────────────────
def fig_sweep(outdir: Path):
    p = HDB_DIR / "hdbscan_sweep.csv"
    if not p.exists():
        print("  [skip sweep] run clustering_hdbscan_sweep.py first"); return
    df = pd.read_csv(p)

    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5), sharex=True)
    ax_a, ax_b, ax_c = axes

    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            sub = df[(df["model"] == m) & (df["condition"] == cond)]
            if len(sub) == 0:
                continue
            sty = "-" if cond == "raw" else "--"
            ax_a.plot(sub["min_cluster_size"], sub["dbcv"],
                       color=MODEL_COLOR[m], linestyle=sty,
                       label=f"{MODEL_LABELS[m]}/{cond}", lw=1.3, marker="o", ms=5)
            # argmax marker
            best = sub.loc[sub["dbcv"].idxmax()]
            ax_a.scatter(best["min_cluster_size"], best["dbcv"],
                          color=MODEL_COLOR[m], marker="*" if cond == "refined" else "o",
                          s=90, edgecolor="black", linewidth=0.5, zorder=5)
            ax_b.plot(sub["min_cluster_size"], sub["n_big"],
                       color=MODEL_COLOR[m], linestyle=sty, lw=1.3, marker="o", ms=5)
            ax_c.plot(sub["min_cluster_size"], sub["noise_frac"],
                       color=MODEL_COLOR[m], linestyle=sty, lw=1.3, marker="o", ms=5)
    for ax in axes:
        ax.set_xlabel("min_cluster_size"); despine(ax)
    ax_a.set_ylabel("DBCV"); ax_a.set_title("A. DBCV vs min_cluster_size", loc="left", fontsize=10)
    ax_a.legend(fontsize=7, frameon=False, ncol=2)
    ax_b.set_ylabel("clusters ≥ 50 papers"); ax_b.set_title("B. Big-cluster count", loc="left", fontsize=10)
    ax_c.set_ylabel("noise fraction"); ax_c.set_title("C. Noise fraction", loc="left", fontsize=10)
    panel_label(ax_a, "A"); panel_label(ax_b, "B"); panel_label(ax_c, "C")

    plt.tight_layout()
    save_fig(fig, outdir / "fig_hdbscan_dbcv_sweep")
    plt.close(fig)
    print("  saved fig_hdbscan_dbcv_sweep.png")


# ─── 3. UMAP panels coloured by HDBSCAN cluster ───────────────────────────
def fig_space(outdir: Path, min_cluster_size: int = 50):
    fig, axes = plt.subplots(2, 3, figsize=(16, 10))
    for row, cond in enumerate(CONDITIONS):
        for col, m in enumerate(MODEL_TAGS):
            ax = axes[row, col]
            ckey = hdb_config_key(m, cond, min_cluster_size)
            coord_path = LEIDEN_UMAP_DIR / f"umap_{m}__{cond}.npy"
            mem_path = HDB_DIR / f"memberships_{ckey}.npy"
            if not (coord_path.exists() and mem_path.exists()):
                ax.set_visible(False); continue
            coords = np.load(coord_path); mem = np.load(mem_path)
            sizes = Counter(mem)
            big = sorted([c for c, n in sizes.items() if c >= 0 and n >= 50],
                          key=lambda c: -sizes[c])
            palette = plt.cm.tab20(np.linspace(0, 1, max(20, len(big))))
            color_map = {c: palette[i % 20] for i, c in enumerate(big)}

            noise_mask = mem == -1
            if noise_mask.any():
                ax.scatter(coords[noise_mask, 0], coords[noise_mask, 1],
                           s=1, c=[WONG["ltgrey"]], alpha=0.25, edgecolor="none",
                           rasterized=True)
            small_mask = ~noise_mask & np.array([c not in color_map for c in mem])
            if small_mask.any():
                ax.scatter(coords[small_mask, 0], coords[small_mask, 1],
                           s=1.5, c=["#888888"], alpha=0.35, edgecolor="none",
                           rasterized=True)
            for c in big:
                m_mask = mem == c
                ax.scatter(coords[m_mask, 0], coords[m_mask, 1],
                            s=2.5, c=[color_map[c]], alpha=0.55,
                            edgecolor="none", rasterized=True)
            ax.set_title(f"{MODEL_LABELS[m]} / {cond} — {len(big)} big clusters"
                         f" (noise = {noise_mask.mean():.0%})",
                         fontsize=10, loc="left")
            ax.set_xticks([]); ax.set_yticks([])
            ax.set_xlabel("UMAP-1", fontsize=8); ax.set_ylabel("UMAP-2", fontsize=8)
            for sp in ax.spines.values():
                sp.set_linewidth(0.4)

    fig.suptitle(f"HDBSCAN cluster space — mcs={min_cluster_size}. "
                 "Grey = noise points.",
                 fontsize=11, y=1.01)
    plt.tight_layout()
    save_fig(fig, outdir / "fig_hdbscan_cluster_space_umap")
    plt.close(fig)
    print("  saved fig_hdbscan_cluster_space_umap.png")


# ─── 4. hierarchy dendrogram ─────────────────────────────────────────────
def fig_hierarchy(outdir: Path, min_cluster_size: int = 50):
    configs = [hdb_config_key(m, c, min_cluster_size)
               for m in MODEL_TAGS for c in CONDITIONS]
    available = [c for c in configs
                 if (HIER_DIR / f"linkage_{c}.npy").exists()]
    if not available:
        print("  [skip hierarchy] no linkage files — run clustering_hdbscan_hierarchy.py")
        return
    ncols = min(3, len(available))
    nrows = int(np.ceil(len(available) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 4.5 * nrows),
                              squeeze=False)
    for idx, ckey in enumerate(available):
        r, c = divmod(idx, ncols)
        ax = axes[r, c]
        Z = np.load(HIER_DIR / f"linkage_{ckey}.npy")
        cluster_ids = np.load(HIER_DIR / f"cluster_ids_{ckey}.npy")
        dendrogram(Z, ax=ax, labels=[f"C{c}" for c in cluster_ids],
                   color_threshold=0.5 * max(Z[:, 2]),
                   leaf_rotation=90, leaf_font_size=7)
        ax.set_title(f"{ckey.replace('__', ' / ')}  ({len(cluster_ids)} clusters)",
                     fontsize=10, loc="left")
        ax.set_ylabel("cosine distance (avg linkage)", fontsize=8)
        despine(ax)
    for j in range(len(available), nrows * ncols):
        r, c = divmod(j, ncols)
        axes[r, c].set_visible(False)
    fig.suptitle(f"HDBSCAN hierarchical cluster-of-clusters (mcs={min_cluster_size})",
                 fontsize=11, y=1.01)
    plt.tight_layout()
    save_fig(fig, outdir / "fig_hdbscan_cluster_hierarchy")
    plt.close(fig)
    print("  saved fig_hdbscan_cluster_hierarchy.png")


# ─── 5. model-selection scorecard (multi-config) ─────────────────────────
def fig_scorecard(outdir: Path):
    scores_path = HDB_DIR / "model_selection_scores.csv"
    if not scores_path.exists():
        print("  [skip scorecard] run clustering_hdbscan_modelselection.py first"); return
    df = pd.read_csv(scores_path)
    metrics_used = (df["metrics_used"].iloc[0].split("+")
                    if "metrics_used" in df.columns else
                    ["stability", "silhouette", "dbcv"])
    mcs_values = sorted(df["min_cluster_size"].unique())
    pair_order = [(m, c) for m in MODEL_TAGS for c in CONDITIONS]
    pair_labels = [f"{MODEL_LABELS[m]} / {c}" for (m, c) in pair_order]

    def make_matrix(col):
        mat = np.full((len(pair_order), len(mcs_values)), np.nan)
        for i, (m, c) in enumerate(pair_order):
            for j, mcs in enumerate(mcs_values):
                row = df[(df["model"] == m) & (df["condition"] == c)
                          & (df["min_cluster_size"] == mcs)]
                if len(row):
                    mat[i, j] = float(row[col].iloc[0])
        return mat

    raw_metrics = list(metrics_used) + ["composite"]
    n_metrics = len(raw_metrics)
    fig = plt.figure(figsize=(4 + 3*n_metrics, 6.5))
    gs = fig.add_gridspec(2, n_metrics + 1,
                           height_ratios=[1, 0.55],
                           width_ratios=[1] * n_metrics + [0.9],
                           wspace=0.35, hspace=0.45)

    winner = df.sort_values("composite", ascending=False).iloc[0]
    w_pair = (winner["model"], winner["condition"])
    w_mcs = int(winner["min_cluster_size"])
    w_i = pair_order.index(w_pair); w_j = mcs_values.index(w_mcs)

    for k, metric in enumerate(raw_metrics):
        ax = fig.add_subplot(gs[0, k])
        mat = make_matrix(metric)
        cmap = "YlGnBu" if metric == "composite" else "viridis"
        im = ax.imshow(mat, cmap=cmap, aspect="auto")
        for i in range(mat.shape[0]):
            for j in range(mat.shape[1]):
                if np.isnan(mat[i, j]):
                    continue
                txt = f"{mat[i, j]:.2f}" if metric != "composite" else f"{mat[i, j]:.3f}"
                vmax = np.nanmax(mat)
                colr = "white" if mat[i, j] > 0.55 * vmax else "black"
                ax.text(j, i, txt, ha="center", va="center", color=colr, fontsize=7)
        if metric == "composite":
            ax.add_patch(plt.Rectangle((w_j - 0.5, w_i - 0.5), 1, 1,
                                         fill=False, edgecolor=WONG["red"],
                                         linewidth=2))
        ax.set_xticks(range(len(mcs_values)))
        ax.set_xticklabels([str(x) for x in mcs_values], fontsize=8)
        if k == 0:
            ax.set_yticks(range(len(pair_order)))
            ax.set_yticklabels(pair_labels, fontsize=8)
        else:
            ax.set_yticks([])
        ax.set_title(metric.upper(), fontsize=10, loc="left",
                      color=WONG["red"] if metric == "composite" else "black")
        ax.set_xlabel("min_cluster_size", fontsize=8)
        fig.colorbar(im, ax=ax, shrink=0.7, pad=0.02)
    panel_label(fig.axes[0], "A", x=-0.12, y=1.10)

    # Right: composite vs mcs curves
    ax_line = fig.add_subplot(gs[0, n_metrics])
    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            sub = df[(df["model"] == m) & (df["condition"] == cond)]
            if len(sub) == 0:
                continue
            sub = sub.sort_values("min_cluster_size")
            ax_line.plot(sub["min_cluster_size"], sub["composite"],
                          color=MODEL_COLOR[m],
                          linestyle="-" if cond == "raw" else "--",
                          marker="o" if cond == "raw" else "s",
                          linewidth=1.3, markersize=5,
                          label=f"{MODEL_LABELS[m]}/{cond}")
    ax_line.scatter([w_mcs], [winner["composite"]], s=140,
                     edgecolor=WONG["red"], facecolor="none", linewidth=2,
                     zorder=10, label="winner")
    ax_line.set_xlabel("min_cluster_size")
    ax_line.set_ylabel("composite")
    ax_line.set_title("B. Composite vs min_cluster_size", fontsize=10, loc="left")
    ax_line.legend(fontsize=6, loc="best", frameon=False, ncol=2)
    despine(ax_line); panel_label(ax_line, "B", x=-0.15, y=1.10)

    # Bottom: ranked bar
    ax_rank = fig.add_subplot(gs[1, :])
    df_sorted = df.sort_values("composite", ascending=True)
    y = np.arange(len(df_sorted))
    colors = [MODEL_COLOR[r["model"]] for _, r in df_sorted.iterrows()]
    alphas = [0.45 if r["condition"] == "raw" else 0.9 for _, r in df_sorted.iterrows()]
    bars = ax_rank.barh(y, df_sorted["composite"].values, color=colors,
                         edgecolor="black", linewidth=0.4)
    for bar, a in zip(bars, alphas):
        bar.set_alpha(a)
    top_idx = df_sorted["composite"].values.argmax()
    ax_rank.barh(y[top_idx], df_sorted["composite"].iloc[top_idx],
                  color="none", edgecolor=WONG["red"], linewidth=2)
    labels_short = [f"{r['model']}/{r['condition']}/mcs{int(r['min_cluster_size']):03d}"
                    for _, r in df_sorted.iterrows()]
    ax_rank.set_yticks(y); ax_rank.set_yticklabels(labels_short, fontsize=7)
    ax_rank.set_xlabel(f"composite score (mean of {len(metrics_used)} normalised metrics)",
                       fontsize=9)
    ax_rank.set_title(f"C. All {len(df)} HDBSCAN configs ranked by composite "
                       "(red = winner)",
                       fontsize=10, loc="left")
    despine(ax_rank); panel_label(ax_rank, "C", x=-0.03, y=1.05)

    plt.suptitle(
        f"HDBSCAN model selection — {len(pair_order)} (model, condition) × "
        f"{len(mcs_values)} min_cluster_sizes   "
        f"[metrics: {', '.join(metrics_used)}]", fontsize=11, y=1.02)
    plt.tight_layout()
    save_fig(fig, outdir / "fig_hdbscan_model_selection_scorecard")
    plt.close(fig)
    print("  saved fig_hdbscan_model_selection_scorecard.png")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--figures", nargs="+", default=["all"])
    parser.add_argument("--min-cluster-size", type=int, default=50,
                        help="default mcs for space + hierarchy figures")
    args = parser.parse_args()
    want = FIG_ALL if args.figures == ["all"] else args.figures
    PUB.mkdir(parents=True, exist_ok=True)
    print(f"Building: {want}")
    if "stability" in want:
        fig_stability(PUB)
    if "sweep" in want:
        fig_sweep(PUB)
    if "space" in want:
        fig_space(PUB, min_cluster_size=args.min_cluster_size)
    if "hierarchy" in want:
        fig_hierarchy(PUB, min_cluster_size=args.min_cluster_size)
    if "scorecard" in want:
        fig_scorecard(PUB)
    print(f"\nAll figures in {PUB}")


if __name__ == "__main__":
    main()
