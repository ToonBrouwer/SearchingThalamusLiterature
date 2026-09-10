"""
clustering_figures.py — Publication figures for the domain-clustering analysis.

Builds five figures that together support the "which embedding + condition
should we use?" decision:

  fig_cluster_stability          Per-cluster stability across 24 configs
                                 + config × config mean-stability heatmap
  fig_modularity_sweep           Modularity vs resolution for each (model,
                                 condition) — justifies the working resolution
  fig_cluster_space_umap         2-D UMAP of each model × condition, points
                                 coloured by Leiden cluster (6 panels)
  fig_cluster_hierarchy          Dendrogram of cluster centroids with umbrella
                                 cuts annotated
  fig_model_selection_scorecard  Normalized radar/bar comparison of the four
                                 metrics across 6 configs; winner highlighted

Run one, many, or all:
    python analysis/clustering_figures.py --figures stability
    python analysis/clustering_figures.py --figures all
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.colors import LinearSegmentedColormap
from scipy.cluster.hierarchy import dendrogram

from plotting import apply_style, save_fig, panel_label, despine, WONG, heatmap

from _clustering_utils import (  # noqa: E402
    PROC, PUB, MODEL_TAGS, MODEL_LABELS, CONDITIONS, DOMAIN_RE, DOMAIN_COLORS,
    load_embeddings, load_papers, compute_domain_flags, config_key,
)

apply_style()

STAB_DIR = PROC / "clustering_stability"
UMAP_DIR = STAB_DIR / "umap"
HIER_DIR = STAB_DIR / "hierarchy"

MODEL_COLOR = {
    "specter2":   WONG["blue"],
    "pubmedbert": WONG["orange"],
    "mpnet":      WONG["green"],
}
CONDITION_STYLE = {"raw": "o", "refined": "s"}

FIG_ALL = ["stability", "modularity", "space", "hierarchy", "scorecard"]


# ═══════════════════════════════════════════════════════════════════════════
# Figure 1: stability showcase
# ═══════════════════════════════════════════════════════════════════════════
def fig_stability(outdir: Path, resolution: float = 0.008):
    """
    Two-panel figure:
      A: Config × config mean-Jaccard heatmap (aggregate stability)
      B: Per-cluster stability stripplot — one strip per config, y = stability
    """
    mat_path = STAB_DIR / "stability_matrix.csv"
    stab_path = STAB_DIR / "stability_full.csv"
    if not (mat_path.exists() and stab_path.exists()):
        print("  [skip stability] prereq CSVs missing")
        return
    mat = pd.read_csv(mat_path, index_col=0)
    stab = pd.read_csv(stab_path)

    # Order: group by model, then condition, then resolution
    def sort_key(c):
        m, cond, r = c.split("__")
        return (list(MODEL_TAGS).index(m), list(CONDITIONS).index(cond), r)
    order = sorted(mat.index.tolist(), key=sort_key)
    mat = mat.loc[order, order]

    fig = plt.figure(figsize=(15, 8.5))
    gs = fig.add_gridspec(2, 3, width_ratios=[1.4, 1, 1], height_ratios=[1, 1],
                          hspace=0.45, wspace=0.35)

    # ── Panel A: heatmap
    ax_a = fig.add_subplot(gs[:, 0])
    im = ax_a.imshow(mat.values, cmap="YlGnBu", vmin=0, vmax=1, aspect="auto")
    n = len(mat)
    ax_a.set_xticks(range(n))
    ax_a.set_yticks(range(n))
    short = [c.replace("__", "/") for c in mat.index]
    ax_a.set_xticklabels(short, rotation=60, ha="right", fontsize=6)
    ax_a.set_yticklabels(short, fontsize=6)
    ax_a.set_title("A. Mean-Jaccard matrix across 24 configs", fontsize=10, loc="left")
    # Subdivider lines between model blocks
    for i in range(1, 3):
        ax_a.axhline(i * (n/3) - 0.5, color="black", linewidth=0.8, alpha=0.4)
        ax_a.axvline(i * (n/3) - 0.5, color="black", linewidth=0.8, alpha=0.4)
    cbar = fig.colorbar(im, ax=ax_a, shrink=0.75, pad=0.02)
    cbar.set_label("mean best-Jaccard", fontsize=8)
    panel_label(ax_a, "A", x=-0.10, y=1.04)

    # ── Panel B (top-right): per-config stability boxplot
    ax_b = fig.add_subplot(gs[0, 1:])
    stab["model"] = stab["config"].str.split("__").str[0]
    stab["condition"] = stab["config"].str.split("__").str[1]
    stab["res"] = stab["config"].str.split("__").str[2]

    cfg_order = sorted(stab["config"].unique(), key=sort_key)
    positions = np.arange(len(cfg_order))
    data = [stab.loc[stab["config"] == c, "stability_mean"].dropna().values
            for c in cfg_order]

    bp = ax_b.boxplot(data, positions=positions, widths=0.55,
                      patch_artist=True, showfliers=False)
    for i, patch in enumerate(bp["boxes"]):
        m = cfg_order[i].split("__")[0]
        cond = cfg_order[i].split("__")[1]
        color = MODEL_COLOR[m]
        alpha = 0.35 if cond == "raw" else 0.75
        patch.set_facecolor(color)
        patch.set_alpha(alpha)
        patch.set_edgecolor("black")
    for element in ["whiskers", "caps", "medians"]:
        for line in bp[element]:
            line.set_color("black")
            line.set_linewidth(0.8)

    # overlay individual points
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
    ax_b.set_ylabel("per-cluster stability\n(mean best-Jaccard vs other 23 configs)",
                    fontsize=9)
    ax_b.set_title("B. Cluster-level stability per config", fontsize=10, loc="left")
    ax_b.set_ylim(0, 1)
    ax_b.axhline(0.5, color="grey", linestyle="--", linewidth=0.7, alpha=0.6)
    despine(ax_b)
    panel_label(ax_b, "B", x=-0.06, y=1.07)

    # ── Panel C (bottom-right): top stable "anchor clusters" across configs
    # For the working resolution, take the top-5 most stable clusters in each
    # config and show how their stability moves across configs.
    ax_c = fig.add_subplot(gs[1, 1:])
    rtag = f"r{int(resolution*1000):03d}"
    top_anchors = stab[stab["res"] == rtag].copy()
    top_anchors = (top_anchors.sort_values(["config", "stability_mean"],
                                            ascending=[True, False])
                   .groupby("config").head(5))

    # Distinct domain colouring
    def pick_domain_color(row):
        dom_pcts = [(d, row.get(f"pct_{d}", 0)) for d in DOMAIN_RE]
        dom_pcts.sort(key=lambda x: -x[1])
        top_d, top_p = dom_pcts[0]
        if top_p < 15:
            return "#bbbbbb"
        return DOMAIN_COLORS.get(top_d, "#bbbbbb")

    for _, r in top_anchors.iterrows():
        m_, c_, _ = r["config"].split("__")
        x_pos = cfg_order.index(r["config"])
        color = pick_domain_color(r)
        ax_c.scatter(x_pos, r["stability_mean"], s=80,
                     color=color, alpha=0.85, edgecolor="black", linewidth=0.3)

    ax_c.set_xticks(positions)
    ax_c.set_xticklabels([c.replace("__", "/") for c in cfg_order],
                         rotation=60, ha="right", fontsize=6)
    ax_c.set_ylabel("stability", fontsize=9)
    ax_c.set_title(f"C. Top-5 stable clusters per config — coloured by primary domain",
                   fontsize=10, loc="left")
    ax_c.set_ylim(0, 1)
    ax_c.axhline(0.5, color="grey", linestyle="--", linewidth=0.7, alpha=0.6)
    despine(ax_c)
    panel_label(ax_c, "C", x=-0.06, y=1.07)

    # Shared legend: model colours + domain colours
    model_handles = [Patch(facecolor=MODEL_COLOR[m], edgecolor="black",
                            label=MODEL_LABELS[m]) for m in MODEL_TAGS]
    cond_handles = [Patch(facecolor=WONG["grey"], alpha=0.35, edgecolor="black",
                           label="raw (768d)"),
                    Patch(facecolor=WONG["grey"], alpha=0.75, edgecolor="black",
                           label="refined (64d)")]
    domain_handles = [Patch(facecolor=c, edgecolor="black", label=d)
                      for d, c in DOMAIN_COLORS.items()]
    fig.legend(handles=model_handles + cond_handles,
               loc="upper center", ncol=5, fontsize=8,
               bbox_to_anchor=(0.65, 1.00), frameon=False)
    fig.legend(handles=domain_handles, loc="lower center", ncol=7,
               fontsize=7, bbox_to_anchor=(0.5, -0.02), frameon=False)

    save_fig(fig, outdir / "fig_cluster_stability")
    plt.close(fig)
    print(f"  saved fig_cluster_stability.png")


# ═══════════════════════════════════════════════════════════════════════════
# Figure 2: modularity sweep
# ═══════════════════════════════════════════════════════════════════════════
def fig_modularity(outdir: Path):
    sweep_path = STAB_DIR / "modularity_sweep.csv"
    if not sweep_path.exists():
        sweep_path = STAB_DIR / "modularity_sweep_dense.csv"
    if not sweep_path.exists():
        print("  [skip modularity] sweep CSV missing")
        return
    df = pd.read_csv(sweep_path)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharex=True)
    # Panel A: modularity vs resolution, colored by model, styled by condition
    ax_a = axes[0]
    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            sub = df[(df["model"] == m) & (df["condition"] == cond)]
            if len(sub) == 0:
                continue
            ax_a.plot(sub["resolution"], sub["modularity"],
                      color=MODEL_COLOR[m],
                      linestyle="-" if cond == "raw" else "--",
                      linewidth=1.2,
                      label=f"{MODEL_LABELS[m]} / {cond}")
            # mark argmax
            best = sub.loc[sub["modularity"].idxmax()]
            ax_a.scatter(best["resolution"], best["modularity"],
                         color=MODEL_COLOR[m],
                         marker="*" if cond == "refined" else "o",
                         s=90, edgecolor="black", linewidth=0.5, zorder=5)
    ax_a.set_xscale("log")
    ax_a.set_xlabel("resolution (CPM parameter)")
    ax_a.set_ylabel("modularity")
    ax_a.set_title("A. Leiden-CPM modularity vs resolution", loc="left", fontsize=10)
    ax_a.legend(fontsize=7, loc="best", frameon=False, ncol=2)
    despine(ax_a)
    panel_label(ax_a, "A")

    # Panel B: n_big (clusters ≥ 50) vs resolution
    ax_b = axes[1]
    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            sub = df[(df["model"] == m) & (df["condition"] == cond)]
            if len(sub) == 0:
                continue
            ax_b.plot(sub["resolution"], sub["n_big"],
                      color=MODEL_COLOR[m],
                      linestyle="-" if cond == "raw" else "--",
                      linewidth=1.2,
                      label=f"{MODEL_LABELS[m]} / {cond}")
    ax_b.set_xscale("log")
    ax_b.set_xlabel("resolution")
    ax_b.set_ylabel("clusters with ≥ 50 papers")
    ax_b.set_title("B. Meaningful cluster count vs resolution",
                   loc="left", fontsize=10)
    ax_b.axvspan(0.005, 0.020, alpha=0.08, color=WONG["grey"],
                 label="working range")
    despine(ax_b)
    panel_label(ax_b, "B")

    plt.tight_layout()
    save_fig(fig, outdir / "fig_modularity_sweep")
    plt.close(fig)
    print(f"  saved fig_modularity_sweep.png")


# ═══════════════════════════════════════════════════════════════════════════
# Figure 3: UMAP panels coloured by Leiden cluster
# ═══════════════════════════════════════════════════════════════════════════
def fig_space(outdir: Path, resolution: float = 0.008):
    fig, axes = plt.subplots(2, 3, figsize=(16, 10))
    rtag = f"r{int(resolution*1000):03d}"
    papers = compute_domain_flags(load_papers())

    for row, cond in enumerate(CONDITIONS):
        for col, m in enumerate(MODEL_TAGS):
            ax = axes[row, col]
            ckey = config_key(m, cond, resolution)
            coord_path = UMAP_DIR / f"umap_{m}__{cond}.npy"
            mem_path = STAB_DIR / f"memberships_{ckey}.npy"
            if not (coord_path.exists() and mem_path.exists()):
                ax.set_visible(False)
                continue
            coords = np.load(coord_path)
            mem = np.load(mem_path)

            # Color scheme: big clusters get distinct colors; noise/small → grey
            from collections import Counter
            sizes = Counter(mem)
            big = sorted([c for c, n in sizes.items() if n >= 50],
                         key=lambda c: -sizes[c])
            # Build a palette that cycles through tab20 for up to ~20 clusters
            palette = plt.cm.tab20(np.linspace(0, 1, max(20, len(big))))
            color_map = {c: palette[i % 20] for i, c in enumerate(big)}

            # Plot small / noise first (grey background)
            mask_small = np.array([c not in color_map for c in mem])
            if mask_small.any():
                ax.scatter(coords[mask_small, 0], coords[mask_small, 1],
                           s=1, c=[WONG["ltgrey"]], alpha=0.3, edgecolor="none",
                           rasterized=True)
            # Plot big clusters on top
            for c in big:
                m_mask = mem == c
                ax.scatter(coords[m_mask, 0], coords[m_mask, 1],
                           s=2.5, c=[color_map[c]], alpha=0.55, edgecolor="none",
                           rasterized=True)

            ax.set_title(f"{MODEL_LABELS[m]} / {cond} — {len(big)} big clusters",
                         fontsize=10, loc="left")
            ax.set_xticks([]); ax.set_yticks([])
            ax.set_xlabel("UMAP-1", fontsize=8)
            ax.set_ylabel("UMAP-2", fontsize=8)
            for spine in ax.spines.values():
                spine.set_linewidth(0.4)

    fig.suptitle(
        f"Cluster space — 2-D UMAP of each (embedding, condition) at res={resolution}\n"
        f"Row 1: raw 768-dim · Row 2: contrastive-refined 64-dim",
        fontsize=11, y=1.01)
    plt.tight_layout()
    save_fig(fig, outdir / "fig_cluster_space_umap")
    plt.close(fig)
    print(f"  saved fig_cluster_space_umap.png")


# ═══════════════════════════════════════════════════════════════════════════
# Figure 4: hierarchical dendrogram
# ═══════════════════════════════════════════════════════════════════════════
def fig_hierarchy(outdir: Path, resolution: float = 0.008):
    rtag = f"r{int(resolution*1000):03d}"
    configs = [config_key(m, c, resolution) for m in MODEL_TAGS for c in CONDITIONS]
    available = [c for c in configs
                 if (HIER_DIR / f"linkage_{c}.npy").exists()]
    if not available:
        print("  [skip hierarchy] no linkage files — run clustering_hierarchy.py")
        return

    ncols = min(3, len(available))
    nrows = int(np.ceil(len(available) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 4.5 * nrows),
                              squeeze=False)

    papers = load_papers()
    for idx, ckey in enumerate(available):
        r, c = divmod(idx, ncols)
        ax = axes[r, c]
        Z = np.load(HIER_DIR / f"linkage_{ckey}.npy")
        cluster_ids = np.load(HIER_DIR / f"cluster_ids_{ckey}.npy")
        # Load the umbrella labels so we can color leaves by umbrella at k=6
        umb = pd.read_csv(HIER_DIR / f"umbrella_labels_{ckey}.csv")
        umb_k = umb[umb["n_umbrella"] == 6]

        # Compute which umbrella each cluster belongs to (at k=6)
        c2u = pd.read_csv(HIER_DIR / f"cluster_to_umbrella_{ckey}.csv")
        cu_map = dict(zip(c2u["cluster"], c2u.get("umbrella_k6",
                                                   c2u.get("umbrella_k8"))))

        # dendrogram
        dendrogram(Z, ax=ax,
                   labels=[f"C{c}" for c in cluster_ids],
                   color_threshold=0.5 * max(Z[:, 2]),
                   leaf_rotation=90, leaf_font_size=7)
        ax.set_title(f"{ckey.replace('__', ' / ')}  "
                     f"({len(cluster_ids)} clusters)",
                     fontsize=10, loc="left")
        ax.set_ylabel("cosine distance (avg linkage)", fontsize=8)
        despine(ax)

    # Hide unused axes
    for j in range(len(available), nrows * ncols):
        r, c = divmod(j, ncols)
        axes[r, c].set_visible(False)

    fig.suptitle(f"Hierarchical clustering of Leiden cluster centroids "
                 f"(res={resolution}, average-linkage cosine)", fontsize=11, y=1.01)
    plt.tight_layout()
    save_fig(fig, outdir / "fig_cluster_hierarchy")
    plt.close(fig)
    print(f"  saved fig_cluster_hierarchy.png")


# ═══════════════════════════════════════════════════════════════════════════
# Figure 5: model-selection scorecard
# ═══════════════════════════════════════════════════════════════════════════
def fig_scorecard(outdir: Path):
    scores_path = STAB_DIR / "model_selection_scores.csv"
    if not scores_path.exists():
        print("  [skip scorecard] run clustering_model_selection.py first")
        return
    df = pd.read_csv(scores_path)

    # Derive which metrics were actually used
    metrics_used = (df["metrics_used"].iloc[0].split("+")
                    if "metrics_used" in df.columns else
                    ["stability", "silhouette", "modularity"])
    n_resolutions = df["resolution"].nunique() if "resolution" in df.columns else 1

    if n_resolutions == 1:
        _scorecard_single_res(df, outdir, metrics_used)
    else:
        _scorecard_multi_res(df, outdir, metrics_used)


def _scorecard_single_res(df, outdir, metrics_used):
    """Original layout: bars + ranking."""
    df = df.sort_values("composite", ascending=False).reset_index(drop=True)
    cfg_names = df["config"].tolist()
    raw_metrics = metrics_used
    metric_labels = [m.replace("domain_cov", "domain\ncoverage") for m in raw_metrics]

    fig = plt.figure(figsize=(13, 6))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.3, 1], wspace=0.25)

    ax_a = fig.add_subplot(gs[0, 0])
    n_cfg = len(df)
    width = 0.8 / n_cfg
    x_pos = np.arange(len(raw_metrics))
    for i, row in df.iterrows():
        m_ = row["model"]
        cond = row["condition"]
        vals = [row[mm] for mm in raw_metrics]
        color = MODEL_COLOR[m_]
        alpha = 0.45 if cond == "raw" else 0.9
        ax_a.bar(x_pos + (i - n_cfg/2 + 0.5) * width, vals, width,
                 color=color, alpha=alpha, edgecolor="black", linewidth=0.4,
                 label=f"{row['config'].replace('__', '/')} "
                       f"(composite={row['composite']:.2f})")
    ax_a.set_xticks(x_pos)
    ax_a.set_xticklabels(metric_labels, fontsize=10)
    ax_a.set_ylabel("raw metric value", fontsize=10)
    ax_a.set_title(f"A. Per-metric raw values across {n_cfg} configs "
                   f"(n={len(raw_metrics)} metrics, higher = better)",
                   loc="left", fontsize=10)
    ax_a.legend(fontsize=6, loc="upper right", ncol=2, frameon=False)
    despine(ax_a); panel_label(ax_a, "A")

    ax_b = fig.add_subplot(gs[0, 1])
    y = np.arange(len(df))
    colors = [MODEL_COLOR[r["model"]] for _, r in df.iterrows()]
    alphas = [0.45 if r["condition"] == "raw" else 0.9 for _, r in df.iterrows()]
    bars = ax_b.barh(y, df["composite"].values, color=colors, edgecolor="black",
                      linewidth=0.5)
    for bar, a in zip(bars, alphas):
        bar.set_alpha(a)
    ax_b.barh(y[0], df["composite"].iloc[0], color="none",
              edgecolor=WONG["red"], linewidth=2)
    ax_b.set_yticks(y)
    ax_b.set_yticklabels([c.replace("__", "/") for c in cfg_names], fontsize=9)
    ax_b.invert_yaxis()
    ax_b.set_xlabel(f"composite score (mean of {len(raw_metrics)} normalised metrics)",
                    fontsize=10)
    ax_b.set_title("B. Composite ranking\n(red outline = winner)",
                   loc="left", fontsize=10)
    for i, v in enumerate(df["composite"].values):
        ax_b.text(v + 0.005, i, f"{v:.3f}", va="center", fontsize=8)
    despine(ax_b); panel_label(ax_b, "B")

    plt.tight_layout()
    save_fig(fig, outdir / "fig_model_selection_scorecard")
    plt.close(fig)
    print(f"  saved fig_model_selection_scorecard.png (single-resolution layout)")


def _scorecard_multi_res(df, outdir, metrics_used):
    """
    Multi-resolution scorecard: one heatmap per metric (rows = model/cond,
    columns = resolutions), plus a composite heatmap and per-model composite
    curves across resolutions.
    """
    raw_metrics = list(metrics_used) + ["composite"]
    resolutions = sorted(df["resolution"].unique())
    pair_order = [(m, c) for m in MODEL_TAGS for c in CONDITIONS]
    pair_labels = [f"{MODEL_LABELS[m]} / {c}" for (m, c) in pair_order]

    # Build a (pairs × resolutions) matrix per metric
    def make_matrix(col):
        mat = np.full((len(pair_order), len(resolutions)), np.nan)
        for i, (m, c) in enumerate(pair_order):
            for j, r in enumerate(resolutions):
                row = df[(df["model"] == m) & (df["condition"] == c)
                          & (df["resolution"] == r)]
                if len(row):
                    mat[i, j] = float(row[col].iloc[0])
        return mat

    n_metrics = len(raw_metrics)
    fig = plt.figure(figsize=(4 + 3*n_metrics, 6.5))
    gs = fig.add_gridspec(2, n_metrics + 1,
                           height_ratios=[1, 0.55],
                           width_ratios=[1] * n_metrics + [0.9],
                           wspace=0.35, hspace=0.45)

    # Identify winner cell
    winner = df.sort_values("composite", ascending=False).iloc[0]
    w_pair = (winner["model"], winner["condition"])
    w_res = float(winner["resolution"])
    w_i = pair_order.index(w_pair)
    w_j = resolutions.index(w_res)

    for k, metric in enumerate(raw_metrics):
        ax = fig.add_subplot(gs[0, k])
        mat = make_matrix(metric)
        cmap = "YlGnBu" if metric == "composite" else "viridis"
        im = ax.imshow(mat, cmap=cmap, aspect="auto")
        # Annotate cells
        for i in range(mat.shape[0]):
            for j in range(mat.shape[1]):
                if np.isnan(mat[i, j]):
                    continue
                txt = f"{mat[i, j]:.2f}" if metric != "composite" \
                                          else f"{mat[i, j]:.3f}"
                val = mat[i, j]
                vmax = np.nanmax(mat)
                col_t = "white" if val > 0.55 * vmax else "black"
                ax.text(j, i, txt, ha="center", va="center",
                        color=col_t, fontsize=7)
        if metric == "composite":
            # Outline the winner
            ax.add_patch(plt.Rectangle((w_j - 0.5, w_i - 0.5), 1, 1,
                                         fill=False, edgecolor=WONG["red"],
                                         linewidth=2))

        ax.set_xticks(range(len(resolutions)))
        ax.set_xticklabels([f"{r:.3f}" for r in resolutions], fontsize=8)
        if k == 0:
            ax.set_yticks(range(len(pair_order)))
            ax.set_yticklabels(pair_labels, fontsize=8)
        else:
            ax.set_yticks([])
        label = metric.replace("_", " ").replace("domain cov", "domain cov")
        ax.set_title(label.upper() if metric != "composite" else "COMPOSITE",
                     fontsize=10, loc="left",
                     color=WONG["red"] if metric == "composite" else "black")
        ax.set_xlabel("resolution", fontsize=8)
        fig.colorbar(im, ax=ax, shrink=0.7, pad=0.02)
    panel_label(fig.axes[0], "A", x=-0.12, y=1.10)

    # Right column: composite vs resolution, one curve per (model, condition)
    ax_line = fig.add_subplot(gs[0, n_metrics])
    for m in MODEL_TAGS:
        for cond in CONDITIONS:
            sub = df[(df["model"] == m) & (df["condition"] == cond)]
            if len(sub) == 0:
                continue
            sub = sub.sort_values("resolution")
            ax_line.plot(sub["resolution"], sub["composite"],
                          color=MODEL_COLOR[m],
                          linestyle="-" if cond == "raw" else "--",
                          marker="o" if cond == "raw" else "s",
                          linewidth=1.3, markersize=5,
                          label=f"{MODEL_LABELS[m]}/{cond}")
    # Mark winner
    ax_line.scatter([w_res], [winner["composite"]], s=140,
                    edgecolor=WONG["red"], facecolor="none", linewidth=2,
                    zorder=10, label="winner")
    ax_line.set_xlabel("resolution")
    ax_line.set_ylabel("composite")
    ax_line.set_title("B. Composite vs resolution", fontsize=10, loc="left")
    ax_line.legend(fontsize=6, loc="best", frameon=False, ncol=2)
    despine(ax_line); panel_label(ax_line, "B", x=-0.15, y=1.10)

    # Bottom row: ranked-all bar (spans full width)
    ax_rank = fig.add_subplot(gs[1, :])
    df_sorted = df.sort_values("composite", ascending=True)
    y = np.arange(len(df_sorted))
    colors = [MODEL_COLOR[r["model"]] for _, r in df_sorted.iterrows()]
    alphas = [0.45 if r["condition"] == "raw" else 0.9
              for _, r in df_sorted.iterrows()]
    bars = ax_rank.barh(y, df_sorted["composite"].values, color=colors,
                         edgecolor="black", linewidth=0.4)
    for bar, a in zip(bars, alphas):
        bar.set_alpha(a)
    # Outline top-1
    top_idx = df_sorted["composite"].values.argmax()
    ax_rank.barh(y[top_idx], df_sorted["composite"].iloc[top_idx],
                  color="none", edgecolor=WONG["red"], linewidth=2)
    labels_short = [f"{r['model']}/{r['condition']}/r{int(r['resolution']*1000):03d}"
                    for _, r in df_sorted.iterrows()]
    ax_rank.set_yticks(y)
    ax_rank.set_yticklabels(labels_short, fontsize=7)
    ax_rank.set_xlabel(f"composite score (mean of {len(metrics_used)} normalised metrics)",
                        fontsize=9)
    ax_rank.set_title(f"C. All {len(df)} configs ranked by composite  "
                       f"(red = winner)",
                       fontsize=10, loc="left")
    despine(ax_rank); panel_label(ax_rank, "C", x=-0.03, y=1.05)

    plt.suptitle(
        f"Model selection across {len(pair_order)} (model, condition) pairs "
        f"× {len(resolutions)} resolutions  —  metrics: "
        f"{', '.join(metrics_used)}", fontsize=11, y=1.02)
    plt.tight_layout()
    save_fig(fig, outdir / "fig_model_selection_scorecard")
    plt.close(fig)
    print(f"  saved fig_model_selection_scorecard.png (multi-resolution layout)")


# ═══════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--figures", nargs="+", default=["all"],
                        help=f"Which to build: {FIG_ALL} or 'all'")
    parser.add_argument("--resolution", type=float, default=0.008)
    args = parser.parse_args()

    want = FIG_ALL if args.figures == ["all"] else args.figures
    PUB.mkdir(parents=True, exist_ok=True)
    print(f"Building: {want}")

    if "stability" in want:
        fig_stability(PUB, resolution=args.resolution)
    if "modularity" in want:
        fig_modularity(PUB)
    if "space" in want:
        fig_space(PUB, resolution=args.resolution)
    if "hierarchy" in want:
        fig_hierarchy(PUB, resolution=args.resolution)
    if "scorecard" in want:
        fig_scorecard(PUB)

    print(f"\nAll figures in {PUB}")


if __name__ == "__main__":
    main()
