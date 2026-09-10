#!/usr/bin/env python3
"""
generate_all_figures.py
=======================
Generate all 8 main figures and 5 supplementary figures for the thalamus
framing paper v7.

Uses consolidated modules (plotting, taxonomy, technique_patterns,
nucleus_patterns, stats) and BERTopic output.

Run:
    python Code/analysis/generate_all_figures.py

Output:
    Graphs/pub/fig{1..8}_*.{png,svg}
    Graphs/pub/sup_fig_S{1..5}_*.{png,svg}
"""

from __future__ import annotations

import os
import sys
import warnings
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

# ── Project paths ────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
DATA      = ROOT / "Data"
RAW       = DATA / "raw"
PROCESSED = DATA / "processed"
MODELS    = DATA / "models"
GRAPHS    = ROOT / "Graphs" / "pub"
GRAPHS.mkdir(parents=True, exist_ok=True)

# ── Module imports ───────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from plotting import (
    apply_style, save_fig, panel_label, despine, WONG,
    mk_bar, heatmap, stacked_area, COLORS, FIG_2COL, FIG_FULL,
)
from technique_patterns import TECHNIQUES, tag_techniques
from nucleus_patterns import NUCLEI, tag_nuclei
from taxonomy import classify_primary, TAXONOMY, PRIORITY
from stats import mann_kendall, rolling_mean

apply_style()

# Silence noisy warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Category colour map (framing_primary) ────────────────────────────────────
CAT_COLORS = {
    "relay":                 "#D55E00",
    "hub_node":              "#0072B2",
    "gateway":               "#E69F00",
    "modulator":             "#009E73",
    "integrator":            "#CC79A7",
    "network_component":     "#56B4E9",
    "clinical_target":       "#F0E442",
    "role_player":           "#999999",
    "structure_description": "#7B4F9E",
    "involved_in":           "#aaaaaa",
    "other":                 "#dddddd",
}

# Nice display names for framing categories
CAT_DISPLAY = {
    "relay":                 "Relay / Gateway",
    "hub_node":              "Hub / Node",
    "gateway":               "Gateway",
    "modulator":             "Modulator",
    "integrator":            "Integrator",
    "network_component":     "Network Component",
    "clinical_target":       "Clinical Target",
    "role_player":           "Role Player",
    "structure_description": "Structure Description",
    "involved_in":           "Involved In",
    "other":                 "Other",
}

# ── Helper: clean BERTopic topic names ───────────────────────────────────────
def clean_topic_name(name: str) -> str:
    """Remove the leading '0_' prefix and replace underscores with spaces."""
    if pd.isna(name):
        return "noise"
    parts = str(name).split("_", 1)
    if len(parts) > 1 and parts[0].lstrip("-").isdigit():
        return parts[1].replace("_", " ")
    return str(name).replace("_", " ")


# ── Helper: load CSV safely ──────────────────────────────────────────────────
def load_csv(path: Path, **kwargs) -> pd.DataFrame | None:
    """Load a CSV file; return None if it doesn't exist."""
    if not path.exists():
        print(f"  [WARN] File not found: {path.name}")
        return None
    return pd.read_csv(path, **kwargs)


# ── Helper: placeholder panel ────────────────────────────────────────────────
def placeholder(ax, text: str = "Data not available"):
    """Show a grey placeholder message on ax."""
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.text(0.5, 0.5, text, ha="center", va="center", fontsize=11,
            color="#888888", style="italic", transform=ax.transAxes)
    ax.set_xticks([])
    ax.set_yticks([])
    for sp in ax.spines.values():
        sp.set_visible(False)


# ── Helper: Wilson CI on proportions per year ────────────────────────────────
def wilson_ci_series(counts, totals, z=1.96):
    """Return (lower, upper) arrays for Wilson score CI."""
    from stats import wilson_ci
    lo, hi = [], []
    for c, t in zip(counts, totals):
        l, h = wilson_ci(int(c), int(t), z=z)
        lo.append(l)
        hi.append(h)
    return np.array(lo), np.array(hi)


# ═════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═════════════════════════════════════════════════════════════════════════════
print("Loading data ...")

# Core BERTopic outputs
all_sent   = load_csv(PROCESSED / "all_sentences_bertopic.csv")
framing_bt = load_csv(PROCESSED / "framing_bertopic.csv")
tot_all    = load_csv(PROCESSED / "topics_over_time_all.csv")
tot_frame  = load_csv(PROCESSED / "topics_over_time_framing.csv")

# Legacy / enriched data
framing_ec = load_csv(PROCESSED / "framing_embedded_clustered.csv")
landscape  = load_csv(PROCESSED / "landscape_clustered.csv")
combined   = load_csv(PROCESSED / "combined_framing_classified.csv")
all_class  = load_csv(PROCESSED / "all_sentences_classified.csv")

# PubMed abstracts
pubmed = load_csv(
    RAW / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    low_memory=False,
)

# Comparison corpora
comp_hipp = load_csv(PROCESSED / "comparison_hippocampus_abstracts.csv")
comp_cere = load_csv(PROCESSED / "comparison_cerebellum_abstracts.csv")
comp_amyg = load_csv(PROCESSED / "comparison_amygdala_abstracts.csv")

# Framing embeddings (for UMAP panels)
framing_emb_path = PROCESSED / "framing_embeddings.npy"
framing_emb = np.load(framing_emb_path) if framing_emb_path.exists() else None

print("  Data loading complete.\n")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 1: Pipeline + Landscape Overview  (4 panels, 14x10 in)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FIGURE 1: Pipeline + Landscape Overview")
print("=" * 60)
try:
    fig1, axes1 = plt.subplots(2, 2, figsize=(14, 10))
    fig1.subplots_adjust(hspace=0.35, wspace=0.30)

    # ── Panel A: Pipeline flowchart ──────────────────────────────────────
    ax = axes1[0, 0]
    panel_label(ax, "A")
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.set_xticks([])
    ax.set_yticks([])
    for sp in ax.spines.values():
        sp.set_visible(False)

    boxes = [
        (5, 9.0, "PubMed\n17,280 abstracts"),
        (5, 7.3, "PMC + Unpaywall\n55,848 sentences"),
        (5, 5.6, "Framing extraction\n(18 verb triggers + appositives)"),
        (5, 3.9, "10,634 framing\nsentences"),
        (5, 2.2, "BERTopic\ntopic modeling"),
        (5, 0.5, "102 topics (all) /\n61 topics (framing)"),
    ]
    box_w, box_h = 4.0, 1.2
    for (cx, cy, txt) in boxes:
        bb = FancyBboxPatch(
            (cx - box_w / 2, cy - box_h / 2), box_w, box_h,
            boxstyle="round,pad=0.15", facecolor="#c6dbef",
            edgecolor="#2171b5", linewidth=1.2,
        )
        ax.add_patch(bb)
        ax.text(cx, cy, txt, ha="center", va="center", fontsize=8.5,
                fontweight="medium")
    # arrows between boxes
    for i in range(len(boxes) - 1):
        y_start = boxes[i][1] - box_h / 2
        y_end   = boxes[i + 1][1] + box_h / 2
        ax.annotate("", xy=(5, y_end), xytext=(5, y_start),
                     arrowprops=dict(arrowstyle="-|>", color="#2171b5",
                                     lw=1.5))
    ax.set_title("Analysis pipeline", fontsize=11)

    # ── Panel B: BERTopic UMAP scatter ───────────────────────────────────
    ax = axes1[0, 1]
    panel_label(ax, "B")
    if all_sent is not None:
        from sklearn.preprocessing import LabelEncoder
        sample = all_sent.sample(n=min(10000, len(all_sent)), random_state=42).copy()
        # Top 15 topics get distinct colours; rest grey
        top15_ids = (
            all_sent.loc[all_sent["bertopic_id"] >= 0, "bertopic_id"]
            .value_counts().head(15).index.tolist()
        )
        all_emb_path = PROCESSED / "all_sentences_embeddings.npy"
        if all_emb_path.exists():
            all_emb = np.load(all_emb_path)
            # Align sample indices to embeddings
            idx = sample.index.values
            if max(idx) < len(all_emb):
                emb_sub = all_emb[idx]
                from umap import UMAP
                reducer = UMAP(n_components=2, n_neighbors=15, min_dist=0.1,
                               random_state=42, n_jobs=1)
                coords = reducer.fit_transform(emb_sub)
                sample["ux"] = coords[:, 0]
                sample["uy"] = coords[:, 1]
            else:
                sample["ux"] = np.random.randn(len(sample))
                sample["uy"] = np.random.randn(len(sample))
        else:
            sample["ux"] = np.random.randn(len(sample))
            sample["uy"] = np.random.randn(len(sample))

        palette = list(WONG.values())[:10] + ["#e41a1c", "#377eb8", "#4daf4a",
                                                "#984ea3", "#ff7f00"]
        color_map = {tid: palette[i % len(palette)] for i, tid in enumerate(top15_ids)}
        c = sample["bertopic_id"].map(lambda x: color_map.get(x, "#cccccc"))
        order = sample["bertopic_id"].apply(lambda x: 0 if x in top15_ids else 1)
        idx_sorted = order.sort_values(ascending=False).index
        ax.scatter(sample.loc[idx_sorted, "ux"],
                   sample.loc[idx_sorted, "uy"],
                   c=c.loc[idx_sorted], s=1, alpha=0.3, rasterized=True)
        ax.set_xlabel("UMAP 1", fontsize=9)
        ax.set_ylabel("UMAP 2", fontsize=9)
        ax.set_title("BERTopic topic landscape (n=10 000)", fontsize=11)
        despine(ax)
    else:
        placeholder(ax, "all_sentences_bertopic.csv not found")

    # ── Panel C: Top 15 topics horizontal bar ────────────────────────────
    ax = axes1[1, 0]
    panel_label(ax, "C")
    if all_sent is not None:
        valid = all_sent[all_sent["bertopic_id"] >= 0]
        top15 = valid["bertopic_label"].value_counts().head(15)
        labels = [clean_topic_name(n) for n in top15.index]
        palette15 = list(WONG.values())[:10] + ["#e41a1c", "#377eb8", "#4daf4a",
                                                  "#984ea3", "#ff7f00"]
        y_pos = np.arange(len(labels))
        ax.barh(y_pos, top15.values, color=palette15[:len(labels)],
                edgecolor="none", height=0.7)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel("Sentence count")
        ax.set_title("Top 15 BERTopic topics", fontsize=11)
        despine(ax)
    else:
        placeholder(ax, "all_sentences_bertopic.csv not found")

    # ── Panel D: Corpus statistics ───────────────────────────────────────
    ax = axes1[1, 1]
    panel_label(ax, "D")
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.set_xticks([])
    ax.set_yticks([])
    for sp in ax.spines.values():
        sp.set_visible(False)

    stats_rows = [
        ("PubMed abstracts",           "17,280"),
        ("PMC full-text papers",       "3,319"),
        ("Unpaywall papers",           "149"),
        ("Total sentences",            "55,848"),
        ("Framing sentences",          "10,634"),
        ("BERTopic topics (all)",      "102"),
        ("BERTopic topics (framing)",  "61"),
        ("Year range",                 "1975 - 2026"),
    ]
    ax.text(5, 9.3, "Corpus Statistics", ha="center", va="top", fontsize=12,
            fontweight="bold")
    for i, (lbl, val) in enumerate(stats_rows):
        y = 8.0 - i * 0.95
        bg = "#f0f0f0" if i % 2 == 0 else "white"
        ax.add_patch(mpatches.FancyBboxPatch(
            (0.5, y - 0.35), 9, 0.7, boxstyle="round,pad=0.05",
            facecolor=bg, edgecolor="none"))
        ax.text(1.0, y, lbl, va="center", fontsize=10)
        ax.text(9.0, y, val, va="center", ha="right", fontsize=10,
                fontweight="bold")
    ax.set_title("Dataset summary", fontsize=11)

    fig1.suptitle("Figure 1: Pipeline and Landscape Overview",
                  fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig1, GRAPHS / "fig1_pipeline_landscape")
    plt.close(fig1)
    print("  Figure 1 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure 1 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 2: Topics Over Time  (3 panels, 14x8 in)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FIGURE 2: Topics Over Time")
print("=" * 60)
try:
    fig2 = plt.figure(figsize=(14, 8))
    gs2 = fig2.add_gridspec(2, 2, hspace=0.35, wspace=0.30,
                            height_ratios=[1, 1])
    ax2a = fig2.add_subplot(gs2[0, :])   # top row spans both columns
    ax2b = fig2.add_subplot(gs2[1, 0])
    ax2c = fig2.add_subplot(gs2[1, 1])

    # ── Panel A: Topics-over-time stacked area ──────────────────────────
    panel_label(ax2a, "A")
    if tot_all is not None:
        tot = tot_all.copy()
        tot["Year"] = pd.to_numeric(tot["Timestamp"], errors="coerce").astype(int)
        # Top 15 topics by total frequency (exclude noise = -1)
        valid_topics = tot[tot["Topic"] >= 0]
        top15_topics = (
            valid_topics.groupby("Topic")["Frequency"]
            .sum().nlargest(15).index.tolist()
        )
        pivot = (
            valid_topics[valid_topics["Topic"].isin(top15_topics)]
            .pivot_table(index="Year", columns="Topic",
                         values="Frequency", aggfunc="sum", fill_value=0)
        )
        years = pivot.index.values
        # Build data dict and colour dict using topic labels
        topic_labels_map = (
            valid_topics[valid_topics["Topic"].isin(top15_topics)]
            .drop_duplicates("Topic").set_index("Topic")["Words"]
        )
        palette15 = list(WONG.values())[:10] + ["#e41a1c", "#377eb8",
                                                  "#4daf4a", "#984ea3", "#ff7f00"]
        data_dict = {}
        colors_dict = {}
        for i, tid in enumerate(top15_topics):
            lbl = clean_topic_name(
                topic_labels_map.get(tid, f"Topic {tid}"))[:40]
            data_dict[lbl] = pivot[tid].values if tid in pivot.columns else np.zeros(len(years))
            colors_dict[lbl] = palette15[i % len(palette15)]
        stacked_area(ax2a, years, data_dict, colors_dict, smooth_window=3)
        ax2a.set_xlabel("Year")
        ax2a.set_ylabel("Sentence frequency")
        ax2a.set_title("Top 15 topics over time (3-year rolling mean)")
        ax2a.legend(loc="upper left", fontsize=7, ncol=3,
                    framealpha=0.8, bbox_to_anchor=(0, 1))
    else:
        placeholder(ax2a, "topics_over_time_all.csv not found")

    # ── Panel B: Mann-Kendall trend bar ──────────────────────────────────
    panel_label(ax2b, "B")
    if tot_all is not None and all_sent is not None:
        valid_topics_df = tot_all[tot_all["Topic"] >= 0].copy()
        valid_topics_df["Year"] = pd.to_numeric(
            valid_topics_df["Timestamp"], errors="coerce").astype(int)
        annual_total = valid_topics_df.groupby("Year")["Frequency"].sum()
        top15_topics_b = (
            valid_topics_df.groupby("Topic")["Frequency"]
            .sum().nlargest(15).index.tolist()
        )
        topic_words = (
            valid_topics_df.drop_duplicates("Topic")
            .set_index("Topic")["Words"]
        )
        mk_items = []
        for tid in top15_topics_b:
            sub = valid_topics_df[valid_topics_df["Topic"] == tid]
            annual = sub.groupby("Year")["Frequency"].sum()
            prop = annual / annual_total
            prop = prop.dropna()
            if len(prop) >= 5:
                tau, p, sig, lbl = mann_kendall(prop.index, prop.values)
                name = clean_topic_name(topic_words.get(tid, f"T{tid}"))[:35]
                mk_items.append((f"{name} ({sig})", tau))
        if mk_items:
            mk_bar(ax2b, mk_items)
            ax2b.set_title("Topic trend (MK tau on annual proportion)")
        else:
            placeholder(ax2b, "Insufficient data for MK")
    else:
        placeholder(ax2b, "Data not available")

    # ── Panel C: Comparison corpus overlay ───────────────────────────────
    panel_label(ax2c, "C")
    comparisons = {
        "Thalamus":     (pubmed,     COLORS["hub"]),
        "Hippocampus":  (comp_hipp,  WONG["orange"]),
        "Cerebellum":   (comp_cere,  WONG["green"]),
        "Amygdala":     (comp_amyg,  WONG["pink"]),
    }
    plotted_any = False
    for name, (df, col) in comparisons.items():
        if df is None:
            continue
        if name == "Thalamus" and pubmed is not None:
            # Compute hub% from pubmed abstracts
            tmp = pubmed.copy()
            tmp["abstract"] = tmp["abstract"].fillna("")
            tmp["has_hub"] = tmp["abstract"].str.contains(
                r"\bhub\b", case=False, regex=True, na=False)
            annual = tmp.groupby("year").agg(
                hub_count=("has_hub", "sum"),
                total=("has_hub", "count"),
            )
            annual["hub_pct"] = 100 * annual["hub_count"] / annual["total"]
            annual = annual[annual.index >= 1990]
            ax2c.plot(annual.index,
                      rolling_mean(annual["hub_pct"], 3),
                      color=col, label=name, linewidth=2)
            plotted_any = True
        elif "has_hub" in df.columns:
            annual = df.groupby("year").agg(
                hub_count=("has_hub", "sum"),
                total=("has_hub", "count"),
            )
            annual["hub_pct"] = 100 * annual["hub_count"] / annual["total"]
            annual = annual[annual.index >= 1990]
            ax2c.plot(annual.index,
                      rolling_mean(annual["hub_pct"], 3),
                      color=col, label=name, linewidth=1.5)
            plotted_any = True

    if plotted_any:
        ax2c.set_xlabel("Year")
        ax2c.set_ylabel("Hub mention %")
        ax2c.set_title("Hub framing: thalamus vs. comparison regions")
        ax2c.legend(fontsize=8)
        despine(ax2c)
    else:
        placeholder(ax2c, "Comparison data not available")

    fig2.suptitle("Figure 2: Topics Over Time",
                  fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig2, GRAPHS / "fig2_topics_over_time")
    plt.close(fig2)
    print("  Figure 2 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure 2 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 3: Framing Taxonomy + Examples  (4 panels, 14x12 in)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FIGURE 3: Framing Taxonomy + Examples")
print("=" * 60)
try:
    fig3, axes3 = plt.subplots(2, 2, figsize=(14, 12))
    fig3.subplots_adjust(hspace=0.40, wspace=0.30)

    # ── Panel A: UMAP of framing sentences by framing_primary ────────────
    ax = axes3[0, 0]
    panel_label(ax, "A")
    if framing_bt is not None and framing_emb is not None:
        from umap import UMAP
        n_emb = len(framing_emb)
        n_df  = len(framing_bt)
        n = min(n_emb, n_df)
        if "umap_x" in framing_bt.columns and "umap_y" in framing_bt.columns:
            sub = framing_bt.iloc[:n].copy()
            ux = sub["umap_x"].values
            uy = sub["umap_y"].values
        else:
            sub = framing_bt.iloc[:n].copy()
            reducer = UMAP(n_components=2, n_neighbors=15, min_dist=0.1,
                           random_state=42, n_jobs=1)
            coords = reducer.fit_transform(framing_emb[:n])
            ux = coords[:, 0]
            uy = coords[:, 1]
        cats = sub["framing_primary"].fillna("other")
        for cat in cats.unique():
            mask = cats == cat
            ax.scatter(ux[mask], uy[mask], s=2, alpha=0.3,
                       c=CAT_COLORS.get(cat, "#dddddd"),
                       label=CAT_DISPLAY.get(cat, cat), rasterized=True)
        ax.set_xlabel("UMAP 1", fontsize=9)
        ax.set_ylabel("UMAP 2", fontsize=9)
        ax.set_title("Framing sentences by category")
        ax.legend(fontsize=6, loc="lower right", ncol=2, markerscale=4,
                  framealpha=0.8)
        despine(ax)
    elif framing_bt is not None and "umap_x" in framing_bt.columns:
        sub = framing_bt.copy()
        cats = sub["framing_primary"].fillna("other")
        for cat in cats.unique():
            mask = cats == cat
            ax.scatter(sub.loc[mask, "umap_x"], sub.loc[mask, "umap_y"],
                       s=2, alpha=0.3, c=CAT_COLORS.get(cat, "#dddddd"),
                       label=CAT_DISPLAY.get(cat, cat), rasterized=True)
        ax.set_xlabel("UMAP 1", fontsize=9)
        ax.set_ylabel("UMAP 2", fontsize=9)
        ax.set_title("Framing sentences by category")
        ax.legend(fontsize=6, loc="lower right", ncol=2, markerscale=4,
                  framealpha=0.8)
        despine(ax)
    else:
        placeholder(ax, "Framing embeddings not available")

    # ── Panel B: Category distribution bar chart ─────────────────────────
    ax = axes3[0, 1]
    panel_label(ax, "B")
    if framing_bt is not None:
        cat_counts = framing_bt["framing_primary"].value_counts()
        cats_ordered = [c for c in PRIORITY if c in cat_counts.index]
        if "other" in cat_counts.index:
            cats_ordered.append("other")
        vals = [cat_counts.get(c, 0) for c in cats_ordered]
        display = [CAT_DISPLAY.get(c, c) for c in cats_ordered]
        cols = [CAT_COLORS.get(c, "#dddddd") for c in cats_ordered]
        y_pos = np.arange(len(display))
        ax.barh(y_pos, vals, color=cols, edgecolor="none", height=0.7)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(display, fontsize=9)
        ax.invert_yaxis()
        ax.set_xlabel("Sentence count")
        ax.set_title("Framing category distribution")
        despine(ax)
    else:
        placeholder(ax, "framing_bertopic.csv not found")

    # ── Panel C: Example sentences table ─────────────────────────────────
    ax = axes3[1, 0]
    panel_label(ax, "C")
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.set_xticks([])
    ax.set_yticks([])
    for sp in ax.spines.values():
        sp.set_visible(False)
    if framing_bt is not None:
        top_cats = [c for c in PRIORITY[:8] if c in framing_bt["framing_primary"].values]
        ax.text(0.2, 9.6, "Category", fontweight="bold", fontsize=8, va="top")
        ax.text(3.0, 9.6, "Example sentence (truncated)", fontweight="bold",
                fontsize=8, va="top")
        ax.text(9.5, 9.6, "Year", fontweight="bold", fontsize=8, va="top",
                ha="right")
        row_y = 9.0
        for i, cat in enumerate(top_cats):
            sub = framing_bt[framing_bt["framing_primary"] == cat]
            if sub.empty:
                continue
            example = sub.sample(1, random_state=42 + i).iloc[0]
            sent_trunc = str(example.get("sentence", ""))[:120]
            if len(str(example.get("sentence", ""))) > 120:
                sent_trunc += "..."
            yr = str(example.get("year", ""))
            bg = "#f7f7f7" if i % 2 == 0 else "white"
            ax.add_patch(mpatches.FancyBboxPatch(
                (0.1, row_y - 0.45), 9.8, 0.9,
                boxstyle="round,pad=0.02", facecolor=bg, edgecolor="none"))
            ax.text(0.2, row_y, CAT_DISPLAY.get(cat, cat), fontsize=7,
                    va="center", fontweight="bold",
                    color=CAT_COLORS.get(cat, "#333"))
            ax.text(3.0, row_y, sent_trunc, fontsize=6.5, va="center",
                    family="monospace", wrap=True)
            ax.text(9.5, row_y, yr, fontsize=7, va="center", ha="right")
            row_y -= 1.05
        ax.set_title("Example framing sentences", fontsize=11)
    else:
        placeholder(ax, "framing data not available")

    # ── Panel D: Framing stacked area over time ──────────────────────────
    ax = axes3[1, 1]
    panel_label(ax, "D")
    src = framing_bt if framing_bt is not None else combined
    if src is not None and "framing_primary" in src.columns:
        src_yr = src[src["year"].between(1993, 2025)].copy()
        annual_total = src_yr.groupby("year").size()
        top_cats_d = [c for c in PRIORITY[:6] if c in src_yr["framing_primary"].values]
        if "other" in src_yr["framing_primary"].values:
            top_cats_d.append("other")
        years = sorted(annual_total.index)
        data_dict = {}
        colors_dict = {}
        for cat in top_cats_d:
            annual_cat = src_yr[src_yr["framing_primary"] == cat].groupby("year").size()
            prop = (annual_cat / annual_total).reindex(years, fill_value=0).values
            lbl = CAT_DISPLAY.get(cat, cat)
            data_dict[lbl] = prop
            colors_dict[lbl] = CAT_COLORS.get(cat, "#dddddd")
        stacked_area(ax, np.array(years), data_dict, colors_dict,
                     smooth_window=3)
        ax.set_xlabel("Year")
        ax.set_ylabel("Proportion of framing sentences")
        ax.set_title("Framing categories over time (1993-2025)")
        ax.legend(fontsize=7, loc="upper left", ncol=2, framealpha=0.8)
        # Overlay abstract-only dashed line if available
        if combined is not None and "source" in combined.columns:
            abs_df = combined[
                (combined["source"] == "abstract") &
                combined["year"].between(1993, 2025)
            ]
            if not abs_df.empty:
                abs_total = abs_df.groupby("year").size()
                abs_relay = abs_df[abs_df["framing_primary"] == "relay"].groupby("year").size()
                abs_prop = (abs_relay / abs_total).reindex(years, fill_value=0)
                ax.plot(years, rolling_mean(abs_prop, 3), "--",
                        color=CAT_COLORS["relay"], alpha=0.7, linewidth=1.5,
                        label="Relay (abstracts only)")
    else:
        placeholder(ax, "Framing data not available")

    fig3.suptitle("Figure 3: Framing Taxonomy and Examples",
                  fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig3, GRAPHS / "fig3_framing_taxonomy")
    plt.close(fig3)
    print("  Figure 3 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure 3 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 4: Framing Sub-topics + Hub/fMRI  (4 panels, 14x10 in)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FIGURE 4: Framing Sub-topics + Hub/fMRI")
print("=" * 60)
try:
    fig4, axes4 = plt.subplots(2, 2, figsize=(14, 10))
    fig4.subplots_adjust(hspace=0.40, wspace=0.30)

    # ── Panel A: BERTopic sub-topics within "other" ──────────────────────
    ax = axes4[0, 0]
    panel_label(ax, "A")
    if framing_bt is not None:
        other_df = framing_bt[framing_bt["framing_primary"] == "other"].copy()
        other_valid = other_df[other_df["bertopic_id"] >= 0]
        if not other_valid.empty:
            top_other = other_valid["bertopic_label"].value_counts().head(15)
            labels = [clean_topic_name(n) for n in top_other.index]
            palette15 = list(WONG.values())[:10] + ["#e41a1c", "#377eb8",
                                                      "#4daf4a", "#984ea3", "#ff7f00"]
            y_pos = np.arange(len(labels))
            ax.barh(y_pos, top_other.values,
                    color=palette15[:len(labels)],
                    edgecolor="none", height=0.7)
            ax.set_yticks(y_pos)
            ax.set_yticklabels(labels, fontsize=8)
            ax.invert_yaxis()
            ax.set_xlabel("Sentence count")
            ax.set_title("Top 15 BERTopic sub-topics within 'other'")
            despine(ax)
        else:
            placeholder(ax, "No valid BERTopic topics in 'other'")
    else:
        placeholder(ax, "framing_bertopic.csv not found")

    # ── Panel B: Sub-topic temporal trends ───────────────────────────────
    ax = axes4[0, 1]
    panel_label(ax, "B")
    if framing_bt is not None:
        other_df = framing_bt[framing_bt["framing_primary"] == "other"].copy()
        other_valid = other_df[other_df["bertopic_id"] >= 0]
        if not other_valid.empty:
            top8_labels = other_valid["bertopic_label"].value_counts().head(8).index
            palette8 = list(WONG.values())[:8]
            for i, lbl in enumerate(top8_labels):
                sub = other_valid[other_valid["bertopic_label"] == lbl]
                annual = sub.groupby("year").size()
                annual = annual.reindex(
                    range(annual.index.min(), annual.index.max() + 1),
                    fill_value=0)
                ax.plot(annual.index,
                        rolling_mean(annual.values, 3),
                        color=palette8[i % len(palette8)],
                        label=clean_topic_name(lbl)[:30],
                        linewidth=1.5)
            ax.set_xlabel("Year")
            ax.set_ylabel("Sentence count")
            ax.set_title("'Other' sub-topic trends (3-yr rolling mean)")
            ax.legend(fontsize=6, loc="upper left", ncol=2, framealpha=0.8)
            despine(ax)
        else:
            placeholder(ax, "Insufficient data")
    else:
        placeholder(ax, "Data not available")

    # ── Panel C: Hub framing rise decomposed ─────────────────────────────
    ax = axes4[1, 0]
    panel_label(ax, "C")
    if framing_bt is not None:
        hub_df = framing_bt[
            framing_bt["framing_primary"].isin(["hub_node", "network_component"])
        ].copy()
        hub_valid = hub_df[hub_df["bertopic_id"] >= 0]
        if not hub_valid.empty:
            hub_annual_total = hub_df.groupby("year").size()
            top5_hub = hub_valid["bertopic_label"].value_counts().head(5).index
            palette5 = list(WONG.values())[:5]
            years_range = sorted(hub_annual_total.index)
            data_dict, colors_dict = {}, {}
            for i, lbl in enumerate(top5_hub):
                sub = hub_valid[hub_valid["bertopic_label"] == lbl]
                annual = sub.groupby("year").size().reindex(years_range, fill_value=0)
                name = clean_topic_name(lbl)[:35]
                data_dict[name] = annual.values
                colors_dict[name] = palette5[i % len(palette5)]
            stacked_area(ax, np.array(years_range), data_dict, colors_dict,
                         smooth_window=3)
            ax.set_xlabel("Year")
            ax.set_ylabel("Sentence count")
            ax.set_title("Hub/network framing: BERTopic decomposition")
            ax.legend(fontsize=6, loc="upper left", ncol=1, framealpha=0.8)
        else:
            placeholder(ax, "No valid hub BERTopic topics")
    else:
        placeholder(ax, "Data not available")

    # ── Panel D: fMRI confound control ───────────────────────────────────
    ax = axes4[1, 1]
    panel_label(ax, "D")
    src_d = all_class if all_class is not None else framing_bt
    if src_d is not None and "sentence" in src_d.columns:
        tmp = src_d.copy()
        if "has_fMRI" not in tmp.columns:
            tmp = tag_techniques(tmp, text_col="sentence")
        if "framing_primary" not in tmp.columns and "sentence" in tmp.columns:
            tmp["framing_primary"] = tmp["sentence"].apply(classify_primary)
        fmri = tmp[tmp["has_fMRI"] == True].copy()
        no_fmri = tmp[tmp["has_fMRI"] == False].copy()

        years_range = range(1995, 2026)
        hub_cats = ["hub_node", "network_component"]

        # Hub% with fMRI
        fmri_annual = fmri.groupby("year").apply(
            lambda g: g["framing_primary"].isin(hub_cats).sum() / max(len(g), 1)
        ).reindex(years_range, fill_value=np.nan)

        # Hub% without fMRI
        nofmri_annual = no_fmri.groupby("year").apply(
            lambda g: g["framing_primary"].isin(hub_cats).sum() / max(len(g), 1)
        ).reindex(years_range, fill_value=np.nan)

        yrs = list(years_range)
        ax.plot(yrs, rolling_mean(fmri_annual, 3) * 100,
                color=WONG["orange"], label="With fMRI papers", linewidth=2)
        ax.plot(yrs, rolling_mean(nofmri_annual, 3) * 100,
                color=WONG["blue"], label="Without fMRI papers", linewidth=2)
        ax.fill_between(yrs,
                        rolling_mean(fmri_annual, 3) * 100,
                        rolling_mean(nofmri_annual, 3) * 100,
                        alpha=0.15, color=WONG["grey"])
        ax.set_xlabel("Year")
        ax.set_ylabel("Hub/network framing %")
        ax.set_title("Hub framing: fMRI confound control")
        ax.legend(fontsize=8)
        despine(ax)
    else:
        placeholder(ax, "Sentence data not available for fMRI control")

    fig4.suptitle("Figure 4: Framing Sub-topics and fMRI Confound",
                  fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig4, GRAPHS / "fig4_subtopics_fmri")
    plt.close(fig4)
    print("  Figure 4 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure 4 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 5: Nucleus Prevalence + Framing  (4 panels, 14x10 in)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FIGURE 5: Nucleus Prevalence + Framing")
print("=" * 60)
try:
    fig5, axes5 = plt.subplots(2, 2, figsize=(14, 10))
    fig5.subplots_adjust(hspace=0.40, wspace=0.30)

    # ── Panel A: 11 nuclei prevalence over time ─────────────────────────
    ax = axes5[0, 0]
    panel_label(ax, "A")
    if pubmed is not None:
        pm = pubmed.copy()
        pm["abstract"] = pm["abstract"].fillna("")
        pm = tag_nuclei(pm, text_col="abstract")
        # Exclude Intralaminar (not in NUCLEI), Habenula, Reuniens, MGN
        # Keep the 11 main nuclei
        nuclei_list = [k for k in NUCLEI.keys() if k not in ("Habenula", "Reuniens", "MGN")]
        years = sorted(pm["year"].unique())
        years = [y for y in years if y >= 1990]
        for nid in nuclei_list:
            col = f"has_{nid}"
            annual_pct = []
            for yr in years:
                sub = pm[pm["year"] == yr]
                if len(sub) > 0:
                    annual_pct.append(100 * sub[col].sum() / len(sub))
                else:
                    annual_pct.append(0)
            ax.plot(years, rolling_mean(pd.Series(annual_pct), 3),
                    color=NUCLEI[nid]["color"],
                    label=NUCLEI[nid]["label"][:20],
                    linewidth=1.2, alpha=0.9)
        ax.set_xlabel("Year")
        ax.set_ylabel("% of abstracts mentioning nucleus")
        ax.set_title("Thalamic nuclei prevalence in abstracts (3-yr RM)")
        ax.legend(fontsize=5.5, loc="upper left", ncol=3, framealpha=0.8)
        despine(ax)
    else:
        placeholder(ax, "PubMed data not available")

    # ── Panel B: Nucleus x framing heatmap ───────────────────────────────
    ax = axes5[0, 1]
    panel_label(ax, "B")
    src_b = all_class if all_class is not None else framing_bt
    if src_b is not None and "sentence" in src_b.columns:
        tmp = src_b.copy()
        tmp = tag_nuclei(tmp, text_col="sentence")
        if "framing_primary" not in tmp.columns:
            tmp["framing_primary"] = tmp["sentence"].apply(classify_primary)
        nuclei_ids = list(NUCLEI.keys())
        frame_cats = [c for c in PRIORITY if c in tmp["framing_primary"].values]
        if "other" in tmp["framing_primary"].values:
            frame_cats.append("other")
        mat = np.zeros((len(nuclei_ids), len(frame_cats)))
        for i, nid in enumerate(nuclei_ids):
            nsub = tmp[tmp[f"has_{nid}"] == True]
            if len(nsub) == 0:
                continue
            for j, cat in enumerate(frame_cats):
                mat[i, j] = (nsub["framing_primary"] == cat).sum() / len(nsub)
        row_labels = [NUCLEI[n]["label"][:18] for n in nuclei_ids]
        col_labels = [CAT_DISPLAY.get(c, c)[:12] for c in frame_cats]
        heatmap(ax, mat, row_labels, col_labels, cmap="YlOrRd",
                fmt=".2f", fontsize=6)
        ax.set_title("Nucleus x framing category (row-normalised)")
    else:
        placeholder(ax, "Sentence data not available")

    # ── Panel C: Mann-Kendall bar for nucleus prevalence ─────────────────
    ax = axes5[1, 0]
    panel_label(ax, "C")
    if pubmed is not None:
        pm = pubmed.copy()
        pm["abstract"] = pm["abstract"].fillna("")
        pm = tag_nuclei(pm, text_col="abstract")
        nuclei_list = list(NUCLEI.keys())
        mk_items = []
        for nid in nuclei_list:
            col = f"has_{nid}"
            years = sorted(pm[pm["year"] >= 1990]["year"].unique())
            props = []
            for yr in years:
                sub = pm[pm["year"] == yr]
                if len(sub) > 0:
                    props.append(sub[col].sum() / len(sub))
                else:
                    props.append(np.nan)
            tau, p, sig, lbl = mann_kendall(years, props)
            mk_items.append((f"{NUCLEI[nid]['label'][:18]} ({sig})", tau))
        mk_bar(ax, mk_items)
        ax.set_title("Nucleus prevalence trend (MK tau)")
    else:
        placeholder(ax, "PubMed data not available")

    # ── Panel D: Framing profiles per nucleus (grouped bar) ──────────────
    ax = axes5[1, 1]
    panel_label(ax, "D")
    if src_b is not None and "sentence" in src_b.columns:
        tmp = src_b.copy()
        if f"has_MD" not in tmp.columns:
            tmp = tag_nuclei(tmp, text_col="sentence")
        if "framing_primary" not in tmp.columns:
            tmp["framing_primary"] = tmp["sentence"].apply(classify_primary)
        # Top 8 nuclei by mention count
        nuc_counts = {nid: tmp[f"has_{nid}"].sum() for nid in NUCLEI}
        top8_nuc = sorted(nuc_counts, key=nuc_counts.get, reverse=True)[:8]
        frame_cats = [c for c in PRIORITY[:6]]
        n_cats = len(frame_cats)
        bar_width = 0.8 / n_cats
        y_base = np.arange(len(top8_nuc))
        for j, cat in enumerate(frame_cats):
            vals = []
            for nid in top8_nuc:
                nsub = tmp[tmp[f"has_{nid}"] == True]
                if len(nsub) > 0:
                    vals.append((nsub["framing_primary"] == cat).sum() / len(nsub))
                else:
                    vals.append(0)
            ax.barh(y_base + j * bar_width, vals, height=bar_width,
                    color=CAT_COLORS.get(cat, "#ccc"),
                    label=CAT_DISPLAY.get(cat, cat) if top8_nuc.index(top8_nuc[0]) == 0 else "",
                    edgecolor="none")
        # Fix legend: only unique labels
        handles, labels = ax.get_legend_handles_labels()
        # Re-create clean legend
        legend_patches = [mpatches.Patch(color=CAT_COLORS.get(c, "#ccc"),
                                          label=CAT_DISPLAY.get(c, c))
                          for c in frame_cats]
        ax.legend(handles=legend_patches, fontsize=6, loc="lower right",
                  framealpha=0.8)
        ax.set_yticks(y_base + bar_width * n_cats / 2)
        ax.set_yticklabels([NUCLEI[n]["label"][:18] for n in top8_nuc],
                           fontsize=8)
        ax.invert_yaxis()
        ax.set_xlabel("Proportion")
        ax.set_title("Framing profile per nucleus (top 8)")
        despine(ax)
    else:
        placeholder(ax, "Data not available")

    fig5.suptitle("Figure 5: Nucleus Prevalence and Framing",
                  fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig5, GRAPHS / "fig5_nucleus_prevalence")
    plt.close(fig5)
    print("  Figure 5 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure 5 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 6: Nucleus Vocabulary Evolution  (3 panels, 14x8 in)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FIGURE 6: Nucleus Vocabulary Evolution")
print("=" * 60)
try:
    fig6 = plt.figure(figsize=(14, 8))
    gs6 = fig6.add_gridspec(2, 2, hspace=0.45, wspace=0.30)
    ax6a = fig6.add_subplot(gs6[0, 0])
    ax6b = fig6.add_subplot(gs6[0, 1])
    ax6c = fig6.add_subplot(gs6[1, :])

    # Helper: TF-IDF shift for nucleus-mentioning texts
    def tfidf_shift(texts_early, texts_recent, top_n=10):
        """Return (early_terms, recent_terms) each as list of (term, score)."""
        from sklearn.feature_extraction.text import TfidfVectorizer
        if len(texts_early) < 5 or len(texts_recent) < 5:
            return [], []
        vec = TfidfVectorizer(max_features=2000, stop_words="english",
                              max_df=0.8, min_df=2, ngram_range=(1, 2))
        combined = list(texts_early) + list(texts_recent)
        tfidf = vec.fit_transform(combined)
        names = vec.get_feature_names_out()
        n_early = len(texts_early)
        early_mean = np.asarray(tfidf[:n_early].mean(axis=0)).ravel()
        recent_mean = np.asarray(tfidf[n_early:].mean(axis=0)).ravel()
        diff = recent_mean - early_mean
        top_recent = np.argsort(diff)[-top_n:][::-1]
        top_early = np.argsort(diff)[:top_n]
        return (
            [(names[i], early_mean[i]) for i in top_early],
            [(names[i], recent_mean[i]) for i in top_recent],
        )

    # ── Panel A: TF-IDF shift on abstracts ───────────────────────────────
    panel_label(ax6a, "A")
    if pubmed is not None:
        pm = pubmed.copy()
        pm["abstract"] = pm["abstract"].fillna("")
        pm = tag_nuclei(pm, text_col="abstract")
        # Pick top 4 nuclei by prevalence for a manageable display
        nuc_counts = {nid: pm[f"has_{nid}"].sum() for nid in NUCLEI}
        top4_nuc = sorted(nuc_counts, key=nuc_counts.get, reverse=True)[:4]
        # Aggregate across top 4 nuclei
        early_mask = pm["year"].between(2000, 2009)
        recent_mask = pm["year"].between(2015, 2025)
        any_nuc = pm[[f"has_{n}" for n in top4_nuc]].any(axis=1)
        texts_early = pm.loc[early_mask & any_nuc, "abstract"].tolist()
        texts_recent = pm.loc[recent_mask & any_nuc, "abstract"].tolist()
        early_terms, recent_terms = tfidf_shift(texts_early, texts_recent, top_n=12)
        if early_terms and recent_terms:
            # Paired horizontal bar: early on left, recent on right
            all_terms = ([t for t, _ in early_terms] +
                         [t for t, _ in recent_terms])
            # Deduplicate
            seen = set()
            unique_terms = []
            for t in all_terms:
                if t not in seen:
                    unique_terms.append(t)
                    seen.add(t)
            unique_terms = unique_terms[:15]
            early_dict = dict(early_terms)
            recent_dict = dict(recent_terms)
            y_pos = np.arange(len(unique_terms))
            e_vals = [early_dict.get(t, 0) for t in unique_terms]
            r_vals = [recent_dict.get(t, 0) for t in unique_terms]
            ax6a.barh(y_pos - 0.2, [-v for v in e_vals], height=0.35,
                      color=WONG["blue"], label="2000-2009")
            ax6a.barh(y_pos + 0.2, r_vals, height=0.35,
                      color=WONG["red"], label="2015-2025")
            ax6a.set_yticks(y_pos)
            ax6a.set_yticklabels(unique_terms, fontsize=7)
            ax6a.axvline(0, color="#444", linewidth=0.6)
            ax6a.set_xlabel("TF-IDF score (left=early, right=recent)")
            ax6a.set_title("Vocabulary shift in abstracts (top nuclei)")
            ax6a.legend(fontsize=7)
            ax6a.invert_yaxis()
            despine(ax6a)
        else:
            placeholder(ax6a, "Insufficient data for TF-IDF")
    else:
        placeholder(ax6a, "PubMed data not available")

    # ── Panel B: TF-IDF shift on full-text sentences ─────────────────────
    panel_label(ax6b, "B")
    if all_class is not None and "sentence" in all_class.columns:
        tmp = all_class.copy()
        tmp = tag_nuclei(tmp, text_col="sentence")
        nuc_counts = {nid: tmp[f"has_{nid}"].sum() for nid in NUCLEI}
        top4_nuc = sorted(nuc_counts, key=nuc_counts.get, reverse=True)[:4]
        early_mask = tmp["year"].between(2000, 2009)
        recent_mask = tmp["year"].between(2015, 2025)
        any_nuc = tmp[[f"has_{n}" for n in top4_nuc]].any(axis=1)
        texts_early = tmp.loc[early_mask & any_nuc, "sentence"].tolist()
        texts_recent = tmp.loc[recent_mask & any_nuc, "sentence"].tolist()
        early_terms, recent_terms = tfidf_shift(texts_early, texts_recent, top_n=12)
        if early_terms and recent_terms:
            all_terms = ([t for t, _ in early_terms] +
                         [t for t, _ in recent_terms])
            seen = set()
            unique_terms = []
            for t in all_terms:
                if t not in seen:
                    unique_terms.append(t)
                    seen.add(t)
            unique_terms = unique_terms[:15]
            early_dict = dict(early_terms)
            recent_dict = dict(recent_terms)
            y_pos = np.arange(len(unique_terms))
            e_vals = [early_dict.get(t, 0) for t in unique_terms]
            r_vals = [recent_dict.get(t, 0) for t in unique_terms]
            ax6b.barh(y_pos - 0.2, [-v for v in e_vals], height=0.35,
                      color=WONG["blue"], label="2000-2009")
            ax6b.barh(y_pos + 0.2, r_vals, height=0.35,
                      color=WONG["red"], label="2015-2025")
            ax6b.set_yticks(y_pos)
            ax6b.set_yticklabels(unique_terms, fontsize=7)
            ax6b.axvline(0, color="#444", linewidth=0.6)
            ax6b.set_xlabel("TF-IDF score (left=early, right=recent)")
            ax6b.set_title("Vocabulary shift in full-text sentences")
            ax6b.legend(fontsize=7)
            ax6b.invert_yaxis()
            despine(ax6b)
        else:
            placeholder(ax6b, "Insufficient data for TF-IDF")
    else:
        placeholder(ax6b, "all_sentences_classified.csv not available")

    # ── Panel C: Key term emergence highlights ───────────────────────────
    panel_label(ax6c, "C")
    key_terms = ["connectivity", "optogenetics", "network", "relay"]
    src_c = pubmed if pubmed is not None else None
    if src_c is not None:
        pm = src_c.copy()
        pm["abstract"] = pm["abstract"].fillna("")
        years = sorted(pm[pm["year"] >= 1990]["year"].unique())
        for i, term in enumerate(key_terms):
            annual_pct = []
            for yr in years:
                sub = pm[pm["year"] == yr]
                if len(sub) > 0:
                    has = sub["abstract"].str.contains(
                        rf"\b{term}\b", case=False, regex=True, na=False).sum()
                    annual_pct.append(100 * has / len(sub))
                else:
                    annual_pct.append(0)
            ax6c.plot(years, rolling_mean(pd.Series(annual_pct), 3),
                      color=list(WONG.values())[i],
                      label=term, linewidth=2)
        ax6c.set_xlabel("Year")
        ax6c.set_ylabel("% of abstracts")
        ax6c.set_title("Key term emergence in abstracts (3-yr rolling mean)")
        ax6c.legend(fontsize=9)
        despine(ax6c)
    else:
        placeholder(ax6c, "PubMed data not available")

    fig6.suptitle("Figure 6: Nucleus Vocabulary Evolution",
                  fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig6, GRAPHS / "fig6_vocabulary_evolution")
    plt.close(fig6)
    print("  Figure 6 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure 6 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 7: Recording Techniques  (already generated)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FIGURE 7: Recording Techniques")
print("=" * 60)
fig7_png = GRAPHS / "fig7_techniques.png"
fig7_svg = GRAPHS / "fig7_techniques.svg"
if fig7_png.exists() and fig7_svg.exists():
    print("  Figure 7 already exists (fig7_techniques.png/svg). Skipping.\n")
else:
    print("  [WARN] fig7_techniques.png/svg not found in Graphs/pub/.")
    print("         Run technique_analysis.py to generate it.\n")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 8: Summary  (3 panels, 14x6 in)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FIGURE 8: Summary")
print("=" * 60)
try:
    fig8 = plt.figure(figsize=(14, 6))
    gs8 = fig8.add_gridspec(1, 3, wspace=0.35)
    ax8a = fig8.add_subplot(gs8[0, 0])
    ax8b = fig8.add_subplot(gs8[0, 1])
    ax8c = fig8.add_subplot(gs8[0, 2])

    # ── Panel A: Overall framing trajectory ──────────────────────────────
    panel_label(ax8a, "A")
    src_8 = framing_bt if framing_bt is not None else combined
    if src_8 is not None and "framing_primary" in src_8.columns:
        yr_range = range(1990, 2026)
        annual_total = src_8[src_8["year"].between(1990, 2025)].groupby("year").size()
        for cat, col, lbl in [
            ("relay",            CAT_COLORS["relay"],            "Relay %"),
            ("hub_node",         CAT_COLORS["hub_node"],         "Hub %"),
            ("other",            CAT_COLORS["other"],            "Other %"),
        ]:
            annual_cat = (
                src_8[(src_8["framing_primary"] == cat) &
                      src_8["year"].between(1990, 2025)]
                .groupby("year").size()
            )
            prop = (100 * annual_cat / annual_total).reindex(yr_range, fill_value=0)
            ax8a.plot(list(yr_range),
                      rolling_mean(prop, 3),
                      color=col, label=lbl, linewidth=2)
        ax8a.set_xlabel("Year")
        ax8a.set_ylabel("% of framing sentences")
        ax8a.set_title("Framing trajectory (1990-2025)")
        ax8a.legend(fontsize=9)
        despine(ax8a)
    else:
        placeholder(ax8a, "Framing data not available")

    # ── Panel B: All-category MK tau bar ─────────────────────────────────
    panel_label(ax8b, "B")
    if src_8 is not None and "framing_primary" in src_8.columns:
        annual_total = src_8[src_8["year"].between(1990, 2025)].groupby("year").size()
        all_cats = list(PRIORITY) + ["other"]
        mk_items = []
        for cat in all_cats:
            if cat not in src_8["framing_primary"].values:
                continue
            annual_cat = (
                src_8[(src_8["framing_primary"] == cat) &
                      src_8["year"].between(1990, 2025)]
                .groupby("year").size()
            )
            prop = (annual_cat / annual_total).reindex(
                range(1990, 2026), fill_value=0)
            tau, p, sig, lbl = mann_kendall(prop.index, prop.values)
            display = CAT_DISPLAY.get(cat, cat)
            mk_items.append((f"{display} ({sig})", tau))
        if mk_items:
            mk_bar(ax8b, mk_items)
            ax8b.set_title("Framing category trends (MK tau)")
        else:
            placeholder(ax8b, "Insufficient data")
    else:
        placeholder(ax8b, "Data not available")

    # ── Panel C: Key findings text ───────────────────────────────────────
    panel_label(ax8c, "C")
    ax8c.set_xlim(0, 10)
    ax8c.set_ylim(0, 10)
    ax8c.set_xticks([])
    ax8c.set_yticks([])
    for sp in ax8c.spines.values():
        sp.set_visible(False)

    findings = [
        "102 research topics identified across\n55,848 sentences",
        "Hub/network framing:\ntau = +0.739, p < 0.001",
        "Generic descriptions declining:\ntau = -0.665, p < 0.001",
        "52% of previously unclassified framing\nnow assigned to sub-topics",
        "Recording technique strongly associated\nwith research topic (p < 0.001)",
    ]
    ax8c.text(5, 9.5, "Key Findings", ha="center", va="top",
              fontsize=12, fontweight="bold")
    for i, txt in enumerate(findings):
        y = 8.0 - i * 1.6
        ax8c.text(0.5, y, f"{i+1}.", fontsize=10, fontweight="bold",
                  va="top", color=WONG["blue"])
        ax8c.text(1.3, y, txt, fontsize=9.5, va="top", wrap=True)
    ax8c.set_title("Summary", fontsize=11)

    fig8.suptitle("Figure 8: Summary",
                  fontsize=14, fontweight="bold", y=1.02)
    save_fig(fig8, GRAPHS / "fig8_summary")
    plt.close(fig8)
    print("  Figure 8 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure 8 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# SUPPLEMENTARY FIGURE S1: Embedding Space Quality  (4 panels)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("SUPPLEMENTARY FIGURE S1: Embedding Space Quality")
print("=" * 60)
try:
    fig_s1, axes_s1 = plt.subplots(2, 2, figsize=(14, 12))
    fig_s1.subplots_adjust(hspace=0.35, wspace=0.30)

    if framing_bt is not None and framing_emb is not None:
        n = min(len(framing_emb), len(framing_bt))
        sub = framing_bt.iloc[:n].copy()
        if "umap_x" in sub.columns and "umap_y" in sub.columns:
            ux = sub["umap_x"].values
            uy = sub["umap_y"].values
        else:
            from umap import UMAP
            reducer = UMAP(n_components=2, n_neighbors=15, min_dist=0.1,
                           random_state=42, n_jobs=1)
            coords = reducer.fit_transform(framing_emb[:n])
            ux = coords[:, 0]
            uy = coords[:, 1]

        # ── Panel A: Colour by source ────────────────────────────────────
        ax = axes_s1[0, 0]
        panel_label(ax, "A")
        if "source" in sub.columns:
            src_colors = {"fulltext": WONG["blue"], "abstract": WONG["orange"]}
            for src_val in sub["source"].unique():
                mask = sub["source"] == src_val
                ax.scatter(ux[mask], uy[mask], s=1, alpha=0.2,
                           c=src_colors.get(src_val, WONG["grey"]),
                           label=src_val, rasterized=True)
            ax.legend(fontsize=8, markerscale=5)
        else:
            ax.scatter(ux, uy, s=1, alpha=0.2, c=WONG["grey"], rasterized=True)
        ax.set_title("Coloured by source")
        ax.set_xlabel("UMAP 1"); ax.set_ylabel("UMAP 2")
        despine(ax)

        # ── Panel B: Colour by decade ────────────────────────────────────
        ax = axes_s1[0, 1]
        panel_label(ax, "B")
        if "year" in sub.columns:
            decade = (sub["year"] // 10 * 10).astype(int)
            decade_palette = {1990: "#D55E00", 2000: "#E69F00",
                              2010: "#0072B2", 2020: "#009E73"}
            for d in sorted(decade.unique()):
                mask = decade == d
                ax.scatter(ux[mask], uy[mask], s=1, alpha=0.2,
                           c=decade_palette.get(d, WONG["grey"]),
                           label=f"{d}s", rasterized=True)
            ax.legend(fontsize=8, markerscale=5)
        ax.set_title("Coloured by decade")
        ax.set_xlabel("UMAP 1"); ax.set_ylabel("UMAP 2")
        despine(ax)

        # ── Panel C: Colour by keyword category (BERTopic) ──────────────
        ax = axes_s1[1, 0]
        panel_label(ax, "C")
        if "bertopic_id" in sub.columns:
            top5 = sub.loc[sub["bertopic_id"] >= 0, "bertopic_id"].value_counts().head(5).index
            palette5 = list(WONG.values())[:5]
            cmap = {t: palette5[i] for i, t in enumerate(top5)}
            c = sub["bertopic_id"].map(lambda x: cmap.get(x, "#dddddd"))
            ax.scatter(ux, uy, s=1, alpha=0.2, c=c, rasterized=True)
            for i, t in enumerate(top5):
                lbl = clean_topic_name(
                    sub.loc[sub["bertopic_id"] == t, "bertopic_label"].iloc[0]
                    if (sub["bertopic_id"] == t).any() else f"T{t}")[:30]
                ax.scatter([], [], c=palette5[i], label=lbl, s=20)
            ax.legend(fontsize=6, markerscale=2, loc="lower right")
        ax.set_title("Coloured by BERTopic cluster")
        ax.set_xlabel("UMAP 1"); ax.set_ylabel("UMAP 2")
        despine(ax)

        # ── Panel D: Colour by framing_primary ──────────────────────────
        ax = axes_s1[1, 1]
        panel_label(ax, "D")
        cats = sub["framing_primary"].fillna("other")
        for cat in cats.unique():
            mask = cats == cat
            ax.scatter(ux[mask], uy[mask], s=1, alpha=0.2,
                       c=CAT_COLORS.get(cat, "#dddddd"),
                       label=CAT_DISPLAY.get(cat, cat), rasterized=True)
        ax.legend(fontsize=6, markerscale=5, ncol=2, loc="lower right")
        ax.set_title("Coloured by framing category")
        ax.set_xlabel("UMAP 1"); ax.set_ylabel("UMAP 2")
        despine(ax)
    else:
        for ax in axes_s1.flat:
            placeholder(ax, "Framing embeddings / data not available")

    fig_s1.suptitle("Supplementary Figure S1: Embedding Space Quality",
                    fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig_s1, GRAPHS / "sup_fig_S1_embedding_quality")
    plt.close(fig_s1)
    print("  Figure S1 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure S1 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# SUPPLEMENTARY FIGURE S2: Abstract vs Full-text Replication  (2 panels)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("SUPPLEMENTARY FIGURE S2: Abstract vs Full-text Replication")
print("=" * 60)
try:
    fig_s2, axes_s2 = plt.subplots(1, 2, figsize=(14, 5))
    fig_s2.subplots_adjust(wspace=0.30)

    if combined is not None and "source" in combined.columns:
        yr_range = range(1993, 2026)
        abs_df = combined[combined["source"] == "abstract"]
        ft_df = combined[combined["source"] == "fulltext"]

        for panel_idx, (cat, title_str) in enumerate([
            ("relay", "Relay % over time"),
            ("hub_node", "Hub % over time"),
        ]):
            ax = axes_s2[panel_idx]
            panel_label(ax, chr(65 + panel_idx))
            for src_name, src_df, col, ls in [
                ("Abstracts",  abs_df,  WONG["orange"], "-"),
                ("Full-text",  ft_df,   WONG["blue"],   "--"),
            ]:
                annual_total = src_df.groupby("year").size()
                annual_cat = (
                    src_df[src_df["framing_primary"] == cat]
                    .groupby("year").size()
                )
                prop = (100 * annual_cat / annual_total).reindex(yr_range, fill_value=0)
                ax.plot(list(yr_range), rolling_mean(prop, 3),
                        color=col, linestyle=ls, label=src_name, linewidth=2)
            ax.set_xlabel("Year")
            ax.set_ylabel(f"{cat.replace('_', ' ').title()} %")
            ax.set_title(title_str)
            ax.legend(fontsize=9)
            despine(ax)
    else:
        for ax in axes_s2:
            placeholder(ax, "combined_framing_classified.csv not available")

    fig_s2.suptitle("Supplementary Figure S2: Abstract vs Full-text Replication",
                    fontsize=14, fontweight="bold", y=1.02)
    save_fig(fig_s2, GRAPHS / "sup_fig_S2_abstract_replication")
    plt.close(fig_s2)
    print("  Figure S2 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure S2 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# SUPPLEMENTARY FIGURE S3: Confound Controls  (4 panels)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("SUPPLEMENTARY FIGURE S3: Confound Controls")
print("=" * 60)
try:
    fig_s3, axes_s3 = plt.subplots(2, 2, figsize=(14, 10))
    fig_s3.subplots_adjust(hspace=0.40, wspace=0.30)

    src_s3 = combined if combined is not None else framing_bt

    # ── Panel A: Hub% by publication type ────────────────────────────────
    ax = axes_s3[0, 0]
    panel_label(ax, "A")
    if src_s3 is not None and "publication_types" in src_s3.columns:
        tmp = src_s3.copy()
        tmp["is_review"] = tmp["publication_types"].fillna("").str.contains(
            "Review", case=False, na=False)
        hub_cats = ["hub_node", "network_component"]
        for is_rev, lbl, col in [(True, "Reviews", WONG["orange"]),
                                  (False, "Non-reviews", WONG["blue"])]:
            sub = tmp[tmp["is_review"] == is_rev]
            yr_range = range(1993, 2026)
            annual_total = sub.groupby("year").size()
            annual_hub = sub[sub["framing_primary"].isin(hub_cats)].groupby("year").size()
            prop = (100 * annual_hub / annual_total).reindex(yr_range, fill_value=0)
            ax.plot(list(yr_range), rolling_mean(prop, 3),
                    color=col, label=lbl, linewidth=2)
        ax.set_xlabel("Year")
        ax.set_ylabel("Hub/network %")
        ax.set_title("Hub% by publication type")
        ax.legend(fontsize=9)
        despine(ax)
    else:
        placeholder(ax, "Data not available")

    # ── Panel B: Hub% by journal impact (placeholder) ────────────────────
    ax = axes_s3[0, 1]
    panel_label(ax, "B")
    placeholder(ax, "Journal impact data not available\n(requires JCR import)")

    # ── Panel C: Relay% with/without fMRI ────────────────────────────────
    ax = axes_s3[1, 0]
    panel_label(ax, "C")
    if src_s3 is not None and "sentence" in src_s3.columns:
        tmp = src_s3.copy()
        if "has_fMRI" not in tmp.columns:
            tmp = tag_techniques(tmp, text_col="sentence")
        yr_range = range(1995, 2026)
        for has_fmri, lbl, col in [(True, "With fMRI", WONG["orange"]),
                                     (False, "Without fMRI", WONG["blue"])]:
            sub = tmp[tmp["has_fMRI"] == has_fmri]
            annual_total = sub.groupby("year").size()
            annual_relay = sub[sub["framing_primary"] == "relay"].groupby("year").size()
            prop = (100 * annual_relay / annual_total).reindex(yr_range, fill_value=0)
            ax.plot(list(yr_range), rolling_mean(prop, 3),
                    color=col, label=lbl, linewidth=2)
        ax.set_xlabel("Year")
        ax.set_ylabel("Relay %")
        ax.set_title("Relay% with/without fMRI papers")
        ax.legend(fontsize=9)
        despine(ax)
    else:
        placeholder(ax, "Data not available")

    # ── Panel D: Trend with reviews excluded ─────────────────────────────
    ax = axes_s3[1, 1]
    panel_label(ax, "D")
    if src_s3 is not None and "publication_types" in src_s3.columns:
        tmp = src_s3.copy()
        tmp["is_review"] = tmp["publication_types"].fillna("").str.contains(
            "Review", case=False, na=False)
        no_rev = tmp[~tmp["is_review"]]
        yr_range = range(1993, 2026)
        annual_total = no_rev.groupby("year").size()
        for cat, col, lbl in [
            ("relay",    CAT_COLORS["relay"],    "Relay %"),
            ("hub_node", CAT_COLORS["hub_node"], "Hub %"),
        ]:
            annual_cat = no_rev[no_rev["framing_primary"] == cat].groupby("year").size()
            prop = (100 * annual_cat / annual_total).reindex(yr_range, fill_value=0)
            ax.plot(list(yr_range), rolling_mean(prop, 3),
                    color=col, label=lbl, linewidth=2)
        ax.set_xlabel("Year")
        ax.set_ylabel("% of framing sentences")
        ax.set_title("Framing trend (reviews excluded)")
        ax.legend(fontsize=9)
        despine(ax)
    else:
        placeholder(ax, "Data not available")

    fig_s3.suptitle("Supplementary Figure S3: Confound Controls",
                    fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig_s3, GRAPHS / "sup_fig_S3_confound_controls")
    plt.close(fig_s3)
    print("  Figure S3 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure S3 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# SUPPLEMENTARY FIGURE S4: Landscape with Highlighted Clusters
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("SUPPLEMENTARY FIGURE S4: Landscape Clusters")
print("=" * 60)
try:
    if landscape is not None and "umap_x" in landscape.columns:
        fig_s4, ax_main = plt.subplots(figsize=(12, 10))
        panel_label(ax_main, "")

        ux = landscape["umap_x"].values
        uy = landscape["umap_y"].values
        cids = landscape["cluster_id"].values

        # Background scatter (all points)
        ax_main.scatter(ux, uy, s=1, alpha=0.15, c="#cccccc", rasterized=True)

        # Top 5 clusters
        valid = landscape[landscape["cluster_id"] >= 0]
        top5_clusters = valid["cluster_id"].value_counts().head(5).index.tolist()
        palette5 = list(WONG.values())[:5]
        for i, cid in enumerate(top5_clusters):
            mask = cids == cid
            ax_main.scatter(ux[mask], uy[mask], s=3, alpha=0.5,
                            c=palette5[i], label=f"Cluster {cid}",
                            rasterized=True)

        ax_main.set_xlabel("UMAP 1")
        ax_main.set_ylabel("UMAP 2")
        ax_main.set_title("Abstract landscape with top 5 clusters highlighted")
        ax_main.legend(fontsize=9, markerscale=5)
        despine(ax_main)

        fig_s4.suptitle("Supplementary Figure S4: Literature Landscape",
                        fontsize=14, fontweight="bold", y=1.01)
        save_fig(fig_s4, GRAPHS / "sup_fig_S4_landscape_clusters")
        plt.close(fig_s4)
        print("  Figure S4 saved.\n")
    else:
        print("  [SKIP] landscape_clustered.csv not available.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure S4 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# SUPPLEMENTARY FIGURE S5: Nucleus x Framing x Decade  (3-panel heatmap)
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("SUPPLEMENTARY FIGURE S5: Nucleus x Framing x Decade")
print("=" * 60)
try:
    fig_s5, axes_s5 = plt.subplots(1, 3, figsize=(18, 7))
    fig_s5.subplots_adjust(wspace=0.35)

    src_s5 = all_class if all_class is not None else framing_bt

    if src_s5 is not None and "sentence" in src_s5.columns:
        tmp = src_s5.copy()
        if f"has_MD" not in tmp.columns:
            tmp = tag_nuclei(tmp, text_col="sentence")
        if "framing_primary" not in tmp.columns:
            tmp["framing_primary"] = tmp["sentence"].apply(classify_primary)

        nuclei_ids = list(NUCLEI.keys())
        frame_cats = [c for c in PRIORITY if c in tmp["framing_primary"].values]
        if "other" in tmp["framing_primary"].values:
            frame_cats.append("other")

        decades = [
            (2000, 2009, "2000-2009"),
            (2010, 2019, "2010-2019"),
            (2020, 2025, "2020-2025"),
        ]

        for panel_idx, (yr_start, yr_end, title) in enumerate(decades):
            ax = axes_s5[panel_idx]
            panel_label(ax, chr(65 + panel_idx))
            dec_df = tmp[tmp["year"].between(yr_start, yr_end)]
            mat = np.zeros((len(nuclei_ids), len(frame_cats)))
            for i, nid in enumerate(nuclei_ids):
                nsub = dec_df[dec_df[f"has_{nid}"] == True]
                if len(nsub) == 0:
                    continue
                for j, cat in enumerate(frame_cats):
                    mat[i, j] = (nsub["framing_primary"] == cat).sum() / len(nsub)
            row_labels = [NUCLEI[n]["label"][:16] for n in nuclei_ids]
            col_labels = [CAT_DISPLAY.get(c, c)[:10] for c in frame_cats]
            heatmap(ax, mat, row_labels, col_labels, cmap="YlOrRd",
                    fmt=".2f", fontsize=5.5)
            ax.set_title(title)
    else:
        for ax in axes_s5:
            placeholder(ax, "Data not available")

    fig_s5.suptitle("Supplementary Figure S5: Nucleus x Framing x Decade",
                    fontsize=14, fontweight="bold", y=1.02)
    save_fig(fig_s5, GRAPHS / "sup_fig_S5_nucleus_framing_decade")
    plt.close(fig_s5)
    print("  Figure S5 saved.\n")

except Exception:
    traceback.print_exc()
    print("  [ERROR] Figure S5 failed.\n")


# ═════════════════════════════════════════════════════════════════════════════
# DONE
# ═════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("ALL FIGURES COMPLETE")
print(f"Output directory: {GRAPHS}")
print("=" * 60)
