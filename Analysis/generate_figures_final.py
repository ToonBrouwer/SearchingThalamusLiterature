from __future__ import annotations
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

"""
generate_figures_final.py  --  FINAL publication figure generator (all user feedback addressed).

Generates:
  Main figures   : fig1 .. fig5   (saved to Graphs/pub/)
  Supplementary  : sup_S1 .. sup_S4

Key changes from v3:
  - Uses all_sentences_final.csv (cleaned topics + mega_theme)
  - Saves to Graphs/pub/ instead of Figures/
  - Fig 1D: ALL 21 frame types shown (no "other" grouping)
  - Fig 2: cleaned topic names from all_sentences_final.csv
  - Fig 3: 3x2 layout with frame derivation panels (A, B new)
  - Fig 4: 2x3 layout with colorbars, nucleus/technique prevalence on ALL abstracts
  - Fig 5: cleaned topic names, precise conclusion themes
  - Colorbars on all heatmaps, font sizes >= 10pt labels, >= 8pt ticks
  - Wilson CI shading annotation
"""

import os
import warnings
import traceback
from pathlib import Path
from collections import OrderedDict

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patheffects as pe
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

# ── Project imports ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from plotting import apply_style, save_fig, panel_label, despine, WONG, mk_bar, heatmap, FIG_2COL, FIG_FULL
from nucleus_patterns import NUCLEI, tag_nuclei
from technique_patterns import TECHNIQUES, tag_techniques
from stats import mann_kendall, rolling_mean, wilson_ci

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).resolve().parents[2]
DATA_PROC = ROOT / "Data" / "processed"
DATA_RAW  = ROOT / "Data" / "raw" / "pubmed"
FIG_DIR   = ROOT / "Graphs" / "pub"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# ── Apply style ──────────────────────────────────────────────────────────────
apply_style()

# ── Load data ────────────────────────────────────────────────────────────────
print("Loading data ...")
df_frame_ft = pd.read_csv(DATA_PROC / "framing_clean.csv")
df_frame_abs = pd.read_csv(DATA_PROC / "framing_clean_abstracts.csv")
df_all = pd.read_csv(DATA_PROC / "all_sentences_final.csv")          # <-- CHANGED
df_pubmed = pd.read_csv(DATA_RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv")

concl_path = DATA_PROC / "conclusion_themes_enriched.csv"
df_concl = pd.read_csv(concl_path) if concl_path.exists() else None

print(f"  framing (full-text) : {len(df_frame_ft):,} rows")
print(f"  framing (abstracts) : {len(df_frame_abs):,} rows")
print(f"  all sentences       : {len(df_all):,} rows")
print(f"  PubMed corpus       : {len(df_pubmed):,} rows")
print(f"  conclusion themes   : {len(df_concl) if df_concl is not None else 0:,} rows")

# ── Common constants ─────────────────────────────────────────────────────────
TOP8_FRAMES = ["relay", "hub", "gate", "gateway", "clinical_target",
               "modulator", "pacemaker", "integrator"]
ALL_FRAMES = df_frame_ft["frame_type"].value_counts().index.tolist()

# Colour map for frame types (Wong colourblind-safe)
_frame_colors_list = list(WONG.values())
FRAME_CMAP = {}
for i, ft in enumerate(ALL_FRAMES):
    FRAME_CMAP[ft] = _frame_colors_list[i % len(_frame_colors_list)]
FRAME_CMAP["relay"]           = WONG["red"]
FRAME_CMAP["hub"]             = WONG["blue"]
FRAME_CMAP["gate"]            = WONG["green"]
FRAME_CMAP["gateway"]         = WONG["orange"]
FRAME_CMAP["clinical_target"] = WONG["pink"]
FRAME_CMAP["modulator"]       = WONG["cyan"]
FRAME_CMAP["pacemaker"]       = WONG["purple"]
FRAME_CMAP["integrator"]      = WONG["yellow"]
FRAME_CMAP["other"]           = WONG["grey"]

# Distinct markers for line charts
MARKERS = ["o", "s", "^", "D", "v", "P", "X", "*", "h", "<", ">", "p", "d", "8", "H"]


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _nice_label(s: str) -> str:
    """Capitalise first word of a topic label."""
    if not s:
        return s
    parts = s.split()
    parts[0] = parts[0].capitalize()
    return " ".join(parts)


def _frame_proportions(df, year_col="year", frame_col="frame_type",
                       year_min=None, year_max=None, smooth=3,
                       top_frames=None, group_other=True):
    """Build year x frame proportion matrix (smoothed)."""
    sub = df.copy()
    if year_min is not None:
        sub = sub[sub[year_col] >= year_min]
    if year_max is not None:
        sub = sub[sub[year_col] <= year_max]

    ct = sub.groupby([year_col, frame_col]).size().unstack(fill_value=0)
    year_totals = ct.sum(axis=1)
    props = ct.div(year_totals, axis=0).fillna(0)
    years = props.index.values

    if top_frames is None:
        top_frames = TOP8_FRAMES

    data = OrderedDict()
    colors = {}
    for ft in top_frames:
        if ft in props.columns:
            vals = rolling_mean(props[ft].values, smooth)
            data[ft] = vals
            colors[ft] = FRAME_CMAP.get(ft, WONG["grey"])

    if group_other:
        other_cols = [c for c in props.columns if c not in top_frames]
        if other_cols:
            other_sum = props[other_cols].sum(axis=1).values
            data["other metaphors"] = rolling_mean(other_sum, smooth)
            colors["other metaphors"] = WONG["ltgrey"]

    return years, data, colors


def _add_colorbar(fig, ax, im, label="", fontsize=9):
    """Add a colorbar to a heatmap with a clear label."""
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label(label, fontsize=fontsize)
    cbar.ax.tick_params(labelsize=8)
    return cbar


def _heatmap_with_cbar(fig, ax, matrix, row_labels, col_labels,
                       cmap="YlOrRd", fmt=".2f", fontsize=7,
                       cbar_label="", vmin=None, vmax=None):
    """Draw heatmap and add colorbar. Returns (im, cbar)."""
    im = heatmap(ax, matrix, row_labels, col_labels,
                 cmap=cmap, fmt=fmt, fontsize=fontsize, vmin=vmin, vmax=vmax)
    cbar = _add_colorbar(fig, ax, im, label=cbar_label)
    return im, cbar


def _wilson_shading(ax, years, counts_series, totals_series, color, smooth=3):
    """Add 95% Wilson CI shading to a line chart."""
    lowers, uppers = [], []
    for yr in years:
        yr_total = int(totals_series.get(yr, 0))
        yr_count = int(counts_series.get(yr, 0))
        lo, hi = wilson_ci(yr_count, yr_total)
        lowers.append(lo)
        uppers.append(hi)
    ax.fill_between(years, rolling_mean(lowers, smooth),
                    rolling_mean(uppers, smooth),
                    color=color, alpha=0.15)


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 1 --- Pipeline + Corpus Overview (14x10, 2x2)
# ══════════════════════════════════════════════════════════════════════════════

def fig1():
    print("\n=== Figure 1: Pipeline & Corpus Overview ===")
    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.30)

    # ── Panel A: Pipeline flowchart ──────────────────────────────────────────
    ax_a = fig.add_subplot(gs[0, 0])
    ax_a.set_xlim(0, 10)
    ax_a.set_ylim(0, 10)
    ax_a.axis("off")
    panel_label(ax_a, "A")

    def _box(ax, x, y, w, h, text, color, fontsize=8):
        box = FancyBboxPatch((x - w/2, y - h/2), w, h,
                             boxstyle="round,pad=0.15",
                             facecolor=color, edgecolor="#333333",
                             linewidth=0.8, alpha=0.85)
        ax.add_patch(box)
        ax.text(x, y, text, ha="center", va="center", fontsize=fontsize,
                wrap=True, linespacing=1.3)

    def _arrow(ax, x1, y1, x2, y2):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                     arrowprops=dict(arrowstyle="-|>", color="#333333",
                                     lw=1.2, shrinkA=2, shrinkB=2))

    _box(ax_a, 5, 9.2, 6.5, 1.1,
         "PubMed query\n('thalamus' OR 'thalamic' in title)\n17,280 articles (1950\u20132026)",
         "#D0E8FF", fontsize=7.5)

    _arrow(ax_a, 3.5, 8.6, 2.8, 7.6)
    _arrow(ax_a, 6.5, 8.6, 7.2, 7.6)

    _box(ax_a, 2.8, 7.0, 4.2, 1.0,
         "Abstract extraction\n45,665 thalamus sentences",
         "#D0F0D0", fontsize=7.5)

    _box(ax_a, 7.2, 7.0, 4.5, 1.0,
         "Full-text retrieval\n(PMC open-access + OA DOI lookup)\n3,468 papers",
         "#FFE0C0", fontsize=7.5)

    _arrow(ax_a, 7.2, 6.4, 7.2, 5.6)

    _box(ax_a, 7.2, 5.0, 4.5, 1.0,
         "Sentence tokenization\n55,848 thalamus sentences",
         "#FFE0C0", fontsize=7.5)

    _arrow(ax_a, 2.8, 6.4, 3.2, 3.6)
    _arrow(ax_a, 7.2, 4.4, 6.8, 3.6)

    _box(ax_a, 3.2, 2.8, 4.8, 1.3,
         "BERTopic topic modeling\nall-MiniLM-L6-v2 embeddings\nUMAP + HDBSCAN\n186 research topics",
         "#E8D0F0", fontsize=7)

    _box(ax_a, 7.2, 2.8, 4.8, 1.3,
         "Frame pattern matching\n21 regex patterns\n1,621 framing sentences (full-text)\n935 framing sentences (abstracts)",
         "#FFD0D0", fontsize=7)

    ax_a.set_title("Data pipeline", fontsize=11, fontweight="bold", pad=4)

    # ── Panel B: Publication volume per year ─────────────────────────────────
    ax_b = fig.add_subplot(gs[0, 1])
    panel_label(ax_b, "B")

    yr_counts = df_pubmed["year"].value_counts().sort_index()
    yr_counts = yr_counts[(yr_counts.index >= 1970) & (yr_counts.index <= 2025)]
    ax_b.bar(yr_counts.index, yr_counts.values, color=WONG["blue"],
             edgecolor="none", width=0.8, alpha=0.85)
    ax_b.set_xlabel("Publication year", fontsize=10)
    ax_b.set_ylabel("Number of articles", fontsize=10)
    ax_b.set_title("Thalamus publications per year", fontsize=11, fontweight="bold")
    despine(ax_b)

    # ── Panel C: Corpus statistics table ─────────────────────────────────────
    ax_c = fig.add_subplot(gs[1, 0])
    ax_c.axis("off")
    panel_label(ax_c, "C")

    n_topics = df_all["topic_clean_v2"].nunique()
    n_assigned = (df_all["topic_clean_v2"] != "unassigned").sum()
    n_ft_papers = df_frame_ft["pmid"].nunique()
    n_abs_papers = df_frame_abs["pmid"].nunique()

    table_data = [
        ["PubMed articles (title search)",  f"{len(df_pubmed):,}"],
        ["Full-text sentences",             f"{len(df_all):,}"],
        ["Abstract framing sentences",      f"{len(df_frame_abs):,}"],
        ["Full-text framing sentences",     f"{len(df_frame_ft):,}"],
        ["Unique papers with frames (FT)",  f"{n_ft_papers:,}"],
        ["Unique papers with frames (Abs)", f"{n_abs_papers:,}"],
        ["BERTopic topics (after merge)",   f"{n_topics}"],
        ["Assigned sentences (non-noise)",  f"{n_assigned:,}"],
        ["Frame types detected",            f"{len(ALL_FRAMES)}"],
        ["Year range",                      f"{int(df_pubmed['year'].min())}\u2013{int(df_pubmed['year'].max())}"],
    ]
    tbl = ax_c.table(cellText=table_data, colLabels=["Metric", "Value"],
                     loc="center", cellLoc="left")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1.0, 1.4)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor("#cccccc")
        if r == 0:
            cell.set_facecolor("#e8e8e8")
            cell.set_text_props(fontweight="bold")
    ax_c.set_title("Corpus statistics", fontsize=11, fontweight="bold", pad=10)

    # ── Panel D: Frame type distribution --- ALL 21 types ────────────────────
    ax_d = fig.add_subplot(gs[1, 1])
    panel_label(ax_d, "D")

    ft_counts = df_frame_ft["frame_type"].value_counts()
    # Show ALL frame types, not just top 12
    bars = ax_d.barh(range(len(ft_counts)), ft_counts.values,
                     color=[FRAME_CMAP.get(f, WONG["grey"]) for f in ft_counts.index],
                     edgecolor="none", height=0.7)
    ax_d.set_yticks(range(len(ft_counts)))
    # Use smaller font for labels if many categories
    label_fontsize = 7 if len(ft_counts) > 15 else 9
    ax_d.set_yticklabels(ft_counts.index, fontsize=label_fontsize)
    ax_d.invert_yaxis()
    ax_d.set_xlabel("Number of sentences", fontsize=10)
    ax_d.set_title("Frame type distribution (full-text, all 21 types)",
                    fontsize=11, fontweight="bold")
    despine(ax_d)
    # Count labels (smaller font for small categories)
    for i, v in enumerate(ft_counts.values):
        fs = 7 if v < 20 else 8
        ax_d.text(v + 3, i, str(v), va="center", fontsize=fs, color="#333333")

    fig.suptitle("Figure 1: Data Pipeline and Corpus Overview",
                 fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig, FIG_DIR / "fig1_pipeline_corpus")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 2 --- Research Topics (14x12, 3x2)
# ══════════════════════════════════════════════════════════════════════════════

def fig2():
    print("\n=== Figure 2: Research Topics ===")
    fig = plt.figure(figsize=(14, 12))
    gs = fig.add_gridspec(3, 2, hspace=0.45, wspace=0.35)

    # ── Panel A: Top 20 topics bar chart (CLEANED names) ─────────────────────
    ax_a = fig.add_subplot(gs[0, 0])
    panel_label(ax_a, "A")

    topic_counts = df_all[df_all["topic_clean_v2"] != "unassigned"]["topic_clean_v2"].value_counts().head(20)
    labels_a = [_nice_label(t) for t in topic_counts.index]
    colors_a = [_frame_colors_list[i % len(_frame_colors_list)] for i in range(len(topic_counts))]

    ax_a.barh(range(len(topic_counts)), topic_counts.values,
              color=colors_a, edgecolor="none", height=0.7)
    ax_a.set_yticks(range(len(topic_counts)))
    ax_a.set_yticklabels(labels_a, fontsize=8)
    ax_a.invert_yaxis()
    ax_a.set_xlabel("Number of sentences", fontsize=10)
    ax_a.set_title("Top 20 research topics (cleaned labels)", fontsize=11, fontweight="bold")
    despine(ax_a)

    # ── Panel B: BERTopic pipeline mini-diagram ──────────────────────────────
    ax_b = fig.add_subplot(gs[0, 1])
    ax_b.set_xlim(0, 10)
    ax_b.set_ylim(0, 3)
    ax_b.axis("off")
    panel_label(ax_b, "B")

    steps = [
        ("384-dim\nembeddings", "#D0E8FF"),
        ("UMAP\n(5D)", "#D0F0D0"),
        ("HDBSCAN\nclustering", "#FFE0C0"),
        ("c-TF-IDF\ntopic labels", "#E8D0F0"),
        ("Manual\nrelabeling", "#FFD0D0"),
    ]
    box_w, box_h = 1.5, 1.4
    gap = 0.35
    total_w = len(steps) * box_w + (len(steps) - 1) * gap
    x_start = (10 - total_w) / 2

    for i, (label, color) in enumerate(steps):
        x = x_start + i * (box_w + gap)
        box = FancyBboxPatch((x, 0.8), box_w, box_h,
                             boxstyle="round,pad=0.1",
                             facecolor=color, edgecolor="#333333",
                             linewidth=0.7, alpha=0.85)
        ax_b.add_patch(box)
        ax_b.text(x + box_w/2, 0.8 + box_h/2, label,
                  ha="center", va="center", fontsize=7.5, linespacing=1.2)
        if i < len(steps) - 1:
            ax_b.annotate("", xy=(x + box_w + gap * 0.15, 1.5),
                          xytext=(x + box_w + gap * 0.02, 1.5),
                          arrowprops=dict(arrowstyle="-|>", color="#333333",
                                          lw=1.0))
    ax_b.set_title("BERTopic pipeline", fontsize=11, fontweight="bold")

    # ── Panels C & D: Topics over time (stacked area) ────────────────────────
    df_assigned = df_all[df_all["topic_clean_v2"] != "unassigned"].copy()
    top10_topics = df_assigned["topic_clean_v2"].value_counts().head(10).index.tolist()
    df_top = df_assigned[df_assigned["topic_clean_v2"].isin(top10_topics)].copy()
    df_other = df_assigned[~df_assigned["topic_clean_v2"].isin(top10_topics)].copy()
    df_other["topic_clean_v2"] = "Other topics"
    df_plot_topics = pd.concat([df_top, df_other], ignore_index=True)

    year_topic_ct = df_plot_topics.groupby(["year", "topic_clean_v2"]).size().unstack(fill_value=0)
    year_topic_ct = year_topic_ct[(year_topic_ct.index >= 1990) & (year_topic_ct.index <= 2025)]

    ordered_topics = top10_topics + ["Other topics"]
    for t in ordered_topics:
        if t not in year_topic_ct.columns:
            year_topic_ct[t] = 0
    year_topic_ct = year_topic_ct[ordered_topics]

    topic_colors = {}
    for i, t in enumerate(ordered_topics[:-1]):
        topic_colors[t] = _frame_colors_list[i % len(_frame_colors_list)]
    topic_colors["Other topics"] = WONG["ltgrey"]

    # Panel C: Absolute stacked area
    ax_c = fig.add_subplot(gs[1, 0])
    panel_label(ax_c, "C")

    years_c = year_topic_ct.index.values
    stack_data_c = []
    stack_labels_c = []
    stack_colors_c = []
    for t in ordered_topics:
        vals = rolling_mean(year_topic_ct[t].values, 3)
        stack_data_c.append(vals)
        stack_labels_c.append(_nice_label(t) if t != "Other topics" else t)
        stack_colors_c.append(topic_colors[t])

    ax_c.stackplot(years_c, stack_data_c, labels=stack_labels_c,
                   colors=stack_colors_c, alpha=0.85)
    ax_c.set_xlabel("Year", fontsize=10)
    ax_c.set_ylabel("Sentence count", fontsize=10)
    ax_c.set_title("Topic prevalence (absolute)", fontsize=11, fontweight="bold")
    despine(ax_c)
    ax_c.legend(loc="upper left", fontsize=5.5, ncol=2, framealpha=0.8)

    # Panel D: Relative stacked area
    ax_d = fig.add_subplot(gs[1, 1])
    panel_label(ax_d, "D")

    year_totals = year_topic_ct.sum(axis=1).values
    year_totals_safe = np.where(year_totals == 0, 1, year_totals)
    stack_data_d = []
    for t in ordered_topics:
        vals = year_topic_ct[t].values / year_totals_safe * 100
        stack_data_d.append(rolling_mean(vals, 3))

    ax_d.stackplot(years_c, stack_data_d, labels=stack_labels_c,
                   colors=stack_colors_c, alpha=0.85)
    ax_d.set_xlabel("Year", fontsize=10)
    ax_d.set_ylabel("Percentage (%)", fontsize=10)
    ax_d.set_ylim(0, 100)
    ax_d.set_title("Topic prevalence (relative)", fontsize=11, fontweight="bold")
    despine(ax_d)
    ax_d.legend(loc="upper left", fontsize=5.5, ncol=2, framealpha=0.8)

    # ── Panel E: Topics x mega_theme distribution ────────────────────────────
    ax_e = fig.add_subplot(gs[2, 0])
    panel_label(ax_e, "E")

    if "mega_theme" in df_all.columns:
        mega_counts = df_all[df_all["mega_theme"] != "Other"]["mega_theme"].value_counts()
        ax_e.barh(range(len(mega_counts)), mega_counts.values,
                  color=[_frame_colors_list[i % len(_frame_colors_list)]
                         for i in range(len(mega_counts))],
                  edgecolor="none", height=0.6)
        ax_e.set_yticks(range(len(mega_counts)))
        ax_e.set_yticklabels(mega_counts.index, fontsize=9)
        ax_e.invert_yaxis()
        ax_e.set_xlabel("Number of sentences", fontsize=10)
        ax_e.set_title("Mega-theme distribution", fontsize=11, fontweight="bold")
        for i, v in enumerate(mega_counts.values):
            ax_e.text(v + 10, i, str(v), va="center", fontsize=8, color="#333333")
    despine(ax_e)

    # ── Panel F: Topic diversity over time ───────────────────────────────────
    ax_f = fig.add_subplot(gs[2, 1])
    panel_label(ax_f, "F")

    # Shannon entropy of topics per year
    topic_yr = df_assigned.groupby(["year", "topic_clean_v2"]).size().unstack(fill_value=0)
    topic_yr = topic_yr[(topic_yr.index >= 1990) & (topic_yr.index <= 2025)]
    entropy_per_year = []
    for yr in topic_yr.index:
        counts = topic_yr.loc[yr].values
        total = counts.sum()
        if total == 0:
            entropy_per_year.append(0)
            continue
        probs = counts / total
        probs = probs[probs > 0]
        entropy_per_year.append(-np.sum(probs * np.log2(probs)))

    ax_f.plot(topic_yr.index, rolling_mean(entropy_per_year, 3),
              color=WONG["blue"], linewidth=2)
    ax_f.fill_between(topic_yr.index,
                      rolling_mean(entropy_per_year, 3),
                      alpha=0.15, color=WONG["blue"])
    ax_f.set_xlabel("Year", fontsize=10)
    ax_f.set_ylabel("Shannon entropy (bits)", fontsize=10)
    ax_f.set_title("Topic diversity over time", fontsize=11, fontweight="bold")
    despine(ax_f)

    fig.suptitle("Figure 2: Research Topic Landscape",
                 fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig, FIG_DIR / "fig2_research_topics")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 3 --- Framing Over Time (14x14, 3x2) -- with frame derivation panels
# ══════════════════════════════════════════════════════════════════════════════

def fig3():
    print("\n=== Figure 3: Framing Over Time (3x2 layout) ===")
    fig = plt.figure(figsize=(14, 14))
    gs = fig.add_gridspec(3, 2, hspace=0.45, wspace=0.35)

    # ── Panel A (NEW): Frame derivation explanation ──────────────────────────
    ax_a = fig.add_subplot(gs[0, 0])
    ax_a.set_xlim(0, 10)
    ax_a.set_ylim(0, 10)
    ax_a.axis("off")
    panel_label(ax_a, "A")

    def _box(ax, x, y, w, h, text, color, fontsize=8):
        box = FancyBboxPatch((x - w/2, y - h/2), w, h,
                             boxstyle="round,pad=0.15",
                             facecolor=color, edgecolor="#333333",
                             linewidth=0.8, alpha=0.85)
        ax.add_patch(box)
        ax.text(x, y, text, ha="center", va="center", fontsize=fontsize,
                wrap=True, linespacing=1.3)

    def _arrow(ax, x1, y1, x2, y2):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                     arrowprops=dict(arrowstyle="-|>", color="#333333",
                                     lw=1.2, shrinkA=2, shrinkB=2))

    # Step 1
    _box(ax_a, 5, 9.0, 8.0, 1.2,
         "Step 1: 55,848 full-text sentences scanned\nfor 21 metaphorical frame patterns",
         "#D0E8FF", fontsize=8)

    _arrow(ax_a, 5, 8.3, 5, 7.5)

    # Step 2
    _box(ax_a, 5, 6.8, 8.0, 1.2,
         "Step 2: 1,621 sentences matched (2.9%)\n= framing sentences for analysis",
         "#D0F0D0", fontsize=8)

    _arrow(ax_a, 5, 6.1, 5, 5.3)

    # Step 3
    _box(ax_a, 5, 4.6, 8.0, 1.2,
         "Step 3: 54,227 non-framing sentences excluded\n(no metaphorical frame detected)",
         "#FFE0E0", fontsize=8)

    # Examples
    ax_a.text(5, 3.0, "Examples:", ha="center", va="center",
              fontsize=9, fontweight="bold")
    ax_a.text(5, 2.3, '"The thalamus is a relay station"  \u2192  relay',
              ha="center", va="center", fontsize=8, style="italic",
              color="#333333",
              bbox=dict(boxstyle="round,pad=0.3", facecolor="#FFFFDD",
                        edgecolor="#cccccc"))
    ax_a.text(5, 1.4, '"The thalamus acts as a hub"  \u2192  hub',
              ha="center", va="center", fontsize=8, style="italic",
              color="#333333",
              bbox=dict(boxstyle="round,pad=0.3", facecolor="#FFFFDD",
                        edgecolor="#cccccc"))
    ax_a.text(5, 0.5, '"The thalamus is a gateway for sensory..."  \u2192  gateway',
              ha="center", va="center", fontsize=8, style="italic",
              color="#333333",
              bbox=dict(boxstyle="round,pad=0.3", facecolor="#FFFFDD",
                        edgecolor="#cccccc"))

    ax_a.set_title("Frame derivation method", fontsize=11, fontweight="bold", pad=4)

    # ── Panel B (NEW): 21 frame types with example sentences ─────────────────
    ax_b = fig.add_subplot(gs[0, 1])
    ax_b.axis("off")
    panel_label(ax_b, "B")

    # Build example table for all 21 frame types
    frame_examples = {}
    for ft in ALL_FRAMES:
        sub = df_frame_ft[df_frame_ft["frame_type"] == ft]
        if len(sub) > 0:
            ex = sub["sentence"].iloc[0]
            if len(ex) > 65:
                ex = ex[:62] + "..."
            frame_examples[ft] = ex
        else:
            frame_examples[ft] = "(no examples)"

    table_data_b = [[ft, frame_examples[ft]] for ft in ALL_FRAMES]
    tbl_b = ax_b.table(cellText=table_data_b,
                       colLabels=["Frame type", "Example sentence (truncated)"],
                       loc="center", cellLoc="left",
                       colWidths=[0.18, 0.82])
    tbl_b.auto_set_font_size(False)
    tbl_b.set_fontsize(6.5)
    tbl_b.scale(1.0, 1.15)
    for (r, c), cell in tbl_b.get_celld().items():
        cell.set_edgecolor("#cccccc")
        if r == 0:
            cell.set_facecolor("#e8e8e8")
            cell.set_text_props(fontweight="bold", fontsize=7)
        elif c == 0:
            cell.set_text_props(fontweight="bold", fontsize=6.5)
    ax_b.set_title("21 frame type categories with examples",
                    fontsize=11, fontweight="bold", pad=4)

    # ── Panel C: Abstract framing proportions (top 8 + other) ────────────────
    ax_c = fig.add_subplot(gs[1, 0])
    panel_label(ax_c, "C")

    years_c, data_c, colors_c = _frame_proportions(
        df_frame_abs, year_min=1980, year_max=2026,
        top_frames=TOP8_FRAMES, group_other=True, smooth=3)

    labels_c = list(data_c.keys())
    stack_c = np.vstack([np.asarray(data_c[k]) for k in labels_c])
    ax_c.stackplot(years_c, stack_c,
                   labels=labels_c,
                   colors=[colors_c[k] for k in labels_c],
                   alpha=0.85)
    ax_c.set_xlabel("Year", fontsize=10)
    ax_c.set_ylabel("Proportion of framing sentences", fontsize=10)
    ax_c.set_title("Abstract framing (1980\u20132026)", fontsize=11, fontweight="bold")
    ax_c.legend(loc="upper left", fontsize=7, ncol=2, framealpha=0.8)
    ax_c.set_ylim(0, 1)
    despine(ax_c)

    # ── Panel D: Full-text framing proportions ───────────────────────────────
    ax_d = fig.add_subplot(gs[1, 1])
    panel_label(ax_d, "D")

    years_d, data_d, colors_d = _frame_proportions(
        df_frame_ft, year_min=2006, year_max=2026,
        top_frames=TOP8_FRAMES, group_other=True, smooth=3)

    labels_d = list(data_d.keys())
    stack_d = np.vstack([np.asarray(data_d[k]) for k in labels_d])
    ax_d.stackplot(years_d, stack_d,
                   labels=labels_d,
                   colors=[colors_d[k] for k in labels_d],
                   alpha=0.85)
    ax_d.set_xlabel("Year", fontsize=10)
    ax_d.set_ylabel("Proportion of framing sentences", fontsize=10)
    ax_d.set_title("Full-text framing (2006\u20132026)", fontsize=11, fontweight="bold")
    ax_d.legend(loc="upper left", fontsize=7, ncol=2, framealpha=0.8)
    ax_d.set_ylim(0, 1)
    despine(ax_d)

    # ── Panel E: Mann-Kendall trends for ALL 21 frame types ──────────────────
    ax_e = fig.add_subplot(gs[2, 0])
    panel_label(ax_e, "E")

    ft_year_frame = df_frame_ft.groupby(["year", "frame_type"]).size().unstack(fill_value=0)
    ft_totals = ft_year_frame.sum(axis=1)
    ft_props = ft_year_frame.div(ft_totals, axis=0).fillna(0)
    ft_years = ft_props.index.values

    mk_items = []
    for ft in ALL_FRAMES:
        if ft in ft_props.columns:
            tau, p, sig, label = mann_kendall(ft_years, ft_props[ft].values)
            mk_items.append((ft, tau, p, sig))

    mk_items.sort(key=lambda x: x[1] if not np.isnan(x[1]) else 0, reverse=True)

    labels_e = [it[0] for it in mk_items]
    taus_e = [it[1] if not np.isnan(it[1]) else 0 for it in mk_items]

    colours_e = [WONG["red"] if t > 0 else WONG["blue"] for t in taus_e]
    y_pos_e = np.arange(len(labels_e))

    ax_e.barh(y_pos_e, taus_e, color=colours_e, edgecolor="none", height=0.6)
    ax_e.set_yticks(y_pos_e)
    ax_e.set_yticklabels(labels_e, fontsize=7)
    ax_e.axvline(0, color="#444444", linewidth=0.6)
    ax_e.set_xlabel("Mann-Kendall \u03c4", fontsize=10)
    ax_e.set_title("Temporal trends (all 21 frame types)", fontsize=11, fontweight="bold")
    ax_e.invert_yaxis()
    despine(ax_e)

    for i, (lbl, tau_val, p_val, sig) in enumerate(mk_items):
        if sig in ("***", "**", "*"):
            x_pos = tau_val + (0.01 if tau_val >= 0 else -0.01)
            ha = "left" if tau_val >= 0 else "right"
            ax_e.text(x_pos, i, sig, va="center", ha=ha, fontsize=7,
                      color="#333333", fontweight="bold")

    # ── Panel F: Relay vs Hub crossover ──────────────────────────────────────
    ax_f = fig.add_subplot(gs[2, 1])
    panel_label(ax_f, "F")

    abs_yf = df_frame_abs.groupby(["year", "frame_type"]).size().unstack(fill_value=0)
    abs_totals = abs_yf.sum(axis=1)
    abs_props = abs_yf.div(abs_totals, axis=0).fillna(0)

    yrs_f = abs_props.index.values
    relay_prop = abs_props.get("relay", pd.Series(0, index=abs_props.index)).values
    hub_prop = abs_props.get("hub", pd.Series(0, index=abs_props.index)).values

    relay_smooth = rolling_mean(relay_prop, 3)
    hub_smooth = rolling_mean(hub_prop, 3)

    ax_f.plot(yrs_f, relay_smooth, color=FRAME_CMAP["relay"], linewidth=2,
              label="Relay", marker="o", markersize=3, markevery=3)
    ax_f.plot(yrs_f, hub_smooth, color=FRAME_CMAP["hub"], linewidth=2,
              label="Hub", marker="s", markersize=3, markevery=3)

    # Wilson CI shading
    for ft_name, prop_vals, color in [("relay", relay_prop, FRAME_CMAP["relay"]),
                                       ("hub", hub_prop, FRAME_CMAP["hub"])]:
        lowers, uppers = [], []
        for yr in yrs_f:
            yr_total = int(abs_totals.get(yr, 0))
            yr_count = int(abs_yf.get(ft_name, pd.Series(0, index=abs_yf.index)).get(yr, 0))
            lo, hi = wilson_ci(yr_count, yr_total)
            lowers.append(lo)
            uppers.append(hi)
        ax_f.fill_between(yrs_f, rolling_mean(lowers, 3),
                          rolling_mean(uppers, 3),
                          color=color, alpha=0.15)

    # Annotate CI shading
    ax_f.text(0.98, 0.02, "shading = 95% Wilson CI",
              transform=ax_f.transAxes, fontsize=7, ha="right", va="bottom",
              color="#777777", style="italic")

    # Find crossover
    for i in range(1, len(yrs_f)):
        if relay_smooth.iloc[i-1] >= hub_smooth.iloc[i-1] and relay_smooth.iloc[i] < hub_smooth.iloc[i]:
            cross_year = yrs_f[i]
            cross_val = (relay_smooth.iloc[i] + hub_smooth.iloc[i]) / 2
            ax_f.axvline(cross_year, color=WONG["grey"], linestyle="--",
                         linewidth=0.8, alpha=0.7)
            ax_f.annotate(f"Crossover ~{cross_year}", xy=(cross_year, cross_val),
                          xytext=(cross_year + 2, cross_val + 0.05),
                          fontsize=8, arrowprops=dict(arrowstyle="->", color="#555555"),
                          color="#555555")
            break

    ax_f.set_xlabel("Year", fontsize=10)
    ax_f.set_ylabel("Proportion", fontsize=10)
    ax_f.set_title("Relay vs Hub framing (abstracts)", fontsize=11, fontweight="bold")
    ax_f.legend(fontsize=9, framealpha=0.8)
    despine(ax_f)

    fig.suptitle("Figure 3: Framing Over Time",
                 fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig, FIG_DIR / "fig3_framing_over_time")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 4 --- Framing x Nuclei x Techniques (14x14, 2x3)
# ══════════════════════════════════════════════════════════════════════════════

def fig4():
    print("\n=== Figure 4: Framing x Nuclei x Techniques ===")
    fig = plt.figure(figsize=(14, 14))
    gs = fig.add_gridspec(3, 2, hspace=0.50, wspace=0.40)

    # Tag framing data
    df_ft_tagged = df_frame_ft.copy()
    tag_nuclei(df_ft_tagged, text_col="sentence")
    tag_techniques(df_ft_tagged, text_col="sentence")

    nuc_ids = list(NUCLEI.keys())
    nuc_labels = [NUCLEI[n]["label"].split("(")[0].strip() if "(" in NUCLEI[n]["label"]
                  else NUCLEI[n]["label"] for n in nuc_ids]
    tech_ids = list(TECHNIQUES.keys())
    tech_labels = [TECHNIQUES[t]["label"] for t in tech_ids]

    top_frames_hm = [ft for ft in TOP8_FRAMES if ft in df_ft_tagged["frame_type"].unique()]

    # ── Panel A: Frame per nucleus heatmap (with colorbar) ───────────────────
    ax_a = fig.add_subplot(gs[0, 0])
    panel_label(ax_a, "A")

    mat_a = np.zeros((len(top_frames_hm), len(nuc_ids)))
    for i, ft in enumerate(top_frames_hm):
        sub = df_ft_tagged[df_ft_tagged["frame_type"] == ft]
        total_ft = len(sub)
        for j, nid in enumerate(nuc_ids):
            mat_a[i, j] = sub[f"has_{nid}"].sum() / max(total_ft, 1) * 100

    _heatmap_with_cbar(fig, ax_a, mat_a, top_frames_hm, nuc_labels,
                       cmap="YlOrRd", fmt=".1f", fontsize=6,
                       cbar_label="% of frame sentences")
    ax_a.set_title("Frame type per nucleus", fontsize=11, fontweight="bold")

    # ── Panel B: Frame per technique heatmap (with colorbar) ─────────────────
    ax_b = fig.add_subplot(gs[0, 1])
    panel_label(ax_b, "B")

    mat_b = np.zeros((len(top_frames_hm), len(tech_ids)))
    for i, ft in enumerate(top_frames_hm):
        sub = df_ft_tagged[df_ft_tagged["frame_type"] == ft]
        total_ft = len(sub)
        for j, tid in enumerate(tech_ids):
            mat_b[i, j] = sub[f"has_{tid}"].sum() / max(total_ft, 1) * 100

    _heatmap_with_cbar(fig, ax_b, mat_b, top_frames_hm, tech_labels,
                       cmap="YlOrRd", fmt=".1f", fontsize=6,
                       cbar_label="% of frame sentences")
    ax_b.set_title("Frame type per technique", fontsize=11, fontweight="bold")

    # ── Panels C-F: Nucleus & Technique prevalence on ALL ABSTRACTS ──────────
    # Tag ALL abstracts with nuclei and techniques
    print("  Tagging all abstracts with nuclei and techniques ...")
    df_abs_all = df_pubmed[df_pubmed["abstract"].notna()].copy()
    tag_nuclei(df_abs_all, text_col="abstract")
    tag_techniques(df_abs_all, text_col="abstract")

    # Nucleus prevalence
    nuc_totals_all = {nid: df_abs_all[f"has_{nid}"].sum() for nid in nuc_ids}
    top12_nuc = sorted(nuc_totals_all, key=nuc_totals_all.get, reverse=True)[:12]

    yrs_abs = sorted(df_abs_all["year"].unique())
    yrs_abs = [y for y in yrs_abs if 1990 <= y <= 2025]

    papers_per_year = df_abs_all[df_abs_all["year"].isin(yrs_abs)].groupby("year").size()

    # Panel C: Nucleus prevalence absolute
    ax_c = fig.add_subplot(gs[1, 0])
    panel_label(ax_c, "C")

    for idx_n, nid in enumerate(top12_nuc):
        yr_cts = df_abs_all.groupby("year")[f"has_{nid}"].sum()
        vals = [yr_cts.get(y, 0) for y in yrs_abs]
        smoothed = rolling_mean(vals, 3)
        marker = MARKERS[idx_n % len(MARKERS)]
        ax_c.plot(yrs_abs, smoothed,
                  label=nuc_labels[nuc_ids.index(nid)][:15],
                  color=NUCLEI[nid]["color"], linewidth=1.5, alpha=0.8,
                  marker=marker, markersize=3, markevery=4)
    ax_c.set_xlabel("Year", fontsize=10)
    ax_c.set_ylabel("Number of abstracts mentioning nucleus", fontsize=10)
    ax_c.set_title("Nucleus prevalence (absolute, all abstracts)",
                    fontsize=11, fontweight="bold")
    ax_c.legend(fontsize=5.5, ncol=3, loc="upper left", framealpha=0.8)
    despine(ax_c)

    # Panel D: Nucleus prevalence relative
    ax_d = fig.add_subplot(gs[1, 1])
    panel_label(ax_d, "D")

    for idx_n, nid in enumerate(top12_nuc):
        yr_cts = df_abs_all.groupby("year")[f"has_{nid}"].sum()
        vals = [yr_cts.get(y, 0) / max(papers_per_year.get(y, 1), 1) * 100
                for y in yrs_abs]
        smoothed = rolling_mean(vals, 3)
        marker = MARKERS[idx_n % len(MARKERS)]
        ax_d.plot(yrs_abs, smoothed,
                  label=nuc_labels[nuc_ids.index(nid)][:15],
                  color=NUCLEI[nid]["color"], linewidth=1.5, alpha=0.8,
                  marker=marker, markersize=3, markevery=4)
    ax_d.set_xlabel("Year", fontsize=10)
    ax_d.set_ylabel("% of papers mentioning nucleus", fontsize=10)
    ax_d.set_title("Nucleus prevalence (relative, all abstracts)",
                    fontsize=11, fontweight="bold")
    ax_d.legend(fontsize=5.5, ncol=3, loc="upper left", framealpha=0.8)
    despine(ax_d)

    # Panel E: Technique prevalence absolute
    ax_e = fig.add_subplot(gs[2, 0])
    panel_label(ax_e, "E")

    for idx_t, tid in enumerate(tech_ids):
        yr_cts = df_abs_all.groupby("year")[f"has_{tid}"].sum()
        vals = [yr_cts.get(y, 0) for y in yrs_abs]
        smoothed = rolling_mean(vals, 3)
        marker = MARKERS[idx_t % len(MARKERS)]
        ax_e.plot(yrs_abs, smoothed,
                  label=TECHNIQUES[tid]["label"],
                  color=TECHNIQUES[tid]["color"], linewidth=1.5, alpha=0.8,
                  marker=marker, markersize=3, markevery=4)
    ax_e.set_xlabel("Year", fontsize=10)
    ax_e.set_ylabel("Number of abstracts mentioning technique", fontsize=10)
    ax_e.set_title("Technique prevalence (absolute, all abstracts)",
                    fontsize=11, fontweight="bold")
    ax_e.legend(fontsize=6, ncol=2, loc="upper left", framealpha=0.8)
    despine(ax_e)

    # Panel F: Technique prevalence relative
    ax_f = fig.add_subplot(gs[2, 1])
    panel_label(ax_f, "F")

    for idx_t, tid in enumerate(tech_ids):
        yr_cts = df_abs_all.groupby("year")[f"has_{tid}"].sum()
        vals = [yr_cts.get(y, 0) / max(papers_per_year.get(y, 1), 1) * 100
                for y in yrs_abs]
        smoothed = rolling_mean(vals, 3)
        marker = MARKERS[idx_t % len(MARKERS)]
        ax_f.plot(yrs_abs, smoothed,
                  label=TECHNIQUES[tid]["label"],
                  color=TECHNIQUES[tid]["color"], linewidth=1.5, alpha=0.8,
                  marker=marker, markersize=3, markevery=4)
    ax_f.set_xlabel("Year", fontsize=10)
    ax_f.set_ylabel("% of papers mentioning technique", fontsize=10)
    ax_f.set_title("Technique prevalence (relative, all abstracts)",
                    fontsize=11, fontweight="bold")
    ax_f.legend(fontsize=6, ncol=2, loc="upper left", framealpha=0.8)
    despine(ax_f)

    fig.suptitle("Figure 4: Framing x Nuclei x Techniques",
                 fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig, FIG_DIR / "fig4_framing_nuclei_techniques")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  FIGURE 5 --- Framing x Topics + Conclusions (14x10, 2x2)
# ══════════════════════════════════════════════════════════════════════════════

def fig5():
    print("\n=== Figure 5: Framing x Topics + Conclusions ===")
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.subplots_adjust(hspace=0.50, wspace=0.40)

    # ── Panel A: Frames per topic (CLEANED names) ────────────────────────────
    ax_a = axes[0, 0]
    panel_label(ax_a, "A")

    # Merge framing with topics via pmid
    df_ft_topics = df_frame_ft.merge(
        df_all[["pmid", "topic_clean_v2"]].drop_duplicates("pmid"),
        on="pmid", how="left"
    )
    df_ft_topics = df_ft_topics.dropna(subset=["topic_clean_v2"])
    df_ft_topics = df_ft_topics[df_ft_topics["topic_clean_v2"] != "unassigned"]

    top8_by_frame = df_ft_topics["topic_clean_v2"].value_counts().head(8).index.tolist()

    show_frames = ["relay", "hub", "gate"]
    n_topics_show = len(top8_by_frame)
    x = np.arange(n_topics_show)
    width = 0.25
    for k, ft in enumerate(show_frames):
        counts = []
        for topic in top8_by_frame:
            c = ((df_ft_topics["topic_clean_v2"] == topic) &
                 (df_ft_topics["frame_type"] == ft)).sum()
            counts.append(c)
        ax_a.bar(x + k * width, counts, width, label=ft,
                 color=FRAME_CMAP.get(ft, WONG["grey"]), edgecolor="none",
                 alpha=0.85)

    ax_a.set_xticks(x + width)
    ax_a.set_xticklabels([_nice_label(t)[:28] for t in top8_by_frame],
                          rotation=35, ha="right", fontsize=8)
    ax_a.set_ylabel("Number of framing sentences", fontsize=10)
    ax_a.set_title("Top 3 frames per research topic", fontsize=11, fontweight="bold")
    ax_a.legend(fontsize=8, framealpha=0.8)
    despine(ax_a)

    # ── Panel B: Conclusion themes x Frame type (precise) ────────────────────
    ax_b = axes[0, 1]
    panel_label(ax_b, "B")

    if df_concl is not None:
        theme_cols = [c for c in df_concl.columns
                      if c not in ("year", "pmid", "sentence", "cluster_id")]

        # Match framing sentences to conclusion themes via pmid
        concl_pmids = set(df_concl["pmid"].unique())
        df_frame_concl = df_frame_ft[df_frame_ft["pmid"].isin(concl_pmids)].copy()

        pmid_themes = {}
        for _, row in df_concl.iterrows():
            pid = row["pmid"]
            for tc in theme_cols:
                if row[tc] == True or row[tc] == "True" or str(row[tc]).lower() == "true":
                    if pid not in pmid_themes:
                        pmid_themes[pid] = []
                    pmid_themes[pid].append(tc)

        show_ft_concl = ["relay", "hub", "gate", "gateway", "clinical_target"]
        show_ft_concl = [f for f in show_ft_concl if f in df_frame_concl["frame_type"].unique()]
        frame_theme_mat = np.zeros((len(show_ft_concl), len(theme_cols)))
        for i, ft in enumerate(show_ft_concl):
            sub = df_frame_concl[df_frame_concl["frame_type"] == ft]
            for pid in sub["pmid"].unique():
                themes = pmid_themes.get(pid, [])
                for th in themes:
                    j = theme_cols.index(th)
                    frame_theme_mat[i, j] += 1

        # Row-normalise
        row_sums = frame_theme_mat.sum(axis=1)
        row_sums_safe = np.where(row_sums == 0, 1, row_sums)
        mat_b_norm = frame_theme_mat / row_sums_safe[:, None]

        # Use full theme names
        short_themes = [t.replace(" / ", "/\n") for t in theme_cols]
        im_b, _ = _heatmap_with_cbar(fig, ax_b, mat_b_norm, show_ft_concl,
                                      short_themes,
                                      cmap="YlOrRd", fmt=".2f", fontsize=7,
                                      cbar_label="Proportion (row-normalised)")
        ax_b.set_title("Frame x Conclusion theme",
                        fontsize=11, fontweight="bold")
        ax_b.set_xlabel("Conclusion themes (from enriched clustering)", fontsize=9)
    else:
        ax_b.text(0.5, 0.5, "Conclusion themes data not available.",
                  transform=ax_b.transAxes, ha="center", va="center", fontsize=10,
                  color="#999999")
        ax_b.set_title("Conclusion themes (N/A)", fontsize=11, fontweight="bold")

    # ── Panel C: Frame diversity per year ────────────────────────────────────
    ax_c = axes[1, 0]
    panel_label(ax_c, "C")

    ft_year_frame = df_frame_ft.groupby(["year", "frame_type"]).size().unstack(fill_value=0)
    ft_year_frame = ft_year_frame[ft_year_frame.index >= 2006]
    entropy_per_year = []
    for yr in ft_year_frame.index:
        counts = ft_year_frame.loc[yr].values
        total = counts.sum()
        if total == 0:
            entropy_per_year.append(0)
            continue
        probs = counts / total
        probs = probs[probs > 0]
        entropy_per_year.append(-np.sum(probs * np.log2(probs)))

    ax_c.plot(ft_year_frame.index, rolling_mean(entropy_per_year, 3),
              color=WONG["blue"], linewidth=2)
    ax_c.fill_between(ft_year_frame.index,
                       rolling_mean(entropy_per_year, 3),
                       alpha=0.15, color=WONG["blue"])
    ax_c.set_xlabel("Year", fontsize=10)
    ax_c.set_ylabel("Shannon entropy (bits)", fontsize=10)
    ax_c.set_title("Framing diversity over time (full-text)",
                    fontsize=11, fontweight="bold")
    despine(ax_c)

    # ── Panel D: Top frames per mega-theme ───────────────────────────────────
    ax_d = axes[1, 1]
    panel_label(ax_d, "D")

    # Use the mega_theme column from all_sentences_final.csv
    if "mega_theme" in df_all.columns:
        df_ft_mega = df_frame_ft.merge(
            df_all[["pmid", "mega_theme"]].drop_duplicates("pmid"),
            on="pmid", how="left"
        )
        df_ft_mega = df_ft_mega.dropna(subset=["mega_theme"])
        df_ft_mega = df_ft_mega[df_ft_mega["mega_theme"] != "Other"]

        if len(df_ft_mega) > 0:
            mega_frame = df_ft_mega.groupby(
                ["mega_theme", "frame_type"]).size().unstack(fill_value=0)

            show_frames_d = ["relay", "hub", "gate"]
            x_d = np.arange(len(mega_frame))
            for k, ft in enumerate(show_frames_d):
                if ft in mega_frame.columns:
                    ax_d.bar(x_d + k * 0.25, mega_frame[ft].values, 0.25,
                             label=ft, color=FRAME_CMAP.get(ft, WONG["grey"]),
                             edgecolor="none", alpha=0.85)
            ax_d.set_xticks(x_d + 0.25)
            ax_d.set_xticklabels(mega_frame.index, rotation=30, ha="right", fontsize=8)
            ax_d.set_ylabel("Number of framing sentences", fontsize=10)
            ax_d.legend(fontsize=8, framealpha=0.8)
    ax_d.set_title("Top frames per research domain (mega-theme)",
                    fontsize=11, fontweight="bold")
    despine(ax_d)

    fig.suptitle("Figure 5: Framing x Topics + Conclusions",
                 fontsize=14, fontweight="bold", y=1.01)
    save_fig(fig, FIG_DIR / "fig5_framing_topics")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  SUPPLEMENTARY S1 --- UMAP by topic (cleaned names)
# ══════════════════════════════════════════════════════════════════════════════

def sup_s1():
    print("\n=== Supplementary S1: UMAP by topic ===")
    emb_path = DATA_PROC / "all_sentences_embeddings.npy"
    if not emb_path.exists():
        print("  SKIP: embeddings file not found.")
        return

    fig, ax = plt.subplots(figsize=(10, 8))
    panel_label(ax, "S1")

    try:
        import umap
        embeddings = np.load(emb_path)
        n_sample = min(10000, len(embeddings))
        rng = np.random.RandomState(42)
        idx = rng.choice(len(embeddings), n_sample, replace=False)
        emb_sub = embeddings[idx]
        topics_sub = df_all.iloc[idx]["topic_clean_v2"].values

        reducer = umap.UMAP(n_components=2, random_state=42, n_neighbors=15,
                            min_dist=0.1, metric="cosine")
        umap_2d = reducer.fit_transform(emb_sub)

        top5 = df_all["topic_clean_v2"].value_counts().head(6).index.tolist()
        if "unassigned" in top5:
            top5.remove("unassigned")
        top5 = top5[:5]

        for i, topic in enumerate(top5):
            mask = topics_sub == topic
            ax.scatter(umap_2d[mask, 0], umap_2d[mask, 1],
                       s=3, alpha=0.4,
                       color=_frame_colors_list[i % len(_frame_colors_list)],
                       label=_nice_label(topic)[:30])

        other_mask = ~np.isin(topics_sub, top5 + ["unassigned"])
        ax.scatter(umap_2d[other_mask, 0], umap_2d[other_mask, 1],
                   s=1, alpha=0.1, color=WONG["ltgrey"], label="Other")

        ax.legend(fontsize=8, markerscale=5, framealpha=0.8)
        ax.set_xlabel("UMAP-1", fontsize=10)
        ax.set_ylabel("UMAP-2", fontsize=10)
    except ImportError:
        ax.text(0.5, 0.5, "UMAP not available", transform=ax.transAxes,
                ha="center", va="center", fontsize=12, color="#999999")
    except Exception as e:
        ax.text(0.5, 0.5, f"Error: {e}", transform=ax.transAxes,
                ha="center", va="center", fontsize=10, color="#999999")

    ax.set_title("Supplementary S1: UMAP projection by topic (cleaned labels)",
                 fontsize=12, fontweight="bold")
    despine(ax)
    save_fig(fig, FIG_DIR / "sup_S1_umap_topics")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  SUPPLEMENTARY S2 --- Abstract vs full-text replication
# ══════════════════════════════════════════════════════════════════════════════

def sup_s2():
    print("\n=== Supplementary S2: Abstract vs full-text replication ===")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.subplots_adjust(wspace=0.35)

    # Left: Scatter of frame proportions
    ax_a = axes[0]
    panel_label(ax_a, "A")

    abs_counts = df_frame_abs["frame_type"].value_counts()
    ft_counts = df_frame_ft["frame_type"].value_counts()
    abs_total = abs_counts.sum()
    ft_total = ft_counts.sum()

    common_frames = sorted(set(abs_counts.index) & set(ft_counts.index))
    abs_props = [abs_counts.get(f, 0) / abs_total for f in common_frames]
    ft_props = [ft_counts.get(f, 0) / ft_total for f in common_frames]

    ax_a.scatter(abs_props, ft_props,
                 c=[FRAME_CMAP.get(f, WONG["grey"]) for f in common_frames],
                 s=60, edgecolor="#333333", linewidth=0.5, zorder=5)
    for i, f in enumerate(common_frames):
        ax_a.annotate(f, (abs_props[i], ft_props[i]),
                      fontsize=7, xytext=(4, 4), textcoords="offset points")

    max_val = max(max(abs_props), max(ft_props)) * 1.1
    ax_a.plot([0, max_val], [0, max_val], "--", color=WONG["grey"],
              linewidth=0.8, alpha=0.6)
    ax_a.set_xlabel("Abstract proportion", fontsize=10)
    ax_a.set_ylabel("Full-text proportion", fontsize=10)
    ax_a.set_title("Frame proportions: abstract vs full-text",
                    fontsize=11, fontweight="bold")
    despine(ax_a)

    # Right: Correlation over time
    ax_b = axes[1]
    panel_label(ax_b, "B")

    from scipy.stats import spearmanr
    abs_yf = df_frame_abs.groupby(["year", "frame_type"]).size().unstack(fill_value=0)
    ft_yf = df_frame_ft.groupby(["year", "frame_type"]).size().unstack(fill_value=0)
    common_years = sorted(set(abs_yf.index) & set(ft_yf.index))
    common_years = [y for y in common_years if y >= 2006]

    all_ft_types = sorted(set(abs_yf.columns) | set(ft_yf.columns))
    corrs = []
    for yr in common_years:
        a_vals = [abs_yf.loc[yr].get(f, 0) for f in all_ft_types]
        f_vals = [ft_yf.loc[yr].get(f, 0) for f in all_ft_types]
        if sum(a_vals) > 0 and sum(f_vals) > 0:
            r, _ = spearmanr(a_vals, f_vals)
            corrs.append(r)
        else:
            corrs.append(np.nan)

    ax_b.plot(common_years, corrs, "o-", color=WONG["blue"], markersize=4)
    ax_b.axhline(0.8, color=WONG["grey"], linestyle="--", linewidth=0.6, alpha=0.5)
    ax_b.set_xlabel("Year", fontsize=10)
    ax_b.set_ylabel("Spearman \u03c1", fontsize=10)
    ax_b.set_title("Abstract\u2013full-text correlation over time",
                    fontsize=11, fontweight="bold")
    ax_b.set_ylim(-0.2, 1.05)
    despine(ax_b)

    fig.suptitle("Supplementary S2: Abstract vs Full-Text Replication",
                 fontsize=13, fontweight="bold", y=1.02)
    save_fig(fig, FIG_DIR / "sup_S2_abstract_vs_fulltext")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  SUPPLEMENTARY S3 --- Frame detection robustness
# ══════════════════════════════════════════════════════════════════════════════

def sup_s3():
    print("\n=== Supplementary S3: Frame detection robustness ===")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.subplots_adjust(wspace=0.35)

    # Left: All frame type counts (log scale)
    ax_a = axes[0]
    panel_label(ax_a, "A")

    ft_counts = df_frame_ft["frame_type"].value_counts()
    ax_a.barh(range(len(ft_counts)), ft_counts.values,
              color=[FRAME_CMAP.get(f, WONG["grey"]) for f in ft_counts.index],
              edgecolor="none", height=0.7)
    ax_a.set_yticks(range(len(ft_counts)))
    ax_a.set_yticklabels(ft_counts.index, fontsize=8)
    ax_a.set_xscale("log")
    ax_a.invert_yaxis()
    ax_a.set_xlabel("Sentence count (log scale)", fontsize=10)
    ax_a.set_title("Frame detection counts (full-text)", fontsize=11, fontweight="bold")
    despine(ax_a)

    # Right: Multi-frame sentences
    ax_b = axes[1]
    panel_label(ax_b, "B")

    if "frame_all" in df_frame_ft.columns:
        n_frames = df_frame_ft["frame_all"].str.count(r"[,;|]") + 1
        n_frames = n_frames.fillna(1).astype(int)
    else:
        n_frames = pd.Series(1, index=df_frame_ft.index)

    frame_dist = n_frames.value_counts().sort_index()
    ax_b.bar(frame_dist.index, frame_dist.values, color=WONG["cyan"],
             edgecolor="none", width=0.7)
    ax_b.set_xlabel("Number of frame types per sentence", fontsize=10)
    ax_b.set_ylabel("Number of sentences", fontsize=10)
    ax_b.set_title("Multi-frame overlap", fontsize=11, fontweight="bold")
    despine(ax_b)

    fig.suptitle("Supplementary S3: Frame Detection Robustness",
                 fontsize=13, fontweight="bold", y=1.02)
    save_fig(fig, FIG_DIR / "sup_S3_robustness")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  SUPPLEMENTARY S4 --- Topic keyword bar charts (cleaned names)
# ══════════════════════════════════════════════════════════════════════════════

def sup_s4():
    print("\n=== Supplementary S4: Topic keyword bar charts ===")
    top12_topics = df_all[df_all["topic_clean_v2"] != "unassigned"]["topic_clean_v2"].value_counts().head(12).index.tolist()

    fig, axes = plt.subplots(3, 4, figsize=(16, 10))
    fig.subplots_adjust(hspace=0.55, wspace=0.35)
    axes_flat = axes.flatten()

    for idx, topic in enumerate(top12_topics):
        ax = axes_flat[idx]
        keywords = topic.split()[:6]
        counts = []
        topic_sents = df_all[df_all["topic_clean_v2"] == topic]["sentence"].fillna("")
        for kw in keywords:
            c = topic_sents.str.contains(kw, case=False, regex=False).sum()
            counts.append(c)

        ax.barh(range(len(keywords)), counts,
                color=_frame_colors_list[idx % len(_frame_colors_list)],
                edgecolor="none", height=0.6)
        ax.set_yticks(range(len(keywords)))
        ax.set_yticklabels(keywords, fontsize=7)
        ax.invert_yaxis()
        ax.set_title(_nice_label(topic)[:35], fontsize=8, fontweight="bold")
        ax.tick_params(axis="x", labelsize=7)
        despine(ax)

    for idx in range(len(top12_topics), len(axes_flat)):
        axes_flat[idx].set_visible(False)

    fig.suptitle("Supplementary S4: Topic Keyword Frequencies (cleaned labels)",
                 fontsize=13, fontweight="bold", y=1.01)
    save_fig(fig, FIG_DIR / "sup_S4_topic_keywords")
    plt.close(fig)
    print("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    figures = [
        ("Figure 1", fig1),
        ("Figure 2", fig2),
        ("Figure 3", fig3),
        ("Figure 4", fig4),
        ("Figure 5", fig5),
        ("Supplementary S1", sup_s1),
        ("Supplementary S2", sup_s2),
        ("Supplementary S3", sup_s3),
        ("Supplementary S4", sup_s4),
    ]

    success = []
    failed = []
    for name, func in figures:
        try:
            func()
            success.append(name)
        except Exception as e:
            failed.append(name)
            print(f"\n  ERROR in {name}: {e}")
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"RESULTS: {len(success)} succeeded, {len(failed)} failed")
    if success:
        print(f"  OK: {', '.join(success)}")
    if failed:
        print(f"  FAILED: {', '.join(failed)}")
    print(f"\nFigures saved to: {FIG_DIR}")
    print("=" * 60)
