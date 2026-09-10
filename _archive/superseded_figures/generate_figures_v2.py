#!/usr/bin/env python3
from __future__ import annotations
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
"""
generate_figures_v2.py
======================
Generate all main + supplementary figures for the thalamus framing paper
using the CLEAN data (v2 BERTopic, deduplicated framing).

Replaces generate_all_figures.py.

Run:
    python Code/analysis/generate_figures_v2.py

Output:
    Graphs/pub/fig{1..5}_*.{png,svg}
    Graphs/pub/sup_S{1..4}_*.{png,svg}
"""

import os
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
GRAPHS    = ROOT / "Graphs" / "pub"
GRAPHS.mkdir(parents=True, exist_ok=True)

# ── Module imports ───────────────────────────────────────────────────────────
sys.path.insert(0, str(ROOT / "Code"))
from plotting import (
    apply_style, save_fig, panel_label, despine, WONG,
    mk_bar, heatmap, stacked_area, COLORS, FIG_2COL, FIG_FULL,
)
from technique_patterns import TECHNIQUES, tag_techniques
from nucleus_patterns import NUCLEI, tag_nuclei
from stats import mann_kendall, wilson_ci, rolling_mean

apply_style()

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Frame-type colour scheme ────────────────────────────────────────────────
# Metaphorical (warm), Functional (cool), Descriptive (neutral)
METAPHORICAL = [
    "relay", "hub", "gateway", "gate", "switchboard",
    "blackboard", "bridge", "bottleneck", "interface",
]
FUNCTIONAL = [
    "modulator", "integrator", "filter", "regulator", "coordinator",
    "pacemaker", "amplifier", "distributor", "processor",
]
DESCRIPTIVE = ["network_node", "clinical_target", "anti_relay"]

_WARM_CMAP = [
    "#D55E00", "#E69F00", "#F0E442", "#CC79A7", "#FFB347",
    "#B22222", "#A0522D", "#FF6B6B", "#E8A87C",
]
_COOL_CMAP = [
    "#0072B2", "#009E73", "#56B4E9", "#7B4F9E", "#4DC5A4",
    "#2E86AB", "#3D9970", "#20B2AA", "#4169E1",
]
_NEUT_CMAP = ["#999999", "#888888", "#aaaaaa"]


def _build_frame_colors():
    """Build a {frame_type: hex} colour map."""
    cm = {}
    for i, ft in enumerate(METAPHORICAL):
        cm[ft] = _WARM_CMAP[i % len(_WARM_CMAP)]
    for i, ft in enumerate(FUNCTIONAL):
        cm[ft] = _COOL_CMAP[i % len(_COOL_CMAP)]
    for i, ft in enumerate(DESCRIPTIVE):
        cm[ft] = _NEUT_CMAP[i % len(_NEUT_CMAP)]
    return cm


FRAME_COLORS = _build_frame_colors()

# Semantic group labels for legend
FRAME_GROUPS = {}
for ft in METAPHORICAL:
    FRAME_GROUPS[ft] = "Metaphorical"
for ft in FUNCTIONAL:
    FRAME_GROUPS[ft] = "Functional"
for ft in DESCRIPTIVE:
    FRAME_GROUPS[ft] = "Descriptive"


# ── Helpers ──────────────────────────────────────────────────────────────────

def clean_topic_name(raw: str) -> str:
    """Strip leading numbers/underscores, replace underscores, capitalise."""
    import re
    s = str(raw)
    # Remove leading digits and underscores like "5_sleep_wakefulness..."
    s = re.sub(r"^\d+_", "", s)
    s = s.replace("_", " ").strip()
    if s == "unassigned" or s == "-1":
        return "Unassigned"
    return s.capitalize() if s else "Unknown"


def frame_display(ft: str) -> str:
    """Pretty-print a frame_type for axes."""
    return ft.replace("_", " ").capitalize()


def safe_color(ft: str) -> str:
    """Return colour for a frame_type, grey fallback."""
    return FRAME_COLORS.get(ft, WONG["grey"])


# ── Load data ────────────────────────────────────────────────────────────────
print("Loading data ...")

framing = pd.read_csv(PROCESSED / "framing_clean.csv")
framing_abs = pd.read_csv(PROCESSED / "framing_clean_abstracts.csv")
all_sent = pd.read_csv(PROCESSED / "all_sentences_clean.csv")
abstracts = pd.read_csv(
    RAW / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    low_memory=False,
)

# Topics-over-time precomputed
tot_path = PROCESSED / "topics_over_time_all.csv"
topics_ot = pd.read_csv(tot_path) if tot_path.exists() else None

# Conclusion themes (optional)
concl_path = PROCESSED / "conclusion_themes_enriched.csv"
conclusions = pd.read_csv(concl_path) if concl_path.exists() else None

print(f"  framing_clean:          {len(framing):,} rows")
print(f"  framing_clean_abstracts:{len(framing_abs):,} rows")
print(f"  all_sentences_clean:    {len(all_sent):,} rows")
print(f"  PubMed abstracts:       {len(abstracts):,} rows")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 1: Pipeline + Corpus Overview
# ═════════════════════════════════════════════════════════════════════════════
try:
    print("\n── Figure 1: Pipeline + Corpus Overview ──")
    fig1, axes1 = plt.subplots(2, 2, figsize=(14, 10))
    fig1.subplots_adjust(hspace=0.35, wspace=0.35)

    # ── Panel A: Pipeline flowchart ──────────────────────────────────────────
    ax = axes1[0, 0]
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis("off")
    panel_label(ax, "A")

    box_kw = dict(
        boxstyle="round,pad=0.4", facecolor=WONG["cyan"],
        edgecolor="#333333", linewidth=1.2,
    )
    box_kw2 = dict(
        boxstyle="round,pad=0.4", facecolor=WONG["orange"],
        edgecolor="#333333", linewidth=1.2,
    )
    box_kw3 = dict(
        boxstyle="round,pad=0.4", facecolor=WONG["green"],
        edgecolor="#333333", linewidth=1.2,
    )

    n_abs = f"{len(abstracts):,}"
    n_sent = f"{len(all_sent):,}"
    n_topics = all_sent.loc[
        all_sent["bertopic_id_v2"] != -1, "topic_clean_v2"
    ].nunique()
    n_frames = f"{len(framing):,}"

    # Boxes
    ax.text(2, 8.5, f"PubMed\n{n_abs} abstracts", ha="center", va="center",
            fontsize=11, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor=WONG["cyan"],
                      edgecolor="#333", lw=1.2))
    ax.text(7, 8.5, f"PMC + Unpaywall\n{n_sent} sentences", ha="center",
            va="center", fontsize=11, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor=WONG["cyan"],
                      edgecolor="#333", lw=1.2))
    ax.text(5, 4.5, f"BERTopic\n{n_topics} topics", ha="center", va="center",
            fontsize=11, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor=WONG["orange"],
                      edgecolor="#333", lw=1.2))
    ax.text(8, 4.5, f"Frame detection\n{n_frames} frames", ha="center",
            va="center", fontsize=11, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor=WONG["green"],
                      edgecolor="#333", lw=1.2))

    # Arrows
    arrow_kw = dict(arrowstyle="-|>", color="#333333", lw=2,
                    mutation_scale=15)
    ax.annotate("", xy=(5.5, 8.5), xytext=(3.5, 8.5),
                arrowprops=arrow_kw)
    ax.annotate("", xy=(5, 5.5), xytext=(6.5, 7.5),
                arrowprops=arrow_kw)
    ax.annotate("", xy=(8, 5.5), xytext=(7.5, 7.5),
                arrowprops=arrow_kw)

    # ── Panel B: Publication volume per year ─────────────────────────────────
    ax = axes1[0, 1]
    panel_label(ax, "B")
    pub_yr = abstracts.groupby("year").size().reset_index(name="count")
    pub_yr = pub_yr[(pub_yr["year"] >= 1975) & (pub_yr["year"] <= 2026)]
    ax.bar(pub_yr["year"], pub_yr["count"], color=WONG["blue"], width=0.8,
           edgecolor="none")
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of abstracts")
    ax.set_title("Publication volume per year")
    despine(ax)

    # ── Panel C: Corpus statistics table ─────────────────────────────────────
    ax = axes1[1, 0]
    ax.axis("off")
    panel_label(ax, "C")

    frame_types = framing["frame_type"].nunique()
    yr_range = f"{int(abstracts['year'].min())}--{int(abstracts['year'].max())}"
    stats_rows = [
        ("PubMed abstracts", f"{len(abstracts):,}"),
        ("Full-text sentences", f"{len(all_sent):,}"),
        ("Framing sentences", f"{len(framing):,}"),
        ("BERTopic topics", str(n_topics)),
        ("Frame types", str(frame_types)),
        ("Years", yr_range),
    ]
    y_start = 0.90
    for i, (lbl, val) in enumerate(stats_rows):
        y = y_start - i * 0.13
        ax.text(0.10, y, lbl, transform=ax.transAxes, fontsize=12,
                va="top", ha="left", fontweight="bold")
        ax.text(0.70, y, val, transform=ax.transAxes, fontsize=12,
                va="top", ha="left")
    ax.set_title("Corpus statistics", fontsize=13, fontweight="bold", pad=10)

    # ── Panel D: Frame type distribution ─────────────────────────────────────
    ax = axes1[1, 1]
    panel_label(ax, "D")
    ft_counts = framing["frame_type"].value_counts()
    ft_labels = [frame_display(ft) for ft in ft_counts.index]
    ft_colors = [safe_color(ft) for ft in ft_counts.index]
    y_pos = np.arange(len(ft_counts))
    ax.barh(y_pos, ft_counts.values, color=ft_colors, edgecolor="none",
            height=0.7)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(ft_labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Number of sentences")
    ax.set_title("Frame type distribution")
    despine(ax)

    # Legend for semantic groups
    legend_handles = [
        mpatches.Patch(color=_WARM_CMAP[0], label="Metaphorical"),
        mpatches.Patch(color=_COOL_CMAP[0], label="Functional"),
        mpatches.Patch(color=_NEUT_CMAP[0], label="Descriptive"),
    ]
    ax.legend(handles=legend_handles, loc="lower right", fontsize=8)

    save_fig(fig1, GRAPHS / "fig1_pipeline_corpus_overview")
    plt.close(fig1)
    print("  Done.")

except Exception:
    traceback.print_exc()
    print("  FAILED: Figure 1")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 2: Research Topics
# ═════════════════════════════════════════════════════════════════════════════
try:
    print("\n── Figure 2: Research Topics ──")
    fig2, axes2 = plt.subplots(2, 2, figsize=(14, 10))
    fig2.subplots_adjust(hspace=0.40, wspace=0.40)

    # ── Panel A: Top 20 BERTopic topics ──────────────────────────────────────
    ax = axes2[0, 0]
    panel_label(ax, "A")

    assigned = all_sent[all_sent["bertopic_id_v2"] != -1].copy()
    topic_counts = assigned["topic_clean_v2"].value_counts().head(20)
    t_labels = [clean_topic_name(t) for t in topic_counts.index]

    y_pos = np.arange(len(topic_counts))
    palette_list = list(WONG.values())
    t_colors = [palette_list[i % len(palette_list)] for i in range(len(topic_counts))]

    ax.barh(y_pos, topic_counts.values, color=t_colors, edgecolor="none",
            height=0.7)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(t_labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("Number of sentences")
    ax.set_title("Top 20 BERTopic topics")
    despine(ax)

    # ── Panel B: Topics over time stacked area ───────────────────────────────
    ax = axes2[0, 1]
    panel_label(ax, "B")

    top15_topics = topic_counts.head(15).index.tolist()

    if topics_ot is not None and "Topic" in topics_ot.columns:
        # Use precomputed topics-over-time
        # Map topic IDs back to names via all_sent
        tid_to_name = (
            all_sent[all_sent["bertopic_id_v2"] != -1]
            .drop_duplicates("bertopic_id_v2")
            .set_index("bertopic_id_v2")["topic_clean_v2"]
            .to_dict()
        )
        tot = topics_ot.copy()
        tot["topic_name"] = tot["Topic"].map(tid_to_name).fillna("Other")
        tot = tot[tot["topic_name"].isin(top15_topics)]
        tot["year"] = tot["Timestamp"].apply(
            lambda x: int(float(x)) if pd.notna(x) else None
        )
        tot = tot.dropna(subset=["year"])
        pivot = tot.pivot_table(
            index="year", columns="topic_name", values="Frequency",
            aggfunc="sum", fill_value=0,
        )
    else:
        # Compute from all_sent
        tmp = assigned[assigned["topic_clean_v2"].isin(top15_topics)].copy()
        pivot = pd.crosstab(tmp["year"], tmp["topic_clean_v2"])

    pivot = pivot.sort_index()
    years = pivot.index.values
    data_dict = {}
    colors_dict = {}
    for i, col in enumerate(pivot.columns):
        data_dict[clean_topic_name(col)] = rolling_mean(
            pivot[col].values, window=3
        ).values
        colors_dict[clean_topic_name(col)] = palette_list[
            i % len(palette_list)
        ]

    if len(data_dict) > 0:
        stacked_area(ax, years, data_dict, colors_dict, smooth_window=1)
        ax.set_xlabel("Year")
        ax.set_ylabel("Number of sentences")
        ax.set_title("Top 15 topics over time")
        ax.legend(fontsize=6, ncol=2, loc="upper left",
                  bbox_to_anchor=(0, 1))
    else:
        ax.text(0.5, 0.5, "No topic-over-time data", transform=ax.transAxes,
                ha="center")
    despine(ax)

    # ── Panel C: Topics x Nuclei heatmap ─────────────────────────────────────
    ax = axes2[1, 0]
    panel_label(ax, "C")

    sent_nuclei = tag_nuclei(assigned.copy(), text_col="sentence")
    nuc_cols = [f"has_{nid}" for nid in NUCLEI.keys()]
    top10_topics = topic_counts.head(10).index.tolist()

    # Count co-occurrences
    nuc_ids = list(NUCLEI.keys())
    nuc_labels_short = [NUCLEI[n]["label"].split("(")[0].strip()
                        if "(" in NUCLEI[n]["label"]
                        else NUCLEI[n]["label"] for n in nuc_ids]

    # Pick top 8 nuclei by total occurrence
    nuc_totals = {nid: sent_nuclei[f"has_{nid}"].sum() for nid in nuc_ids}
    top8_nuc = sorted(nuc_totals, key=nuc_totals.get, reverse=True)[:8]
    top8_nuc_labels = [
        NUCLEI[n]["label"].split("(")[0].strip()
        if "(" in NUCLEI[n]["label"] else NUCLEI[n]["label"]
        for n in top8_nuc
    ]

    mat_c = np.zeros((len(top10_topics), len(top8_nuc)))
    for i, topic in enumerate(top10_topics):
        sub = sent_nuclei[sent_nuclei["topic_clean_v2"] == topic]
        for j, nid in enumerate(top8_nuc):
            mat_c[i, j] = sub[f"has_{nid}"].sum()

    topic_row_labels = [clean_topic_name(t) for t in top10_topics]
    heatmap(ax, mat_c, topic_row_labels, top8_nuc_labels,
            cmap="YlOrRd", fmt=".0f", fontsize=7)
    ax.set_title("Topics x Nuclei co-occurrence")

    # ── Panel D: Topics x Techniques heatmap ─────────────────────────────────
    ax = axes2[1, 1]
    panel_label(ax, "D")

    sent_tech = tag_techniques(assigned.copy(), text_col="sentence")
    tech_ids = list(TECHNIQUES.keys())
    tech_totals = {tid: sent_tech[f"has_{tid}"].sum() for tid in tech_ids}
    top8_tech = sorted(tech_totals, key=tech_totals.get, reverse=True)[:8]
    top8_tech_labels = [TECHNIQUES[t]["label"] for t in top8_tech]

    mat_d = np.zeros((len(top10_topics), len(top8_tech)))
    for i, topic in enumerate(top10_topics):
        sub = sent_tech[sent_tech["topic_clean_v2"] == topic]
        for j, tid in enumerate(top8_tech):
            mat_d[i, j] = sub[f"has_{tid}"].sum()

    heatmap(ax, mat_d, topic_row_labels, top8_tech_labels,
            cmap="YlOrRd", fmt=".0f", fontsize=7)
    ax.set_title("Topics x Techniques co-occurrence")

    save_fig(fig2, GRAPHS / "fig2_research_topics")
    plt.close(fig2)
    print("  Done.")

except Exception:
    traceback.print_exc()
    print("  FAILED: Figure 2")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 3: Framing Over Time — THE KEY FIGURE
# ═════════════════════════════════════════════════════════════════════════════
try:
    print("\n── Figure 3: Framing Over Time ──")
    fig3, axes3 = plt.subplots(2, 2, figsize=(14, 10))
    fig3.subplots_adjust(hspace=0.40, wspace=0.35)

    # ── Helper: compute proportions over time ────────────────────────────────
    def frame_proportions(df, year_min=None, year_max=None, min_per_year=3):
        """Return (years, {frame_type: proportion_array})."""
        tmp = df.copy()
        if year_min:
            tmp = tmp[tmp["year"] >= year_min]
        if year_max:
            tmp = tmp[tmp["year"] <= year_max]
        yr_ft = pd.crosstab(tmp["year"], tmp["frame_type"])
        yr_total = yr_ft.sum(axis=1)
        # Filter years with enough data
        keep = yr_total >= min_per_year
        yr_ft = yr_ft.loc[keep]
        yr_total = yr_total.loc[keep]
        yr_prop = yr_ft.div(yr_total, axis=0)
        return yr_prop

    # ── Panel A: Framing proportions — ABSTRACTS ─────────────────────────────
    ax = axes3[0, 0]
    panel_label(ax, "A")

    prop_abs = frame_proportions(framing_abs, year_min=1980, year_max=2026,
                                 min_per_year=3)
    years_a = prop_abs.index.values
    # Sort frame types by total proportion (descending)
    ft_order_abs = prop_abs.sum().sort_values(ascending=False).index.tolist()

    data_a = {}
    colors_a = {}
    for ft in ft_order_abs:
        smooth = rolling_mean(prop_abs[ft].values, window=3)
        data_a[frame_display(ft)] = smooth.values
        colors_a[frame_display(ft)] = safe_color(ft)

    stacked_area(ax, years_a, data_a, colors_a, smooth_window=1)
    ax.set_xlabel("Year")
    ax.set_ylabel("Proportion of framing sentences")
    ax.set_title("Frame proportions over time (abstracts)")
    ax.set_ylim(0, 1)
    ax.legend(fontsize=5, ncol=3, loc="upper left", bbox_to_anchor=(0, 1))
    despine(ax)

    # ── Panel B: Framing proportions — FULL TEXT ─────────────────────────────
    ax = axes3[0, 1]
    panel_label(ax, "B")

    prop_ft = frame_proportions(framing, year_min=2000, year_max=2026,
                                min_per_year=3)
    years_b = prop_ft.index.values
    ft_order_ft = prop_ft.sum().sort_values(ascending=False).index.tolist()

    data_b = {}
    colors_b = {}
    for ft in ft_order_ft:
        smooth = rolling_mean(prop_ft[ft].values, window=3)
        data_b[frame_display(ft)] = smooth.values
        colors_b[frame_display(ft)] = safe_color(ft)

    stacked_area(ax, years_b, data_b, colors_b, smooth_window=1)
    ax.set_xlabel("Year")
    ax.set_ylabel("Proportion of framing sentences")
    ax.set_title("Frame proportions over time (full text)")
    ax.set_ylim(0, 1)
    ax.legend(fontsize=5, ncol=3, loc="upper left", bbox_to_anchor=(0, 1))
    despine(ax)

    # ── Panel C: Mann-Kendall trends ─────────────────────────────────────────
    ax = axes3[1, 0]
    panel_label(ax, "C")

    mk_items = []
    for ft in prop_abs.columns:
        yrs = prop_abs.index.values.astype(float)
        vals = prop_abs[ft].values.astype(float)
        tau, p, sig, lbl = mann_kendall(yrs, vals)
        if not np.isnan(tau):
            display = frame_display(ft)
            if sig in ("*", "**", "***"):
                display += f" {sig}"
            mk_items.append((display, tau))

    mk_bar(ax, mk_items, sort=True)
    ax.set_title("Mann-Kendall trends (abstracts)")

    # ── Panel D: Relay vs Hub crossover ──────────────────────────────────────
    ax = axes3[1, 1]
    panel_label(ax, "D")

    # Compute annual proportions for relay and hub from abstracts
    yr_counts_abs = framing_abs.groupby("year").size().rename("total")
    has_relay = "relay" in framing_abs["frame_type"].values
    has_hub = "hub" in framing_abs["frame_type"].values

    if has_relay and has_hub:
        relay_yr = (
            framing_abs[framing_abs["frame_type"] == "relay"]
            .groupby("year").size().rename("relay")
        )
        hub_yr = (
            framing_abs[framing_abs["frame_type"] == "hub"]
            .groupby("year").size().rename("hub")
        )
        cross_df = pd.concat([yr_counts_abs, relay_yr, hub_yr], axis=1).fillna(0)
        cross_df["relay_pct"] = cross_df["relay"] / cross_df["total"]
        cross_df["hub_pct"] = cross_df["hub"] / cross_df["total"]
        cross_df = cross_df[cross_df["total"] >= 3].sort_index()

        yrs = cross_df.index.values
        relay_sm = rolling_mean(cross_df["relay_pct"], window=3).values
        hub_sm = rolling_mean(cross_df["hub_pct"], window=3).values

        ax.plot(yrs, relay_sm, color=FRAME_COLORS.get("relay", WONG["red"]),
                linewidth=2.5, label="Relay")
        ax.plot(yrs, hub_sm, color=FRAME_COLORS.get("hub", WONG["blue"]),
                linewidth=2.5, label="Hub")

        # Wilson CI shading
        for ft_name, col, pct_col in [
            ("relay", FRAME_COLORS.get("relay", WONG["red"]), "relay_pct"),
            ("hub", FRAME_COLORS.get("hub", WONG["blue"]), "hub_pct"),
        ]:
            lo, hi = [], []
            for _, row in cross_df.iterrows():
                cnt = int(row.get(ft_name, 0))
                tot = int(row["total"])
                l, u = wilson_ci(cnt, tot)
                lo.append(l)
                hi.append(u)
            ax.fill_between(yrs, lo, hi, color=col, alpha=0.12)

        # Find and annotate crossover
        diff = relay_sm - hub_sm
        sign_changes = np.where(np.diff(np.sign(diff)))[0]
        if len(sign_changes) > 0:
            cx_idx = sign_changes[-1]
            cx_year = (yrs[cx_idx] + yrs[cx_idx + 1]) / 2
            cx_val = (relay_sm[cx_idx] + hub_sm[cx_idx]) / 2
            ax.annotate(
                f"Crossover ~{int(cx_year)}",
                xy=(cx_year, cx_val),
                xytext=(cx_year - 5, cx_val + 0.10),
                fontsize=9, fontweight="bold",
                arrowprops=dict(arrowstyle="->", color="#333"),
            )

        ax.set_xlabel("Year")
        ax.set_ylabel("Proportion of framing sentences")
        ax.set_title("Relay vs Hub crossover (abstracts)")
        ax.legend(fontsize=10)
    else:
        ax.text(0.5, 0.5, "Relay or Hub not found", transform=ax.transAxes,
                ha="center")
    despine(ax)

    save_fig(fig3, GRAPHS / "fig3_framing_over_time")
    plt.close(fig3)
    print("  Done.")

except Exception:
    traceback.print_exc()
    print("  FAILED: Figure 3")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 4: Framing x Nuclei x Techniques
# ═════════════════════════════════════════════════════════════════════════════
try:
    print("\n── Figure 4: Framing x Nuclei x Techniques ──")
    fig4, axes4 = plt.subplots(2, 2, figsize=(14, 10))
    fig4.subplots_adjust(hspace=0.45, wspace=0.40)

    # Tag framing sentences
    fr_nuc = tag_nuclei(framing.copy(), text_col="sentence")
    fr_tech = tag_techniques(framing.copy(), text_col="sentence")

    # Top 10 frame types by count
    top10_ft = framing["frame_type"].value_counts().head(10).index.tolist()
    ft_row_labels = [frame_display(ft) for ft in top10_ft]

    # Top 8 nuclei by occurrence in framing sentences
    nuc_ids = list(NUCLEI.keys())
    nuc_totals_fr = {nid: fr_nuc[f"has_{nid}"].sum() for nid in nuc_ids}
    top8_nuc_fr = sorted(nuc_totals_fr, key=nuc_totals_fr.get, reverse=True)[:8]
    top8_nuc_labels_fr = [
        NUCLEI[n]["label"].split("(")[0].strip()
        if "(" in NUCLEI[n]["label"] else NUCLEI[n]["label"]
        for n in top8_nuc_fr
    ]

    # ── Panel A: Framing per nucleus heatmap (column-normalised) ─────────────
    ax = axes4[0, 0]
    panel_label(ax, "A")

    mat_a = np.zeros((len(top10_ft), len(top8_nuc_fr)))
    for i, ft in enumerate(top10_ft):
        sub = fr_nuc[fr_nuc["frame_type"] == ft]
        for j, nid in enumerate(top8_nuc_fr):
            mat_a[i, j] = sub[f"has_{nid}"].sum()

    # Column-normalise: proportion of each nucleus's framed sentences
    col_sums = mat_a.sum(axis=0, keepdims=True)
    col_sums[col_sums == 0] = 1
    mat_a_norm = mat_a / col_sums

    heatmap(ax, mat_a_norm, ft_row_labels, top8_nuc_labels_fr,
            cmap="YlOrRd", fmt=".2f", fontsize=7)
    ax.set_title("Frame type per nucleus (col-normalised)")

    # ── Panel B: Framing per technique heatmap ───────────────────────────────
    ax = axes4[0, 1]
    panel_label(ax, "B")

    tech_ids = list(TECHNIQUES.keys())
    tech_totals_fr = {tid: fr_tech[f"has_{tid}"].sum() for tid in tech_ids}
    top8_tech_fr = sorted(tech_totals_fr, key=tech_totals_fr.get,
                          reverse=True)[:8]
    top8_tech_labels_fr = [TECHNIQUES[t]["label"] for t in top8_tech_fr]

    mat_b = np.zeros((len(top10_ft), len(top8_tech_fr)))
    for i, ft in enumerate(top10_ft):
        sub = fr_tech[fr_tech["frame_type"] == ft]
        for j, tid in enumerate(top8_tech_fr):
            mat_b[i, j] = sub[f"has_{tid}"].sum()

    col_sums_b = mat_b.sum(axis=0, keepdims=True)
    col_sums_b[col_sums_b == 0] = 1
    mat_b_norm = mat_b / col_sums_b

    heatmap(ax, mat_b_norm, ft_row_labels, top8_tech_labels_fr,
            cmap="YlOrRd", fmt=".2f", fontsize=7)
    ax.set_title("Frame type per technique (col-normalised)")

    # ── Panel C: Nucleus prevalence over time (abstracts) ────────────────────
    ax = axes4[1, 0]
    panel_label(ax, "C")

    abs_text = abstracts[["pmid", "year", "abstract"]].dropna(
        subset=["abstract"]
    ).copy()
    abs_nuc = tag_nuclei(abs_text.copy(), text_col="abstract")

    # Compute per-year proportion for each nucleus
    nuc_ids_all = list(NUCLEI.keys())
    nuc_total_abs = {nid: abs_nuc[f"has_{nid}"].sum() for nid in nuc_ids_all}
    top10_nuc_abs = sorted(nuc_total_abs, key=nuc_total_abs.get,
                           reverse=True)[:10]

    yr_groups = abs_nuc.groupby("year")
    yr_total_abs = yr_groups.size()

    for idx, nid in enumerate(top10_nuc_abs):
        yr_nuc = yr_groups[f"has_{nid}"].sum()
        pct = (yr_nuc / yr_total_abs).dropna()
        pct = pct[(pct.index >= 1985) & (pct.index <= 2026)]
        if len(pct) < 3:
            continue
        smooth = rolling_mean(pct, window=3)
        col = NUCLEI[nid]["color"]
        short_label = (
            NUCLEI[nid]["label"].split("(")[0].strip()
            if "(" in NUCLEI[nid]["label"]
            else NUCLEI[nid]["label"]
        )
        ax.plot(smooth.index, smooth.values, color=col, linewidth=1.5,
                label=short_label)

    ax.set_xlabel("Year")
    ax.set_ylabel("Proportion of abstracts")
    ax.set_title("Nucleus prevalence over time (abstracts)")
    ax.legend(fontsize=6, ncol=2, loc="upper left")
    despine(ax)

    # ── Panel D: Technique prevalence over time (abstracts) ──────────────────
    ax = axes4[1, 1]
    panel_label(ax, "D")

    abs_tech = tag_techniques(abs_text.copy(), text_col="abstract")
    tech_ids_all = list(TECHNIQUES.keys())
    tech_total_abs = {tid: abs_tech[f"has_{tid}"].sum() for tid in tech_ids_all}
    top8_tech_abs = sorted(tech_total_abs, key=tech_total_abs.get,
                           reverse=True)[:8]

    yr_groups_t = abs_tech.groupby("year")
    yr_total_t = yr_groups_t.size()

    for idx, tid in enumerate(top8_tech_abs):
        yr_tech = yr_groups_t[f"has_{tid}"].sum()
        pct = (yr_tech / yr_total_t).dropna()
        pct = pct[(pct.index >= 1990) & (pct.index <= 2026)]
        if len(pct) < 3:
            continue
        smooth = rolling_mean(pct, window=3)
        col = TECHNIQUES[tid]["color"]
        ax.plot(smooth.index, smooth.values, color=col, linewidth=1.5,
                label=TECHNIQUES[tid]["label"])

    ax.set_xlabel("Year")
    ax.set_ylabel("Proportion of abstracts")
    ax.set_title("Technique prevalence over time (abstracts)")
    ax.legend(fontsize=6, ncol=2, loc="upper left")
    despine(ax)

    save_fig(fig4, GRAPHS / "fig4_framing_nuclei_techniques")
    plt.close(fig4)
    print("  Done.")

except Exception:
    traceback.print_exc()
    print("  FAILED: Figure 4")


# ═════════════════════════════════════════════════════════════════════════════
# FIGURE 5: Framing x Topics
# ═════════════════════════════════════════════════════════════════════════════
try:
    print("\n── Figure 5: Framing x Topics ──")
    fig5, axes5 = plt.subplots(1, 2, figsize=(14, 8))
    fig5.subplots_adjust(wspace=0.40)

    # ── Panel A: Frames stratified by BERTopic topic ─────────────────────────
    ax = axes5[0]
    panel_label(ax, "A")

    # framing_clean has bertopic_id — merge with topic names from all_sent
    topic_map = (
        all_sent[all_sent["bertopic_id_v2"] != -1]
        .drop_duplicates("bertopic_id_v2")
        .set_index("bertopic_id_v2")["topic_clean_v2"]
        .to_dict()
    )
    # framing_clean uses bertopic_id (v1) — map via bertopic_label or use
    # sentence matching.  Actually, framing_clean has bertopic_id which is v1.
    # We merge by pmid+sentence to get topic_clean_v2.
    framing_with_topic = framing.merge(
        all_sent[["pmid", "sentence", "topic_clean_v2"]],
        on=["pmid", "sentence"],
        how="left",
    )
    framing_with_topic = framing_with_topic.dropna(subset=["topic_clean_v2"])
    framing_with_topic = framing_with_topic[
        framing_with_topic["topic_clean_v2"] != "unassigned"
    ]

    if len(framing_with_topic) > 0:
        # Top 10 topics with most framing sentences
        top_frame_topics = (
            framing_with_topic["topic_clean_v2"]
            .value_counts().head(10).index.tolist()
        )
        sub = framing_with_topic[
            framing_with_topic["topic_clean_v2"].isin(top_frame_topics)
        ]
        cross = pd.crosstab(sub["topic_clean_v2"], sub["frame_type"])
        # Reorder rows by total
        cross = cross.loc[
            cross.sum(axis=1).sort_values(ascending=True).index
        ]

        # Select top frame types present
        ft_order = cross.sum().sort_values(ascending=False).index.tolist()
        cross = cross[ft_order]

        # Stacked horizontal bar
        left = np.zeros(len(cross))
        for ft_col in cross.columns:
            vals = cross[ft_col].values.astype(float)
            col = safe_color(ft_col)
            ax.barh(
                np.arange(len(cross)), vals, left=left,
                color=col, edgecolor="none", height=0.7,
                label=frame_display(ft_col),
            )
            left += vals

        ax.set_yticks(np.arange(len(cross)))
        ax.set_yticklabels([clean_topic_name(t) for t in cross.index],
                           fontsize=8)
        ax.set_xlabel("Number of framing sentences")
        ax.set_title("Frame types per BERTopic topic")
        # Avoid duplicate legend entries
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax.legend(by_label.values(), by_label.keys(), fontsize=6, ncol=2,
                  loc="lower right")
    else:
        ax.text(0.5, 0.5, "No topic-framing data available",
                transform=ax.transAxes, ha="center")
    despine(ax)

    # ── Panel B: Frame x Conclusion theme or section analysis ────────────────
    ax = axes5[1]
    panel_label(ax, "B")

    if conclusions is not None and len(conclusions) > 0:
        # Cross-tab frame_type x conclusion theme
        # conclusion_themes_enriched has theme columns as booleans
        theme_cols = [c for c in conclusions.columns
                      if c not in ("year", "pmid", "sentence", "cluster_id",
                                   "frame_type", "frame_all")]
        if "frame_type" not in conclusions.columns:
            # Merge framing info by sentence
            concl_merged = conclusions.merge(
                framing[["pmid", "sentence", "frame_type"]],
                on=["pmid", "sentence"], how="left",
            )
        else:
            concl_merged = conclusions.copy()

        concl_merged = concl_merged.dropna(subset=["frame_type"])

        if len(concl_merged) > 0 and len(theme_cols) > 0:
            # Build matrix: rows = frame types, cols = themes
            top_ft_concl = (
                concl_merged["frame_type"].value_counts().head(8).index.tolist()
            )
            mat_e = np.zeros((len(top_ft_concl), len(theme_cols)))
            for i, ft in enumerate(top_ft_concl):
                sub = concl_merged[concl_merged["frame_type"] == ft]
                for j, tc in enumerate(theme_cols):
                    if tc in sub.columns:
                        mat_e[i, j] = sub[tc].sum()

            # Normalise by row
            row_sums = mat_e.sum(axis=1, keepdims=True)
            row_sums[row_sums == 0] = 1
            mat_e_norm = mat_e / row_sums

            short_themes = [t.replace("_", " ").replace("/", "\n")[:20]
                            for t in theme_cols]
            heatmap(ax, mat_e_norm,
                    [frame_display(ft) for ft in top_ft_concl],
                    short_themes, cmap="YlGnBu", fmt=".2f", fontsize=7)
            ax.set_title("Frame type x Conclusion theme")
        else:
            ax.text(0.5, 0.5, "Insufficient conclusion-frame overlap",
                    transform=ax.transAxes, ha="center")
            ax.axis("off")
    else:
        # Fallback: frame distribution across abstract vs full-text
        abs_ft_counts = framing_abs["frame_type"].value_counts(normalize=True)
        ft_ft_counts = framing["frame_type"].value_counts(normalize=True)
        all_fts = sorted(
            set(abs_ft_counts.index) | set(ft_ft_counts.index)
        )
        # Side-by-side bar
        x = np.arange(len(all_fts))
        w = 0.35
        abs_vals = [abs_ft_counts.get(ft, 0) for ft in all_fts]
        ft_vals = [ft_ft_counts.get(ft, 0) for ft in all_fts]
        ax.barh(x - w / 2, abs_vals, height=w, color=WONG["blue"],
                label="Abstracts", edgecolor="none")
        ax.barh(x + w / 2, ft_vals, height=w, color=WONG["orange"],
                label="Full text", edgecolor="none")
        ax.set_yticks(x)
        ax.set_yticklabels([frame_display(ft) for ft in all_fts], fontsize=7)
        ax.set_xlabel("Proportion")
        ax.set_title("Frame distribution: Abstracts vs Full text")
        ax.legend(fontsize=9)
        ax.invert_yaxis()
        despine(ax)

    save_fig(fig5, GRAPHS / "fig5_framing_topics")
    plt.close(fig5)
    print("  Done.")

except Exception:
    traceback.print_exc()
    print("  FAILED: Figure 5")


# ═════════════════════════════════════════════════════════════════════════════
# SUPPLEMENTARY FIGURES
# ═════════════════════════════════════════════════════════════════════════════

# ── S1: UMAP of all sentences coloured by topic ─────────────────────────────
try:
    print("\n── Supplementary S1: UMAP by topic ──")

    embed_path = PROCESSED / "all_sentences_embeddings.npy"
    if embed_path.exists():
        from sklearn.decomposition import PCA
        try:
            from umap import UMAP
            has_umap = True
        except ImportError:
            has_umap = False

        if has_umap:
            embeddings = np.load(embed_path)
            n_samples = min(len(embeddings), len(all_sent))
            embeddings = embeddings[:n_samples]
            topics = all_sent["topic_clean_v2"].iloc[:n_samples].values

            # Reduce first with PCA for speed
            if embeddings.shape[0] > 5000:
                pca = PCA(n_components=50, random_state=42)
                embeddings_pca = pca.fit_transform(embeddings)
            else:
                embeddings_pca = embeddings

            reducer = UMAP(n_components=2, random_state=42, n_neighbors=15,
                           min_dist=0.1, metric="cosine")
            coords = reducer.fit_transform(embeddings_pca)

            fig_s1, ax_s1 = plt.subplots(figsize=(10, 8))
            panel_label(ax_s1, "S1")

            # Top 15 topics get colours, rest grey
            top15 = (
                all_sent[all_sent["topic_clean_v2"] != "unassigned"]
                ["topic_clean_v2"].value_counts().head(15).index.tolist()
            )
            palette_list = list(WONG.values())

            # Plot grey points first
            mask_other = ~np.isin(topics, top15)
            ax_s1.scatter(coords[mask_other, 0], coords[mask_other, 1],
                          c=WONG["ltgrey"], s=1, alpha=0.3, rasterized=True)

            for i, topic in enumerate(top15):
                mask = topics == topic
                col = palette_list[i % len(palette_list)]
                ax_s1.scatter(coords[mask, 0], coords[mask, 1],
                              c=col, s=3, alpha=0.5, rasterized=True,
                              label=clean_topic_name(topic))

            ax_s1.set_xlabel("UMAP-1")
            ax_s1.set_ylabel("UMAP-2")
            ax_s1.set_title("UMAP of all sentences coloured by BERTopic topic")
            ax_s1.legend(fontsize=6, ncol=2, markerscale=4,
                         loc="upper right")
            despine(ax_s1)

            save_fig(fig_s1, GRAPHS / "sup_S1_umap_topics")
            plt.close(fig_s1)
            print("  Done.")
        else:
            print("  SKIPPED: umap-learn not installed.")
    else:
        print("  SKIPPED: embeddings file not found.")

except Exception:
    traceback.print_exc()
    print("  FAILED: S1")


# ── S2: Abstract vs full-text framing replication ────────────────────────────
try:
    print("\n── Supplementary S2: Abstract vs full-text replication ──")
    fig_s2, (ax_s2a, ax_s2b) = plt.subplots(1, 2, figsize=(14, 6))
    fig_s2.subplots_adjust(wspace=0.30)
    panel_label(ax_s2a, "A")
    panel_label(ax_s2b, "B")

    for ft_name, color, ax_target, title in [
        ("relay", FRAME_COLORS.get("relay", WONG["red"]), ax_s2a,
         "Relay % over time"),
        ("hub", FRAME_COLORS.get("hub", WONG["blue"]), ax_s2b,
         "Hub % over time"),
    ]:
        # Abstracts
        yr_total_a = framing_abs.groupby("year").size()
        yr_ft_a = (
            framing_abs[framing_abs["frame_type"] == ft_name]
            .groupby("year").size()
        )
        pct_a = (yr_ft_a / yr_total_a).dropna()
        pct_a = pct_a[pct_a.index >= 1985]
        pct_a_sm = rolling_mean(pct_a, window=3)

        # Full text
        yr_total_f = framing.groupby("year").size()
        yr_ft_f = (
            framing[framing["frame_type"] == ft_name]
            .groupby("year").size()
        )
        pct_f = (yr_ft_f / yr_total_f).dropna()
        pct_f_sm = rolling_mean(pct_f, window=3)

        ax_target.plot(pct_a_sm.index, pct_a_sm.values, color=color,
                       linewidth=2, label="Abstracts", linestyle="-")
        ax_target.plot(pct_f_sm.index, pct_f_sm.values, color=color,
                       linewidth=2, label="Full text", linestyle="--")
        ax_target.set_xlabel("Year")
        ax_target.set_ylabel("Proportion")
        ax_target.set_title(title)
        ax_target.legend(fontsize=9)
        despine(ax_target)

    save_fig(fig_s2, GRAPHS / "sup_S2_abstract_vs_fulltext_replication")
    plt.close(fig_s2)
    print("  Done.")

except Exception:
    traceback.print_exc()
    print("  FAILED: S2")


# ── S3: Robustness — frame detection with/without specific patterns ──────────
try:
    print("\n── Supplementary S3: Frame detection robustness ──")
    fig_s3, ax_s3 = plt.subplots(figsize=(10, 6))
    panel_label(ax_s3, "S3")

    # Show frame_all column — some sentences match multiple frames
    # Compare frame_type (primary) counts vs frame_all (all matches)
    primary_counts = framing["frame_type"].value_counts()
    # frame_all may contain comma-separated values
    all_frames_exploded = (
        framing["frame_all"].str.split(",").explode().str.strip()
    )
    all_counts = all_frames_exploded.value_counts()

    # Merge for comparison
    comparison = pd.DataFrame({
        "Primary detection": primary_counts,
        "All pattern matches": all_counts,
    }).fillna(0).sort_values("Primary detection", ascending=True)

    x = np.arange(len(comparison))
    w = 0.35
    ax_s3.barh(x - w / 2, comparison["Primary detection"], height=w,
               color=WONG["blue"], label="Primary (frame_type)",
               edgecolor="none")
    ax_s3.barh(x + w / 2, comparison["All pattern matches"], height=w,
               color=WONG["orange"], label="All matches (frame_all)",
               edgecolor="none")
    ax_s3.set_yticks(x)
    ax_s3.set_yticklabels([frame_display(ft) for ft in comparison.index],
                          fontsize=8)
    ax_s3.set_xlabel("Number of sentences")
    ax_s3.set_title("Frame detection: primary vs all pattern matches")
    ax_s3.legend(fontsize=9)
    despine(ax_s3)

    save_fig(fig_s3, GRAPHS / "sup_S3_robustness_frame_detection")
    plt.close(fig_s3)
    print("  Done.")

except Exception:
    traceback.print_exc()
    print("  FAILED: S3")


# ── S4: BERTopic coherence + word clouds for top 10 topics ──────────────────
try:
    print("\n── Supplementary S4: Topic word clouds ──")

    # Get top 10 topics and their representative words
    assigned = all_sent[all_sent["bertopic_id_v2"] != -1].copy()
    top10_t = assigned["topic_clean_v2"].value_counts().head(10)

    try:
        from wordcloud import WordCloud
        has_wc = True
    except ImportError:
        has_wc = False

    if has_wc:
        fig_s4, axes_s4 = plt.subplots(2, 5, figsize=(16, 7))
        axes_s4 = axes_s4.ravel()

        for i, (topic_name, count) in enumerate(top10_t.items()):
            ax = axes_s4[i]
            # Get sentences for this topic
            sents = assigned.loc[
                assigned["topic_clean_v2"] == topic_name, "sentence"
            ].tolist()
            text = " ".join(str(s) for s in sents[:500])
            if len(text.strip()) < 10:
                ax.text(0.5, 0.5, "No text", transform=ax.transAxes,
                        ha="center")
                ax.axis("off")
                continue
            wc = WordCloud(
                width=300, height=200, max_words=40,
                background_color="white", colormap="viridis",
            ).generate(text)
            ax.imshow(wc, interpolation="bilinear")
            ax.set_title(clean_topic_name(topic_name), fontsize=9,
                         fontweight="bold")
            ax.axis("off")

        fig_s4.suptitle("Word clouds for top 10 BERTopic topics",
                        fontsize=14, fontweight="bold", y=1.01)
        fig_s4.tight_layout()
        save_fig(fig_s4, GRAPHS / "sup_S4_topic_wordclouds")
        plt.close(fig_s4)
        print("  Done.")
    else:
        # Fallback: simple bar chart of topic label words
        fig_s4, ax_s4 = plt.subplots(figsize=(10, 6))

        # Use bertopic_label_v2 as a proxy for keywords
        for i, (topic_name, count) in enumerate(top10_t.items()):
            label_row = assigned.loc[
                assigned["topic_clean_v2"] == topic_name,
                "bertopic_label_v2",
            ].iloc[0] if len(assigned[
                assigned["topic_clean_v2"] == topic_name
            ]) > 0 else ""
            # bertopic_label_v2 looks like "5_word1_word2_word3"
            words = str(label_row).split("_")[1:5]
            print(f"  Topic '{topic_name}': keywords = {words}")

        ax_s4.barh(
            np.arange(len(top10_t)),
            top10_t.values,
            color=[list(WONG.values())[i % len(WONG)]
                   for i in range(len(top10_t))],
            edgecolor="none",
        )
        ax_s4.set_yticks(np.arange(len(top10_t)))
        ax_s4.set_yticklabels(
            [clean_topic_name(t) for t in top10_t.index], fontsize=8
        )
        ax_s4.invert_yaxis()
        ax_s4.set_xlabel("Number of sentences")
        ax_s4.set_title("Top 10 BERTopic topics (wordcloud requires "
                        "'wordcloud' package)")
        despine(ax_s4)
        save_fig(fig_s4, GRAPHS / "sup_S4_topic_wordclouds")
        plt.close(fig_s4)
        print("  Done (fallback, no wordcloud package).")

except Exception:
    traceback.print_exc()
    print("  FAILED: S4")


# ═════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("All figures complete. Output in:", GRAPHS)
print("=" * 60)
