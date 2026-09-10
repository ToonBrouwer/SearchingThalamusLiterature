"""
fig_frame_derivation.py — Supplementary Figure S4: Frame Derivation Pipeline

Shows the 4-stage process of how 21 frame types were derived:
  A: Verb-trigger extraction counts
  B: Proto-category distribution from verb-triggered sentences
  C: Final 21 regex patterns with N matches (colored by group)
  D: Mega-theme composition (keyword-based, with "Other" %)

Also generates a nucleus prevalence trend panel.
"""
import sys, os, re
os.environ["PYTHONIOENCODING"] = "utf-8"
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

from taxonomy import FRAMES_CLEAN

# Wong palette
WONG = {
    "blue": "#0072B2", "orange": "#E69F00", "green": "#009E73",
    "red": "#D55E00", "purple": "#CC79A7", "cyan": "#56B4E9",
    "yellow": "#F0E442", "grey": "#999999",
}
GROUP_COLORS = {
    "Metaphorical": WONG["blue"],
    "Functional": WONG["orange"],
    "Descriptive": WONG["green"],
}

def despine(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

def panel_label(ax, letter, x=-0.08, y=1.05):
    ax.text(x, y, letter, transform=ax.transAxes, fontsize=14, fontweight="bold", va="top")

# Load data
print("Loading data...")
df = pd.read_csv(PROC / "all_sentences_final.csv", low_memory=False)
print(f"  {len(df):,} sentences, {df['pmid'].nunique():,} papers")


# ═══════════════════════════════════════════════════════════════════════════
# STAGE 1: VERB-TRIGGER EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════
print("\nStage 1: Verb triggers...")

verb_triggers = {
    "serves as": r"thalamus\s+(?:\w+\s+)*?serves?\s+as",
    "acts as": r"thalamus\s+(?:\w+\s+)*?acts?\s+as",
    "functions as": r"thalamus\s+(?:\w+\s+)*?functions?\s+as",
    "is a/the": r"thalamus\s+(?:\w+\s+)*?is\s+(?:a|the|an)\s+(?!not)",
    "described as": r"thalamus\s+(?:\w+\s+)*?(?:described|considered|regarded|viewed|known)\s+as",
}

trigger_counts = {}
trigger_sentences = set()
for name, pattern in verb_triggers.items():
    matches = df["sentence"].str.contains(pattern, case=False, na=False, regex=True)
    trigger_counts[name] = matches.sum()
    trigger_sentences.update(df.loc[matches].index.tolist())
    print(f"  '{name}': {matches.sum():,} matches")

total_triggered = len(trigger_sentences)
print(f"  Total unique verb-triggered sentences: {total_triggered:,}")


# ═══════════════════════════════════════════════════════════════════════════
# STAGE 2: PROTO-CATEGORIES FROM TRIGGERED SENTENCES
# ═══════════════════════════════════════════════════════════════════════════
print("\nStage 2: Proto-categories...")

proto_categories = {
    "relay-like": r"relay|transmit|forward|pass(?:es|ing)\s+(?:on|through)",
    "hub-like": r"hub|central\s+(?:node|station|structure)|convergence",
    "gate-like": r"gat(?:e|ing)|select(?:ive|ion)|control\s+(?:of\s+)?flow",
    "gateway-like": r"gateway|entry|portal|door|entrance",
    "modulation-like": r"modulat|regulat|adjust|tune|gain\s+control",
    "integration-like": r"integrat|combin|merg|synthesiz",
    "oscillation-like": r"pacemak|oscillat|rhythm|spindle|clock",
    "clinical-like": r"target|stimulat|DBS|therap|treatment",
    "filter-like": r"filter|screen|block|suppress|attenu",
    "network-like": r"node|network|connect|circuit\s+element",
}

triggered_df = df.loc[list(trigger_sentences)]
proto_counts = {}
for name, pattern in proto_categories.items():
    matches = triggered_df["sentence"].str.contains(pattern, case=False, na=False, regex=True)
    proto_counts[name] = matches.sum()
    print(f"  {name}: {matches.sum():,}")


# ═══════════════════════════════════════════════════════════════════════════
# STAGE 3: FINAL 21 FRAME PATTERNS
# ═══════════════════════════════════════════════════════════════════════════
print("\nStage 3: Final 21 patterns...")

frame_counts = df[df["frame_type"].notna() & (df["frame_type"] != "")]["frame_type"].value_counts()
print(f"  Total framing sentences: {frame_counts.sum():,}")

# Assign groups
frame_list = list(FRAMES_CLEAN.keys())
frame_groups = {}
for i, f in enumerate(frame_list):
    if i < 9:
        frame_groups[f] = "Metaphorical"
    elif i < 18:
        frame_groups[f] = "Functional"
    else:
        frame_groups[f] = "Descriptive"


# ═══════════════════════════════════════════════════════════════════════════
# STAGE 4: MEGA-THEME COMPOSITION
# ═══════════════════════════════════════════════════════════════════════════
print("\nStage 4: Mega-theme composition...")
mt_counts = df["mega_theme"].value_counts()
for theme, count in mt_counts.items():
    print(f"  {theme}: {count:,} ({count/len(df)*100:.1f}%)")


# ═══════════════════════════════════════════════════════════════════════════
# FIGURE
# ═══════════════════════════════════════════════════════════════════════════
print("\nGenerating figure...")

fig, axes = plt.subplots(2, 2, figsize=(16, 14))
fig.subplots_adjust(hspace=0.35, wspace=0.35)
fig.suptitle("Supplementary Figure S4: Frame and Theme Derivation Pipeline",
             fontsize=13, fontweight="bold", y=0.98)

# ── Panel A: Verb triggers ──
ax = axes[0, 0]
triggers = pd.Series(trigger_counts).sort_values(ascending=True)
bars = ax.barh(range(len(triggers)), triggers.values, color=WONG["blue"], alpha=0.8)
ax.set_yticks(range(len(triggers)))
ax.set_yticklabels([f'"{t}"' for t in triggers.index], fontsize=10)
ax.set_xlabel("Matching sentences", fontsize=10)
ax.set_title(f"Stage 1: Verb-trigger extraction\n(n={total_triggered:,} unique candidates from {len(df):,} sentences)",
             fontsize=11, fontweight="bold")
# Add count labels
for bar, val in zip(bars, triggers.values):
    ax.text(val + 20, bar.get_y() + bar.get_height()/2, f"{val:,}", va="center", fontsize=9)
despine(ax)
panel_label(ax, "A")

# ── Panel B: Proto-categories ──
ax = axes[0, 1]
proto = pd.Series(proto_counts).sort_values(ascending=True)
colors_b = [WONG["cyan"]] * len(proto)
bars = ax.barh(range(len(proto)), proto.values, color=colors_b, alpha=0.8)
ax.set_yticks(range(len(proto)))
ax.set_yticklabels(proto.index, fontsize=9)
ax.set_xlabel("Matching sentences (in verb-triggered set)", fontsize=10)
ax.set_title(f"Stage 2: Proto-categories from manual inspection\n({len(triggered_df):,} verb-triggered sentences)",
             fontsize=11, fontweight="bold")
for bar, val in zip(bars, proto.values):
    ax.text(val + 5, bar.get_y() + bar.get_height()/2, f"{val:,}", va="center", fontsize=9)
despine(ax)
panel_label(ax, "B")

# ── Panel C: Final 21 frame patterns ──
ax = axes[1, 0]
# Sort by count
fc_sorted = frame_counts.sort_values(ascending=True)
colors_c = [GROUP_COLORS.get(frame_groups.get(f, ""), WONG["grey"]) for f in fc_sorted.index]
bars = ax.barh(range(len(fc_sorted)), fc_sorted.values, color=colors_c, alpha=0.85)
ax.set_yticks(range(len(fc_sorted)))
ax.set_yticklabels(fc_sorted.index, fontsize=8)
ax.set_xlabel("Matching sentences", fontsize=10)
ax.set_title(f"Stage 3: Final 21 regex patterns\n(n={frame_counts.sum():,} framing sentences)",
             fontsize=11, fontweight="bold")
for bar, val in zip(bars, fc_sorted.values):
    ax.text(val + 5, bar.get_y() + bar.get_height()/2, f"{val:,}", va="center", fontsize=8)
ax.legend(handles=[
    mpatches.Patch(color=WONG["blue"], label="Metaphorical (9)"),
    mpatches.Patch(color=WONG["orange"], label="Functional (9)"),
    mpatches.Patch(color=WONG["green"], label="Descriptive (3)"),
], fontsize=8, loc="lower right")
despine(ax)
panel_label(ax, "C")

# ── Panel D: Mega-theme composition ──
ax = axes[1, 1]
mt_sorted = mt_counts.sort_values(ascending=True)
# Color Other differently
colors_d = []
theme_palette = {
    "Clinical / disease": WONG["red"],
    "Sensory / perception": WONG["cyan"],
    "Motor": WONG["orange"],
    "Cognitive / behavioral": WONG["purple"],
    "Sleep / oscillations": WONG["blue"],
    "Circuit / cellular": WONG["green"],
    "Development / anatomy": "#A0522D",
    "Methodology / imaging": "#6A5ACD",
    "Other": WONG["grey"],
}
for theme in mt_sorted.index:
    colors_d.append(theme_palette.get(theme, WONG["grey"]))

bars = ax.barh(range(len(mt_sorted)), mt_sorted.values, color=colors_d, alpha=0.85)
ax.set_yticks(range(len(mt_sorted)))
ax.set_yticklabels(mt_sorted.index, fontsize=9)
ax.set_xlabel("Sentences", fontsize=10)
ax.set_title(f"Mega-theme assignment (keyword-based)\n({len(df):,} sentences, {df['pmid'].nunique():,} papers)",
             fontsize=11, fontweight="bold")
for bar, val in zip(bars, mt_sorted.values):
    pct = val / len(df) * 100
    ax.text(val + 200, bar.get_y() + bar.get_height()/2, f"{val:,} ({pct:.1f}%)", va="center", fontsize=8)
despine(ax)
panel_label(ax, "D")

# Save
fig.savefig(PUB / "sup_S4_frame_derivation.png", dpi=150, bbox_inches="tight", facecolor="white")
fig.savefig(PUB / "sup_S4_frame_derivation.svg", bbox_inches="tight", facecolor="white")
print(f"  Saved sup_S4_frame_derivation.png")
plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════════
# NUCLEUS PREVALENCE TRENDS
# ═══════════════════════════════════════════════════════════════════════════
print("\nGenerating nucleus prevalence trends...")

from nucleus_patterns import NUCLEI

nuc_cols = [f"has_{nid}" for nid in NUCLEI if f"has_{nid}" in df.columns]
if not nuc_cols:
    from nucleus_patterns import tag_nuclei
    df = tag_nuclei(df, text_col="sentence")
    nuc_cols = [f"has_{nid}" for nid in NUCLEI]

# Compute prevalence per decade
df["decade"] = (df["year"] // 10 * 10).astype(int)
decades = sorted(df["decade"].unique())
decades = [d for d in decades if d >= 1980]

prevalence = {}
for nid in NUCLEI:
    col = f"has_{nid}"
    if col not in df.columns:
        continue
    prev = []
    for dec in decades:
        dec_df = df[df["decade"] == dec]
        if len(dec_df) > 0:
            prev.append(dec_df[col].mean() * 100)
        else:
            prev.append(0)
    prevalence[nid] = prev

# Mann-Kendall trends
from scipy.stats import kendalltau
trends = {}
for nid, prev in prevalence.items():
    if len(prev) >= 3:
        tau, p = kendalltau(range(len(prev)), prev)
        trends[nid] = {"tau": tau, "p": p, "prev": prev}

# Sort by absolute trend
sorted_nucs = sorted(trends.keys(), key=lambda n: abs(trends[n]["tau"]), reverse=True)

fig2, ax = plt.subplots(1, 1, figsize=(14, 8))
fig2.suptitle("Nucleus Prevalence Trends Over Time", fontsize=13, fontweight="bold")

# Plot top emerging and declining
colors_cycle = list(NUCLEI.values())
for i, nid in enumerate(sorted_nucs):
    t = trends[nid]
    label = NUCLEI[nid]["label"]
    tau = t["tau"]
    sig = "*" if t["p"] < 0.05 else ""
    style = "-" if t["p"] < 0.05 else "--"
    lw = 2.0 if t["p"] < 0.05 else 1.0
    alpha = 0.9 if t["p"] < 0.05 else 0.4
    color = NUCLEI[nid]["color"]
    ax.plot(decades, t["prev"], style, color=color, linewidth=lw, alpha=alpha,
            label=f"{label} ({chr(0x03C4)}={tau:+.2f}{sig})", marker="o", markersize=4)

ax.set_xlabel("Decade", fontsize=11)
ax.set_ylabel("% of sentences mentioning nucleus", fontsize=11)
ax.set_title(f"Nucleus prevalence across decades (n={len(df):,} sentences)\n"
             f"* = Mann-Kendall p < 0.05", fontsize=11)
ax.legend(fontsize=7, ncol=2, loc="upper left", bbox_to_anchor=(1.01, 1))
despine(ax)

fig2.savefig(PUB / "sup_S5_nucleus_trends.png", dpi=150, bbox_inches="tight", facecolor="white")
fig2.savefig(PUB / "sup_S5_nucleus_trends.svg", bbox_inches="tight", facecolor="white")
print(f"  Saved sup_S5_nucleus_trends.png")
plt.close(fig2)

# Print trends
print("\n  Nucleus trends (Mann-Kendall):")
for nid in sorted_nucs:
    t = trends[nid]
    sig = "***" if t["p"] < 0.001 else "**" if t["p"] < 0.01 else "*" if t["p"] < 0.05 else ""
    print(f"    {NUCLEI[nid]['label']:25s}: tau={t['tau']:+.3f} {sig}")

print("\nDone.")
