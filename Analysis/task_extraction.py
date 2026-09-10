"""
Task extraction — Detect experimental tasks/paradigms in thalamus literature.

Tags both sentence-level and abstract-level data with task paradigms,
then cross-tabulates tasks with nuclei, techniques, and frame types.

Outputs:
    Data/processed/task_tagged.csv
    Graphs/pub/fig_task_extraction.{png,svg}
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

from plotting import apply_style, save_fig, panel_label, despine, WONG, heatmap
from nucleus_patterns import NUCLEI, tag_nuclei
from technique_patterns import TECHNIQUES, tag_techniques
from stats import mann_kendall

apply_style()

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT  = Path(__file__).resolve().parents[2]
PROC  = ROOT / "Data" / "processed"
RAW   = ROOT / "Data" / "raw" / "pubmed"
PUB   = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

# ── Task definitions ─────────────────────────────────────────────────────────
TASKS = {
    "working_memory": {
        "label": "Working memory",
        "pattern": r"working\s+memory|n-back|delayed\s+(match|response|non-match)",
    },
    "spatial_memory": {
        "label": "Spatial memory",
        "pattern": (
            r"spatial\s+(memory|navigation|learning)|Morris\s+water\s+maze"
            r"|T-maze|radial\s+arm\s+maze|place\s+(cell|field)"
        ),
    },
    "fear_conditioning": {
        "label": "Fear conditioning",
        "pattern": (
            r"fear\s+condition|contextual\s+fear|cued\s+fear"
            r"|fear\s+extinction|freezing\s+behav"
        ),
    },
    "sensory_discrimination": {
        "label": "Sensory discrimination",
        "pattern": (
            r"whisker\s+stimulat|visual\s+discriminat|auditory\s+oddball"
            r"|sensory\s+discriminat|go/no-go|tone\s+discriminat"
        ),
    },
    "attention": {
        "label": "Attention",
        "pattern": (
            r"attentional\s+set|sustained\s+attention|selective\s+attention"
            r"|5-CSRTT|attention(al)?\s+(task|paradigm)|Stroop|flanker"
        ),
    },
    "motor_task": {
        "label": "Motor tasks",
        "pattern": (
            r"rotarod|reaching\s+task|skilled\s+reaching|treadmill"
            r"|locomot|gait\s+analysis|forelimb\s+movement"
        ),
    },
    "pain_test": {
        "label": "Pain assessment",
        "pattern": (
            r"hot\s+plate|von\s+Frey|formalin\s+test|CFA\s+inject"
            r"|capsaicin|tail\s+flick|mechanical\s+(allodynia|hyperalgesia)"
        ),
    },
    "sleep_recording": {
        "label": "Sleep recording",
        "pattern": (
            r"polysomnograph|sleep\s+(stage|architecture|record|deprivat)"
            r"|EEG\s+record.*sleep|NREM|REM\s+sleep"
        ),
    },
    "reward_task": {
        "label": "Reward/motivation",
        "pattern": (
            r"operant\s+condition|self-stimulat|place\s+preference"
            r"|sucrose\s+preference|reward\s+(task|learning|processing)"
        ),
    },
    "decision_making": {
        "label": "Decision-making",
        "pattern": (
            r"decision.making|choice\s+task|two-alternative"
            r"|probabilistic\s+learning|reversal\s+learning"
        ),
    },
}


# ── Tagging function ─────────────────────────────────────────────────────────
def tag_tasks(df, text_col="sentence"):
    """Add boolean ``task_<ID>`` columns for every task in TASKS."""
    text = df[text_col].fillna("")
    for tid, info in TASKS.items():
        df[f"task_{tid}"] = text.str.contains(
            info["pattern"], case=False, regex=True, na=False
        )
    return df


# ── Load data ────────────────────────────────────────────────────────────────
print("Loading data ...")
sent = pd.read_csv(PROC / "all_sentences_clean.csv")
abstracts = pd.read_csv(RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv")

# Also load framing data to get frame_type for sentences
framing = pd.read_csv(PROC / "framing_clean.csv")

print(f"  Sentences: {len(sent):,}")
print(f"  Abstracts: {len(abstracts):,}")
print(f"  Framing sentences: {len(framing):,}")

# ── Tag sentences ────────────────────────────────────────────────────────────
print("Tagging sentences with tasks, nuclei, techniques ...")
sent = tag_tasks(sent, text_col="sentence")
sent = tag_nuclei(sent, text_col="sentence")
sent = tag_techniques(sent, text_col="sentence")

# ── Tag abstracts ────────────────────────────────────────────────────────────
print("Tagging abstracts ...")
abstracts = tag_tasks(abstracts, text_col="abstract")
abstracts = tag_nuclei(abstracts, text_col="abstract")
abstracts = tag_techniques(abstracts, text_col="abstract")

# ── Tag framing sentences ────────────────────────────────────────────────────
print("Tagging framing sentences ...")
framing = tag_tasks(framing, text_col="sentence")
framing = tag_nuclei(framing, text_col="sentence")
framing = tag_techniques(framing, text_col="sentence")

# ── Save tagged data ─────────────────────────────────────────────────────────
task_cols = [c for c in sent.columns if c.startswith("task_")]
save_cols = ["pmid", "year", "sentence", "topic_clean_v2"] + task_cols
sent[save_cols].to_csv(PROC / "task_tagged.csv", index=False)
print(f"Saved {PROC / 'task_tagged.csv'}")

# ── Helper: task label list ──────────────────────────────────────────────────
task_ids = list(TASKS.keys())
task_labels = [TASKS[t]["label"] for t in task_ids]
task_cols = [f"task_{t}" for t in task_ids]

nuc_ids = list(NUCLEI.keys())
nuc_labels = [NUCLEI[n]["label"] for n in nuc_ids]
nuc_cols = [f"has_{n}" for n in nuc_ids]

tech_ids = list(TECHNIQUES.keys())
tech_labels = [TECHNIQUES[t]["label"] for t in tech_ids]
tech_cols = [f"has_{t}" for t in tech_ids]

frame_types = sorted(framing["frame_type"].dropna().unique())

# ── Panel A: Task prevalence over time (abstracts, 1990-2026) ────────────────
print("Building Panel A: task prevalence over time ...")
abs_sub = abstracts[(abstracts["year"] >= 1990) & (abstracts["year"] <= 2026)].copy()
years_range = np.arange(1990, 2027)

# Count per year per task
task_by_year = {}
for tid in task_ids:
    col = f"task_{tid}"
    counts = abs_sub.groupby("year")[col].sum().reindex(years_range, fill_value=0)
    task_by_year[tid] = counts.values

# Total abstracts per year for normalisation
total_per_year = abs_sub.groupby("year").size().reindex(years_range, fill_value=0).values

# ── Panel B: Task x nucleus heatmap (sentences) ─────────────────────────────
print("Building Panel B: task x nucleus heatmap ...")
# Use abstracts for cross-tab
task_nuc_matrix = np.zeros((len(task_ids), len(nuc_ids)))
for i, tid in enumerate(task_ids):
    tcol = f"task_{tid}"
    for j, nid in enumerate(nuc_ids):
        ncol = f"has_{nid}"
        task_nuc_matrix[i, j] = abstracts[abstracts[tcol] & abstracts[ncol]].shape[0]

# ── Panel C: Task x technique heatmap ────────────────────────────────────────
print("Building Panel C: task x technique heatmap ...")
task_tech_matrix = np.zeros((len(task_ids), len(tech_ids)))
for i, tid in enumerate(task_ids):
    tcol = f"task_{tid}"
    for j, techid in enumerate(tech_ids):
        tecol = f"has_{techid}"
        task_tech_matrix[i, j] = abstracts[abstracts[tcol] & abstracts[tecol]].shape[0]

# ── Panel D: Task x frame type heatmap ───────────────────────────────────────
print("Building Panel D: task x frame type heatmap ...")
task_frame_matrix = np.zeros((len(task_ids), len(frame_types)))
for i, tid in enumerate(task_ids):
    tcol = f"task_{tid}"
    for j, ft in enumerate(frame_types):
        subset = framing[(framing[tcol]) & (framing["frame_type"] == ft)]
        task_frame_matrix[i, j] = len(subset)

# ── FIGURE ───────────────────────────────────────────────────────────────────
print("Plotting ...")
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.subplots_adjust(hspace=0.38, wspace=0.35)

# --- Panel A: line chart of task prevalence ---
ax = axes[0, 0]
colors_list = list(WONG.values())
for idx, tid in enumerate(task_ids):
    vals = task_by_year[tid]
    # Normalise by total abstracts per year
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = np.where(total_per_year > 0, vals / total_per_year * 100, 0)
    # 3-year rolling smooth
    pct_smooth = pd.Series(pct).rolling(3, center=True, min_periods=1).mean().values
    ax.plot(years_range, pct_smooth, label=TASKS[tid]["label"],
            color=colors_list[idx % len(colors_list)], linewidth=1.5)

ax.set_xlabel("Year")
ax.set_ylabel("% of abstracts mentioning task")
ax.set_title("Task prevalence over time (abstracts)")
ax.legend(fontsize=6, ncol=2, loc="upper left", frameon=False)
despine(ax)
panel_label(ax, "A")

# --- Panel B: task x nucleus heatmap ---
ax = axes[0, 1]
# Shorten nucleus labels
nuc_short = [NUCLEI[n]["label"].split("(")[0].strip() if "(" in NUCLEI[n]["label"]
             else NUCLEI[n]["label"] for n in nuc_ids]
# Filter to nuclei with at least some hits
nuc_sums = task_nuc_matrix.sum(axis=0)
keep_nuc = nuc_sums > 0
if keep_nuc.sum() > 0:
    mat_b = task_nuc_matrix[:, keep_nuc]
    nuc_short_filt = [nuc_short[i] for i in range(len(nuc_short)) if keep_nuc[i]]
else:
    mat_b = task_nuc_matrix
    nuc_short_filt = nuc_short

heatmap(ax, mat_b, task_labels, nuc_short_filt, cmap="YlOrRd", fmt=".0f", fontsize=6)
ax.set_title("Task x Nucleus (abstract co-occurrence)")
panel_label(ax, "B")

# --- Panel C: task x technique heatmap ---
ax = axes[1, 0]
tech_short = [TECHNIQUES[t]["label"] for t in tech_ids]
# Filter to techniques with at least some hits
tech_sums = task_tech_matrix.sum(axis=0)
keep_tech = tech_sums > 0
if keep_tech.sum() > 0:
    mat_c = task_tech_matrix[:, keep_tech]
    tech_short_filt = [tech_short[i] for i in range(len(tech_short)) if keep_tech[i]]
else:
    mat_c = task_tech_matrix
    tech_short_filt = tech_short

heatmap(ax, mat_c, task_labels, tech_short_filt, cmap="YlOrRd", fmt=".0f", fontsize=6)
ax.set_title("Task x Technique (abstract co-occurrence)")
panel_label(ax, "C")

# --- Panel D: task x frame type heatmap ---
ax = axes[1, 1]
# Filter to frame types with at least some hits
frame_sums = task_frame_matrix.sum(axis=0)
keep_frame = frame_sums > 0
if keep_frame.sum() > 0:
    mat_d = task_frame_matrix[:, keep_frame]
    frame_filt = [frame_types[i] for i in range(len(frame_types)) if keep_frame[i]]
else:
    mat_d = task_frame_matrix
    frame_filt = frame_types

heatmap(ax, mat_d, task_labels, frame_filt, cmap="YlOrRd", fmt=".0f", fontsize=6)
ax.set_title("Task x Frame type (sentence co-occurrence)")
panel_label(ax, "D")

# ── Save ─────────────────────────────────────────────────────────────────────
save_fig(fig, PUB / "fig_task_extraction.png", dpi=300)
save_fig(fig, PUB / "fig_task_extraction.svg")
plt.close(fig)

# ── Print summary ────────────────────────────────────────────────────────────
print("\n=== Task extraction summary ===")
for tid in task_ids:
    n_sent = sent[f"task_{tid}"].sum()
    n_abs = abstracts[f"task_{tid}"].sum()
    print(f"  {TASKS[tid]['label']:25s}  sentences={n_sent:5d}  abstracts={n_abs:5d}")

print(f"\nFigure saved to {PUB / 'fig_task_extraction.png'}")
print("Done.")
