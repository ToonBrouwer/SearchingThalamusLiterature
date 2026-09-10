"""
task_extraction_v2.py -- Detect experimental tasks/paradigms in thalamus literature.

Tags both sentence-level and abstract-level data with task paradigms,
then cross-tabulates tasks with nuclei, techniques, and frame types.

Changes from v1:
  - Loads framing_combined_v2.csv for Panel D
  - annotate_n() helper on every panel
  - Panel D: abstract-level frame detection on ALL 17,280 abstracts
    using detect_frame from taxonomy.py
  - All heatmaps: row/column marginal N, mask cells with N < 5
  - Only include frame types with N >= 20 in heatmaps

Outputs:
    Data/processed/task_tagged_v2.csv
    Graphs/pub/fig_task_extraction_v2.{png,svg}
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
from taxonomy import FRAMES_CLEAN, detect_frame, FRAME_NAMES
from stats import mann_kendall

apply_style()


# ── annotate_n helper ──────────────────────────────────────────────────────
def annotate_n(ax, n, loc="upper right", fontsize=8):
    ha = "right" if "right" in loc else "left"
    va = "top" if "upper" in loc else "bottom"
    x = 0.97 if "right" in loc else 0.03
    y = 0.97 if "upper" in loc else 0.03
    ax.text(x, y, f"n = {n:,}", transform=ax.transAxes,
            fontsize=fontsize, ha=ha, va=va,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                      alpha=0.8, edgecolor="grey", linewidth=0.5))


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

# Load framing_combined_v2.csv for Panel D
framing = pd.read_csv(PROC / "framing_combined_v2.csv")

print(f"  Sentences: {len(sent):,}")
print(f"  Abstracts: {len(abstracts):,}")
print(f"  Framing sentences (v2): {len(framing):,}")

# ── Tag sentences ────────────────────────────────────────────────────────────
print("Tagging sentences with tasks, nuclei, techniques ...")
sent = tag_tasks(sent, text_col="sentence")
sent = tag_nuclei(sent, text_col="sentence")
sent = tag_techniques(sent, text_col="sentence")

# ── Tag abstracts ────────────────────────────────────────────────────────────
print("Tagging abstracts ...")
abstracts["abstract"] = abstracts["abstract"].fillna("")
abstracts = tag_tasks(abstracts, text_col="abstract")
abstracts = tag_nuclei(abstracts, text_col="abstract")
abstracts = tag_techniques(abstracts, text_col="abstract")

# ── Detect frames at abstract level using detect_frame from taxonomy.py ──────
print("Detecting frames at abstract level (detect_frame on all abstracts)...")
abstracts["frame_type"] = abstracts["abstract"].apply(detect_frame)
n_framed_abs = (abstracts["frame_type"] != "").sum()
print(f"  Abstracts with detected frame: {n_framed_abs:,} / {len(abstracts):,}")

# ── Tag framing sentences ────────────────────────────────────────────────────
print("Tagging framing sentences ...")
framing["sentence"] = framing["sentence"].fillna("")
framing = tag_tasks(framing, text_col="sentence")
framing = tag_nuclei(framing, text_col="sentence")
framing = tag_techniques(framing, text_col="sentence")

# ── Save tagged data ─────────────────────────────────────────────────────────
task_cols_save = [c for c in sent.columns if c.startswith("task_")]
topic_col = "topic_clean_v2" if "topic_clean_v2" in sent.columns else "bertopic_label_v2"
save_cols = ["pmid", "year", "sentence", topic_col] + task_cols_save
sent[save_cols].to_csv(PROC / "task_tagged_v2.csv", index=False)
print(f"Saved {PROC / 'task_tagged_v2.csv'}")

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

# Frame types from abstract-level detection (for Panel D)
# Only include frame types with N >= 20
abs_frame_counts = abstracts[abstracts["frame_type"] != ""]["frame_type"].value_counts()
frames_ge20 = abs_frame_counts[abs_frame_counts >= 20].index.tolist()
print(f"  Frame types with N >= 20 (abstract-level): {len(frames_ge20)}")

# For sentence-level framing
sent_frame_types = sorted(framing["frame_type"].dropna().unique())

# ── Panel A: Task prevalence over time (abstracts, 1990-2026) ────────────────
print("Building Panel A: task prevalence over time ...")
abs_sub = abstracts[(abstracts["year"] >= 1990) & (abstracts["year"] <= 2026)].copy()
years_range = np.arange(1990, 2027)

task_by_year = {}
for tid in task_ids:
    col = f"task_{tid}"
    counts = abs_sub.groupby("year")[col].sum().reindex(years_range, fill_value=0)
    task_by_year[tid] = counts.values

total_per_year = abs_sub.groupby("year").size().reindex(years_range, fill_value=0).values

# ── Panel B: Task x nucleus heatmap (abstracts) ─────────────────────────────
print("Building Panel B: task x nucleus heatmap ...")
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

# ── Panel D: Task x frame type heatmap (ABSTRACT-LEVEL) ─────────────────────
print("Building Panel D: task x frame type (abstract-level) ...")
# Use abstract-level frame detection on ALL abstracts
abs_with_frame = abstracts[abstracts["frame_type"].isin(frames_ge20)].copy()
print(f"  Abstracts with frame (N>=20 types): {len(abs_with_frame):,}")

# Tag tasks on this subset (already done above)
task_frame_matrix = np.zeros((len(task_ids), len(frames_ge20)))
for i, tid in enumerate(task_ids):
    tcol = f"task_{tid}"
    for j, ft in enumerate(frames_ge20):
        subset = abs_with_frame[(abs_with_frame[tcol]) & (abs_with_frame["frame_type"] == ft)]
        task_frame_matrix[i, j] = len(subset)


# ── Helper: add marginal N and mask N<5 ──────────────────────────────────────
def add_marginals_and_mask(matrix, row_labels, col_labels, min_n=5):
    """Mask cells with N < min_n (set to NaN). Return modified matrix,
    row labels with marginal N, col labels with marginal N."""
    mat = matrix.copy().astype(float)
    row_n = mat.sum(axis=1).astype(int)
    col_n = mat.sum(axis=0).astype(int)
    # Mask cells with N < min_n
    mat[mat < min_n] = np.nan
    # Add marginal N to labels
    new_row = [f"{r} (n={row_n[i]})" for i, r in enumerate(row_labels)]
    new_col = [f"{c} (n={col_n[j]})" for j, c in enumerate(col_labels)]
    return mat, new_row, new_col


# ── FIGURE ───────────────────────────────────────────────────────────────────
print("Plotting ...")
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.subplots_adjust(hspace=0.38, wspace=0.35)

# --- Panel A: line chart of task prevalence ---
ax = axes[0, 0]
colors_list = list(WONG.values())
for idx, tid in enumerate(task_ids):
    vals = task_by_year[tid]
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = np.where(total_per_year > 0, vals / total_per_year * 100, 0)
    pct_smooth = pd.Series(pct).rolling(3, center=True, min_periods=1).mean().values
    ax.plot(years_range, pct_smooth, label=TASKS[tid]["label"],
            color=colors_list[idx % len(colors_list)], linewidth=1.5)

ax.set_xlabel("Year")
ax.set_ylabel("% of abstracts mentioning task")
ax.set_title("Task prevalence over time (abstracts)")
ax.legend(fontsize=6, ncol=2, loc="upper left", frameon=False)
despine(ax)
panel_label(ax, "A")
annotate_n(ax, len(abs_sub), loc="lower right")

# --- Panel B: task x nucleus heatmap ---
ax = axes[0, 1]
nuc_short = [NUCLEI[n]["label"].split("(")[0].strip() if "(" in NUCLEI[n]["label"]
             else NUCLEI[n]["label"] for n in nuc_ids]
nuc_sums = task_nuc_matrix.sum(axis=0)
keep_nuc = nuc_sums > 0
if keep_nuc.sum() > 0:
    mat_b = task_nuc_matrix[:, keep_nuc]
    nuc_short_filt = [nuc_short[i] for i in range(len(nuc_short)) if keep_nuc[i]]
else:
    mat_b = task_nuc_matrix
    nuc_short_filt = nuc_short

mat_b_masked, rl_b, cl_b = add_marginals_and_mask(mat_b, task_labels, nuc_short_filt)
heatmap(ax, mat_b_masked, rl_b, cl_b, cmap="YlOrRd", fmt=".0f", fontsize=6)
ax.set_title("Task x Nucleus (abstract co-occurrence)")
panel_label(ax, "B")
annotate_n(ax, int(task_nuc_matrix.sum()), loc="upper left")

# --- Panel C: task x technique heatmap ---
ax = axes[1, 0]
tech_short = [TECHNIQUES[t]["label"] for t in tech_ids]
tech_sums = task_tech_matrix.sum(axis=0)
keep_tech = tech_sums > 0
if keep_tech.sum() > 0:
    mat_c = task_tech_matrix[:, keep_tech]
    tech_short_filt = [tech_short[i] for i in range(len(tech_short)) if keep_tech[i]]
else:
    mat_c = task_tech_matrix
    tech_short_filt = tech_short

mat_c_masked, rl_c, cl_c = add_marginals_and_mask(mat_c, task_labels, tech_short_filt)
heatmap(ax, mat_c_masked, rl_c, cl_c, cmap="YlOrRd", fmt=".0f", fontsize=6)
ax.set_title("Task x Technique (abstract co-occurrence)")
panel_label(ax, "C")
annotate_n(ax, int(task_tech_matrix.sum()), loc="upper left")

# --- Panel D: task x frame type heatmap (abstract-level) ---
ax = axes[1, 1]
mat_d_masked, rl_d, cl_d = add_marginals_and_mask(
    task_frame_matrix, task_labels, frames_ge20)
heatmap(ax, mat_d_masked, rl_d, cl_d, cmap="YlOrRd", fmt=".0f", fontsize=6)
ax.set_title("Task x Frame type (abstract-level, N>=20 frames)")
panel_label(ax, "D")
annotate_n(ax, int(np.nansum(task_frame_matrix)), loc="upper left")

# ── Save ─────────────────────────────────────────────────────────────────────
save_fig(fig, PUB / "fig_task_extraction_v2.png", dpi=300)
save_fig(fig, PUB / "fig_task_extraction_v2.svg")
plt.close(fig)

# ── Print summary ────────────────────────────────────────────────────────────
print("\n=== Task extraction summary ===")
for tid in task_ids:
    n_sent = sent[f"task_{tid}"].sum()
    n_abs = abstracts[f"task_{tid}"].sum()
    print(f"  {TASKS[tid]['label']:25s}  sentences={n_sent:5d}  abstracts={n_abs:5d}")

print(f"\n  Abstract-level frame types with N >= 20: {frames_ge20}")
print(f"\nFigure saved to {PUB / 'fig_task_extraction_v2.png'}")
print("Done.")
