"""
llm_frame_classification.py — Paper-level frame classification using BART-NLI.

Uses facebook/bart-large-mnli (zero-shot NLI) to classify each introduction
into one of 8 frame categories. Runs locally on GPU.

Codebook (8 categories derived from inductive coding of 200 intros):
  1. SENSORY RELAY     — thalamus as relay/throughput for sensory signals
  2. MOTOR INTEGRATION — thalamus integrating cerebellar/BG output for motor cortex
  3. INHIBITORY GATE   — TRN/reticular gating and filtering
  4. NETWORK HUB       — thalamus as connectivity hub, network node
  5. OSCILLATORY       — sleep, arousal, rhythms, pacemaker function
  6. COGNITIVE         — attention, memory, emotion, decision-making
  7. CLINICAL          — DBS, pathology, treatment, disease
  8. DEVELOPMENTAL     — axon guidance, circuit formation, progenitor cells

Output:
  Data/processed/llm_frame_per_paper.csv
  Graphs/pub/fig_llm_frames_over_time.png
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import torch
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

# ═══════════════════════════════════════════════════════════════════════════
# CODEBOOK
# ═══════════════════════════════════════════════════════════════════════════
FRAME_HYPOTHESES = {
    "sensory_relay": "The thalamus relays or transmits sensory information to the cortex",
    "motor_integration": "The thalamus integrates motor signals from the cerebellum and basal ganglia",
    "inhibitory_gate": "The thalamus gates or filters information flow through inhibition",
    "network_hub": "The thalamus is a hub connecting distributed brain networks and cortical regions",
    "oscillatory": "The thalamus generates oscillations, sleep rhythms, or regulates arousal states",
    "cognitive": "The thalamus is involved in cognition, attention, memory, or emotional processing",
    "clinical": "The thalamus is a target for clinical intervention or is affected by neurological disease",
    "developmental": "The thalamus is studied in the context of brain development or circuit formation",
}

# ═══════════════════════════════════════════════════════════════════════════
# LOAD INTRODUCTIONS
# ═══════════════════════════════════════════════════════════════════════════
print("Loading introductions...")
intros = pd.read_csv(PROC / "introductions_all.csv", low_memory=False)
intros["decade"] = (intros["year"] // 10) * 10
print(f"  Introductions: {len(intros):,}")

# Truncate intros to ~500 tokens (~2000 chars) for NLI model
intros["intro_short"] = intros["intro_text"].str[:2000]

# ═══════════════════════════════════════════════════════════════════════════
# LOAD NLI MODEL
# ═══════════════════════════════════════════════════════════════════════════
print("\nLoading BART-large-MNLI model...")
from transformers import pipeline

device = 0 if torch.cuda.is_available() else -1
print(f"  Device: {'GPU' if device == 0 else 'CPU'}")

classifier = pipeline("zero-shot-classification",
                       model="facebook/bart-large-mnli",
                       device=device)

# ═══════════════════════════════════════════════════════════════════════════
# CLASSIFY ALL INTRODUCTIONS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\nClassifying {len(intros):,} introductions...")
t0 = time.time()

labels = list(FRAME_HYPOTHESES.keys())
hypotheses = list(FRAME_HYPOTHESES.values())

results = []
batch_size = 8

for i in range(0, len(intros), batch_size):
    batch = intros.iloc[i:i+batch_size]
    texts = batch["intro_short"].tolist()

    for j, text in enumerate(texts):
        try:
            out = classifier(text, hypotheses, multi_label=False)
            # Map back to frame names
            top_label_idx = hypotheses.index(out["labels"][0])
            top_frame = labels[top_label_idx]
            top_score = out["scores"][0]

            # Second best
            second_label_idx = hypotheses.index(out["labels"][1])
            second_frame = labels[second_label_idx]
            second_score = out["scores"][1]

            row_idx = i + j
            results.append({
                "pmid": int(batch.iloc[j]["pmid"]),
                "year": int(batch.iloc[j]["year"]),
                "decade": int(batch.iloc[j]["decade"]),
                "frame_1": top_frame,
                "score_1": round(top_score, 4),
                "frame_2": second_frame,
                "score_2": round(second_score, 4),
                "confidence": "high" if top_score > 0.5 else ("medium" if top_score > 0.3 else "low"),
            })
        except Exception as e:
            results.append({
                "pmid": int(batch.iloc[j]["pmid"]),
                "year": int(batch.iloc[j]["year"]),
                "decade": int(batch.iloc[j]["decade"]),
                "frame_1": "error", "score_1": 0,
                "frame_2": "", "score_2": 0,
                "confidence": "error",
            })

    if (i + batch_size) % 200 < batch_size:
        elapsed = time.time() - t0
        rate = (i + batch_size) / elapsed * 60
        print(f"  {i+batch_size:,}/{len(intros):,} ({rate:.0f}/min)")

# Save
results_df = pd.DataFrame(results)
results_df.to_csv(PROC / "llm_frame_per_paper.csv", index=False)
elapsed = time.time() - t0
print(f"\n  Classified {len(results_df):,} papers in {elapsed/60:.1f} min")

# ═══════════════════════════════════════════════════════════════════════════
# RESULTS SUMMARY
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("CLASSIFICATION RESULTS")
print("=" * 70)

print(f"\n  Frame distribution:")
for frame, count in results_df["frame_1"].value_counts().items():
    pct = count / len(results_df) * 100
    print(f"    {frame:20s}: {count:>5,} ({pct:.1f}%)")

print(f"\n  Confidence distribution:")
for conf, count in results_df["confidence"].value_counts().items():
    pct = count / len(results_df) * 100
    print(f"    {conf:10s}: {count:>5,} ({pct:.1f}%)")

print(f"\n  Frame distribution per decade:")
DECADES = [1980, 1990, 2000, 2010, 2020]
for dec in DECADES:
    dec_df = results_df[results_df["decade"] == dec]
    if len(dec_df) == 0:
        continue
    top_frames = dec_df["frame_1"].value_counts().head(3)
    frame_str = ", ".join(f"{f}({c})" for f, c in top_frames.items())
    print(f"    {dec}s (n={len(dec_df)}): {frame_str}")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE: Frame prevalence over time
# ═══════════════════════════════════════════════════════════════════════════
print("\nGenerating figure...")

fig, axes = plt.subplots(1, 2, figsize=(16, 7))

# Panel A: Stacked bar of frame proportions per decade
ax = axes[0]
frame_dec = results_df[results_df["decade"].isin(DECADES)].groupby(
    ["decade", "frame_1"]).size().unstack(fill_value=0)
frame_pct = (frame_dec.T / frame_dec.sum(axis=1) * 100).T

frame_colors = {
    "sensory_relay": WONG["blue"], "motor_integration": WONG["orange"],
    "inhibitory_gate": WONG["green"], "network_hub": WONG["red"],
    "oscillatory": WONG["purple"], "cognitive": WONG["yellow"],
    "clinical": WONG["grey"], "developmental": "#8c564b",
}

bottom = np.zeros(len(DECADES))
for frame in labels:
    if frame in frame_pct.columns:
        vals = [frame_pct.loc[d, frame] if d in frame_pct.index else 0 for d in DECADES]
        ax.bar(DECADES, vals, bottom=bottom, width=8,
               label=frame, color=frame_colors.get(frame, "grey"), alpha=0.85)
        bottom += vals

ax.set_xlabel("Decade")
ax.set_ylabel("% of papers")
ax.set_title("A. Paper-level frame classification\n(BART-NLI, 8 categories)")
ax.legend(fontsize=7, ncol=2, loc="upper left")
despine(ax)
panel_label(ax, "A")

# Panel B: Confidence distribution per decade
ax = axes[1]
conf_dec = results_df[results_df["decade"].isin(DECADES)].groupby(
    ["decade", "confidence"]).size().unstack(fill_value=0)
conf_pct = (conf_dec.T / conf_dec.sum(axis=1) * 100).T

conf_colors = {"high": WONG["green"], "medium": WONG["orange"], "low": WONG["red"]}
bottom = np.zeros(len(DECADES))
for conf in ["high", "medium", "low"]:
    if conf in conf_pct.columns:
        vals = [conf_pct.loc[d, conf] if d in conf_pct.index else 0 for d in DECADES]
        ax.bar(DECADES, vals, bottom=bottom, width=8,
               label=conf, color=conf_colors.get(conf, "grey"), alpha=0.85)
        bottom += vals

ax.set_xlabel("Decade")
ax.set_ylabel("% of classifications")
ax.set_title("B. Classification confidence per decade")
ax.legend(fontsize=9)
despine(ax)
panel_label(ax, "B")

plt.suptitle("LLM Paper-Level Frame Classification (zero-shot BART-NLI)",
             fontsize=14, fontweight="bold", y=1.02)
plt.tight_layout()
save_fig(fig, PUB / "fig_llm_frames_over_time.png")
print("  Saved fig_llm_frames_over_time.png")

print("\nDone.")
