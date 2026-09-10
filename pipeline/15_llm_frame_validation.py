"""
15_llm_frame_validation.py — LLM-based validation of embedding-detected frames.

Uses a zero-shot classification model (BART-large-MNLI) on GPU to validate
whether embedding-detected candidate sentences are genuine frames.

Two-stage approach:
  Stage 1: Embedding similarity found 1,902 high-confidence candidates
  Stage 2: This script validates each with an NLI model

The NLI model determines if a sentence entails a frame hypothesis like
"This sentence describes the thalamus as a relay station."
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"

# ═══════════════════════════════════════════════════════════════════════════
# 1. LOAD EMBEDDING-DETECTED CANDIDATES
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("1. LOADING CANDIDATES")
print("=" * 70)

df = pd.read_csv(PROC / "all_sentences_emb_framing.csv")
df["frame_type"] = df["frame_type"].fillna("")
df["frame_type_emb"] = df["frame_type_emb"].fillna("")
df["frame_similarity"] = pd.to_numeric(df["frame_similarity"], errors="coerce").fillna(0)

# Get high-confidence embedding detections (not regex-detected)
candidates = df[
    (df["frame_type"].str.strip() == "") &  # NOT regex-detected
    (df["frame_type_emb"].str.strip() != "") &  # IS embedding-detected
    (df["frame_similarity"] >= 0.80)  # high confidence
].copy()

print(f"  Total sentences: {len(df):,}")
print(f"  High-confidence embedding candidates: {len(candidates):,}")
print(f"  Frame distribution of candidates:")
for f, c in candidates["frame_type_emb"].value_counts().head(10).items():
    print(f"    {f:20s} {c:4d}")

# ═══════════════════════════════════════════════════════════════════════════
# 2. DEFINE FRAME HYPOTHESES FOR NLI
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. FRAME HYPOTHESES")
print("=" * 70)

# For each frame type, define a natural language hypothesis
# The NLI model will check if the sentence ENTAILS this hypothesis
FRAME_HYPOTHESES = {
    "relay": "This sentence describes the thalamus as a relay station that transmits or forwards sensory information.",
    "hub": "This sentence describes the thalamus as a central hub or nexus connecting multiple brain regions.",
    "gateway": "This sentence describes the thalamus as a gateway or gatekeeper that controls information flow.",
    "gate": "This sentence describes a gating mechanism where the thalamus selectively allows or blocks neural signals.",
    "modulator": "This sentence describes the thalamus as modulating or adjusting cortical or neural activity.",
    "integrator": "This sentence describes the thalamus as integrating information from multiple sources.",
    "filter": "This sentence describes the thalamus as filtering sensory information.",
    "regulator": "This sentence describes the thalamus as regulating brain states like arousal or sleep.",
    "coordinator": "This sentence describes the thalamus as coordinating activity across brain regions.",
    "pacemaker": "This sentence describes the thalamus as a pacemaker generating neural oscillations or rhythms.",
    "processor": "This sentence describes the thalamus as processing sensory or cognitive information.",
    "clinical_target": "This sentence describes the thalamus as a target for clinical intervention like deep brain stimulation.",
    "network_node": "This sentence describes the thalamus as a node within a large-scale brain network.",
    "anti_relay": "This sentence argues that the thalamus is more than just a relay station.",
    "interface": "This sentence describes the thalamus as an interface between different brain systems.",
    "blackboard": "This sentence describes the thalamus as a shared workspace or blackboard for cortical computation.",
    "bridge": "This sentence describes the thalamus as a bridge connecting different brain structures.",
    "bottleneck": "This sentence describes the thalamus as a bottleneck or capacity-limited processing stage.",
    "amplifier": "This sentence describes the thalamus as amplifying neural signals.",
    "distributor": "This sentence describes the thalamus as distributing information to multiple targets.",
    "switchboard": "This sentence describes the thalamus as a switchboard routing neural signals.",
}

# Also define a negative hypothesis
NEGATIVE_HYPOTHESIS = "This sentence is a generic description of thalamus anatomy, methods, or results without any metaphorical framing."

print(f"  Defined {len(FRAME_HYPOTHESES)} frame hypotheses")

# ═══════════════════════════════════════════════════════════════════════════
# 3. LOAD NLI MODEL (GPU)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. LOADING NLI MODEL")
print("=" * 70)

import torch
from transformers import pipeline

device = 0 if torch.cuda.is_available() else -1
print(f"  Device: {'GPU' if device == 0 else 'CPU'}")

# Use DeBERTa-v3-base-mnli — smaller than BART-large, good performance
# Falls back to BART-large-mnli if not available
try:
    classifier = pipeline(
        "zero-shot-classification",
        model="MoritzLaurer/DeBERTa-v3-base-mnli-fever-anli",
        device=device,
        batch_size=16,
    )
    print(f"  Model: DeBERTa-v3-base-mnli-fever-anli")
except Exception:
    classifier = pipeline(
        "zero-shot-classification",
        model="facebook/bart-large-mnli",
        device=device,
        batch_size=8,
    )
    print(f"  Model: facebook/bart-large-mnli")

# ═══════════════════════════════════════════════════════════════════════════
# 4. VALIDATE CANDIDATES
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. VALIDATING CANDIDATES")
print("=" * 70)

t0 = time.time()
validated = []
batch_size = 32

sentences = candidates["sentence"].tolist()
emb_frames = candidates["frame_type_emb"].tolist()
indices = candidates.index.tolist()

for batch_start in range(0, len(sentences), batch_size):
    batch_end = min(batch_start + batch_size, len(sentences))
    batch_sents = sentences[batch_start:batch_end]
    batch_frames = emb_frames[batch_start:batch_end]
    batch_idx = indices[batch_start:batch_end]

    for i, (sent, emb_frame, idx) in enumerate(zip(batch_sents, batch_frames, batch_idx)):
        # Truncate long sentences
        sent_trunc = sent[:512] if len(sent) > 512 else sent

        # Check: does this sentence entail the frame hypothesis?
        frame_hyp = FRAME_HYPOTHESES.get(emb_frame, "")
        if not frame_hyp:
            continue

        candidate_labels = [
            f"describes thalamus as {emb_frame.replace('_', ' ')}",
            "generic thalamus description without metaphor",
        ]

        try:
            result = classifier(sent_trunc, candidate_labels, multi_label=False)
            is_frame = result["labels"][0] == candidate_labels[0]
            confidence = result["scores"][0]

            validated.append({
                "index": idx,
                "sentence": sent[:200],
                "emb_frame": emb_frame,
                "llm_validated": is_frame,
                "llm_confidence": confidence,
                "llm_label": result["labels"][0],
            })
        except Exception as e:
            validated.append({
                "index": idx,
                "sentence": sent[:200],
                "emb_frame": emb_frame,
                "llm_validated": False,
                "llm_confidence": 0,
                "llm_label": f"error: {e}",
            })

    if (batch_start + batch_size) % 100 < batch_size:
        elapsed = time.time() - t0
        rate = (batch_start + batch_size) / elapsed if elapsed > 0 else 0
        print(f"  Processed {batch_start + batch_size}/{len(sentences)} "
              f"({elapsed:.0f}s, {rate:.1f} sent/s)")

elapsed = time.time() - t0
print(f"\n  Total: {len(validated)} validated in {elapsed:.1f}s")

# ═══════════════════════════════════════════════════════════════════════════
# 5. RESULTS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. VALIDATION RESULTS")
print("=" * 70)

val_df = pd.DataFrame(validated)
if len(val_df) == 0:
    print("  No candidates were validated. Check filtering logic.")
    sys.exit(1)
n_valid = val_df["llm_validated"].sum()
n_total = len(val_df)
print(f"  LLM-validated as genuine frames: {n_valid:,} / {n_total:,} ({n_valid/n_total*100:.1f}%)")

# Per-frame validation rates
print(f"\n  Validation rate per frame type:")
for frame, group in val_df.groupby("emb_frame"):
    n_v = group["llm_validated"].sum()
    n_t = len(group)
    print(f"    {frame:20s}  {n_v:4d}/{n_t:4d} ({n_v/n_t*100:.0f}% validated)")

# Save
val_df.to_csv(PROC / "llm_frame_validation.csv", index=False)
print(f"\n  Saved llm_frame_validation.csv ({len(val_df):,} rows)")

# Show examples of validated sentences
print(f"\n  Example validated sentences:")
for frame in ["relay", "hub", "gate", "clinical_target", "modulator"]:
    subset = val_df[(val_df["emb_frame"] == frame) & val_df["llm_validated"]]
    if len(subset) > 0:
        row = subset.iloc[0]
        print(f"    [{frame}] (conf={row['llm_confidence']:.2f}) {row['sentence'][:100]}...")

print(f"\n{'='*70}")
print(f"DONE. {n_valid:,} embedding candidates validated as genuine frames by LLM.")
print(f"{'='*70}")
