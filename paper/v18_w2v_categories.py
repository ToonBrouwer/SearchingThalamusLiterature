"""
v18_w2v_categories.py — semantic closeness of 'thalamus' to CATEGORIZED
anchor words, per decade (aligned word2vec).

Extends the v17 analysis per 2026-09 feedback: anchors are split into
interpretable categories (brain regions vs separated function-word
categories) instead of one mixed list.

Uses the cached decade models in Data/models/w2v_<decade>.model, aligned
to the 2010s space by Orthogonal Procrustes (same procedure as
Analysis/diachronic_embeddings.py).

Output: Data/processed/v18/w2v_category_similarities.csv
        (decade, category, word, similarity)
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "Data" / "models"
OUT = ROOT / "Data" / "processed" / "v18"
OUT.mkdir(exist_ok=True)

from gensim.models import Word2Vec
from scipy.linalg import orthogonal_procrustes

DECADES = [1980, 1990, 2000, 2010, 2020]
ANCHOR_DECADE = 2010

CATEGORIES = {
    "brain regions": ["cortex", "hippocampus", "cerebellum", "striatum",
                       "amygdala", "hypothalamus"],
    "relay/transmission": ["relay", "transmission", "gate", "gating"],
    "hub/network": ["hub", "network", "connectivity", "integration"],
    "oscillation/state": ["oscillation", "spindle", "arousal", "rhythm"],
    "clinical": ["patients", "stimulation", "treatment", "lesion"],
}

print("Loading cached decade models ...")
models = {d: Word2Vec.load(str(MODELS / f"w2v_{d}.model")) for d in DECADES}
anchor_wv = models[ANCHOR_DECADE].wv

rows = []
for dec in DECADES:
    wv = models[dec].wv
    if dec == ANCHOR_DECADE:
        aligned = {w: wv[w] for w in wv.key_to_index}
    else:
        shared = sorted(set(wv.key_to_index) & set(anchor_wv.key_to_index))
        A = np.array([wv[w] for w in shared])
        B = np.array([anchor_wv[w] for w in shared])
        R, _ = orthogonal_procrustes(A, B)
        aligned = {w: wv[w] @ R for w in wv.key_to_index}
        print(f"  {dec}s aligned on {len(shared):,} shared words")
    if "thalamus" not in aligned:
        continue
    t = aligned["thalamus"]
    t = t / np.linalg.norm(t)
    for cat, words in CATEGORIES.items():
        for w in words:
            if w not in aligned:
                print(f"  {dec}s: '{w}' not in vocabulary — skipped")
                continue
            v = aligned[w]
            sim = float(t @ (v / np.linalg.norm(v)))
            rows.append({"decade": dec, "category": cat, "word": w,
                         "similarity": sim})

df = pd.DataFrame(rows)
df.to_csv(OUT / "w2v_category_similarities.csv", index=False)
print(f"\nSaved {len(df)} rows to v18/w2v_category_similarities.csv")
print(df.groupby(["category", "decade"]).similarity.mean().round(3)
      .unstack().to_string())
