"""
11_embedding_frame_detection.py — Semantic frame detection using sentence embeddings.

Strategy:
  1. Load existing 55,848 sentence embeddings (384-dim, all-MiniLM-L6-v2)
  2. For each of the 21 frame types, compute a prototype embedding
     (mean of all known regex-matched sentences for that frame)
  3. Compute cosine similarity of ALL sentences to each prototype
  4. Sentences above threshold = candidate NEW framing sentences
  5. Validate: check precision by sampling candidates
  6. Also apply to abstract sentences for additional expansion

GPU-accelerated: uses torch for matrix operations on RTX 3060 (12GB).
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
from pathlib import Path

# Try GPU torch, fall back to CPU numpy
try:
    import torch
    USE_GPU = torch.cuda.is_available()
    if USE_GPU:
        DEVICE = torch.device("cuda")
        print(f"GPU: {torch.cuda.get_device_name(0)} ({torch.cuda.get_device_properties(0).total_mem/1e9:.1f} GB)")
    else:
        DEVICE = torch.device("cpu")
        print("CUDA not available, using CPU torch")
except ImportError:
    USE_GPU = False
    print("PyTorch not installed, using numpy (CPU only)")

from taxonomy import FRAMES_CLEAN, detect_frame, FRAME_NAMES

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"

# ═══════════════════════════════════════════════════════════════════════════
# 1. LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. LOADING DATA")
print("=" * 70)

t0 = time.time()

# Sentence data
df = pd.read_csv(PROC / "all_sentences_final.csv")
df["sentence"] = df["sentence"].fillna("")
df["frame_type"] = df["frame_type"].fillna("")
print(f"  Sentences: {len(df):,}")

# Pre-computed embeddings
emb_path = PROC / "all_sentences_embeddings.npy"
if not emb_path.exists():
    print(f"  ERROR: embeddings not found at {emb_path}")
    sys.exit(1)

embeddings = np.load(str(emb_path))
print(f"  Embeddings: {embeddings.shape} ({embeddings.dtype})")
assert len(df) == embeddings.shape[0], "Mismatch between sentences and embeddings"

# Normalize embeddings (L2)
norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
norms[norms == 0] = 1.0
embeddings_norm = embeddings / norms

print(f"  Loaded in {time.time()-t0:.1f}s")

# ═══════════════════════════════════════════════════════════════════════════
# 2. COMPUTE PROTOTYPE EMBEDDINGS PER FRAME TYPE
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. COMPUTING FRAME PROTOTYPES")
print("=" * 70)

# Get indices of regex-matched sentences per frame type
prototypes = {}
for fname in FRAME_NAMES:
    mask = df["frame_type"] == fname
    indices = np.where(mask.values)[0]
    if len(indices) >= 3:  # need at least 3 examples
        proto_emb = embeddings_norm[indices].mean(axis=0)
        proto_emb /= np.linalg.norm(proto_emb)  # re-normalize
        prototypes[fname] = {
            "embedding": proto_emb,
            "n_examples": len(indices),
            "indices": indices,
        }
        print(f"  {fname:20s}: {len(indices):4d} regex examples -> prototype computed")
    else:
        print(f"  {fname:20s}: {len(indices):4d} regex examples -> SKIPPED (too few)")

print(f"\n  {len(prototypes)} prototypes computed out of {len(FRAME_NAMES)} frame types")

# ═══════════════════════════════════════════════════════════════════════════
# 3. GPU-ACCELERATED SIMILARITY COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. COMPUTING SIMILARITIES (GPU)" if USE_GPU else "3. COMPUTING SIMILARITIES (CPU)")
print("=" * 70)

t1 = time.time()

if USE_GPU or 'torch' in dir():
    # Use torch for matrix multiply (GPU or CPU)
    emb_tensor = torch.tensor(embeddings_norm, dtype=torch.float32, device=DEVICE)

    similarities = {}
    for fname, proto in prototypes.items():
        proto_tensor = torch.tensor(proto["embedding"], dtype=torch.float32, device=DEVICE)
        # Cosine similarity = dot product of L2-normalized vectors
        sim = torch.mv(emb_tensor, proto_tensor)
        similarities[fname] = sim.cpu().numpy()

    del emb_tensor  # free GPU memory
    if USE_GPU:
        torch.cuda.empty_cache()
else:
    # Numpy fallback
    similarities = {}
    for fname, proto in prototypes.items():
        sim = embeddings_norm @ proto["embedding"]
        similarities[fname] = sim

print(f"  Computed {len(similarities)} similarity vectors in {time.time()-t1:.1f}s")

# ═══════════════════════════════════════════════════════════════════════════
# 4. FIND NEW CANDIDATE FRAMING SENTENCES
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. FINDING NEW CANDIDATE FRAMING SENTENCES")
print("=" * 70)

# Threshold selection: for each frame, find the similarity at the
# bottom of the known regex matches (e.g., 10th percentile)
# Then use a slightly lower threshold to find new candidates

THRESHOLD_PERCENTILE = 25  # use 25th percentile of known matches as cutoff
MIN_SIMILARITY = 0.4       # absolute floor — below this is noise

new_candidates = {}
total_new = 0

for fname, sim in similarities.items():
    proto = prototypes[fname]
    known_indices = proto["indices"]

    # Similarity of known regex matches to their own prototype
    known_sims = sim[known_indices]
    threshold = max(
        np.percentile(known_sims, THRESHOLD_PERCENTILE),
        MIN_SIMILARITY
    )

    # Find sentences above threshold that are NOT already regex-matched
    already_matched = set(known_indices)
    candidates_mask = (sim >= threshold) & (df["frame_type"].values == "")
    candidate_indices = np.where(candidates_mask)[0]

    # Sort by similarity (highest first)
    candidate_sims = sim[candidate_indices]
    sort_order = np.argsort(-candidate_sims)
    candidate_indices = candidate_indices[sort_order]
    candidate_sims = candidate_sims[sort_order]

    new_candidates[fname] = {
        "indices": candidate_indices,
        "similarities": candidate_sims,
        "threshold": threshold,
        "n_known": len(known_indices),
        "n_new": len(candidate_indices),
    }
    total_new += len(candidate_indices)

    print(f"  {fname:20s}: threshold={threshold:.3f}, "
          f"known={len(known_indices):4d}, new candidates={len(candidate_indices):5d}")

print(f"\n  Total new candidates: {total_new:,}")

# ═══════════════════════════════════════════════════════════════════════════
# 5. QUALITY CHECK — SHOW TOP CANDIDATES PER FRAME
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. TOP NEW CANDIDATES PER FRAME TYPE (quality check)")
print("=" * 70)

for fname in ["relay", "hub", "gate", "gateway", "clinical_target", "modulator", "pacemaker"]:
    if fname not in new_candidates or new_candidates[fname]["n_new"] == 0:
        continue
    cands = new_candidates[fname]
    print(f"\n  --- {fname.upper()} (threshold={cands['threshold']:.3f}, "
          f"n_new={cands['n_new']:,}) ---")
    for i in range(min(5, len(cands["indices"]))):
        idx = cands["indices"][i]
        sim_val = cands["similarities"][i]
        sent = df.iloc[idx]["sentence"][:120]
        print(f"    [{sim_val:.3f}] {sent}...")

# ═══════════════════════════════════════════════════════════════════════════
# 6. SAVE EXPANDED FRAME ASSIGNMENTS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("6. SAVING RESULTS")
print("=" * 70)

# Create a new column for embedding-based frame detection
df["frame_type_emb"] = df["frame_type"].copy()
df["frame_similarity"] = 0.0

# For each unassigned sentence, assign the highest-similarity frame
# (only if above that frame's threshold)
unassigned_mask = df["frame_type_emb"] == ""
unassigned_indices = np.where(unassigned_mask.values)[0]

if len(unassigned_indices) > 0:
    # Build a matrix of similarities for unassigned sentences
    n_frames = len(prototypes)
    frame_names_list = list(prototypes.keys())

    best_frame = [""] * len(unassigned_indices)
    best_sim = np.zeros(len(unassigned_indices))

    for fi, fname in enumerate(frame_names_list):
        sim = similarities[fname]
        threshold = new_candidates[fname]["threshold"]

        for ui, idx in enumerate(unassigned_indices):
            s = sim[idx]
            if s >= threshold and s > best_sim[ui]:
                best_sim[ui] = s
                best_frame[ui] = fname

    # Apply
    n_assigned = sum(1 for f in best_frame if f != "")
    for ui, idx in enumerate(unassigned_indices):
        if best_frame[ui] != "":
            df.iloc[idx, df.columns.get_loc("frame_type_emb")] = best_frame[ui]
            df.iloc[idx, df.columns.get_loc("frame_similarity")] = best_sim[ui]

n_regex = (df["frame_type"] != "").sum()
n_emb = (df["frame_type_emb"] != "").sum()
print(f"  Regex-only framing sentences: {n_regex:,}")
print(f"  Regex + embedding framing sentences: {n_emb:,}")
print(f"  New embedding-detected sentences: {n_emb - n_regex:,}")

# Frame distribution comparison
print(f"\n  Frame distribution (regex vs regex+embedding):")
for fname in FRAME_NAMES:
    n_regex_f = (df["frame_type"] == fname).sum()
    n_emb_f = (df["frame_type_emb"] == fname).sum()
    if n_regex_f > 0 or n_emb_f > 0:
        print(f"    {fname:20s}  regex={n_regex_f:5d}  +emb={n_emb_f:5d}  "
              f"(+{n_emb_f - n_regex_f:4d})")

# Save
df.to_csv(PROC / "all_sentences_emb_framing.csv", index=False)
print(f"\n  Saved all_sentences_emb_framing.csv ({len(df):,} rows)")

# Also create a framing-only CSV
framing_emb = df[df["frame_type_emb"] != ""].copy()
framing_emb.to_csv(PROC / "framing_embedding_detected.csv", index=False)
print(f"  Saved framing_embedding_detected.csv ({len(framing_emb):,} rows)")

print(f"\n{'='*70}")
print(f"DONE. Embedding-based detection found {n_emb - n_regex:,} additional framing sentences.")
print(f"{'='*70}")
