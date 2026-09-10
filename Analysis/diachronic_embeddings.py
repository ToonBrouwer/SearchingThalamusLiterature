"""
diachronic_embeddings.py — Analysis 7: Diachronic word embeddings.

Follows Hamilton et al. (2016) HistWords methodology:
  7a. Train word2vec (skip-gram) per decade
  7b. Align via Orthogonal Procrustes
  7c. Semantic neighborhood shifts
  7d. Pairwise similarity trajectories
  7e. Semantic displacement curves
  7f. Per-decade UMAP panels

Outputs:
  Data/models/w2v_{decade}.model
  Graphs/pub/fig_w2v_neighbors.png
  Graphs/pub/fig_w2v_similarity.png
  Graphs/pub/fig_w2v_displacement.png
  Graphs/pub/fig_w2v_umap_decades.png
"""
import sys, io, os, re, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import Counter

from gensim.models import Word2Vec
from scipy.linalg import orthogonal_procrustes
from sklearn.metrics.pairwise import cosine_similarity

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
MODELS = ROOT / "Data" / "models"
PUB  = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)
MODELS.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
DECADES = [1980, 1990, 2000, 2010, 2020]
ANCHOR_DECADE = 2010  # largest decade, used as reference space
W2V_DIM = 200
W2V_WINDOW = 5
W2V_MIN_COUNT = 10
W2V_EPOCHS = 15
W2V_SG = 1  # skip-gram

# Target words for neighborhood + displacement analysis
TARGET_WORDS = [
    "thalamus", "thalamic",
    "mediodorsal", "pulvinar", "geniculate", "reticular", "ventral", "anterior",
]

# Conceptual anchors for pairwise similarity
ANCHOR_CONCEPTS = [
    "relay", "hub", "modulator", "gate", "gating", "oscillation",
    "computation", "network", "cortex", "attention", "consciousness",
    "memory", "sleep", "circuit", "integration", "sensory",
]

# Control words (stable, shouldn't drift much)
CONTROL_WORDS = ["cortex", "neuron", "brain", "receptor", "synaptic"]

# Color palette for decades
DECADE_COLORS = {
    1980: WONG["blue"], 1990: WONG["orange"], 2000: WONG["green"],
    2010: WONG["red"], 2020: WONG["purple"],
}

# ═══════════════════════════════════════════════════════════════════════════
# LOAD AND PREPROCESS
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
t0 = time.time()

df = pd.read_csv(PROC / "all_sentences_final.csv",
                 usecols=["sentence", "year"], low_memory=False)
df["decade"] = (df["year"] // 10) * 10
df = df[df["decade"].isin(DECADES)].reset_index(drop=True)
print(f"  {len(df):,} sentences across {len(DECADES)} decades")

# Preprocess: lowercase, basic tokenization
print("Tokenizing...")

def tokenize(text):
    """Simple tokenizer: lowercase, keep alphabetic tokens >= 3 chars."""
    text = str(text).lower()
    tokens = re.findall(r'\b[a-z]{3,}\b', text)
    return tokens

df["tokens"] = df["sentence"].apply(tokenize)

# Build per-decade corpora
decade_corpora = {}
for dec in DECADES:
    mask = df["decade"] == dec
    corpus = df.loc[mask, "tokens"].tolist()
    n_tokens = sum(len(t) for t in corpus)
    decade_corpora[dec] = corpus
    print(f"  {dec}s: {mask.sum():,} sentences, {n_tokens:,} tokens")

# ═══════════════════════════════════════════════════════════════════════════
# 7a. TRAIN WORD2VEC PER DECADE
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("7a. TRAINING WORD2VEC PER DECADE")
print("=" * 70)

models = {}
for dec in DECADES:
    model_path = MODELS / f"w2v_{dec}.model"
    if model_path.exists():
        print(f"  {dec}s: loading cached model")
        models[dec] = Word2Vec.load(str(model_path))
    else:
        print(f"  {dec}s: training (dim={W2V_DIM}, window={W2V_WINDOW}, "
              f"min_count={W2V_MIN_COUNT})...")
        model = Word2Vec(
            sentences=decade_corpora[dec],
            vector_size=W2V_DIM,
            window=W2V_WINDOW,
            min_count=W2V_MIN_COUNT,
            sg=W2V_SG,
            epochs=W2V_EPOCHS,
            workers=4,
            seed=42,
        )
        model.save(str(model_path))
        models[dec] = model
    vocab_size = len(models[dec].wv)
    print(f"    vocab: {vocab_size:,} words")

# ═══════════════════════════════════════════════════════════════════════════
# 7b. ALIGN EMBEDDINGS (ORTHOGONAL PROCRUSTES)
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"7b. ALIGNING TO {ANCHOR_DECADE}s (Orthogonal Procrustes)")
print("=" * 70)

anchor_wv = models[ANCHOR_DECADE].wv
aligned_vectors = {}  # {decade: {word: aligned_vector}}

for dec in DECADES:
    wv = models[dec].wv
    # Find shared vocabulary with anchor
    shared = sorted(set(wv.key_to_index.keys()) & set(anchor_wv.key_to_index.keys()))
    print(f"  {dec}s: {len(shared):,} shared words with {ANCHOR_DECADE}s")

    if dec == ANCHOR_DECADE:
        # No alignment needed for anchor
        aligned_vectors[dec] = {w: anchor_wv[w] for w in wv.key_to_index}
        continue

    # Build matrices of shared vocabulary
    A = np.array([wv[w] for w in shared])       # source
    B = np.array([anchor_wv[w] for w in shared]) # target (anchor)

    # Compute Procrustes rotation
    R, _ = orthogonal_procrustes(A, B)

    # Apply rotation to ALL words in this decade
    all_words = list(wv.key_to_index.keys())
    all_vecs = np.array([wv[w] for w in all_words])
    aligned = all_vecs @ R

    aligned_vectors[dec] = {w: aligned[i] for i, w in enumerate(all_words)}
    print(f"    aligned (Procrustes rotation applied)")

# ═══════════════════════════════════════════════════════════════════════════
# 7c. SEMANTIC NEIGHBORHOOD ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("7c. SEMANTIC NEIGHBORHOOD SHIFTS")
print("=" * 70)

TOP_K = 20  # top-20 neighbors

neighborhood_data = {}  # {target: {decade: [(word, sim), ...]}}

for target in TARGET_WORDS:
    neighborhood_data[target] = {}
    for dec in DECADES:
        if target not in aligned_vectors[dec]:
            continue
        target_vec = aligned_vectors[dec][target]
        # Compute similarity to all other words
        words = [w for w in aligned_vectors[dec] if w != target]
        vecs = np.array([aligned_vectors[dec][w] for w in words])
        sims = cosine_similarity(target_vec.reshape(1, -1), vecs)[0]
        top_idx = np.argsort(sims)[-TOP_K:][::-1]
        neighbors = [(words[i], sims[i]) for i in top_idx]
        neighborhood_data[target][dec] = neighbors

    # Print summary for key targets
    if target in ("thalamus", "thalamic"):
        print(f"\n  '{target}' top-10 neighbors per decade:")
        for dec in DECADES:
            if dec in neighborhood_data[target]:
                top10 = neighborhood_data[target][dec][:10]
                words_str = ", ".join(f"{w}({s:.2f})" for w, s in top10)
                print(f"    {dec}s: {words_str}")

# ═══════════════════════════════════════════════════════════════════════════
# 7d. PAIRWISE SIMILARITY OVER TIME
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("7d. PAIRWISE SIMILARITY TRAJECTORIES")
print("=" * 70)

sim_trajectories = {}  # {concept: {decade: sim_to_thalamus}}

for concept in ANCHOR_CONCEPTS:
    sim_trajectories[concept] = {}
    for dec in DECADES:
        if "thalamus" in aligned_vectors[dec] and concept in aligned_vectors[dec]:
            v1 = aligned_vectors[dec]["thalamus"]
            v2 = aligned_vectors[dec][concept]
            sim = cosine_similarity(v1.reshape(1, -1), v2.reshape(1, -1))[0, 0]
            sim_trajectories[concept][dec] = sim

    if sim_trajectories[concept]:
        vals = list(sim_trajectories[concept].values())
        trend = "↑" if len(vals) >= 2 and vals[-1] > vals[0] else "↓" if len(vals) >= 2 and vals[-1] < vals[0] else "→"
        delta = vals[-1] - vals[0] if len(vals) >= 2 else 0
        print(f"  thalamus ↔ {concept:15s}: {trend} Δ={delta:+.3f}  "
              f"[{', '.join(f'{v:.3f}' for v in vals)}]")

# ═══════════════════════════════════════════════════════════════════════════
# 7e. SEMANTIC DISPLACEMENT
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("7e. SEMANTIC DISPLACEMENT (consecutive decades)")
print("=" * 70)

displacement_data = {}  # {word: {decade_pair: cosine_distance}}
all_targets = TARGET_WORDS + CONTROL_WORDS

for word in all_targets:
    displacement_data[word] = {}
    for i in range(len(DECADES) - 1):
        d1, d2 = DECADES[i], DECADES[i + 1]
        if word in aligned_vectors[d1] and word in aligned_vectors[d2]:
            v1 = aligned_vectors[d1][word]
            v2 = aligned_vectors[d2][word]
            dist = 1 - cosine_similarity(v1.reshape(1, -1), v2.reshape(1, -1))[0, 0]
            displacement_data[word][(d1, d2)] = dist

    if displacement_data[word]:
        vals = list(displacement_data[word].values())
        print(f"  {word:15s}: {' → '.join(f'{v:.4f}' for v in vals)}  "
              f"(max={max(vals):.4f})")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURES
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("GENERATING FIGURES")
print("=" * 70)

# ── Figure 1: Neighborhood heatmap for "thalamus" ───────────────────────

fig, ax = plt.subplots(figsize=(14, 8))

# Collect all neighbor words across all decades for "thalamus"
all_neighbor_words = set()
for dec in DECADES:
    if dec in neighborhood_data["thalamus"]:
        for w, _ in neighborhood_data["thalamus"][dec][:TOP_K]:
            all_neighbor_words.add(w)

# Score each word in each decade
neighbor_list = sorted(all_neighbor_words)
heatmap = np.full((len(neighbor_list), len(DECADES)), np.nan)
for j, dec in enumerate(DECADES):
    if dec in neighborhood_data["thalamus"]:
        neighbor_dict = {w: s for w, s in neighborhood_data["thalamus"][dec]}
        for i, word in enumerate(neighbor_list):
            if word in neighbor_dict:
                heatmap[i, j] = neighbor_dict[word]

# Sort by mean similarity (most similar at top)
mean_sim = np.nanmean(heatmap, axis=1)
sort_idx = np.argsort(-mean_sim)
heatmap = heatmap[sort_idx][:30]  # top 30
neighbor_list_sorted = [neighbor_list[i] for i in sort_idx][:30]

im = ax.imshow(heatmap, aspect="auto", cmap="YlOrRd")
ax.set_xticks(range(len(DECADES)))
ax.set_xticklabels([f"{d}s" for d in DECADES])
ax.set_yticks(range(len(neighbor_list_sorted)))
ax.set_yticklabels(neighbor_list_sorted, fontsize=8)
ax.set_title("Semantic Neighbors of 'thalamus' Across Decades\n(cosine similarity, aligned word2vec)")
plt.colorbar(im, ax=ax, label="Cosine similarity", shrink=0.8)
plt.tight_layout()
save_fig(fig, PUB / "fig_w2v_neighbors.png")
print("  Saved fig_w2v_neighbors.png")

# ── Figure 2: Pairwise similarity trajectories ──────────────────────────

fig, ax = plt.subplots(figsize=(12, 7))
color_cycle = list(WONG.values())
for idx, concept in enumerate(ANCHOR_CONCEPTS):
    if sim_trajectories[concept]:
        decs = sorted(sim_trajectories[concept].keys())
        vals = [sim_trajectories[concept][d] for d in decs]
        ax.plot(decs, vals, "o-", color=color_cycle[idx % len(color_cycle)],
                label=concept, linewidth=2, markersize=6)

ax.set_xlabel("Decade")
ax.set_ylabel("Cosine similarity to 'thalamus'")
ax.set_title("How Close Is 'thalamus' to Key Concepts?\n(aligned word2vec, Procrustes)")
ax.legend(ncol=3, fontsize=8, loc="upper left", bbox_to_anchor=(0, -0.12))
despine(ax)
plt.tight_layout()
save_fig(fig, PUB / "fig_w2v_similarity.png")
print("  Saved fig_w2v_similarity.png")

# ── Figure 3: Semantic displacement curves ───────────────────────────────

fig, ax = plt.subplots(figsize=(10, 6))
decade_pairs = [(DECADES[i], DECADES[i+1]) for i in range(len(DECADES)-1)]
x_labels = [f"{d1}s→{d2}s" for d1, d2 in decade_pairs]

for word in TARGET_WORDS[:6]:  # top 6 targets
    if displacement_data[word]:
        vals = [displacement_data[word].get(dp, np.nan) for dp in decade_pairs]
        ax.plot(range(len(vals)), vals, "o-", label=word, linewidth=2, markersize=7)

# Control words as grey dashed
for word in CONTROL_WORDS:
    if displacement_data[word]:
        vals = [displacement_data[word].get(dp, np.nan) for dp in decade_pairs]
        ax.plot(range(len(vals)), vals, "s--", color="grey", alpha=0.5,
                label=word, linewidth=1, markersize=4)

ax.set_xticks(range(len(x_labels)))
ax.set_xticklabels(x_labels)
ax.set_ylabel("Cosine distance (semantic displacement)")
ax.set_title("Semantic Displacement: How Much Did Each Word's Meaning Shift?\n"
             "(colored = target words, grey = control words)")
ax.legend(ncol=3, fontsize=8)
despine(ax)
plt.tight_layout()
save_fig(fig, PUB / "fig_w2v_displacement.png")
print("  Saved fig_w2v_displacement.png")

# ── Figure 4: Per-decade UMAP panels ────────────────────────────────────

try:
    from umap import UMAP

    fig, axes = plt.subplots(1, len(DECADES), figsize=(5 * len(DECADES), 5))

    # Highlight words
    highlight = set(TARGET_WORDS + ANCHOR_CONCEPTS + CONTROL_WORDS)

    for idx, dec in enumerate(DECADES):
        ax = axes[idx]
        vecs_dict = aligned_vectors[dec]

        # Subsample to manageable size (top 2000 by frequency)
        word_freq = Counter()
        for tokens in decade_corpora[dec]:
            word_freq.update(tokens)

        top_words = [w for w, _ in word_freq.most_common(2000) if w in vecs_dict]
        # Always include highlight words
        for w in highlight:
            if w in vecs_dict and w not in top_words:
                top_words.append(w)

        vecs = np.array([vecs_dict[w] for w in top_words])
        reducer = UMAP(n_components=2, random_state=42, n_neighbors=15, min_dist=0.1)
        coords = reducer.fit_transform(vecs)

        # Plot all words as small grey dots
        ax.scatter(coords[:, 0], coords[:, 1], s=3, c="lightgrey", alpha=0.3)

        # Highlight target words in red, anchors in blue
        for i, w in enumerate(top_words):
            if w in TARGET_WORDS:
                ax.scatter(coords[i, 0], coords[i, 1], s=50, c="red", zorder=5)
                ax.annotate(w, (coords[i, 0], coords[i, 1]), fontsize=6,
                            color="red", fontweight="bold")
            elif w in ANCHOR_CONCEPTS:
                ax.scatter(coords[i, 0], coords[i, 1], s=30, c=WONG["blue"], zorder=4)
                ax.annotate(w, (coords[i, 0], coords[i, 1]), fontsize=5,
                            color=WONG["blue"])

        ax.set_title(f"{dec}s", fontsize=14, fontweight="bold")
        ax.set_xticks([])
        ax.set_yticks([])

    plt.suptitle("Word Embedding Space Per Decade (UMAP projection, aligned word2vec)",
                 fontsize=14, y=1.02)
    plt.tight_layout()
    save_fig(fig, PUB / "fig_w2v_umap_decades.png")
    print("  Saved fig_w2v_umap_decades.png")

except ImportError:
    print("  UMAP not available — skipping per-decade panels")

# ═══════════════════════════════════════════════════════════════════════════
# SAVE SUMMARY DATA
# ═══════════════════════════════════════════════════════════════════════════

# Save pairwise similarity trajectories
sim_df_rows = []
for concept in ANCHOR_CONCEPTS:
    for dec, sim in sim_trajectories[concept].items():
        sim_df_rows.append({"concept": concept, "decade": dec,
                            "similarity_to_thalamus": sim})
pd.DataFrame(sim_df_rows).to_csv(PROC / "w2v_similarity_trajectories.csv", index=False)

# Save displacement data
disp_rows = []
for word in all_targets:
    for (d1, d2), dist in displacement_data[word].items():
        disp_rows.append({"word": word, "from_decade": d1, "to_decade": d2,
                          "cosine_distance": dist})
pd.DataFrame(disp_rows).to_csv(PROC / "w2v_displacement.csv", index=False)

elapsed = time.time() - t0
print(f"\nDone in {elapsed/60:.1f} minutes.")
