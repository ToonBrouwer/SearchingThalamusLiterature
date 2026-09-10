"""
refined_analyses.py — Refined versions of several analyses.

1. Semantic displacement with 5-year bins (not just decades)
2. Per-nucleus semantic displacement
3. Filtered semantic neighbors (remove brain regions + morphological variants)
4. Per-decade co-occurrence networks (filtered, meaningful words only)
5. Most common words per decade (heavy filtering)
6. Fix source labels: merge fulltext/institutional/oa_pdf → 'fulltext'
"""
import sys, io, os, re, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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

# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
df = pd.read_csv(PROC / "all_sentences_final.csv",
                 usecols=["sentence", "year", "source", "frame_type", "mega_theme"],
                 low_memory=False)
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df["decade"] = (df["year"] // 10) * 10
df["half_decade"] = (df["year"] // 5) * 5
print(f"  {len(df):,} sentences")

# ═══════════════════════════════════════════════════════════════════════════
# 0. FIX SOURCE LABELS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("0. FIXING SOURCE LABELS")
print("=" * 70)

# Merge all fulltext variants
df["source_clean"] = df["source"].replace({
    "fulltext": "fulltext",
    "institutional": "fulltext",
    "oa_pdf": "fulltext",
    "abstract": "abstract",
})
print(f"  Source distribution (clean):")
for s, c in df["source_clean"].value_counts().items():
    print(f"    {s}: {c:,} ({c/len(df)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════════════════════
# BRAIN REGION FILTER LIST (for neighbor/co-occurrence filtering)
# ═══════════════════════════════════════════════════════════════════════════
BRAIN_REGIONS = {
    # Major brain structures
    "cortex", "cortical", "hippocampus", "hippocampal", "amygdala",
    "cerebellum", "cerebellar", "striatum", "striatal", "basal",
    "ganglia", "brainstem", "hypothalamus", "hypothalamic",
    "midbrain", "pons", "pontine", "medulla", "spinal",
    "prefrontal", "frontal", "parietal", "temporal", "occipital",
    "cingulate", "insular", "insula", "putamen", "caudate",
    "pallidum", "pallidal", "globus", "subthalamic", "colliculus",
    "cerebral", "neocortex", "neocortical", "allocortex",
    "entorhinal", "perirhinal", "subiculum", "fornix",
    "accumbens", "amygdaloid", "basolateral", "parahippocampal",
    "fusiform", "paracingulate", "subcortex", "subcortical",
    "subcortically", "diencephalon", "epithalamus",
    # Extended anatomical sub-regions and terms
    "frontoparietal", "intraparietal", "interpeduncular", "tubercle",
    "choroidea", "choroidal", "heschl", "splenium", "fissure",
    "cerebrum", "rhomboid", "terminalis", "lateralis", "externus",
    "parvocellular", "magnocellular", "funiculus", "olivary",
    "pontomesencephalic", "reticularis", "interposed",
    "centromedial", "parahippocampus", "cingulated",
    "radiata", "corona", "fastigial", "operculum",
    "mpoa", "traped", "beige", "vlventral", "plex",
    "geniculate", "pulvinar", "mediodorsal", "reticular",
    "intralaminar", "paraventricular", "ventrobasal",
    "somatosensory", "spinothalamic",
    # Generic anatomical descriptors
    "afferents", "afferent", "efferents", "efferent",
    "projections", "projection", "innervation", "innervates",
    "ipsilateral", "contralateral", "bilateral",
    "fibers", "fiber", "fibres", "fibre", "tract", "tracts",
    "lamina", "laminar", "capsule", "peduncle",
    # Thalamic terms (not useful as neighbors OF thalamus)
    "thalamus", "thalamic", "thalamocortical", "thalami",
    "corticothalamic", "thalamocortial", "thalam", "thalamo",
    # Anatomical direction words
    "lateral", "medial", "dorsal", "ventral", "anterior", "posterior",
    "rostral", "caudal", "ipsilateral", "contralateral",
    "rostromedial", "lateroposterior", "ventroanterior", "ventrocaudal",
    "dorsomedian", "dorsolateral", "ventromedial", "ventrolateral",
    # OCR fragments and broken words
    "amus", "mus", "lamic", "tha", "sal", "dor", "poste", "thala",
    "lat", "ventr", "med", "ant", "nucl", "ipsilat", "cephalon",
    "pocampus", "ammonis", "cornu", "gion", "ento", "hipp",
    "bnst", "mofc", "cttha", "ncx", "mdc", "cpl", "epc", "avvl",
    "lein", "expt", "neous", "erties", "ular", "cium", "hyperpo",
    # Species names
    "fascicularis", "sciureus", "macaque",
}

GENERAL_STOP = {
    # Standard English stopwords
    "the", "and", "was", "were", "with", "that", "this", "for", "from",
    "are", "has", "had", "have", "been", "not", "but", "our", "their",
    "these", "those", "which", "more", "also", "than", "can", "may",
    "its", "between", "into", "both", "each", "all", "other", "such",
    "when", "where", "how", "what", "who", "will", "did", "does",
    "one", "two", "three", "using", "used", "showed", "found", "results",
    "study", "studies", "suggest", "present", "however", "thus",
    "therefore", "although", "could", "would", "should", "being",
    "during", "after", "before", "through", "within", "about",
    "only", "most", "some", "very", "well", "significantly",
    "compared", "observed", "respectively", "either", "whether",
    "fig", "figure", "table", "supplementary", "doi", "http",
    "serves", "acts", "plays", "shows", "suggests", "indicates",
    # PDF boilerplate from institutional downloads
    "downloaded", "march", "april", "crossref", "medline",
    "universiteitsbibliotheek", "pubmed", "copyright",
    "elsevier", "wiley", "springer", "rights", "reserved",
    "journal", "article", "published", "author", "authors",
    # Author surnames that leak through (common ones in thalamus lit)
    "steriade", "jones", "sherman", "guillery", "llinas", "crick",
    "halassa", "saalmann", "kastner", "pinault", "mccormick",
    "hwang", "behrens", "feeser", "lavallee", "francesconi",
    "urbain", "ramcharan", "meuth", "padberg", "arcelli",
    "busenbark", "hubble", "wilkinson", "troster", "tarsy",
    "pahwa", "donald", "koller", "taha", "graham", "landgren",
    "yingling", "skinner", "sillito", "contreras", "debay",
    "bertolero", "esposito",
}

EXTRA_FILTER = BRAIN_REGIONS | GENERAL_STOP | {
    "cells", "cell", "neurons", "neuron", "neuronal", "nucleus", "nuclei",
    "axons", "axonal", "dendrites", "dendritic", "synapse", "synapses",
    "receptor", "receptors", "protein", "proteins", "gene", "genes",
    "mice", "rats", "monkey", "monkeys", "cat", "cats", "animal", "animals",
    "patients", "patient", "subjects", "group", "groups", "control",
    "region", "regions", "area", "areas", "side", "left", "right",
    "increased", "decreased", "higher", "lower", "changes", "change",
    "data", "number", "time", "level", "levels", "effect", "effects",
    "response", "responses", "activity", "recording", "stimulation",
    "analysis", "method", "methods", "technique", "performed",
    "measured", "determined", "examined", "investigated", "reported",
    "controls", "healthy", "matter", "white", "gray", "grey",
    "magnetic", "resonance", "play", "role", "different", "based",
    "associated", "related", "significant", "specific", "including",
}


def tokenize(text):
    return re.findall(r'\b[a-z]{3,}\b', str(text).lower())


def is_junk_token(word):
    """Heuristic: detect OCR fragments and nonsense tokens."""
    if len(word) < 4:
        return True
    # Too many consonants in a row (broken word)
    if re.search(r'[bcdfghjklmnpqrstvwxyz]{5,}', word):
        return True
    # Starts or ends with unlikely patterns
    if word.endswith(("eous", "ular", "ting", "ness")) and len(word) < 6:
        return True
    # No vowels at all
    if not re.search(r'[aeiou]', word):
        return True
    # Looks like an abbreviation leaked through
    if len(word) <= 4 and word.upper() == word:
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# 1. SEMANTIC DISPLACEMENT — 5-YEAR BINS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. SEMANTIC DISPLACEMENT — 5-YEAR BINS")
print("=" * 70)

HALF_DECADES = sorted([hd for hd in df["half_decade"].dropna().unique()
                        if 1980 <= hd <= 2025])
print(f"  5-year bins: {HALF_DECADES}")

# Train word2vec per 5-year bin
hd_models = {}
for hd in HALF_DECADES:
    model_path = MODELS / f"w2v_5yr_{int(hd)}.model"
    hd_df = df[df["half_decade"] == hd]
    corpus = hd_df["sentence"].apply(tokenize).tolist()
    n_tokens = sum(len(t) for t in corpus)

    if n_tokens < 50000:
        print(f"  {int(hd)}: {len(hd_df):,} sents, {n_tokens:,} tokens — TOO SMALL, skip")
        continue

    if model_path.exists():
        hd_models[hd] = Word2Vec.load(str(model_path))
        print(f"  {int(hd)}: loaded cached ({len(hd_models[hd].wv)} words)")
    else:
        print(f"  {int(hd)}: training ({n_tokens:,} tokens)...")
        model = Word2Vec(sentences=corpus, vector_size=200, window=5,
                         min_count=10, sg=1, epochs=15, workers=4, seed=42)
        model.save(str(model_path))
        hd_models[hd] = model
        print(f"    → {len(model.wv)} words")

# Align all to largest bin
anchor_hd = max(hd_models.keys(), key=lambda h: len(hd_models[h].wv))
print(f"\n  Anchor bin: {int(anchor_hd)} ({len(hd_models[anchor_hd].wv)} words)")

aligned_5yr = {}
anchor_wv = hd_models[anchor_hd].wv

for hd, model in hd_models.items():
    wv = model.wv
    shared = sorted(set(wv.key_to_index.keys()) & set(anchor_wv.key_to_index.keys()))

    if hd == anchor_hd:
        aligned_5yr[hd] = {w: anchor_wv[w] for w in wv.key_to_index}
        continue

    if len(shared) < 100:
        print(f"  {int(hd)}: only {len(shared)} shared — skip alignment")
        continue

    A = np.array([wv[w] for w in shared])
    B = np.array([anchor_wv[w] for w in shared])
    R, _ = orthogonal_procrustes(A, B)

    all_words = list(wv.key_to_index.keys())
    all_vecs = np.array([wv[w] for w in all_words])
    aligned = all_vecs @ R
    aligned_5yr[hd] = {w: aligned[i] for i, w in enumerate(all_words)}
    print(f"  {int(hd)}: aligned ({len(shared)} shared)")

# Compute displacement for target words
TARGET_WORDS = ["thalamus", "thalamic", "relay", "hub", "gate", "gating",
                "modulator", "network", "oscillation", "connectivity"]
NUCLEUS_WORDS = ["mediodorsal", "pulvinar", "geniculate", "reticular",
                 "ventral", "anterior", "intralaminar", "paraventricular"]
CONTROL_WORDS = ["cortex", "neuron", "brain", "receptor", "synaptic"]
ALL_TARGETS = TARGET_WORDS + NUCLEUS_WORDS + CONTROL_WORDS

sorted_hds = sorted(aligned_5yr.keys())
disp_5yr_rows = []

print("\n  Displacement (5-year bins):")
for word in ALL_TARGETS:
    disps = []
    for i in range(len(sorted_hds) - 1):
        h1, h2 = sorted_hds[i], sorted_hds[i + 1]
        if word in aligned_5yr[h1] and word in aligned_5yr[h2]:
            v1 = aligned_5yr[h1][word]
            v2 = aligned_5yr[h2][word]
            d = 1 - cosine_similarity(v1.reshape(1, -1), v2.reshape(1, -1))[0, 0]
            disps.append(d)
            disp_5yr_rows.append({
                "word": word, "from": int(h1), "to": int(h2),
                "cosine_distance": d,
            })
        else:
            disps.append(np.nan)

    if disps:
        valid = [d for d in disps if not np.isnan(d)]
        if valid:
            print(f"  {word:20s}: mean={np.mean(valid):.4f}, "
                  f"max={np.max(valid):.4f}, n_bins={len(valid)}")

pd.DataFrame(disp_5yr_rows).to_csv(PROC / "w2v_displacement_5yr.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════════
# 2. FILTERED SEMANTIC NEIGHBORS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. FILTERED SEMANTIC NEIGHBORS")
print("=" * 70)

# Use decade models (already trained)
DECADE_BINS = [1980, 1990, 2000, 2010, 2020]
decade_models = {}
for dec in DECADE_BINS:
    mp = MODELS / f"w2v_{dec}.model"
    if mp.exists():
        decade_models[dec] = Word2Vec.load(str(mp))

# Align decade models
aligned_dec = {}
anchor_dec = 2010
anchor_wv_dec = decade_models[anchor_dec].wv

for dec, model in decade_models.items():
    wv = model.wv
    shared = sorted(set(wv.key_to_index.keys()) & set(anchor_wv_dec.key_to_index.keys()))
    if dec == anchor_dec:
        aligned_dec[dec] = {w: anchor_wv_dec[w] for w in wv.key_to_index}
        continue
    A = np.array([wv[w] for w in shared])
    B = np.array([anchor_wv_dec[w] for w in shared])
    R, _ = orthogonal_procrustes(A, B)
    all_words = list(wv.key_to_index.keys())
    all_vecs = np.array([wv[w] for w in all_words])
    aligned = all_vecs @ R
    aligned_dec[dec] = {w: aligned[i] for i, w in enumerate(all_words)}

# Get filtered neighbors
for target in ["thalamus", "relay", "hub", "gate"]:
    print(f"\n  '{target}' filtered neighbors:")
    for dec in DECADE_BINS:
        if target not in aligned_dec[dec]:
            continue
        target_vec = aligned_dec[dec][target]
        # Filter: exclude brain regions, thalamic variants, stopwords, OCR junk
        candidates = {w: v for w, v in aligned_dec[dec].items()
                      if w not in BRAIN_REGIONS
                      and w not in GENERAL_STOP
                      and w not in EXTRA_FILTER
                      and not is_junk_token(w)
                      and len(w) >= 4
                      and w != target}
        words = list(candidates.keys())
        vecs = np.array([candidates[w] for w in words])
        sims = cosine_similarity(target_vec.reshape(1, -1), vecs)[0]
        top_idx = np.argsort(sims)[-10:][::-1]
        neighbors = [(words[i], sims[i]) for i in top_idx]
        nstr = ", ".join(f"{w}({s:.2f})" for w, s in neighbors)
        print(f"    {dec}s: {nstr}")

# ═══════════════════════════════════════════════════════════════════════════
# 3. MOST COMMON WORDS PER DECADE (heavily filtered)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. MOST COMMON WORDS PER DECADE")
print("=" * 70)

word_freq_rows = []
for dec in DECADE_BINS:
    dec_df = df[df["decade"] == dec]
    word_counts = Counter()
    for sent in dec_df["sentence"]:
        tokens = tokenize(sent)
        word_counts.update(t for t in tokens if t not in EXTRA_FILTER
                          and t not in BRAIN_REGIONS and t not in GENERAL_STOP
                          and not is_junk_token(t) and len(t) >= 4)

    top30 = word_counts.most_common(30)
    print(f"\n  {dec}s (top 20):")
    for w, c in top30[:20]:
        freq = c / len(dec_df) * 100
        print(f"    {w:25s}: {c:>6,} ({freq:.2f}%)")
        word_freq_rows.append({"decade": dec, "word": w, "count": c,
                               "pct": round(freq, 3)})

pd.DataFrame(word_freq_rows).to_csv(PROC / "word_freq_per_decade.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════════
# 4. CO-OCCURRENCE NETWORKS PER DECADE (filtered)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. FILTERED CO-OCCURRENCE NETWORKS PER DECADE")
print("=" * 70)

for dec in DECADE_BINS:
    dec_df = df[df["decade"] == dec]
    pair_counts = Counter()
    word_counts = Counter()

    for sent in dec_df["sentence"]:
        tokens = tokenize(sent)
        filtered = [t for t in tokens if t not in EXTRA_FILTER
                    and t not in BRAIN_REGIONS and t not in GENERAL_STOP
                    and not is_junk_token(t) and len(t) >= 4]
        unique = list(set(filtered))
        word_counts.update(unique)
        for i in range(len(unique)):
            for j in range(i + 1, len(unique)):
                pair = tuple(sorted([unique[i], unique[j]]))
                pair_counts[pair] += 1

    # Top edges
    top_pairs = pair_counts.most_common(30)
    print(f"\n  {dec}s — top 15 co-occurring pairs:")
    for (w1, w2), c in top_pairs[:15]:
        print(f"    {w1:20s} — {w2:20s}: {c:>5,}")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURES
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("GENERATING FIGURES")
print("=" * 70)

# ── Figure 1: 5-year semantic displacement ──
disp_df = pd.DataFrame(disp_5yr_rows)

fig, axes = plt.subplots(1, 3, figsize=(18, 6))

# Panel A: Thalamus + frame terms
ax = axes[0]
for word in ["thalamus", "relay", "hub", "gate", "network", "connectivity"]:
    wd = disp_df[disp_df["word"] == word].sort_values("from")
    if len(wd) > 1:
        midpoints = (wd["from"] + wd["to"]) / 2
        ax.plot(midpoints, wd["cosine_distance"], "o-", label=word,
                linewidth=2, markersize=5)
ax.set_xlabel("Period midpoint")
ax.set_ylabel("Cosine distance (displacement)")
ax.set_title("A. Semantic displacement — frame terms\n(5-year bins)")
ax.legend(fontsize=7, ncol=2)
despine(ax)
panel_label(ax, "A")

# Panel B: Nucleus terms
ax = axes[1]
for word in NUCLEUS_WORDS[:6]:
    wd = disp_df[disp_df["word"] == word].sort_values("from")
    if len(wd) > 1:
        midpoints = (wd["from"] + wd["to"]) / 2
        ax.plot(midpoints, wd["cosine_distance"], "o-", label=word,
                linewidth=2, markersize=5)
ax.set_xlabel("Period midpoint")
ax.set_ylabel("Cosine distance")
ax.set_title("B. Semantic displacement — nucleus terms")
ax.legend(fontsize=7, ncol=2)
despine(ax)
panel_label(ax, "B")

# Panel C: Control words
ax = axes[2]
for word in CONTROL_WORDS:
    wd = disp_df[disp_df["word"] == word].sort_values("from")
    if len(wd) > 1:
        midpoints = (wd["from"] + wd["to"]) / 2
        ax.plot(midpoints, wd["cosine_distance"], "s--", label=word,
                linewidth=1.5, markersize=4, color="grey", alpha=0.6)
# Add thalamus for comparison
wd = disp_df[disp_df["word"] == "thalamus"].sort_values("from")
if len(wd) > 1:
    midpoints = (wd["from"] + wd["to"]) / 2
    ax.plot(midpoints, wd["cosine_distance"], "o-", label="thalamus",
            linewidth=2.5, markersize=6, color=WONG["red"])
ax.set_xlabel("Period midpoint")
ax.set_ylabel("Cosine distance")
ax.set_title("C. Thalamus vs control words")
ax.legend(fontsize=7)
despine(ax)
panel_label(ax, "C")

plt.suptitle("Semantic Displacement Over Time (5-year bins, Procrustes-aligned word2vec)",
             fontsize=13, fontweight="bold", y=1.02)
plt.tight_layout()
save_fig(fig, PUB / "fig_displacement_5yr.png")
print("  Saved fig_displacement_5yr.png")

# ── Figure 2: Filtered neighbors heatmap ──
fig, axes = plt.subplots(1, 3, figsize=(22, 8))

# Extra filter: common author surnames (leak through as "words")
AUTHOR_NAMES = {
    "mink", "norregaard", "janszen", "hauser", "kumar", "british",
    "lyons", "favre", "disord", "sime", "baltimore", "florida",
    "miyawaki", "requests", "wolfart", "hudetz", "mcalonan",
    "hess", "maendly", "chan", "govindaiah", "berman", "singer",
    "lavalle", "bushy", "prince", "huguenard",
}

def is_likely_author(word):
    """Heuristic: detect author names leaking as word2vec tokens."""
    if word in AUTHOR_NAMES:
        return True
    return False

# Morphological variants to collapse (plural/verb forms → base)
LEMMA_MAP = {
    "relays": "relay", "relayed": "relay", "relaying": "relay",
    "gates": "gate", "gated": "gate", "gating": "gating",  # keep gating distinct
    "hubs": "hub",
    "stations": "station",
    "nodes": "node", "nodal": "node",
    "connectors": "connector",
    "oscillations": "oscillation",
    "inputs": "input",
    "outputs": "output",
    "connections": "connection",
    "projections": "projection",
    "transmits": "transmit", "transmitting": "transmit",
    "modulates": "modulate", "modulating": "modulate",
    "inhibits": "inhibit", "inhibiting": "inhibit",
    "integrates": "integrate", "integrating": "integrate",
    "coordinates": "coordinate", "coordinating": "coordinate",
    "transfers": "transfer",
    "conveyed": "convey", "conveying": "convey",
    "boosts": "boost",
    "filters": "filter", "filtering": "filter",
    "interneurons": "interneuron",
    "channels": "channel",
    "receptors": "receptor",
}

# Minimum word count per decade to include a target in the plot
MIN_TARGET_COUNT = 50

for panel_idx, target in enumerate(["relay", "hub", "gate"]):
    ax = axes[panel_idx]
    all_words = set()
    decade_neighbors = {}

    for dec in DECADE_BINS:
        if target not in aligned_dec[dec]:
            continue
        # Check if target has enough occurrences in this decade
        dec_df_check = df[df["decade"] == dec]
        target_count = dec_df_check["sentence"].str.lower().str.contains(
            r'\b' + target + r'\b', na=False).sum()
        if target_count < MIN_TARGET_COUNT:
            continue  # skip decades where this word is too rare

        target_vec = aligned_dec[dec][target]
        candidates = {w: v for w, v in aligned_dec[dec].items()
                      if w not in BRAIN_REGIONS
                      and w not in GENERAL_STOP
                      and w not in EXTRA_FILTER
                      and w not in AUTHOR_NAMES
                      and not is_junk_token(w)
                      and not is_likely_author(w)
                      and len(w) >= 4 and w != target}
        words = list(candidates.keys())
        if not words:
            continue
        vecs = np.array([candidates[w] for w in words])
        sims = cosine_similarity(target_vec.reshape(1, -1), vecs)[0]
        top_idx = np.argsort(sims)[-30:][::-1]  # get more, then collapse
        # Collapse morphological variants: keep highest-similarity form
        lemma_best = {}
        for idx in top_idx:
            w = words[idx]
            s = sims[idx]
            lemma = LEMMA_MAP.get(w, w)
            if lemma not in lemma_best or s > lemma_best[lemma]:
                lemma_best[lemma] = s
        # Take top 15 after collapsing
        sorted_lemmas = sorted(lemma_best.items(), key=lambda x: x[1], reverse=True)[:15]
        decade_neighbors[dec] = {w: s for w, s in sorted_lemmas}
        all_words.update(w for w, _ in sorted_lemmas)

    if not decade_neighbors:
        ax.text(0.5, 0.5, f"Not enough data for '{target}'",
                ha="center", transform=ax.transAxes)
        ax.axis("off")
        continue

    word_list = sorted(all_words)
    valid_decades = sorted(decade_neighbors.keys())
    heatmap = np.full((len(word_list), len(valid_decades)), np.nan)
    for j, dec in enumerate(valid_decades):
        for i, w in enumerate(word_list):
            if w in decade_neighbors[dec]:
                heatmap[i, j] = decade_neighbors[dec][w]

    # Sort by mean similarity
    mean_sim = np.nanmean(heatmap, axis=1)
    sort_idx = np.argsort(-mean_sim)[:15]
    heatmap_show = heatmap[sort_idx]
    words_show = [word_list[i] for i in sort_idx]

    # Use full 0-1 range for colorscale so differences are visible
    vmin = np.nanmin(heatmap_show) * 0.9
    vmax = np.nanmax(heatmap_show) * 1.05
    im = ax.imshow(heatmap_show, aspect="auto", cmap="YlOrRd",
                   vmin=vmin, vmax=vmax)
    ax.set_xticks(range(len(valid_decades)))
    ax.set_xticklabels([f"{d}s" for d in valid_decades])
    ax.set_yticks(range(len(words_show)))
    ax.set_yticklabels(words_show, fontsize=9, fontweight="bold")

    # Add similarity values as text
    for yi in range(heatmap_show.shape[0]):
        for xi in range(heatmap_show.shape[1]):
            val = heatmap_show[yi, xi]
            if not np.isnan(val):
                ax.text(xi, yi, f"{val:.2f}", ha="center", va="center",
                        fontsize=6, color="white" if val > (vmin+vmax)/2 else "black")

    panel_letter = chr(65 + panel_idx)
    ax.set_title(f"{panel_letter}. Neighbors of '{target}'\n"
                 f"(decades with n>{MIN_TARGET_COUNT} occurrences)")
    plt.colorbar(im, ax=ax, label="Cosine similarity", shrink=0.7)

plt.suptitle("Semantic Neighbors (brain regions + OCR artifacts removed)",
             fontsize=13, fontweight="bold", y=1.02)
plt.tight_layout()
save_fig(fig, PUB / "fig_neighbors_filtered.png")
print("  Saved fig_neighbors_filtered.png")

# ── Figure 3: Word frequency trends ──
wf = pd.DataFrame(word_freq_rows)
fig, ax = plt.subplots(figsize=(14, 8))

# Track words that rise or fall most
pivot = wf.pivot_table(index="word", columns="decade", values="pct", fill_value=0)
if 1980 in pivot.columns and 2020 in pivot.columns:
    pivot["delta"] = pivot[2020] - pivot[1980]
    risers = pivot.nlargest(8, "delta").index.tolist()
    fallers = pivot.nsmallest(8, "delta").index.tolist()

    for w in risers:
        vals = [pivot.loc[w, d] if d in pivot.columns else 0 for d in DECADE_BINS]
        ax.plot(DECADE_BINS, vals, "o-", label=f"↑ {w}", linewidth=2, markersize=5)

    for w in fallers[:5]:
        vals = [pivot.loc[w, d] if d in pivot.columns else 0 for d in DECADE_BINS]
        ax.plot(DECADE_BINS, vals, "s--", label=f"↓ {w}", linewidth=1.5,
                markersize=4, alpha=0.6, color="grey")

ax.set_xlabel("Decade")
ax.set_ylabel("Frequency (% of sentences)")
ax.set_title("Rising and Falling Vocabulary in Thalamus Literature\n"
             "(filtered: no stopwords, brain regions, or method terms)")
ax.legend(fontsize=8, ncol=2, loc="upper left")
despine(ax)
plt.tight_layout()
save_fig(fig, PUB / "fig_word_trends.png")
print("  Saved fig_word_trends.png")

elapsed = time.time()
print("\nDone.")
