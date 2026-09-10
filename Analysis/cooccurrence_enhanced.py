"""
cooccurrence_enhanced.py — Analysis 8: Enhanced co-occurrence network.

Adds to existing co-occurrence analysis:
  - Sentence-level word co-occurrence (not just concept-level)
  - Per-nucleus co-occurrence networks (top 5 nuclei)
  - Louvain community detection
  - Temporal comparison: networks per decade with centrality rankings

Output:
  Data/processed/cooccurrence_temporal.csv
  Graphs/pub/fig_cooccurrence_temporal.png
"""
import sys, io, os, re, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
from pathlib import Path
from collections import Counter, defaultdict

import community as community_louvain

from plotting import apply_style, save_fig, panel_label, despine, WONG
from nucleus_patterns import NUCLEI

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
df = pd.read_csv(PROC / "all_sentences_final.csv", low_memory=False)
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df["decade"] = (df["year"] // 10) * 10
print(f"  {len(df):,} sentences")

DECADES = [1990, 2000, 2010, 2020]
TOP_NUCLEI = ["MD", "VP", "LGN", "TRN", "Pulvinar"]

# Stopwords for co-occurrence
STOP = {"the", "and", "was", "were", "with", "that", "this", "for", "from",
        "are", "has", "had", "have", "been", "not", "but", "our", "their",
        "these", "those", "which", "more", "also", "than", "can", "may",
        "its", "between", "into", "both", "each", "all", "other", "such",
        "when", "where", "how", "what", "who", "will", "did", "does",
        "one", "two", "using", "used", "showed", "found", "results",
        "study", "studies", "suggest", "present", "however", "thus",
        "therefore", "although", "could", "would", "should", "being",
        "during", "after", "before", "through", "within", "about",
        "only", "most", "some", "very", "well", "significantly"}


def tokenize_for_cooccurrence(text):
    """Extract meaningful words from a sentence."""
    text = str(text).lower()
    tokens = re.findall(r'\b[a-z]{3,}\b', text)
    return [t for t in tokens if t not in STOP]


# ═══════════════════════════════════════════════════════════════════════════
# 1. SENTENCE-LEVEL CO-OCCURRENCE PER DECADE
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("1. SENTENCE-LEVEL CO-OCCURRENCE PER DECADE")
print("=" * 70)

decade_networks = {}
decade_centrality = {}

for dec in DECADES:
    dec_df = df[df["decade"] == dec]
    print(f"\n  {dec}s: {len(dec_df):,} sentences")

    # Count word co-occurrences within sentences
    pair_counts = Counter()
    word_counts = Counter()

    for sent in dec_df["sentence"]:
        tokens = tokenize_for_cooccurrence(sent)
        unique_tokens = list(set(tokens))
        word_counts.update(unique_tokens)

        # Co-occurrence: pairs of unique words in same sentence
        for i in range(len(unique_tokens)):
            for j in range(i + 1, len(unique_tokens)):
                pair = tuple(sorted([unique_tokens[i], unique_tokens[j]]))
                pair_counts[pair] += 1

    # Build network (top edges by weight)
    G = nx.Graph()
    min_count = max(20, len(dec_df) * 0.001)  # adaptive threshold
    for (w1, w2), count in pair_counts.most_common(500):
        if count >= min_count:
            G.add_edge(w1, w2, weight=count)

    # Add node sizes by frequency
    for node in G.nodes():
        G.nodes[node]["freq"] = word_counts.get(node, 0)

    decade_networks[dec] = G
    print(f"    Network: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # Degree centrality
    if G.number_of_nodes() > 0:
        dc = nx.degree_centrality(G)
        top10 = sorted(dc.items(), key=lambda x: x[1], reverse=True)[:10]
        decade_centrality[dec] = top10
        print(f"    Top 10 central: {', '.join(w for w, _ in top10)}")

# ═══════════════════════════════════════════════════════════════════════════
# 2. PER-NUCLEUS CO-OCCURRENCE NETWORKS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("2. PER-NUCLEUS CO-OCCURRENCE")
print("=" * 70)

nucleus_cooccurrence = {}

for nid in TOP_NUCLEI:
    col = f"has_{nid}"
    if col not in df.columns:
        continue

    nuc_df = df[df[col] == True]
    print(f"\n  {nid}: {len(nuc_df):,} sentences")

    pair_counts = Counter()
    word_counts = Counter()

    for sent in nuc_df["sentence"]:
        tokens = tokenize_for_cooccurrence(sent)
        unique_tokens = list(set(tokens))
        word_counts.update(unique_tokens)
        for i in range(len(unique_tokens)):
            for j in range(i + 1, len(unique_tokens)):
                pair = tuple(sorted([unique_tokens[i], unique_tokens[j]]))
                pair_counts[pair] += 1

    # Top co-occurring words (excluding thalamus-related terms)
    thal_terms = {"thalamus", "thalamic", "thalamocortical", "thalami"}
    top_words = [(w, c) for w, c in word_counts.most_common(30) if w not in thal_terms]
    nucleus_cooccurrence[nid] = top_words[:15]
    print(f"    Top words: {', '.join(w for w, _ in top_words[:10])}")

# ═══════════════════════════════════════════════════════════════════════════
# 3. COMMUNITY DETECTION ON FULL NETWORK
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("3. COMMUNITY DETECTION (full corpus)")
print("=" * 70)

# Build full-corpus network
full_pairs = Counter()
full_words = Counter()

for sent in df["sentence"]:
    tokens = tokenize_for_cooccurrence(sent)
    unique = list(set(tokens))
    full_words.update(unique)
    for i in range(len(unique)):
        for j in range(i + 1, len(unique)):
            pair = tuple(sorted([unique[i], unique[j]]))
            full_pairs[pair] += 1

G_full = nx.Graph()
min_full = max(100, len(df) * 0.0005)
for (w1, w2), count in full_pairs.most_common(1000):
    if count >= min_full:
        G_full.add_edge(w1, w2, weight=count)

print(f"  Full network: {G_full.number_of_nodes()} nodes, {G_full.number_of_edges()} edges")

if G_full.number_of_nodes() > 10:
    partition = community_louvain.best_partition(G_full, random_state=42)
    n_comm = len(set(partition.values()))
    print(f"  Louvain communities: {n_comm}")

    for c_id in range(min(n_comm, 8)):
        members = [n for n, c in partition.items() if c == c_id]
        members_sorted = sorted(members, key=lambda w: full_words.get(w, 0), reverse=True)
        print(f"    Community {c_id} ({len(members)} words): "
              f"{', '.join(members_sorted[:8])}")

# ═══════════════════════════════════════════════════════════════════════════
# 4. TEMPORAL CENTRALITY TRACKING
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("4. CENTRALITY CHANGES OVER TIME")
print("=" * 70)

# Track which words gain/lose centrality
all_central_words = set()
for dec, top_list in decade_centrality.items():
    for w, _ in top_list:
        all_central_words.add(w)

centrality_over_time = {}
for word in all_central_words:
    centrality_over_time[word] = {}
    for dec in DECADES:
        G = decade_networks[dec]
        if word in G:
            centrality_over_time[word][dec] = nx.degree_centrality(G).get(word, 0)

# Find words with biggest centrality change
rising = []
falling = []
for word, vals in centrality_over_time.items():
    if len(vals) >= 3:
        decades_sorted = sorted(vals.keys())
        first_val = vals[decades_sorted[0]]
        last_val = vals[decades_sorted[-1]]
        delta = last_val - first_val
        if delta > 0.02:
            rising.append((word, delta))
        elif delta < -0.02:
            falling.append((word, delta))

rising.sort(key=lambda x: -x[1])
falling.sort(key=lambda x: x[1])

print("  Rising centrality (new important concepts):")
for w, d in rising[:10]:
    print(f"    {w:20s}: +{d:.3f}")
print("  Falling centrality (declining concepts):")
for w, d in falling[:10]:
    print(f"    {w:20s}: {d:.3f}")

# Save temporal centrality
rows = []
for word, vals in centrality_over_time.items():
    for dec, cent in vals.items():
        rows.append({"word": word, "decade": dec, "centrality": cent})
pd.DataFrame(rows).to_csv(PROC / "cooccurrence_temporal.csv", index=False)

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE: 4-panel co-occurrence analysis
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("GENERATING FIGURES")
print("=" * 70)

fig, axes = plt.subplots(2, 2, figsize=(16, 14))
fig.suptitle("Co-occurrence Network Analysis", fontsize=14, fontweight="bold", y=0.98)

# Panel A & B: Network for 1990s vs 2020s
for panel_idx, (dec, ax) in enumerate(zip([1990, 2020], [axes[0, 0], axes[0, 1]])):
    G = decade_networks[dec]
    if G.number_of_nodes() < 5:
        ax.text(0.5, 0.5, f"Not enough data for {dec}s", ha="center")
        continue

    # Layout
    pos = nx.spring_layout(G, seed=42, k=2.0 / max(np.sqrt(G.number_of_nodes()), 1))

    # Node sizes by degree
    degrees = dict(G.degree())
    node_sizes = [degrees[n] * 15 + 20 for n in G.nodes()]

    # Edge widths
    edge_weights = [G[u][v]["weight"] for u, v in G.edges()]
    max_w = max(edge_weights) if edge_weights else 1
    edge_widths = [w / max_w * 3 + 0.3 for w in edge_weights]

    nx.draw_networkx_edges(G, pos, ax=ax, alpha=0.2, width=edge_widths, edge_color="grey")
    nx.draw_networkx_nodes(G, pos, ax=ax, node_size=node_sizes,
                           node_color=WONG["blue"], alpha=0.7)
    # Label top nodes
    top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:15]
    labels = {n: n for n, _ in top_nodes}
    nx.draw_networkx_labels(G, pos, labels=labels, ax=ax, font_size=7, font_weight="bold")

    ax.set_title(f"{'A' if panel_idx == 0 else 'B'}. Co-occurrence network — {dec}s")
    ax.axis("off")

# Panel C: Per-nucleus vocabulary comparison
ax = axes[1, 0]
# Show top 8 words per nucleus as a table-like visualization
n_show = 8
y_offset = 0
colors_nuc = [WONG["blue"], WONG["red"], WONG["green"], WONG["orange"], WONG["purple"]]
for i, nid in enumerate(TOP_NUCLEI):
    if nid in nucleus_cooccurrence:
        words = [w for w, _ in nucleus_cooccurrence[nid][:n_show]]
        for j, w in enumerate(words):
            ax.text(j * 0.12 + 0.02, 1 - i * 0.2 - 0.05, w, fontsize=7,
                    transform=ax.transAxes, color=colors_nuc[i % len(colors_nuc)])
        ax.text(-0.05, 1 - i * 0.2 - 0.05, nid, fontsize=9, fontweight="bold",
                transform=ax.transAxes)
ax.set_title("C. Top co-occurring words per nucleus")
ax.axis("off")
panel_label(ax, "C")

# Panel D: Rising/falling centrality words
ax = axes[1, 1]
# Bar chart of biggest movers
movers = rising[:6] + falling[:6]
words_m = [w for w, _ in movers]
deltas_m = [d for _, d in movers]
colors_m = [WONG["green"] if d > 0 else WONG["red"] for d in deltas_m]
y_pos = np.arange(len(words_m))
ax.barh(y_pos, deltas_m, color=colors_m, alpha=0.8)
ax.set_yticks(y_pos)
ax.set_yticklabels(words_m, fontsize=8)
ax.axvline(0, color="grey", linewidth=0.5)
ax.set_xlabel("Change in degree centrality (1990s → 2020s)")
ax.set_title("D. Rising and falling concepts")
ax.invert_yaxis()
despine(ax)
panel_label(ax, "D")

plt.tight_layout(rect=[0, 0, 1, 0.96])
save_fig(fig, PUB / "fig_cooccurrence_temporal.png")
print("  Saved fig_cooccurrence_temporal.png")

print("\nDone.")
