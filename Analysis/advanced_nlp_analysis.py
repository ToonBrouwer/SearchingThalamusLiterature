"""
advanced_nlp_analysis.py — Advanced NLP analyses for thalamus framing paper.

Four new analysis modules:
  1. Dependency Parsing + Relation Extraction — SVO triples, agency measurement
  2. Embedding-based Change Detection — semantic drift over decades
  3. Dynamic Topic Modeling — topic content evolution over time
  4. Co-occurrence Network Analysis — concept co-occurrence graphs

GPU-accelerated where possible (embeddings, batched spaCy).
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
from collections import Counter, defaultdict

from plotting import apply_style, save_fig, panel_label, despine, WONG
from nucleus_patterns import NUCLEI, tag_nuclei
from taxonomy import FRAMES_CLEAN, detect_frame, FRAME_NAMES

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
df = pd.read_csv(PROC / "all_sentences_final.csv")
df["sentence"] = df["sentence"].fillna("")
df["frame_type"] = df["frame_type"].fillna("")
df["year"] = pd.to_numeric(df["year"], errors="coerce")
print(f"  {len(df):,} sentences, {df['pmid'].nunique():,} papers")

# Framing subset
framing = df[df["frame_type"] != ""].copy()
print(f"  {len(framing):,} framing sentences")

# Load embeddings
# Try full corpus embeddings first, fall back to old
for _ep in ["embeddings_full_corpus.npy", "embeddings_final.npy", "all_sentences_embeddings.npy"]:
    emb_path = PROC / _ep
    if emb_path.exists():
        break
embeddings = np.load(str(emb_path)) if emb_path.exists() else None
if embeddings is not None:
    print(f"  Embeddings: {embeddings.shape}")


# ═══════════════════════════════════════════════════════════════════════════
# 1. DEPENDENCY PARSING + RELATION EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. DEPENDENCY PARSING — FUNCTIONAL ROLE ANALYSIS")
print("=" * 70)

import spacy
try:
    nlp = spacy.load("en_core_web_md", disable=["ner"])
except:
    nlp = spacy.load("en_core_web_sm", disable=["ner"])
print(f"  spaCy model: {nlp.meta['name']}")

# Extract SVO triples where thalamus (or a nucleus) is subject
THAL_TERMS = {"thalamus", "thalamic", "thalamocortical", "thalamocortial"}
for nid, info in NUCLEI.items():
    label = info["label"].split("(")[0].strip().lower()
    THAL_TERMS.add(label)
    THAL_TERMS.add(nid.lower())

def extract_svo(doc):
    """Extract Subject-Verb-Object triples from a spaCy Doc."""
    triples = []
    for token in doc:
        # Find verbs
        if token.pos_ == "VERB":
            verb = token.lemma_.lower()

            # Find subject
            subj = None
            for child in token.children:
                if child.dep_ in ("nsubj", "nsubjpass"):
                    # Get the full noun phrase
                    subj_phrase = " ".join([t.text for t in child.subtree
                                            if t.dep_ not in ("punct", "cc")])
                    subj = subj_phrase.strip()
                    break

            # Find object
            obj = None
            for child in token.children:
                if child.dep_ in ("dobj", "attr", "oprd", "pobj"):
                    obj_phrase = " ".join([t.text for t in child.subtree
                                           if t.dep_ not in ("punct", "cc")])
                    obj = obj_phrase.strip()
                    break
                # Also check prepositional objects
                if child.dep_ == "prep":
                    for grandchild in child.children:
                        if grandchild.dep_ == "pobj":
                            obj_phrase = " ".join([t.text for t in grandchild.subtree
                                                    if t.dep_ not in ("punct",)])
                            obj = child.text + " " + obj_phrase.strip()
                            break

            if subj and obj:
                # Check if thalamus is the subject
                is_thal_subj = any(t in subj.lower() for t in THAL_TERMS)
                triples.append({
                    "subject": subj,
                    "verb": verb,
                    "object": obj,
                    "is_thal_subject": is_thal_subj,
                    "full_triple": f"{subj} → {verb} → {obj}",
                })
    return triples


# Process sentences in batches
print("  Parsing sentences with spaCy (batched)...")
t0 = time.time()

# Focus on framing sentences + a sample of non-framing
sample_size = min(10000, len(df))
sample_idx = np.concatenate([
    framing.index.values,
    np.random.RandomState(42).choice(
        df[df["frame_type"] == ""].index.values,
        size=min(sample_size - len(framing), len(df) - len(framing)),
        replace=False,
    )
])
sample = df.loc[sample_idx].copy()

all_triples = []
batch_size = 500
sentences = sample["sentence"].tolist()
years = sample["year"].tolist()
pmids = sample["pmid"].tolist()
frames = sample["frame_type"].tolist()

for batch_start in range(0, len(sentences), batch_size):
    batch = sentences[batch_start:batch_start + batch_size]
    docs = list(nlp.pipe(batch, batch_size=batch_size))

    for i, doc in enumerate(docs):
        idx = batch_start + i
        triples = extract_svo(doc)
        for t in triples:
            t["year"] = years[idx]
            t["pmid"] = pmids[idx]
            t["frame_type"] = frames[idx]
        all_triples.extend(triples)

    if (batch_start + batch_size) % 2000 < batch_size:
        print(f"    Processed {batch_start + batch_size}/{len(sentences)}, "
              f"{len(all_triples):,} triples so far")

triples_df = pd.DataFrame(all_triples)
thal_triples = triples_df[triples_df["is_thal_subject"]].copy()
print(f"  Total triples: {len(triples_df):,}")
print(f"  Thalamus-as-subject triples: {len(thal_triples):,}")
print(f"  Parsing time: {time.time()-t0:.1f}s")

# Agency analysis: what verbs are used with thalamus as subject?
print(f"\n  Top 20 verbs where thalamus is SUBJECT (= agency):")
verb_counts = thal_triples["verb"].value_counts()
for v, c in verb_counts.head(20).items():
    print(f"    {v:25s} {c:4d}")

# Verb categories for agency measurement
ACTIVE_VERBS = {"modulate", "regulate", "gate", "control", "integrate", "coordinate",
                "generate", "amplify", "filter", "process", "shape", "drive", "mediate",
                "transform", "compute", "select", "suppress", "enhance", "inhibit",
                "facilitate", "influence", "orchestrate"}
PASSIVE_VERBS = {"relay", "transmit", "convey", "forward", "pass", "transfer",
                 "receive", "carry", "conduct"}

thal_triples["agency"] = thal_triples["verb"].apply(
    lambda v: "active" if v in ACTIVE_VERBS else ("passive" if v in PASSIVE_VERBS else "neutral")
)

# Agency over time
thal_triples["decade"] = (thal_triples["year"] // 10 * 10).astype("Int64")
agency_by_decade = thal_triples.groupby(["decade", "agency"]).size().unstack(fill_value=0)
agency_pct = (agency_by_decade.T / agency_by_decade.sum(axis=1) * 100).T

print(f"\n  Agency over decades (% of thalamus-subject verbs):")
print(agency_pct.to_string())

# Save
triples_df.to_csv(PROC / "svo_triples.csv", index=False)
thal_triples.to_csv(PROC / "thalamus_agency_triples.csv", index=False)
print(f"  Saved svo_triples.csv ({len(triples_df):,}) and thalamus_agency_triples.csv ({len(thal_triples):,})")


# ═══════════════════════════════════════════════════════════════════════════
# 2. EMBEDDING-BASED CHANGE DETECTION
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. EMBEDDING-BASED SEMANTIC DRIFT")
print("=" * 70)

if embeddings is not None:
    # Compute centroid embedding per year
    df_with_year = df[df["year"].notna() & (df["year"] >= 1980) & (df["year"] <= 2025)].copy()
    df_with_year["year_int"] = df_with_year["year"].astype(int)

    years_range = range(1980, 2026)
    centroids = {}
    for yr in years_range:
        mask = df_with_year["year_int"] == yr
        idx = df_with_year[mask].index.values
        if len(idx) >= 10:
            c = embeddings[idx].mean(axis=0)
            c /= np.linalg.norm(c)
            centroids[yr] = c

    # Compute drift: cosine distance between consecutive year centroids
    drift_years = sorted(centroids.keys())
    drift_vals = []
    for i in range(1, len(drift_years)):
        y1, y2 = drift_years[i-1], drift_years[i]
        sim = np.dot(centroids[y1], centroids[y2])
        drift_vals.append({"year": y2, "cosine_similarity": sim, "drift": 1 - sim})

    drift_df = pd.DataFrame(drift_vals)
    print(f"  Computed semantic drift for {len(drift_df)} year pairs")
    print(f"  Mean drift: {drift_df['drift'].mean():.4f}")
    print(f"  Max drift year: {drift_df.loc[drift_df['drift'].idxmax(), 'year']:.0f} "
          f"(drift={drift_df['drift'].max():.4f})")

    # Also: drift of FRAMING sentences specifically
    framing_centroids = {}
    for yr in years_range:
        mask = (df_with_year["year_int"] == yr) & (df_with_year["frame_type"] != "")
        idx = df_with_year[mask].index.values
        if len(idx) >= 5:
            c = embeddings[idx].mean(axis=0)
            c /= np.linalg.norm(c)
            framing_centroids[yr] = c

    framing_drift = []
    f_years = sorted(framing_centroids.keys())
    for i in range(1, len(f_years)):
        y1, y2 = f_years[i-1], f_years[i]
        sim = np.dot(framing_centroids[y1], framing_centroids[y2])
        framing_drift.append({"year": y2, "drift": 1 - sim})

    framing_drift_df = pd.DataFrame(framing_drift)

    # Per-frame centroid drift
    print(f"\n  Per-frame semantic drift (distance from 1990s centroid to 2020s):")
    for fname in ["relay", "hub", "gate", "clinical_target", "modulator", "pacemaker"]:
        early = df_with_year[(df_with_year["frame_type"] == fname) &
                              (df_with_year["year_int"].between(1990, 2005))]
        late = df_with_year[(df_with_year["frame_type"] == fname) &
                             (df_with_year["year_int"].between(2015, 2025))]
        if len(early) >= 5 and len(late) >= 5:
            c_early = embeddings[early.index.values].mean(axis=0)
            c_early /= np.linalg.norm(c_early)
            c_late = embeddings[late.index.values].mean(axis=0)
            c_late /= np.linalg.norm(c_late)
            drift = 1 - np.dot(c_early, c_late)
            print(f"    {fname:20s} drift={drift:.4f} (n_early={len(early)}, n_late={len(late)})")

    drift_df.to_csv(PROC / "semantic_drift.csv", index=False)
    print(f"  Saved semantic_drift.csv")


# ═══════════════════════════════════════════════════════════════════════════
# 3. DYNAMIC TOPIC MODELING (topic content evolution)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. DYNAMIC TOPIC MODELING")
print("=" * 70)

# Approach: for each mega-theme, track how the TOP WORDS change per decade
# Using the actual sentences (not BERTopic's c-TF-IDF, but our own TF-IDF per decade)

from sklearn.feature_extraction.text import TfidfVectorizer

df_topics = df[
    (df["mega_theme"].notna()) &
    (df["mega_theme"] != "Other") &
    (df["mega_theme"] != "unassigned") &
    (df["year"].notna()) &
    (df["year"] >= 1990)
].copy()
df_topics["decade"] = (df_topics["year"] // 10 * 10).astype(int)

mega_themes = df_topics["mega_theme"].unique()
dynamic_topics = {}

for theme in sorted(mega_themes):
    theme_sents = df_topics[df_topics["mega_theme"] == theme]
    decades = sorted(theme_sents["decade"].unique())

    theme_evolution = {}
    for dec in decades:
        dec_sents = theme_sents[theme_sents["decade"] == dec]["sentence"].tolist()
        if len(dec_sents) < 20:
            continue

        # TF-IDF to find characteristic words of this decade
        tfidf = TfidfVectorizer(max_features=500, stop_words="english",
                                 min_df=3, max_df=0.8, ngram_range=(1, 2))
        try:
            X = tfidf.fit_transform(dec_sents)
            # Top words by mean TF-IDF
            mean_tfidf = X.mean(axis=0).A1
            top_idx = mean_tfidf.argsort()[-10:][::-1]
            top_words = [tfidf.get_feature_names_out()[i] for i in top_idx]
            theme_evolution[dec] = {
                "n_sentences": len(dec_sents),
                "top_words": top_words,
                "n_papers": theme_sents[theme_sents["decade"] == dec]["pmid"].nunique(),
            }
        except:
            continue

    dynamic_topics[theme] = theme_evolution

# Print evolution
for theme in sorted(dynamic_topics.keys()):
    evo = dynamic_topics[theme]
    if len(evo) >= 2:
        print(f"\n  {theme}:")
        for dec, info in sorted(evo.items()):
            words = ", ".join(info["top_words"][:6])
            print(f"    {dec}s (n={info['n_sentences']:,}, {info['n_papers']:,} papers): {words}")

# Save — convert numpy int64 keys to plain int for JSON
import json

def _jsonify(obj):
    """Recursively convert numpy int keys to str for JSON serialization."""
    if isinstance(obj, dict):
        return {str(k): _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonify(i) for i in obj]
    return obj

with open(PROC / "dynamic_topics.json", "w") as f:
    json.dump(_jsonify(dynamic_topics), f, indent=2, default=str)
print(f"\n  Saved dynamic_topics.json")


# ═══════════════════════════════════════════════════════════════════════════
# 4. CO-OCCURRENCE NETWORK ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. CO-OCCURRENCE NETWORK ANALYSIS")
print("=" * 70)

import networkx as nx

# Build co-occurrence at PAPER level: which concepts appear in the same paper?
# Concepts: nuclei, frames, techniques, mega-themes

# Tag nuclei on the full dataset (may already be tagged)
nuc_cols = [f"has_{nid}" for nid in NUCLEI if f"has_{nid}" in df.columns]
if not nuc_cols:
    print("  Tagging nuclei...")
    df = tag_nuclei(df, text_col="sentence")
    nuc_cols = [f"has_{nid}" for nid in NUCLEI]

# Build paper-level concept vectors
print("  Building paper-level concept co-occurrence...")
papers = df.groupby("pmid").agg({
    "year": "first",
    "frame_type": lambda x: set(x[x != ""]),
    "mega_theme": lambda x: set(x.dropna()) - {"Other", "unassigned"},
    **{col: "max" for col in nuc_cols},
}).reset_index()

# Build edges: two concepts co-occur if they appear in the same paper
edges = Counter()
concept_counts = Counter()

for _, row in papers.iterrows():
    concepts = set()

    # Add frames
    for f in row["frame_type"]:
        if f:
            concepts.add(f"frame:{f}")

    # Add mega-themes
    for m in row["mega_theme"]:
        if m:
            concepts.add(f"theme:{m}")

    # Add nuclei
    for col in nuc_cols:
        if row[col]:
            nid = col.replace("has_", "")
            concepts.add(f"nucleus:{nid}")

    # Count co-occurrences
    concepts = sorted(concepts)
    for c in concepts:
        concept_counts[c] += 1
    for i in range(len(concepts)):
        for j in range(i + 1, len(concepts)):
            edges[(concepts[i], concepts[j])] += 1

# Build networkx graph
G = nx.Graph()
for concept, count in concept_counts.items():
    if count >= 10:  # min 10 papers
        ctype = concept.split(":")[0]
        cname = concept.split(":")[1]
        G.add_node(concept, label=cname, type=ctype, count=count)

for (c1, c2), weight in edges.items():
    if c1 in G.nodes and c2 in G.nodes and weight >= 5:
        G.add_edge(c1, c2, weight=weight)

print(f"  Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

# Community detection
from networkx.algorithms.community import greedy_modularity_communities
communities = list(greedy_modularity_communities(G, weight="weight"))
print(f"  Communities detected: {len(communities)}")
for i, comm in enumerate(communities[:5]):
    members = sorted(comm, key=lambda n: -G.nodes[n].get("count", 0))[:8]
    labels = [G.nodes[m]["label"] for m in members]
    print(f"    Community {i+1} ({len(comm)} members): {', '.join(labels)}")

# Node centrality
degree_cent = nx.degree_centrality(G)
betweenness = nx.betweenness_centrality(G, weight="weight")
print(f"\n  Top 10 nodes by betweenness centrality (bridge concepts):")
for node, cent in sorted(betweenness.items(), key=lambda x: -x[1])[:10]:
    print(f"    {G.nodes[node]['label']:25s} ({G.nodes[node]['type']:8s}) "
          f"betweenness={cent:.3f}, degree={degree_cent[node]:.3f}")

# Save graph
nx.write_graphml(G, str(PROC / "concept_cooccurrence_network.graphml"))
print(f"\n  Saved concept_cooccurrence_network.graphml")

# Also save node/edge data as CSVs for easy plotting
node_data = []
for node in G.nodes:
    node_data.append({
        "id": node,
        "label": G.nodes[node]["label"],
        "type": G.nodes[node]["type"],
        "count": G.nodes[node]["count"],
        "betweenness": betweenness.get(node, 0),
        "degree": G.degree(node),
    })
pd.DataFrame(node_data).to_csv(PROC / "network_nodes.csv", index=False)

edge_data = [{"source": u, "target": v, "weight": d["weight"]}
             for u, v, d in G.edges(data=True)]
pd.DataFrame(edge_data).to_csv(PROC / "network_edges.csv", index=False)


# ═══════════════════════════════════════════════════════════════════════════
# 5. FIGURE: COMBINED ADVANCED NLP FIGURE
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. GENERATING FIGURE")
print("=" * 70)

fig, axes = plt.subplots(2, 3, figsize=(18, 12))
fig.subplots_adjust(hspace=0.35, wspace=0.30)
fig.suptitle("Advanced NLP Analysis", fontsize=14, fontweight="bold", y=0.98)

# ── Panel A: Top verbs (agency) ──
ax = axes[0, 0]
top_verbs = verb_counts.head(15)
colors = []
for v in top_verbs.index:
    if v in ACTIVE_VERBS:
        colors.append(WONG["blue"])
    elif v in PASSIVE_VERBS:
        colors.append(WONG["red"])
    else:
        colors.append(WONG["grey"])
ax.barh(range(len(top_verbs)), top_verbs.values, color=colors)
ax.set_yticks(range(len(top_verbs)))
ax.set_yticklabels(top_verbs.index, fontsize=8)
ax.invert_yaxis()
ax.set_xlabel("Count", fontsize=10)
ax.set_title(f"Verbs with thalamus as subject\n(n={len(thal_triples):,} triples)", fontsize=11, fontweight="bold")
# Legend
ax.legend(handles=[
    mpatches.Patch(color=WONG["blue"], label="Active (agency)"),
    mpatches.Patch(color=WONG["red"], label="Passive (relay)"),
    mpatches.Patch(color=WONG["grey"], label="Neutral"),
], fontsize=7, loc="lower right")
despine(ax)
panel_label(ax, "A")

# ── Panel B: Agency over time ──
ax = axes[0, 1]
if len(agency_pct) > 0:
    decades = agency_pct.index.values
    for col, color, label in [
        ("active", WONG["blue"], "Active verbs"),
        ("passive", WONG["red"], "Passive verbs"),
    ]:
        if col in agency_pct.columns:
            ax.plot(decades, agency_pct[col].values, "o-", color=color, label=label, linewidth=2)
    ax.set_xlabel("Decade", fontsize=10)
    ax.set_ylabel("% of thalamus-subject verbs", fontsize=10)
    ax.set_title("Agency in thalamus descriptions", fontsize=11, fontweight="bold")
    ax.legend(fontsize=8)
    despine(ax)
panel_label(ax, "B")

# ── Panel C: Semantic drift ──
ax = axes[0, 2]
if embeddings is not None and len(drift_df) > 0:
    ax.plot(drift_df["year"], drift_df["drift"] * 1000, "-", color=WONG["blue"],
            linewidth=1.5, alpha=0.5, label="All sentences")
    # Smooth with rolling mean
    drift_smooth = drift_df.set_index("year")["drift"].rolling(3, center=True).mean() * 1000
    ax.plot(drift_smooth.index, drift_smooth.values, "-", color=WONG["blue"],
            linewidth=2.5, label="3-year rolling mean")
    if len(framing_drift_df) > 0:
        f_smooth = framing_drift_df.set_index("year")["drift"].rolling(3, center=True).mean() * 1000
        ax.plot(f_smooth.index, f_smooth.values, "--", color=WONG["red"],
                linewidth=2, label="Framing sentences only")
    ax.set_xlabel("Year", fontsize=10)
    ax.set_ylabel("Semantic drift (x1000)", fontsize=10)
    ax.set_title("Embedding-based semantic change", fontsize=11, fontweight="bold")
    ax.legend(fontsize=7)
    despine(ax)
panel_label(ax, "C")

# ── Panel D: Network visualization ──
ax = axes[1, 0]
if G.number_of_nodes() > 0:
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42, weight="weight")
    type_colors = {"frame": WONG["blue"], "nucleus": WONG["orange"],
                   "theme": WONG["green"]}
    node_colors = [type_colors.get(G.nodes[n]["type"], WONG["grey"]) for n in G.nodes]
    node_sizes = [np.sqrt(G.nodes[n]["count"]) * 15 for n in G.nodes]

    nx.draw_networkx_edges(G, pos, alpha=0.15, width=0.5, ax=ax)
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=node_sizes,
                           alpha=0.8, ax=ax)
    # Label only high-degree nodes
    labels = {n: G.nodes[n]["label"] for n in G.nodes
              if G.degree(n) >= np.percentile([G.degree(m) for m in G.nodes], 75)}
    nx.draw_networkx_labels(G, pos, labels, font_size=6, ax=ax)

    ax.legend(handles=[
        mpatches.Patch(color=WONG["blue"], label="Frame"),
        mpatches.Patch(color=WONG["orange"], label="Nucleus"),
        mpatches.Patch(color=WONG["green"], label="Theme"),
    ], fontsize=7, loc="upper left")
    ax.set_title(f"Concept co-occurrence network\n({G.number_of_nodes()} nodes, "
                 f"{G.number_of_edges()} edges)", fontsize=11, fontweight="bold")
ax.axis("off")
panel_label(ax, "D")

# ── Panel E: Dynamic topic evolution (selected themes) ──
ax = axes[1, 1]
# Show word cloud evolution for one theme
theme_to_show = "Clinical / disease"
if theme_to_show in dynamic_topics:
    evo = dynamic_topics[theme_to_show]
    decades_sorted = sorted(evo.keys())
    y_positions = range(len(decades_sorted))
    for yi, dec in enumerate(decades_sorted):
        words = evo[dec]["top_words"][:5]
        n = evo[dec]["n_sentences"]
        word_str = ", ".join(words)
        ax.text(0.02, yi, f"{dec}s (n={n:,}): {word_str}", fontsize=7,
                va="center", transform=ax.transData)
    ax.set_ylim(-0.5, len(decades_sorted) - 0.5)
    ax.set_xlim(0, 1)
    ax.set_title(f"Topic evolution: {theme_to_show}", fontsize=11, fontweight="bold")
    ax.axis("off")
panel_label(ax, "E")

# ── Panel F: Top object phrases (what does thalamus act on?) ──
ax = axes[1, 2]
# Most common objects when thalamus is subject
obj_counts = thal_triples["object"].str.lower().value_counts()
# Clean and group similar objects
top_objects = obj_counts.head(15)
ax.barh(range(len(top_objects)), top_objects.values, color=WONG["cyan"])
ax.set_yticks(range(len(top_objects)))
ax.set_yticklabels(top_objects.index, fontsize=7)
ax.invert_yaxis()
ax.set_xlabel("Count", fontsize=10)
ax.set_title(f"What does the thalamus act on?\n(top objects in SVO triples)", fontsize=11, fontweight="bold")
despine(ax)
panel_label(ax, "F")

# Save
save_fig(fig, PUB / "fig_advanced_nlp.png", dpi=150)
print(f"  Saved fig_advanced_nlp.png")

print(f"\n{'='*70}")
print(f"ADVANCED NLP ANALYSIS COMPLETE")
print(f"  SVO triples: {len(triples_df):,} ({len(thal_triples):,} with thalamus as subject)")
print(f"  Semantic drift: {len(drift_df)} year pairs")
print(f"  Dynamic topics: {len(dynamic_topics)} themes tracked")
print(f"  Network: {G.number_of_nodes()} concepts, {G.number_of_edges()} edges")
print(f"{'='*70}")
