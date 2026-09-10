"""
06_cluster_bertopic.py — Run BERTopic on two corpora:
  Model 1: All thalamus sentences (~60k+) for general topic structure
  Model 2: Framing sentences only (~10k) with semi-supervised labels

Strategy: reuse existing embeddings where possible, only embed new data.
"""
import sys, io, os, gc
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
from pathlib import Path

from embeddings import embed_texts, load_embeddings, save_embeddings
from clustering import (run_bertopic, run_bertopic_semisupervised,
                        get_topics_over_time, get_hierarchical_topics,
                        save_topic_model)

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
MODELS = ROOT / "Data" / "models"
MODELS.mkdir(parents=True, exist_ok=True)

# ════════════════════════════════════════════════════════════════════════════
# MODEL 1: All thalamus sentences
# ════════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("MODEL 1: All thalamus sentences — general topic structure")
print("=" * 70)

# Load PMC sentences
print("\nLoading PMC sentences...")
pmc = pd.read_csv(PROC / "all_sentences_classified.csv")
pmc["sentence"] = pmc["sentence"].fillna("")
pmc["year"] = pd.to_numeric(pmc["year"], errors="coerce")
pmc = pmc[pmc["sentence"].str.len() > 20].reset_index(drop=True)
print(f"  PMC sentences: {len(pmc):,}")

# Load Unpaywall cleaned sentences
upw_path = PROC / "unpaywall_clean.csv"
if not upw_path.exists():
    upw_path = PROC / "unpaywall_new_sentences.csv"
if upw_path.exists():
    print("Loading Unpaywall sentences...")
    upw = pd.read_csv(upw_path)
    upw["sentence"] = upw["sentence"].fillna("")
    upw["year"] = pd.to_numeric(upw["year"], errors="coerce")
    upw = upw[upw["sentence"].str.len() > 20]
    upw = upw[~upw["sentence"].isin(pmc["sentence"].values)].reset_index(drop=True)
    print(f"  Unpaywall new sentences: {len(upw):,}")
else:
    upw = pd.DataFrame()
    print("  No Unpaywall data.")

# ── Embeddings: reuse existing, embed only new ──
print("\nPreparing embeddings...")

# Check for pre-existing full embeddings cache
full_emb_cache = PROC / "all_sentences_embeddings.npy"
if full_emb_cache.exists():
    cached_emb = load_embeddings(str(full_emb_cache))
    total_expected = len(pmc) + (len(upw) if len(upw) > 0 else 0)
    if cached_emb.shape[0] == total_expected:
        print(f"  Using cached embeddings: {cached_emb.shape}")
        emb_1 = cached_emb
        all_sents = pd.concat([pmc[["pmid","year","sentence"]],
                                upw[["pmid","year","sentence"]] if len(upw) > 0 else pd.DataFrame()],
                               ignore_index=True)
        all_sents = all_sents[all_sents["sentence"].str.len() > 20].reset_index(drop=True)
    else:
        cached_emb = None  # shape mismatch, re-embed
else:
    cached_emb = None

if cached_emb is None:
    # Embed PMC sentences in batches
    print("  Embedding PMC sentences (batch)...")
    pmc_emb_cache = PROC / "pmc_sentences_embeddings.npy"
    pmc_docs = pmc["sentence"].tolist()
    emb_pmc = embed_texts(pmc_docs, cache_path=str(pmc_emb_cache), batch_size=512)
    print(f"    PMC embeddings: {emb_pmc.shape}")

    if len(upw) > 0:
        print("  Embedding Unpaywall sentences...")
        upw_emb_cache = PROC / "upw_sentences_embeddings.npy"
        upw_docs = upw["sentence"].tolist()
        emb_upw = embed_texts(upw_docs, cache_path=str(upw_emb_cache), batch_size=512)
        print(f"    Unpaywall embeddings: {emb_upw.shape}")
        emb_1 = np.vstack([emb_pmc, emb_upw])
        all_sents = pd.concat([pmc[["pmid","year","sentence"]],
                                upw[["pmid","year","sentence"]]],
                               ignore_index=True)
    else:
        emb_1 = emb_pmc
        all_sents = pmc[["pmid","year","sentence"]].copy()

    all_sents = all_sents[all_sents["sentence"].str.len() > 20].reset_index(drop=True)
    # Ensure alignment
    if emb_1.shape[0] != len(all_sents):
        print(f"  WARNING: embeddings ({emb_1.shape[0]}) != sentences ({len(all_sents)}), truncating")
        n = min(emb_1.shape[0], len(all_sents))
        emb_1 = emb_1[:n]
        all_sents = all_sents.iloc[:n].reset_index(drop=True)

    save_embeddings(emb_1, str(full_emb_cache))
    print(f"  Saved combined embeddings: {emb_1.shape}")

docs_1 = all_sents["sentence"].tolist()
print(f"\nTotal for Model 1: {len(docs_1):,} sentences, embeddings {emb_1.shape}")

# ── Run BERTopic ──
print("\nFitting BERTopic Model 1...")
gc.collect()
model_1, topics_1, probs_1 = run_bertopic(
    docs_1, emb_1, min_topic_size=50, nr_topics="auto"
)

n_topics_1 = len(set(topics_1)) - (1 if -1 in topics_1 else 0)
n_noise_1 = sum(1 for t in topics_1 if t == -1)
print(f"  Topics: {n_topics_1}, Noise: {n_noise_1:,} ({n_noise_1/len(topics_1)*100:.1f}%)")

# Save
all_sents["bertopic_id"] = topics_1
topic_info_1 = model_1.get_topic_info()
topic_map_1 = dict(zip(topic_info_1["Topic"], topic_info_1["Name"]))
all_sents["bertopic_label"] = all_sents["bertopic_id"].map(topic_map_1).fillna("noise")
all_sents.to_csv(PROC / "all_sentences_bertopic.csv", index=False)
print(f"  Saved all_sentences_bertopic.csv")

# Topics over time
print("  Topics over time...")
timestamps_1 = all_sents["year"].fillna(2000).astype(int).tolist()
try:
    tot_1 = get_topics_over_time(model_1, docs_1, timestamps_1, nr_bins=15)
    tot_1.to_csv(PROC / "topics_over_time_all.csv", index=False)
    print(f"  Saved topics_over_time_all.csv")
except Exception as e:
    print(f"  topics_over_time error: {e}")

# Hierarchical
print("  Hierarchical topics...")
try:
    hier_1 = get_hierarchical_topics(model_1)
    hier_1.to_csv(PROC / "hierarchical_topics_all.csv", index=False)
    print(f"  Saved hierarchical_topics_all.csv")
except Exception as e:
    print(f"  hierarchical_topics error: {e}")

# Save model
save_topic_model(model_1, str(MODELS / "bertopic_all_sentences"))
print("  Model 1 saved.")

# Top topics
print("\n── Model 1 Top 15 Topics ──")
for _, row in topic_info_1.head(16).iterrows():
    if row["Topic"] != -1:
        print(f"  Topic {row['Topic']:3d} (n={row['Count']:5d}): {row['Name']}")

# Free memory
del emb_1, model_1
gc.collect()

# ════════════════════════════════════════════════════════════════════════════
# MODEL 2: Framing sentences (semi-supervised)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("MODEL 2: Framing sentences — semi-supervised")
print("=" * 70)

framing = pd.read_csv(PROC / "framing_embedded_clustered.csv")
framing["sentence"] = framing["sentence"].fillna("")
framing["year"] = pd.to_numeric(framing["year"], errors="coerce")
print(f"  Framing sentences: {len(framing):,}")

# Load embeddings
framing_emb_path = PROC / "framing_embeddings.npy"
if framing_emb_path.exists():
    emb_2 = load_embeddings(str(framing_emb_path))
    if emb_2.shape[0] != len(framing):
        print(f"  Embedding shape mismatch, re-embedding...")
        docs_2 = framing["sentence"].tolist()
        emb_2 = embed_texts(docs_2, cache_path=str(PROC / "framing_embeddings_v2.npy"))
    else:
        docs_2 = framing["sentence"].tolist()
else:
    docs_2 = framing["sentence"].tolist()
    emb_2 = embed_texts(docs_2, cache_path=str(framing_emb_path))

print(f"  Embeddings: {emb_2.shape}")

# Semi-supervised labels: known framing_primary → int, "other" → -1
label_map = {}
idx = 0
for cat in framing["framing_primary"].dropna().unique():
    if cat != "other":
        label_map[cat] = idx
        idx += 1
label_map["other"] = -1

y_labels = framing["framing_primary"].map(label_map).fillna(-1).astype(int).tolist()
n_labeled = sum(1 for y in y_labels if y != -1)
print(f"  Labeled: {n_labeled:,}, Unlabeled (other): {len(y_labels) - n_labeled:,}")

print("\nFitting BERTopic Model 2 (semi-supervised)...")
model_2, topics_2, probs_2 = run_bertopic_semisupervised(
    docs_2, emb_2, y_labels, min_topic_size=20
)

n_topics_2 = len(set(topics_2)) - (1 if -1 in topics_2 else 0)
n_noise_2 = sum(1 for t in topics_2 if t == -1)
print(f"  Topics: {n_topics_2}, Noise: {n_noise_2:,} ({n_noise_2/len(topics_2)*100:.1f}%)")

framing["bertopic_id"] = topics_2
topic_info_2 = model_2.get_topic_info()
topic_map_2 = dict(zip(topic_info_2["Topic"], topic_info_2["Name"]))
framing["bertopic_label"] = framing["bertopic_id"].map(topic_map_2).fillna("noise")
framing.to_csv(PROC / "framing_bertopic.csv", index=False)
print(f"  Saved framing_bertopic.csv")

# Topics over time
timestamps_2 = framing["year"].fillna(2000).astype(int).tolist()
try:
    tot_2 = get_topics_over_time(model_2, docs_2, timestamps_2, nr_bins=15)
    tot_2.to_csv(PROC / "topics_over_time_framing.csv", index=False)
except Exception as e:
    print(f"  topics_over_time error: {e}")

# Hierarchical
try:
    hier_2 = get_hierarchical_topics(model_2)
    hier_2.to_csv(PROC / "hierarchical_topics_framing.csv", index=False)
except Exception as e:
    print(f"  hierarchical_topics error: {e}")

save_topic_model(model_2, str(MODELS / "bertopic_framing"))
print("  Model 2 saved.")

# ── Summary ──
print("\n── Model 2 Top 15 Topics ──")
for _, row in topic_info_2.head(16).iterrows():
    if row["Topic"] != -1:
        print(f"  Topic {row['Topic']:3d} (n={row['Count']:5d}): {row['Name']}")

# Check "other" resolution
if "framing_primary" in framing.columns:
    other = framing["framing_primary"] == "other"
    n_other = other.sum()
    other_in_topic = (framing.loc[other, "bertopic_id"] != -1).sum()
    print(f"\n── 'Other' Resolution ──")
    print(f"  Original 'other': {n_other:,}")
    print(f"  Now in BERTopic topics: {other_in_topic:,} ({other_in_topic/max(n_other,1)*100:.1f}%)")
    print(f"  Still noise: {n_other - other_in_topic:,}")

print("\n✓ Done.")
