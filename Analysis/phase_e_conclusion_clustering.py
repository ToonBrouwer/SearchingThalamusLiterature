"""
Phase E — Conclusion clustering and framing–conclusion mapping.

Extracts Discussion/Conclusion sentences from the 3,319 PMC full-text
articles, embeds them with sentence-transformers, clusters them with
HDBSCAN, and maps each paper's dominant framing type to its conclusion
cluster(s).

Pipeline:
  1. Extract conclusion-section sentences (section-header splitting with
     position-based fallback).
  2. Embed with all-MiniLM-L6-v2.
  3. Reduce to 2D with UMAP (stored for visualisation).
  4. Cluster with HDBSCAN.
  5. For every paper that has both framing sentences AND conclusion
     sentences, record the (framing_type, conclusion_cluster) pair.
  6. Save cluster examples (5 per cluster, closest to centroid).

Inputs (Data/interim/fulltext/):
    pmc_fulltext_2026-03-26.csv        — full_text column, 3,319 ok rows

Inputs (Data/processed/):
    framing_embedded_clustered.csv     — cluster_id, pmid, framing_primary

Outputs (Data/processed/):
    conclusion_sentences_clustered.csv — one row per conclusion sentence with
                                         sid, pmid, year, sentence, cluster_id,
                                         umap_x, umap_y, framing_broad
    conclusion_cluster_examples.csv    — 5 representative sentences per cluster
    conclusion_framing_matrix.csv      — n papers with each (framing, concl_cluster) pair
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import re
import numpy as np
import pandas as pd
from pathlib import Path
from sentence_transformers import SentenceTransformer
import umap
import hdbscan

# ── Paths ─────────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT    = _ANALYSIS_DIR.parents[1]
INTERIM = ROOT / "Data" / "interim"
PROC    = ROOT / "Data" / "processed"

# ── Constants ─────────────────────────────────────────────────────────────────
RELAY_IDS = {55, 56, 46}
HUB_IDS   = {53, 54}

# Regex to detect the start of a Discussion / Conclusion section in full text
SECTION_PAT = re.compile(
    r"(?:^|\n)\s*(?:\d+[\.\)]\s*)?(?:DISCUSSION|CONCLUSIONS?|SUMMARY|"
    r"GENERAL\s+DISCUSSION|CONCLUDING\s+REMARKS?|FINAL\s+REMARKS?|"
    r"Discussion|Conclusion|Conclusions|Summary)\s*\n",
    re.MULTILINE,
)

# Sentence splitter (same as framing extraction)
def split_sentences(text):
    if not isinstance(text, str) or not text.strip():
        return []
    text = re.sub(r"\s+", " ", text).strip()
    return re.split(r"(?<=[.!?])\s+", text)


# ── Load data ─────────────────────────────────────────────────────────────────
print("Loading full-text corpus …")
ft_raw = pd.read_csv(
    INTERIM / "fulltext" / "pmc_fulltext_2026-03-26.csv",
    dtype={"pmid": str, "pmcid": str},
)
ft_raw = ft_raw[ft_raw["xml_status"] == "ok"].copy()
ft_raw["year"] = pd.to_numeric(ft_raw["year"], errors="coerce")
print(f"  Full-text papers: {len(ft_raw):,}")

print("Loading framing clusters …")
fr = pd.read_csv(PROC / "framing_embedded_clustered.csv", dtype={"pmid": str})
fr = fr[fr["cluster_id"] != -1].copy()

def broad_frame(cid):
    if cid in RELAY_IDS: return "relay"
    if cid in HUB_IDS:   return "hub"
    return "other"

fr["broad"] = fr["cluster_id"].apply(broad_frame)

# Dominant framing per paper (most common framing sentence type)
paper_frame = (
    fr[fr["broad"] != "other"]
    .groupby(["pmid", "broad"]).size()
    .reset_index(name="n")
    .sort_values("n", ascending=False)
    .drop_duplicates("pmid", keep="first")
    .set_index("pmid")["broad"]
)
print(f"  Papers with relay framing: {(paper_frame=='relay').sum()}")
print(f"  Papers with hub framing  : {(paper_frame=='hub').sum()}")


# ── Extract conclusion/discussion sentences ───────────────────────────────────
def extract_conclusion_text(full_text):
    """
    Try to isolate the Discussion/Conclusion section of *full_text*.
    Strategy:
      1. Find the last Discussion/Conclusion section header.
      2. Return text from that point to end (capped at 6,000 chars to avoid
         references lists).
    Fallback: if no section header found, return the last 30% of the text.
    """
    if not isinstance(full_text, str):
        return ""

    matches = list(SECTION_PAT.finditer(full_text))
    if matches:
        start = matches[-1].start()
        return full_text[start: start + 6000]
    else:
        # Fallback: last 30% of text
        cutoff = int(len(full_text) * 0.70)
        return full_text[cutoff: cutoff + 6000]


print("\nExtracting conclusion sentences …")
# Focus on papers that have relay or hub framing — these are the ones
# where the framing→conclusion link is testable.  Also include a random
# sample of 200 "other" papers for comparison.
relay_hub_pmids = set(paper_frame.index)
other_pmids     = set(
    ft_raw[~ft_raw["pmid"].isin(relay_hub_pmids)]["pmid"].sample(
        min(200, len(ft_raw)), random_state=42
    )
)
target_pmids = relay_hub_pmids | other_pmids

MAX_SENTS_PER_PAPER = 8    # cap to keep embedding fast

rows = []
for _, row in ft_raw[ft_raw["pmid"].isin(target_pmids)].iterrows():
    pmid  = row["pmid"]
    year  = row["year"]
    concl = extract_conclusion_text(row.get("full_text"))
    sents = [s for s in split_sentences(concl) if len(s.split()) >= 6]
    for i, s in enumerate(sents[:MAX_SENTS_PER_PAPER]):
        rows.append({
            "sid":      f"{pmid}_concl_{i}",
            "pmid":     pmid,
            "year":     year,
            "sentence": s,
        })

concl_df = pd.DataFrame(rows)
print(f"  Target papers             : {len(target_pmids):,}")
print(f"  Conclusion sentences      : {len(concl_df):,} from {concl_df['pmid'].nunique():,} papers")

# Attach broad framing label to each conclusion sentence (via paper)
concl_df["framing_broad"] = concl_df["pmid"].map(paper_frame).fillna("other")


# ── Embedding ─────────────────────────────────────────────────────────────────
print("\nEmbedding conclusion sentences (all-MiniLM-L6-v2) …")
model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = model.encode(
    concl_df["sentence"].tolist(),
    batch_size=256,
    show_progress_bar=True,
    convert_to_numpy=True,
)
print(f"  Embedding shape: {embeddings.shape}")


# ── UMAP dimensionality reduction ─────────────────────────────────────────────
print("\nRunning UMAP …")
reducer = umap.UMAP(
    n_components=2,
    n_neighbors=20,
    min_dist=0.05,
    metric="cosine",
    random_state=42,
    verbose=False,
)
umap_2d = reducer.fit_transform(embeddings)
concl_df["umap_x"] = umap_2d[:, 0]
concl_df["umap_y"] = umap_2d[:, 1]


# ── HDBSCAN clustering ────────────────────────────────────────────────────────
print("Running HDBSCAN …")
clusterer = hdbscan.HDBSCAN(
    min_cluster_size=20,
    min_samples=3,
    metric="euclidean",   # on UMAP 2D
    cluster_selection_epsilon=0.3,
)
concl_df["cluster_id"] = clusterer.fit_predict(umap_2d)
n_clusters = concl_df["cluster_id"].nunique() - (1 if -1 in concl_df["cluster_id"].values else 0)
noise_n    = (concl_df["cluster_id"] == -1).sum()
print(f"  Clusters found : {n_clusters}")
print(f"  Noise points   : {noise_n:,} ({noise_n/len(concl_df)*100:.1f}%)")


# ── Cluster examples (5 closest to centroid) ──────────────────────────────────
example_rows = []
for cid in sorted(concl_df["cluster_id"].unique()):
    sub = concl_df[concl_df["cluster_id"] == cid].copy()
    if cid == -1:
        sample = sub.sample(min(5, len(sub)), random_state=42)
    else:
        cx = sub["umap_x"].mean(); cy = sub["umap_y"].mean()
        sub["dist"] = np.hypot(sub["umap_x"] - cx, sub["umap_y"] - cy)
        sample = sub.nsmallest(5, "dist")
    for _, r in sample.iterrows():
        example_rows.append({
            "cluster_id":     cid,
            "cluster_size":   len(sub),
            "framing_mix":    sub["framing_broad"].value_counts().to_dict(),
            "pmid":           r["pmid"],
            "year":           r["year"],
            "sentence":       r["sentence"],
            "framing_broad":  r["framing_broad"],
        })

examples_df = pd.DataFrame(example_rows)


# ── Framing × conclusion cluster matrix ──────────────────────────────────────
# One row per paper: dominant framing + dominant conclusion cluster
paper_concl_cluster = (
    concl_df[concl_df["cluster_id"] != -1]
    .groupby(["pmid", "cluster_id"]).size()
    .reset_index(name="n_sentences")
    .sort_values("n_sentences", ascending=False)
    .drop_duplicates("pmid", keep="first")
    .rename(columns={"cluster_id": "conclusion_cluster"})
)
paper_concl_cluster["framing_broad"] = paper_concl_cluster["pmid"].map(paper_frame).fillna("other")

matrix = (
    paper_concl_cluster
    .groupby(["framing_broad", "conclusion_cluster"]).size()
    .unstack(fill_value=0)
)


# ── Save ──────────────────────────────────────────────────────────────────────
PROC.mkdir(parents=True, exist_ok=True)
concl_df.to_csv(PROC / "conclusion_sentences_clustered.csv", index=False)
examples_df.to_csv(PROC / "conclusion_cluster_examples.csv", index=False)
matrix.to_csv(PROC / "conclusion_framing_matrix.csv")

print("\n── Framing × conclusion cluster (top clusters) ───────────")
print(matrix.head(5).to_string())
print(f"\nSaved conclusion_sentences_clustered.csv  ({len(concl_df):,} rows)")
print(f"Saved conclusion_cluster_examples.csv     ({len(examples_df):,} rows)")
print(f"Saved conclusion_framing_matrix.csv")
