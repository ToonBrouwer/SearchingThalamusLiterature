"""
18_full_corpus_build.py — Build complete corpus from ALL sources:
  1. PMC fulltext sentences (3,354 papers with XML fulltext)
  2. Institutional download text (3,851 papers via UvA LibKey)
  3. OA PDF text extracts (267 papers)
  4. Abstract sentences (all 17,280 papers)

For each paper, prefer fulltext > institutional > OA PDF > abstract.
Only keep sentences that mention "thalam".
Apply frame detection, nucleus tagging, technique tagging.
Compute GPU embeddings.
"""
import sys, os, time
os.environ["PYTHONIOENCODING"] = "utf-8"
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import numpy as np
from nltk import sent_tokenize
import nltk
nltk.download("punkt_tab", quiet=True)

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"

from taxonomy import detect_frame, detect_all_frames, FRAMES_CLEAN
from nucleus_patterns import NUCLEI, tag_nuclei

# ═══════════════════════════════════════════════════════════════════════════
# LOAD ALL SOURCES
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("BUILDING COMPLETE CORPUS FROM ALL SOURCES")
print("=" * 70)

# 1. Raw PubMed data (abstracts)
pubmed = pd.read_csv(
    ROOT / "Data" / "raw" / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    low_memory=False
)
print(f"PubMed papers: {len(pubmed):,}")

# 2. PMC fulltext
pmc = pd.read_csv(INT / "fulltext" / "pmc_fulltext_2026-03-26.csv", low_memory=False)
pmc_ft = pmc[pmc["full_text"].notna()].set_index("pmid")
print(f"PMC fulltext papers: {len(pmc_ft):,}")

# 3. Institutional download text (UvA LibKey)
inst_dir = INT / "institutional_text"
inst_texts = {}
if inst_dir.exists():
    for f in inst_dir.glob("*.txt"):
        if f.stat().st_size > 100 and not f.name.startswith("unmatched"):
            try:
                pmid = int(f.stem)
                inst_texts[pmid] = f.read_text(encoding="utf-8", errors="replace")
            except (ValueError, UnicodeDecodeError):
                pass
print(f"Institutional text extracts: {len(inst_texts):,}")

# 4. OA PDF text extracts
txt_dir = INT / "scihub_text"
oa_texts = {}
if txt_dir.exists():
    for f in txt_dir.glob("*.txt"):
        if f.stat().st_size > 100:
            try:
                pmid = int(f.stem)
                oa_texts[pmid] = f.read_text(encoding="utf-8", errors="replace")
            except (ValueError, UnicodeDecodeError):
                pass
print(f"OA PDF text extracts: {len(oa_texts):,}")

# ═══════════════════════════════════════════════════════════════════════════
# SENTENCE TOKENIZATION
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TOKENIZING ALL PAPERS")
print("=" * 70)

MIN_LEN = 20
MAX_LEN = 600
all_rows = []
source_counts = {"fulltext": 0, "institutional": 0, "oa_pdf": 0, "abstract": 0}

for i, (_, row) in enumerate(pubmed.iterrows()):
    pmid = int(row["pmid"])
    year = row.get("year", None)

    # Choose best text source
    text = None
    source = None

    if pmid in pmc_ft.index:
        ft = pmc_ft.loc[pmid, "full_text"]
        if isinstance(ft, str) and len(ft) > 100:
            text = ft
            source = "fulltext"

    if text is None and pmid in inst_texts:
        text = inst_texts[pmid]
        source = "institutional"

    if text is None and pmid in oa_texts:
        text = oa_texts[pmid]
        source = "oa_pdf"

    if text is None:
        abstract = row.get("abstract")
        if isinstance(abstract, str) and len(abstract) > 20:
            text = abstract
            source = "abstract"

    if text is None:
        continue

    # Sentence tokenize
    sentences = sent_tokenize(text)

    # Filter: must mention thalamus, reasonable length
    for sent in sentences:
        sent = sent.strip()
        if len(sent) < MIN_LEN or len(sent) > MAX_LEN:
            continue
        if "thalam" not in sent.lower():
            continue

        all_rows.append({
            "pmid": pmid,
            "year": year,
            "sentence": sent,
            "source": source,
        })

    source_counts[source] = source_counts.get(source, 0) + 1

    if (i + 1) % 2000 == 0:
        print(f"  Processed {i+1:,}/{len(pubmed):,} papers, {len(all_rows):,} sentences so far")

df = pd.DataFrame(all_rows)
print(f"\nTokenization complete:")
print(f"  Total sentences: {len(df):,}")
print(f"  Unique papers: {df['pmid'].nunique():,}")
print(f"  Source breakdown: {source_counts}")

# ═══════════════════════════════════════════════════════════════════════════
# FRAME DETECTION + NUCLEUS TAGGING
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("DETECTING FRAMES AND TAGGING NUCLEI")
print("=" * 70)

print("  Detecting frames...")
df["frame_type"] = df["sentence"].apply(detect_frame)
n_framed = (df["frame_type"] != "").sum()
print(f"  Framing sentences: {n_framed:,} ({n_framed/len(df)*100:.1f}%)")

print("  Tagging nuclei...")
df = tag_nuclei(df, text_col="sentence")

# Frame group
frame_to_group = {}
for group_name, frames in [
    ("Metaphorical", list(FRAMES_CLEAN.keys())[:9]),
    ("Functional", list(FRAMES_CLEAN.keys())[9:18]),
    ("Descriptive", list(FRAMES_CLEAN.keys())[18:]),
]:
    for f in frames:
        frame_to_group[f] = group_name
df["frame_group"] = df["frame_type"].map(frame_to_group).fillna("")

# ═══════════════════════════════════════════════════════════════════════════
# GPU EMBEDDINGS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("COMPUTING EMBEDDINGS (GPU)")
print("=" * 70)

import torch
print(f"  CUDA available: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"  GPU: {torch.cuda.get_device_name(0)}")

from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-MiniLM-L6-v2")
if torch.cuda.is_available():
    model = model.to(torch.device("cuda"))

t0 = time.time()
embeddings = model.encode(
    df["sentence"].tolist(),
    batch_size=512,
    show_progress_bar=True,
    convert_to_numpy=True,
)
dt = time.time() - t0
print(f"  Embeddings: {embeddings.shape} in {dt:.1f}s")

# ═══════════════════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("SAVING")
print("=" * 70)

# Save sentences
out_path = PROC / "all_sentences_full_corpus.csv"
df.to_csv(out_path, index=False)
print(f"  Saved {out_path.name}: {len(df):,} rows")

# Save embeddings
emb_path = PROC / "embeddings_full_corpus.npy"
np.save(emb_path, embeddings)
print(f"  Saved {emb_path.name}: {embeddings.shape}")

# Save framing subset
framing = df[df["frame_type"] != ""].copy()
framing_path = PROC / "framing_full_corpus.csv"
framing.to_csv(framing_path, index=False)
print(f"  Saved {framing_path.name}: {len(framing):,} framing sentences")

# ═══════════════════════════════════════════════════════════════════════════
# COMPARISON WITH OLD
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("COMPARISON: OLD vs NEW CORPUS")
print("=" * 70)
old = pd.read_csv(PROC / "all_sentences_final.csv", low_memory=False)
print(f"  OLD: {len(old):,} sentences, {old['pmid'].nunique():,} papers")
print(f"  NEW: {len(df):,} sentences, {df['pmid'].nunique():,} papers")
print(f"  Gain: +{len(df)-len(old):,} sentences, +{df['pmid'].nunique()-old['pmid'].nunique():,} papers")

old_framed = (old["frame_type"].notna() & (old["frame_type"] != "")).sum()
print(f"  OLD framing: {old_framed:,}")
print(f"  NEW framing: {n_framed:,}")
print(f"  Framing gain: +{n_framed - old_framed:,}")
print("=" * 70)
