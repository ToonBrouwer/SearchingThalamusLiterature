"""
14_integrate_scihub_text.py — Integrate Sci-Hub full-text into the pipeline.

Takes text files from Data/interim/scihub_text/, runs sentence tokenization,
applies all detectors (frames, nuclei, techniques, tasks), and merges
into the existing data files.

GPU-accelerated embedding computation if available.
"""
import sys, io, os, re, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
from pathlib import Path

from taxonomy import FRAMES_CLEAN, detect_frame, FRAME_NAMES
from nucleus_patterns import NUCLEI, tag_nuclei
from technique_patterns import TECHNIQUES, tag_techniques

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
TEXT_DIR = ROOT / "Data" / "interim" / "scihub_text"
RAW = ROOT / "Data" / "raw" / "pubmed"

# ═══════════════════════════════════════════════════════════════════════════
# 1. LOAD EXISTING DATA
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("1. LOADING EXISTING DATA")
print("=" * 70)

# PubMed metadata (for year, journal, etc.)
abs_file = list(RAW.glob("pubmed_broad_title_only_*.csv"))[0]
meta = pd.read_csv(abs_file, usecols=["pmid", "year", "journal", "title"])
meta_dict = meta.set_index("pmid").to_dict("index")
print(f"  PubMed metadata: {len(meta):,} papers")

# Existing sentences
existing = pd.read_csv(PROC / "all_sentences_final.csv", usecols=["pmid"])
existing_pmids = set(existing["pmid"].unique())
print(f"  Existing full-text papers: {len(existing_pmids):,}")

# Count available Sci-Hub text files
text_files = list(TEXT_DIR.glob("*.txt"))
print(f"  Sci-Hub text files: {len(text_files):,}")

# Filter to NEW papers only
new_files = []
for tf in text_files:
    pmid = int(tf.stem)
    if pmid not in existing_pmids:
        new_files.append(tf)
print(f"  New papers (not in existing): {len(new_files):,}")

# ═══════════════════════════════════════════════════════════════════════════
# 2. TOKENIZE SENTENCES
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. TOKENIZING SENTENCES")
print("=" * 70)

try:
    import nltk
    nltk.data.find('tokenizers/punkt_tab')
except:
    try:
        import nltk
        nltk.download('punkt_tab', quiet=True)
    except:
        pass

from nltk.tokenize import sent_tokenize

rows = []
for i, tf in enumerate(new_files):
    pmid = int(tf.stem)
    text = tf.read_text(encoding="utf-8", errors="replace")

    # Get metadata
    info = meta_dict.get(pmid, {"year": None, "journal": "", "title": ""})

    # Split into sentences
    for sent in sent_tokenize(text):
        sent = sent.strip()
        # Filter: must mention thalamus, reasonable length
        if len(sent) < 20 or len(sent) > 600:
            continue
        if not re.search(r"thalam", sent, re.I):
            continue
        rows.append({
            "pmid": pmid,
            "year": info.get("year"),
            "sentence": sent,
            "source": "scihub",
        })

    if (i + 1) % 100 == 0:
        print(f"  Processed {i+1}/{len(new_files)} files, {len(rows):,} sentences so far")

new_sents = pd.DataFrame(rows)
print(f"  New thalamus-mentioning sentences: {len(new_sents):,} from {new_sents['pmid'].nunique():,} papers")

if len(new_sents) == 0:
    print("  No new sentences to integrate.")
    sys.exit(0)

# ═══════════════════════════════════════════════════════════════════════════
# 3. APPLY FRAME DETECTION
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. APPLYING FRAME DETECTION")
print("=" * 70)

new_sents["frame_type"] = new_sents["sentence"].apply(detect_frame)
n_framed = (new_sents["frame_type"] != "").sum()
print(f"  Framing sentences: {n_framed:,} ({n_framed/len(new_sents)*100:.1f}%)")

if n_framed > 0:
    print("  Frame distribution:")
    for f, c in new_sents["frame_type"].value_counts().head(10).items():
        if f:
            print(f"    {f:20s} {c:4d}")

# ═══════════════════════════════════════════════════════════════════════════
# 4. APPLY NUCLEUS & TECHNIQUE TAGGING
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. TAGGING NUCLEI AND TECHNIQUES")
print("=" * 70)

t0 = time.time()
new_sents = tag_nuclei(new_sents, text_col="sentence")
new_sents = tag_techniques(new_sents, text_col="sentence")
print(f"  Tagged in {time.time()-t0:.1f}s")

# Nucleus mention counts
nuc_counts = {nid: new_sents[f"has_{nid}"].sum() for nid in NUCLEI}
top_nucs = sorted(nuc_counts.items(), key=lambda x: -x[1])[:10]
print("  Top mentioned nuclei:")
for nid, cnt in top_nucs:
    if cnt > 0:
        print(f"    {NUCLEI[nid]['label']:30s} {cnt:4d}")

# ═══════════════════════════════════════════════════════════════════════════
# 5. COMPUTE EMBEDDINGS (GPU if available)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. COMPUTING EMBEDDINGS")
print("=" * 70)

try:
    from sentence_transformers import SentenceTransformer
    import torch

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"  Device: {device}")

    model = SentenceTransformer("all-MiniLM-L6-v2", device=device)
    t0 = time.time()
    embeddings = model.encode(
        new_sents["sentence"].tolist(),
        batch_size=256,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    print(f"  Computed {len(embeddings)} embeddings in {time.time()-t0:.1f}s")

    # Save embeddings
    emb_path = PROC / "scihub_sentences_embeddings.npy"
    np.save(str(emb_path), embeddings.astype(np.float32))
    print(f"  Saved: {emb_path}")

except ImportError:
    print("  sentence-transformers not available, skipping embeddings")
    embeddings = None

# ═══════════════════════════════════════════════════════════════════════════
# 6. SAVE AND MERGE
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("6. SAVING AND MERGING")
print("=" * 70)

# Save standalone Sci-Hub sentences
new_sents.to_csv(PROC / "scihub_sentences.csv", index=False)
print(f"  Saved scihub_sentences.csv ({len(new_sents):,} rows)")

# Merge framing sentences into framing_combined_v2
framing_new = new_sents[new_sents["frame_type"] != ""].copy()
framing_new["source"] = "scihub"

if len(framing_new) > 0:
    fc = pd.read_csv(PROC / "framing_combined_v2.csv")
    common_cols = [c for c in ["pmid", "year", "sentence", "frame_type", "source"] if c in fc.columns]
    for col in common_cols:
        if col not in framing_new.columns:
            framing_new[col] = ""

    combined = pd.concat([fc, framing_new[common_cols]], ignore_index=True)
    combined = combined.drop_duplicates(subset="sentence", keep="first")
    combined.to_csv(PROC / "framing_combined_v3.csv", index=False)
    print(f"  framing_combined_v3: {len(combined):,} rows "
          f"(was {len(fc):,}, added {len(combined)-len(fc):,})")

# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"DONE. Integrated {len(new_sents):,} sentences from "
      f"{new_sents['pmid'].nunique():,} new Sci-Hub papers.")
print(f"  Framing sentences: {n_framed:,}")
print(f"{'='*70}")
