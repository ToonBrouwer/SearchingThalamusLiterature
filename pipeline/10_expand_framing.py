"""
10_expand_framing.py — Expand framing sentence extraction.

Strategy:
  1. Apply 21 frame regexes to ALL 17,280 raw abstracts (full text blocks)
  2. Apply to ALL 45,665 individual abstract sentences
  3. Combine with existing 1,609 full-text framing sentences
  4. Deduplicate → framing_combined_v2.csv
  5. Re-merge frame_type into all_sentences_final.csv
"""
import sys, io, os, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from pathlib import Path
from taxonomy import FRAMES_CLEAN, detect_frame, FRAME_NAMES

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW  = ROOT / "Data" / "raw" / "pubmed"
INT  = ROOT / "Data" / "interim"

# ═══════════════════════════════════════════════════════════════════════════
# 1. LOAD DATA SOURCES
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("1. LOADING DATA SOURCES")
print("=" * 70)

# Raw abstracts
abstracts_path = list(RAW.glob("pubmed_broad_title_only_*.csv"))
if abstracts_path:
    abs_raw = pd.read_csv(abstracts_path[0])
    abs_raw["abstract"] = abs_raw["abstract"].fillna("")
    print(f"  Raw abstracts: {len(abs_raw):,}")
else:
    print("  ERROR: No raw abstract file found"); sys.exit(1)

# Abstract sentences (if available)
abs_sent_path = INT / "thalamus_abstract_all_sentences.csv"
if abs_sent_path.exists():
    abs_sents = pd.read_csv(abs_sent_path)
    abs_sents["sentence"] = abs_sents["sentence"].fillna("")
    print(f"  Abstract sentences: {len(abs_sents):,}")
else:
    # Fall back: split abstracts into sentences ourselves
    import nltk
    try: nltk.data.find('tokenizers/punkt')
    except: nltk.download('punkt', quiet=True)
    from nltk.tokenize import sent_tokenize

    rows = []
    for _, row in abs_raw.iterrows():
        for s in sent_tokenize(str(row["abstract"])):
            if re.search(r"thalam", s, re.I) and len(s) > 20:
                rows.append({"pmid": row["pmid"], "year": row["year"], "sentence": s})
    abs_sents = pd.DataFrame(rows)
    print(f"  Abstract sentences (freshly tokenized): {len(abs_sents):,}")

# Existing full-text framing
ft_frame = pd.read_csv(PROC / "framing_clean.csv")
ft_frame["source"] = "fulltext"
print(f"  Existing full-text framing: {len(ft_frame):,}")

# ═══════════════════════════════════════════════════════════════════════════
# 2. DETECT FRAMES IN ABSTRACT SENTENCES
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. DETECTING FRAMES IN ABSTRACT SENTENCES")
print("=" * 70)

abs_sents["frame_type"] = abs_sents["sentence"].apply(detect_frame)
abs_framed = abs_sents[abs_sents["frame_type"] != ""].copy()
abs_framed["source"] = "abstract"
print(f"  Abstract sentences with frame: {len(abs_framed):,} out of {len(abs_sents):,}")

# ═══════════════════════════════════════════════════════════════════════════
# 3. DETECT FRAMES IN FULL ABSTRACTS (cross-sentence patterns)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. DETECTING FRAMES IN FULL ABSTRACT TEXT")
print("=" * 70)

extra_rows = []
for _, row in abs_raw.iterrows():
    abstract_text = str(row["abstract"])
    if len(abstract_text) < 50:
        continue
    if not re.search(r"thalam", abstract_text, re.I):
        continue

    for fname, pat in FRAMES_CLEAN.items():
        for m in pat.finditer(abstract_text):
            # Extract the containing sentence
            start = abstract_text.rfind(".", 0, m.start())
            start = start + 1 if start >= 0 else 0
            end = abstract_text.find(".", m.end())
            end = end + 1 if end >= 0 else len(abstract_text)
            sent = abstract_text[start:end].strip()

            if len(sent) > 20 and len(sent) < 600:
                extra_rows.append({
                    "pmid": row["pmid"],
                    "year": row["year"],
                    "sentence": sent,
                    "frame_type": fname,
                    "source": "abstract_fullblock",
                })

extra_df = pd.DataFrame(extra_rows) if extra_rows else pd.DataFrame(columns=["pmid","year","sentence","frame_type","source"])
print(f"  Extra sentences from full-abstract search: {len(extra_df):,}")

# ═══════════════════════════════════════════════════════════════════════════
# 4. COMBINE AND DEDUPLICATE
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. COMBINING AND DEDUPLICATING")
print("=" * 70)

# Align columns
common_cols = ["pmid", "year", "sentence", "frame_type", "source"]
for col in common_cols:
    if col not in ft_frame.columns:
        ft_frame[col] = ""
    if col not in abs_framed.columns:
        abs_framed[col] = ""
    if col not in extra_df.columns:
        extra_df[col] = ""

combined = pd.concat([
    ft_frame[common_cols],
    abs_framed[common_cols],
    extra_df[common_cols],
], ignore_index=True)

# Deduplicate by sentence text (keep first occurrence)
before = len(combined)
combined = combined.drop_duplicates(subset="sentence", keep="first")
after = len(combined)
print(f"  Before dedup: {before:,}")
print(f"  After dedup:  {after:,} (removed {before - after:,} duplicates)")

combined["year"] = pd.to_numeric(combined["year"], errors="coerce")

# Source breakdown
print(f"\n  Source breakdown:")
for src, cnt in combined["source"].value_counts().items():
    print(f"    {src:25s} {cnt:5d}")

# Frame distribution
print(f"\n  Frame distribution:")
for f, c in combined["frame_type"].value_counts().items():
    print(f"    {f:20s} {c:5d} ({c/len(combined)*100:4.1f}%)")

# Save
combined["frame_all"] = combined["sentence"].apply(
    lambda s: ",".join([fn for fn, p in FRAMES_CLEAN.items() if p.search(str(s))])
)
combined.to_csv(PROC / "framing_combined_v2.csv", index=False)
print(f"\n  Saved framing_combined_v2.csv ({len(combined):,} rows)")

# ═══════════════════════════════════════════════════════════════════════════
# 5. RE-MERGE FRAME_TYPE INTO ALL_SENTENCES_FINAL
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. RE-MERGING FRAME_TYPE INTO ALL_SENTENCES_FINAL")
print("=" * 70)

df = pd.read_csv(PROC / "all_sentences_final.csv")
df["frame_type"] = df["sentence"].fillna("").apply(detect_frame)
n_framed = (df["frame_type"] != "").sum()
print(f"  Sentences with frame: {n_framed:,} out of {len(df):,}")
df.to_csv(PROC / "all_sentences_final.csv", index=False)
print(f"  Saved all_sentences_final.csv")

# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════
old_n = 2536  # previous framing_combined.csv
new_n = len(combined)
print(f"\n{'='*70}")
print(f"SUMMARY: Framing expanded from {old_n:,} to {new_n:,} sentences ({(new_n-old_n)/old_n*100:+.0f}%)")
print(f"{'='*70}")
