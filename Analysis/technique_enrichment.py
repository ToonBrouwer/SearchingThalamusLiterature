"""
technique_enrichment.py — Comprehensive paper-level technique tagging.

Combines technique detection from:
  1. Fulltext sentences (all_sentences_final.csv)
  2. Abstracts (raw PubMed CSV)
  3. New subcategories + histology/clinical/molecular patterns

Saves enriched paper-level technique profile for temporal analysis.

Output:
  Data/processed/paper_techniques.csv (one row per paper, all technique booleans)
"""
import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from pathlib import Path

from technique_patterns import (TECHNIQUES, TECHNIQUE_SUBCATEGORIES,
                                 tag_techniques, tag_technique_subcategories)

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW  = ROOT / "Data" / "raw" / "pubmed"

# ═══════════════════════════════════════════════════════════════════════════
# 1. TAG SENTENCES
# ═══════════════════════════════════════════════════════════════════════════
print("Loading sentences...")
df = pd.read_csv(PROC / "all_sentences_final.csv",
                 usecols=["pmid", "year", "sentence", "source"], low_memory=False)
df["year"] = pd.to_numeric(df["year"], errors="coerce")
print(f"  {len(df):,} sentences")

print("Tagging techniques on sentences...")
tag_techniques(df)
tag_technique_subcategories(df)

all_tech_ids = list(TECHNIQUES.keys()) + list(TECHNIQUE_SUBCATEGORIES.keys())
tech_cols = [f"has_{t}" for t in all_tech_ids]

# Aggregate to paper level (any sentence mentions technique)
print("Aggregating to paper level (sentences)...")
paper_from_sents = df.groupby("pmid")[tech_cols].any()
paper_year = df.groupby("pmid")["year"].first()
print(f"  {len(paper_from_sents):,} papers from sentences")

# ═══════════════════════════════════════════════════════════════════════════
# 2. TAG ABSTRACTS
# ═══════════════════════════════════════════════════════════════════════════
print("\nLoading abstracts...")
raw = pd.read_csv(RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                   usecols=["pmid", "year", "abstract"], low_memory=False)
raw = raw[raw["abstract"].notna() & (raw["abstract"].str.len() > 50)].copy()
print(f"  {len(raw):,} abstracts")

print("Tagging techniques on abstracts...")
tag_techniques(raw, text_col="abstract")
tag_technique_subcategories(raw, text_col="abstract")

paper_from_abs = raw.set_index("pmid")[tech_cols]
abs_year = raw.set_index("pmid")["year"]
print(f"  {len(paper_from_abs):,} papers from abstracts")

# ═══════════════════════════════════════════════════════════════════════════
# 3. MERGE: UNION OF BOTH SOURCES
# ═══════════════════════════════════════════════════════════════════════════
print("\nMerging (union of sentence + abstract tags)...")

all_pmids = sorted(set(paper_from_sents.index) | set(paper_from_abs.index))
print(f"  Total unique papers: {len(all_pmids):,}")

# Reindex both to full PMID set
sent_reindexed = paper_from_sents.reindex(all_pmids, fill_value=False)
abs_reindexed = paper_from_abs.reindex(all_pmids, fill_value=False)

# Union: True if either source detected the technique
merged = sent_reindexed | abs_reindexed

# Add year
year_all = paper_year.reindex(all_pmids)
year_abs = abs_year.reindex(all_pmids)
merged["year"] = year_all.fillna(year_abs)
merged["pmid"] = merged.index

# ═══════════════════════════════════════════════════════════════════════════
# 4. REPORT COVERAGE
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TECHNIQUE COVERAGE COMPARISON")
print("=" * 70)

any_sent = paper_from_sents[tech_cols[:len(TECHNIQUES)]].any(axis=1).sum()
any_abs = paper_from_abs[tech_cols[:len(TECHNIQUES)]].any(axis=1).sum()
any_merged = merged[tech_cols[:len(TECHNIQUES)]].any(axis=1).sum()

print(f"\n  Papers with ANY technique (main 11 categories):")
print(f"    Sentences only:  {any_sent:>6,} / {len(paper_from_sents):,} "
      f"({any_sent/len(paper_from_sents)*100:.1f}%)")
print(f"    Abstracts only:  {any_abs:>6,} / {len(paper_from_abs):,} "
      f"({any_abs/len(paper_from_abs)*100:.1f}%)")
print(f"    Merged (union):  {any_merged:>6,} / {len(merged):,} "
      f"({any_merged/len(merged)*100:.1f}%)")

print(f"\n  Per technique (merged vs sentence-only):")
for tid in all_tech_ids:
    col = f"has_{tid}"
    n_sent = paper_from_sents[col].sum() if col in paper_from_sents else 0
    n_merged = merged[col].sum()
    gain = n_merged - n_sent
    gain_pct = gain / max(n_sent, 1) * 100
    if n_merged > 10:
        print(f"    {tid:25s}: {n_sent:>5,} → {n_merged:>5,} (+{gain:>4,}, +{gain_pct:.0f}%)")

# Coverage by decade
print(f"\n  Coverage by decade (merged, % with any technique):")
merged["decade"] = (merged["year"] // 10) * 10
for dec in [1980, 1990, 2000, 2010, 2020]:
    dec_m = merged[merged["decade"] == dec]
    any_t = dec_m[tech_cols[:len(TECHNIQUES)]].any(axis=1).mean() * 100
    print(f"    {dec}s: {len(dec_m):,} papers, {any_t:.1f}% have technique")

# ═══════════════════════════════════════════════════════════════════════════
# 5. SAVE
# ═══════════════════════════════════════════════════════════════════════════
out_path = PROC / "paper_techniques.csv"
merged.to_csv(out_path, index=False)
print(f"\nSaved: {out_path} ({len(merged):,} rows)")
print("Done.")
