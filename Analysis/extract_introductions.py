"""
extract_introductions.py — Extract introduction sections from full-text papers.

Extracts intros from PMC XML fulltext and institutional PDFs.
Falls back to first ~2000 chars if no clear intro section found.
Saves a stratified sample for inductive codebook development.

Output:
  Data/processed/introductions_all.csv (all extractable intros)
  Data/processed/introductions_sample_200.csv (stratified sample for codebook)
"""
import sys, io, os, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INT = ROOT / "Data" / "interim"
PROC = ROOT / "Data" / "processed"
RAW = ROOT / "Data" / "raw" / "pubmed"

# ═══════════════════════════════════════════════════════════════════════════
# PATTERNS FOR INTRO EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

INTRO_START = re.compile(
    r'(?:^|\n)\s*(?:1\.?\s*)?(?:Introduction|INTRODUCTION|Background|BACKGROUND)'
    r'\s*\n+',
    re.IGNORECASE
)

INTRO_END = re.compile(
    r'\n\s*(?:'
    r'2\.?\s*(?:Method|Material|Experimental)|'
    r'(?:Method|METHODS?|MATERIAL|Material|Experimental\s+Procedure|EXPERIMENTAL)|'
    r'(?:Result|RESULTS?)'
    r')\s*(?:\n|$)',
    re.IGNORECASE
)


def extract_intro(text):
    """Extract introduction section from full text.
    Returns (intro_text, method) where method is 'section' or 'first_2000'."""
    if not text or len(text) < 300:
        return None, None

    # Try section-based extraction
    m_start = INTRO_START.search(text)
    if m_start:
        after_header = text[m_start.end():]
        m_end = INTRO_END.search(after_header)
        if m_end:
            intro = after_header[:m_end.start()].strip()
            if len(intro) > 200:
                # Cap at 8000 chars (some intros are very long)
                return intro[:8000], "section"

    # Fallback: first ~2000 chars that mention thalamus
    first_part = text[:3000].strip()
    if "thalam" in first_part.lower() and len(first_part) > 300:
        return first_part[:2000], "first_2000"

    return None, None


# ═══════════════════════════════════════════════════════════════════════════
# EXTRACT FROM ALL SOURCES
# ═══════════════════════════════════════════════════════════════════════════
print("Extracting introductions from all full-text sources...")

# Load metadata for year/title
raw = pd.read_csv(RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                   usecols=["pmid", "year", "title", "journal"], low_memory=False)
meta = raw.set_index("pmid")

all_intros = []

# 1. PMC fulltext
print("\n  PMC fulltext...")
pmc = pd.read_csv(INT / "fulltext" / "pmc_fulltext_2026-03-26.csv",
                   usecols=["pmid", "full_text"], low_memory=False)
pmc_ft = pmc[pmc["full_text"].notna() & (pmc["full_text"].str.len() > 500)]

for _, row in pmc_ft.iterrows():
    intro, method = extract_intro(str(row["full_text"]))
    if intro:
        pmid = int(row["pmid"])
        m = meta.loc[pmid] if pmid in meta.index else {}
        all_intros.append({
            "pmid": pmid,
            "year": m.get("year", None) if isinstance(m, pd.Series) else None,
            "title": m.get("title", "") if isinstance(m, pd.Series) else "",
            "journal": m.get("journal", "") if isinstance(m, pd.Series) else "",
            "source": "pmc",
            "extraction_method": method,
            "intro_text": intro,
            "intro_length": len(intro),
        })

print(f"    Extracted: {len(all_intros):,} intros from PMC")

# 2. Institutional PDFs
print("  Institutional PDFs...")
inst_dir = INT / "institutional_text"
n_inst = 0
if inst_dir.exists():
    for f in inst_dir.glob("*.txt"):
        if f.stat().st_size < 500 or f.name.startswith("unmatched"):
            continue
        try:
            pmid = int(f.stem)
        except ValueError:
            continue

        text = f.read_text(encoding="utf-8", errors="replace")
        intro, method = extract_intro(text)
        if intro:
            m = meta.loc[pmid] if pmid in meta.index else {}
            all_intros.append({
                "pmid": pmid,
                "year": m.get("year", None) if isinstance(m, pd.Series) else None,
                "title": m.get("title", "") if isinstance(m, pd.Series) else "",
                "journal": m.get("journal", "") if isinstance(m, pd.Series) else "",
                "source": "institutional",
                "extraction_method": method,
                "intro_text": intro,
                "intro_length": len(intro),
            })
            n_inst += 1

print(f"    Extracted: {n_inst:,} intros from institutional PDFs")

# 3. OA PDFs
print("  OA PDFs...")
oa_dir = INT / "scihub_text"
n_oa = 0
if oa_dir.exists():
    for f in oa_dir.glob("*.txt"):
        if f.stat().st_size < 500:
            continue
        try:
            pmid = int(f.stem)
        except ValueError:
            continue

        text = f.read_text(encoding="utf-8", errors="replace")
        intro, method = extract_intro(text)
        if intro:
            m = meta.loc[pmid] if pmid in meta.index else {}
            all_intros.append({
                "pmid": pmid,
                "year": m.get("year", None) if isinstance(m, pd.Series) else None,
                "title": m.get("title", "") if isinstance(m, pd.Series) else "",
                "journal": m.get("journal", "") if isinstance(m, pd.Series) else "",
                "source": "oa_pdf",
                "extraction_method": method,
                "intro_text": intro,
                "intro_length": len(intro),
            })
            n_oa += 1

print(f"    Extracted: {n_oa:,} intros from OA PDFs")

# ═══════════════════════════════════════════════════════════════════════════
# SAVE ALL
# ═══════════════════════════════════════════════════════════════════════════
intros_df = pd.DataFrame(all_intros)
intros_df["decade"] = (intros_df["year"] // 10) * 10

# Remove duplicates (same PMID from multiple sources — keep section over first_2000)
intros_df = intros_df.sort_values("extraction_method", ascending=True)  # 'first_2000' < 'section'
intros_df = intros_df.drop_duplicates(subset="pmid", keep="last")

intros_df.to_csv(PROC / "introductions_all.csv", index=False)
print(f"\n  Total unique intros: {len(intros_df):,}")
print(f"  By extraction method:")
for m, c in intros_df["extraction_method"].value_counts().items():
    print(f"    {m}: {c:,}")
print(f"  By decade:")
for d in sorted(intros_df["decade"].dropna().unique()):
    n = (intros_df["decade"] == d).sum()
    print(f"    {int(d)}s: {n:,}")

# ═══════════════════════════════════════════════════════════════════════════
# STRATIFIED SAMPLE (40 per decade, 200 total)
# ═══════════════════════════════════════════════════════════════════════════
print("\n  Creating stratified sample...")
DECADES = [1980, 1990, 2000, 2010, 2020]
SAMPLE_PER_DECADE = 40
rng = np.random.RandomState(42)

sample_parts = []
for dec in DECADES:
    dec_df = intros_df[intros_df["decade"] == dec]
    # Prefer section-extracted intros
    section_intros = dec_df[dec_df["extraction_method"] == "section"]
    fallback = dec_df[dec_df["extraction_method"] == "first_2000"]

    n_section = min(len(section_intros), SAMPLE_PER_DECADE)
    n_fallback = min(len(fallback), SAMPLE_PER_DECADE - n_section)

    if n_section > 0:
        sample_parts.append(section_intros.sample(n=n_section, random_state=rng))
    if n_fallback > 0:
        sample_parts.append(fallback.sample(n=n_fallback, random_state=rng))

    total = n_section + n_fallback
    print(f"    {int(dec)}s: {total} intros ({n_section} section + {n_fallback} fallback)")

sample = pd.concat(sample_parts, ignore_index=True)
sample = sample.sort_values(["decade", "year"]).reset_index(drop=True)
sample.to_csv(PROC / "introductions_sample_200.csv", index=False)
print(f"\n  Stratified sample: {len(sample)} intros saved to introductions_sample_200.csv")
