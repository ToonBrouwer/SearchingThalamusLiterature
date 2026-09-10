"""
25_add_sections.py — add the SECTION column to the canonical corpus files
and report what excluding reference-list sentences changes.

Joins v18/sentence_sections_all.csv (built by 23_pmc_sections.py +
24_pdf_sections.py) onto the canonical CSVs on (pmid, normalized text).
Section values: abstract / front / introduction / methods / results /
discussion / conclusion / other / references / unknown (unmatched
full-text). Abstract-source sentences are 'abstract' by definition.

Downstream, v18_data excludes section == 'references' by default —
reference-list text is cited TITLES, not the paper's own prose.
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
V18 = PROC / "v18"


def norm_sentence(s: str) -> str:
    return re.sub(r"\W+", " ", str(s).lower()).strip()


allmap = pd.read_csv(V18 / "sentence_sections_all.csv",
                     usecols=["pmid", "norm", "section"])
allmap = allmap.drop_duplicates(subset=["pmid", "norm"], keep="first")
print(f"Section map: {len(allmap):,} (pmid, sentence) pairs")

for name in ["all_sentences_full_corpus.csv", "framing_full_corpus.csv"]:
    path = PROC / name
    print(f"\n=== {name} ===")
    df = pd.read_csv(path, low_memory=False)
    if "section" in df.columns:
        df = df.drop(columns=["section"])
    df["_norm"] = df.sentence.map(norm_sentence)
    df = df.merge(allmap.rename(columns={"norm": "_norm"}),
                  on=["pmid", "_norm"], how="left")
    df.loc[df.source == "abstract", "section"] = "abstract"
    df["section"] = df.section.fillna("unknown")
    df = df.drop(columns=["_norm"])
    print(df.section.value_counts().to_string())
    n_ref = (df.section == "references").sum()
    print(f"-> excluding references drops {n_ref:,} of {len(df):,} rows "
          f"({100 * n_ref / len(df):.1f}%)")
    df.to_csv(path, index=False)
    print(f"written: {path.name}")

# Derived caches built from the sentence corpus are now stale
stale = V18 / "paper_techniques.csv"
if stale.exists():
    stale.unlink()
    print("\nDeleted stale cache v18/paper_techniques.csv (will rebuild "
          "from the references-excluded corpus)")
