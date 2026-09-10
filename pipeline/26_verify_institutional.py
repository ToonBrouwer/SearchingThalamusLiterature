"""
26_verify_institutional.py — QC: does each institutional_text/<pmid>.txt
actually contain the paper it claims to be?

The LibKey downloader matched PDFs to PMIDs by DOI-in-text with a
FIFO+title fallback; the v18 pilot found 2/10 sampled files holding a
DIFFERENT article (same era, wrong paper). This script checks every file:
fraction of the title's content words present in the file's first 3,000
characters (PDF front matter includes the title in virtually all cases).

Output: Data/processed/v18/institutional_title_check.csv
        (pmid, title_word_frac, n_title_words, verdict)
Verdict: ok (>= 0.6), suspect (0.35-0.6), mismatch (< 0.35).
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
TXT = ROOT / "Data" / "interim" / "institutional_text"
OUT = ROOT / "Data" / "processed" / "v18"

STOP = set("the a an of and or in on for to with by from at as is are was "
           "were be been its their during after before between under over "
           "not no do does".split())

raw = pd.read_csv(
    ROOT / "Data" / "raw" / "pubmed" /
    "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    usecols=["pmid", "title"], low_memory=False).drop_duplicates("pmid")
titles = dict(zip(raw.pmid, raw.title))

rows = []
files = sorted(TXT.glob("*.txt"))
print(f"Checking {len(files):,} institutional text files ...")
for i, f in enumerate(files):
    if i % 500 == 0:
        print(f"  {i}/{len(files)}")
    try:
        pmid = int(f.stem)
    except ValueError:
        continue
    title = titles.get(pmid)
    if not isinstance(title, str) or not title.strip():
        rows.append((pmid, None, 0, "no_title"))
        continue
    words = [w for w in re.findall(r"[a-z]{3,}", title.lower())
             if w not in STOP]
    if len(words) < 3:
        rows.append((pmid, None, len(words), "title_too_short"))
        continue
    head = f.read_text(encoding="utf-8", errors="replace")[:3000].lower()
    head = re.sub(r"\s+", " ", head)
    frac = sum(1 for w in set(words) if w in head) / len(set(words))
    verdict = ("ok" if frac >= 0.6 else
               "suspect" if frac >= 0.35 else "mismatch")
    rows.append((pmid, round(frac, 3), len(set(words)), verdict))

df = pd.DataFrame(rows, columns=["pmid", "title_word_frac",
                                  "n_title_words", "verdict"])
df.to_csv(OUT / "institutional_title_check.csv", index=False)
print("\nVerdicts:")
print(df.verdict.value_counts().to_string())
print(f"\nmismatch rate: "
      f"{100 * (df.verdict == 'mismatch').mean():.1f}%  "
      f"(suspect: {100 * (df.verdict == 'suspect').mean():.1f}%)")
