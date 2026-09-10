"""
24_pdf_sections.py — section provenance for PDF-derived full text
(institutional LibKey + Sci-Hub sources), via header heuristics.

Companion to 23_pmc_sections.py (which covers PMC XML). PDF text has no
markup, but section headers usually sit on their own short lines
("MATERIALS AND METHODS", "3. Results", ...). Strategy:

  1. Scan each Data/interim/{institutional_text,scihub_text}/<pmid>.txt for
     header lines; build (position -> section) spans. Text before the first
     recognised header is 'front' (title/abstract zone); text after a
     REFERENCES header is 'references'.
  2. Locate every canonical sentence of that paper inside a
     whitespace/punctuation-normalized copy of the document (exact
     normalized substring search, robust to line breaks), and assign the
     section of the span it falls in.

Outputs (Data/processed/v18/):
  sentence_sections_pdf.csv — pmid, norm, section
  sentence_sections_all.csv — union with the PMC mapping
Prints per-source match rates and framing-by-section stats for the union.
"""
from __future__ import annotations

import io
import re
import sys
from bisect import bisect_right
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
OUT = PROC / "v18"

TXT_DIRS = [ROOT / "Data" / "interim" / "institutional_text",
            ROOT / "Data" / "interim" / "scihub_text"]

HEADER_PATTERNS = [
    ("abstract", r"abstract"),
    ("introduction", r"introduction|background"),
    ("methods", r"(materials?\s+and\s+)?methods?|materials|"
                 r"experimental\s+procedures?|study\s+design"),
    ("results", r"results?(\s+and\s+discussion)?|findings"),
    ("discussion", r"(general\s+)?discussion"),
    ("conclusion", r"conclusions?|concluding\s+remarks|summary"),
    ("references", r"references?|bibliography|literature\s+cited|"
                    r"acknowledge?ments?"),
]
HEADER_RE = re.compile(
    r"^\s{0,8}(?:[0-9IVX]+[\.\)]?\s*)?(" +
    "|".join(f"(?P<{name}>{pat})" for name, pat in HEADER_PATTERNS) +
    r")\s*[:.]?\s*$", re.IGNORECASE)


def find_headers(text: str):
    """Return sorted [(char_pos, bucket)] for header-looking lines."""
    spans = []
    pos = 0
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if 0 < len(stripped) <= 45:
            m = HEADER_RE.match(line)
            if m:
                bucket = next(name for name, _ in HEADER_PATTERNS
                              if m.group(name))
                spans.append((pos, bucket))
        pos += len(line)
    return spans


def build_norm_map(text: str):
    """Normalized string (lowercase alnum, single spaces) plus a list
    mapping each normalized char back to its original index."""
    chars, idx = [], []
    prev_space = True
    for i, ch in enumerate(text):
        if ch.isalnum():
            chars.append(ch.lower())
            idx.append(i)
            prev_space = False
        else:
            if not prev_space:
                chars.append(" ")
                idx.append(i)
                prev_space = True
    return "".join(chars), idx


def norm_sentence(s: str) -> str:
    return re.sub(r"\W+", " ", s.lower()).strip()


# ── Load canonical sentences for the PDF-derived sources ─────────────────
print("Loading canonical corpus ...")
sent = pd.read_csv(PROC / "all_sentences_full_corpus.csv", low_memory=False,
                   usecols=["pmid", "sentence", "source", "frame_type"])
pdf_sent = sent[sent.source.isin(["institutional", "oa_pdf"])].copy()
pdf_sent["norm"] = pdf_sent.sentence.map(norm_sentence)
by_pmid = dict(tuple(pdf_sent.groupby("pmid")))
print(f"  {len(pdf_sent):,} sentences from {len(by_pmid):,} papers "
      f"(institutional + oa_pdf sources)")

rows = []
n_files = n_with_headers = 0
for txt_dir in TXT_DIRS:
    files = sorted(txt_dir.glob("*.txt"))
    print(f"\nScanning {len(files):,} files in {txt_dir.name} ...")
    for i, f in enumerate(files):
        if i % 500 == 0:
            print(f"  {i}/{len(files)}")
        try:
            pmid = int(f.stem)
        except ValueError:
            continue
        if pmid not in by_pmid:
            continue
        text = f.read_text(encoding="utf-8", errors="replace")
        headers = find_headers(text)
        n_files += 1
        if headers:
            n_with_headers += 1
        norm_doc, idx_map = build_norm_map(text)
        h_pos = [p for p, _ in headers]
        h_buck = [b for _, b in headers]
        for _, row in by_pmid[pmid].iterrows():
            ns = row["norm"]
            if len(ns) < 15:
                continue
            j = norm_doc.find(ns)
            if j < 0:
                # try the first 80 normalized chars (PDF artefacts often
                # corrupt sentence tails)
                j = norm_doc.find(ns[:80])
                if j < 0:
                    continue
            orig = idx_map[min(j, len(idx_map) - 1)]
            k = bisect_right(h_pos, orig) - 1
            section = h_buck[k] if k >= 0 else "front"
            rows.append((pmid, section, ns))

pdf_map = (pd.DataFrame(rows, columns=["pmid", "section", "norm"])
           .drop_duplicates(subset=["pmid", "norm"], keep="first"))
pdf_map.to_csv(OUT / "sentence_sections_pdf.csv", index=False)
print(f"\nFiles scanned: {n_files:,} ({n_with_headers:,} with detected "
      f"headers)")
print(f"Mapped {len(pdf_map):,} / {len(pdf_sent):,} PDF-source sentences "
      f"({100 * len(pdf_map) / len(pdf_sent):.1f}%)")
print("Section distribution (PDF sources):")
print(pdf_map.section.value_counts().to_string())

# ── Union with the PMC mapping ───────────────────────────────────────────
pmc_map = pd.read_csv(OUT / "sentence_sections_fulltext.csv")
allmap = pd.concat([pmc_map.assign(src="pmc"),
                    pdf_map.assign(src="pdf")], ignore_index=True)
allmap.to_csv(OUT / "sentence_sections_all.csv", index=False)

full = sent[sent.source.isin(["institutional", "oa_pdf", "fulltext"])].copy()
full["norm"] = full.sentence.map(norm_sentence)
merged = full.merge(allmap[["pmid", "norm", "section"]],
                    on=["pmid", "norm"], how="left")
print(f"\n=== Union coverage: {merged.section.notna().sum():,} / "
      f"{len(merged):,} full-text sentences "
      f"({100 * merged.section.notna().mean():.1f}%) ===")

framed = merged[merged.frame_type.notna() & (merged.frame_type != "")]
tab = pd.DataFrame({
    "all_pct": (merged.section.fillna("UNMATCHED")
                .value_counts(normalize=True) * 100).round(1),
    "framing_pct": (framed.section.fillna("UNMATCHED")
                    .value_counts(normalize=True) * 100).round(1),
})
tab["enrichment"] = (tab.framing_pct / tab.all_pct).round(2)
print("\nFraming by section (ALL full-text sources):")
print(tab.to_string())
