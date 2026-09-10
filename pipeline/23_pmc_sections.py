"""
23_pmc_sections.py — recover SECTION provenance for PMC full-text sentences.

The sentence-extraction pipeline kept only thalamus-mentioning sentences, so
section headers were discarded and the canonical corpus has no record of
whether a sentence came from the introduction, results, or discussion.
This script re-reads the PMC XML sources (Data/raw/pmc_xml/*.xml), which
carry explicit <sec><title> structure, and rebuilds that mapping.

Steps
  1. Parse every PMC XML: abstract + top-level body sections, classify
     section titles into buckets (abstract / introduction / methods /
     results / discussion / conclusion / other).
  2. Sentence-tokenize each section's paragraphs, keep thalamus-mentioning
     sentences with the pipeline's filters ('thalam', 20-600 chars).
  3. Match against the canonical corpus (source == 'fulltext') on
     (pmid, normalized sentence text).

Outputs (Data/processed/v18/):
  pmc_sentence_sections.csv     — pmid, section, norm  (all PMC sentences)
  sentence_sections_fulltext.csv — pmid, norm, section (matched canonical)
Prints match-rate and framing-by-section statistics.
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
XML_DIR = ROOT / "Data" / "raw" / "pmc_xml"
PROC = ROOT / "Data" / "processed"
OUT = PROC / "v18"
OUT.mkdir(exist_ok=True)

import nltk
try:
    nltk.sent_tokenize("test sentence.")
except LookupError:
    nltk.download("punkt")
    nltk.download("punkt_tab")

SECTION_BUCKETS = [
    ("introduction", re.compile(r"^\s*(introduction|background)", re.I)),
    ("methods", re.compile(
        r"method|materials|experimental procedure|study design|subjects",
        re.I)),
    ("results", re.compile(r"^\s*(results|findings)", re.I)),
    ("discussion", re.compile(r"discussion", re.I)),
    ("conclusion", re.compile(r"^\s*(conclusion|concluding|summary)", re.I)),
]
SKIP_TAGS = {"table-wrap", "fig", "supplementary-material", "ref-list",
             "ack", "table", "disp-formula"}


def classify_title(title: str) -> str:
    for bucket, pat in SECTION_BUCKETS:
        if pat.search(title or ""):
            return bucket
    return "other"


def sec_paragraph_text(sec) -> str:
    """Text of all <p> under *sec*, skipping tables/figures/references."""
    chunks = []
    for p in sec.iter("p"):
        skip = False
        # cheap ancestor check: ElementTree lacks parent links, so test the
        # paragraph's own subtree for skip-tags instead (tables inside p)
        for bad in SKIP_TAGS:
            if p.find(f".//{bad}") is not None:
                skip = True
                break
        if not skip:
            chunks.append(" ".join(p.itertext()))
    return "\n".join(chunks)


def norm(s: str) -> str:
    return re.sub(r"\W+", " ", s.lower()).strip()


def sentences_of(text: str):
    for s in nltk.sent_tokenize(re.sub(r"\s+", " ", text)):
        if "thalam" in s.lower() and 20 <= len(s) <= 600:
            yield s


rows = []
files = sorted(XML_DIR.glob("*.xml"))
print(f"Parsing {len(files):,} PMC XML files ...")
n_no_pmid = n_err = 0
for i, f in enumerate(files):
    if i % 500 == 0:
        print(f"  {i}/{len(files)}")
    try:
        tree = ET.parse(f)
    except ET.ParseError:
        n_err += 1
        continue
    root = tree.getroot()
    # Files are OAI-PMH envelopes; the JATS article inside uses default
    # namespaces (version varies per file) — strip them so plain tag
    # names match.
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    pmid_el = root.find(".//article-id[@pub-id-type='pmid']")
    if pmid_el is None or not (pmid_el.text or "").strip().isdigit():
        n_no_pmid += 1
        continue
    pmid = int(pmid_el.text.strip())

    abstract = root.find(".//front//abstract")
    if abstract is not None:
        for s in sentences_of(" ".join(abstract.itertext())):
            rows.append((pmid, "abstract", norm(s)))

    body = root.find(".//body")
    if body is None:
        continue
    top_secs = body.findall("sec")
    if top_secs:
        for sec in top_secs:
            title_el = sec.find("title")
            bucket = classify_title(title_el.text if title_el is not None
                                     else "")
            for s in sentences_of(sec_paragraph_text(sec)):
                rows.append((pmid, bucket, norm(s)))
    else:
        for s in sentences_of(sec_paragraph_text(body)):
            rows.append((pmid, "other", norm(s)))

pmc = (pd.DataFrame(rows, columns=["pmid", "section", "norm"])
       .drop_duplicates(subset=["pmid", "norm"], keep="first"))
pmc.to_csv(OUT / "pmc_sentence_sections.csv", index=False)
print(f"\nParsed: {len(pmc):,} thalamus sentences from "
      f"{pmc.pmid.nunique():,} papers "
      f"({n_err} parse errors, {n_no_pmid} without pmid)")
print("Section distribution (PMC re-extraction):")
print(pmc.section.value_counts().to_string())

# ── Match against the canonical corpus ───────────────────────────────────
print("\nMatching against canonical corpus (source == 'fulltext') ...")
sent = pd.read_csv(PROC / "all_sentences_full_corpus.csv", low_memory=False,
                   usecols=["pmid", "sentence", "source", "frame_type"])
ft = sent[sent.source == "fulltext"].copy()
ft["norm"] = ft.sentence.map(norm)
matched = ft.merge(pmc, on=["pmid", "norm"], how="left")
rate = matched.section.notna().mean()
print(f"  matched {matched.section.notna().sum():,} / {len(ft):,} "
      f"({100 * rate:.1f}%)")

out = matched.loc[matched.section.notna(), ["pmid", "norm", "section"]]
out.to_csv(OUT / "sentence_sections_fulltext.csv", index=False)

print("\nSection distribution, matched canonical sentences:")
print(matched.section.fillna("UNMATCHED").value_counts().to_string())

framed = matched[matched.frame_type.notna() & (matched.frame_type != "")]
print("\nFraming sentences by section (PMC subset):")
tab = (framed.section.fillna("UNMATCHED").value_counts()
       .to_frame("framing_n"))
tab["all_pct"] = (matched.section.fillna("UNMATCHED").value_counts(
    normalize=True) * 100).round(1)
tab["framing_pct"] = (framed.section.fillna("UNMATCHED").value_counts(
    normalize=True) * 100).round(1)
print(tab.to_string())
