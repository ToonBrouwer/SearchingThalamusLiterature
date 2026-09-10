"""
Phase B — Step 1: Extract framing sentences from PubMed abstracts.

Applies the same verb-trigger and appositive-pattern extraction used on
full-text articles (extract_framing_sentences.py) to the full PubMed abstract
corpus (1950–2026).  This extends the temporal coverage back to 1950 and
provides a replication dataset independent of the PMC full-text sample.

A sentence ID (sid) in the format {pmid}_abs_{sentence_index} is added to
every row so sentences can be traced back to their source paper.

Inputs (Data/raw/pubmed/):
    pubmed_broad_title_only_1950_2026_<date>.csv
        Required columns: pmid, year, journal, title, publication_types, abstract
        Update the filename below if a newer export has been produced.

Outputs (Data/interim/sentence_extraction/):
    thalamus_abstract_all_sentences.csv      — all thalamus-mentioning sentences
    thalamus_abstract_framing_sentences.csv  — framing subset (verb-trigger or
                                               appositive match)

Note on patterns:
    VERB_TRIGGERS and APPOSITIVE_PATTERN are intentionally copied from
    extract_framing_sentences.py rather than imported, to keep this script
    self-contained.  Any change to the triggers should be mirrored here and
    in classify_framing.py.

Note on paths:
    RAW_PM and OUT_DIR are resolved from __file__; Code/config.py /
    paths.py are not wired in here.
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import re
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple

# ── Paths ─────────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT    = _ANALYSIS_DIR.parents[1]
RAW_PM  = ROOT / "Data" / "raw" / "pubmed"
OUT_DIR = ROOT / "Data" / "interim" / "sentence_extraction"

# ── Framing detection patterns (mirrors extract_framing_sentences.py) ─────────
THALAMUS_PATTERN = re.compile(r"\bthalamus\b", re.IGNORECASE)

VERB_TRIGGERS = [
    "thalamus is", "the thalamus is",
    "thalamus acts", "the thalamus acts",
    "thalamus relays", "the thalamus relays",
    "thalamus plays", "the thalamus plays",
    "thalamus serves", "the thalamus serves",
    "thalamus has", "the thalamus has",
    "thalamus may", "the thalamus may",
    "thalamus could", "the thalamus could",
    "thalamus contributes", "the thalamus contributes",
    "thalamus mediates", "the thalamus mediates",
    "thalamus provides", "the thalamus provides",
    "thalamus is associated with", "the thalamus is associated with",
    "thalamus was associated with", "the thalamus was associated with",
    "thalamus has been identified as", "the thalamus has been identified as",
    "thalamus could modulate", "the thalamus could modulate",
    "thalamus which provides", "the thalamus which provides",
]

APPOSITIVE_PATTERN = re.compile(
    r"\b(the\s+)?thalamus\s*,\s*([^,]{5,200})\s*,",
    re.IGNORECASE,
)

BAD_APPOSITIVES = {"however", "therefore", "for example", "for instance",
                   "in contrast", "in turn"}


def split_sentences(text: str) -> list:
    if not isinstance(text, str) or not text.strip():
        return []
    text = re.sub(r"\s+", " ", text).strip()
    return re.split(r"(?<=[.!?])\s+", text)


def match_verb_trigger(sentence: str) -> Optional[str]:
    s = sentence.lower()
    for trigger in VERB_TRIGGERS:
        if trigger in s:
            return trigger
    return None


def extract_appositive(sentence: str) -> Optional[str]:
    match = APPOSITIVE_PATTERN.search(sentence)
    if not match:
        return None
    phrase = match.group(2).strip()
    return None if phrase.lower() in BAD_APPOSITIVES else phrase


def classify_sentence(sentence: str) -> Tuple[bool, Optional[str], Optional[str]]:
    verb_trigger     = match_verb_trigger(sentence)
    appositive_phrase = extract_appositive(sentence)
    is_framing       = verb_trigger is not None or appositive_phrase is not None
    return is_framing, verb_trigger, appositive_phrase


# ── Load PubMed data ──────────────────────────────────────────────────────────
# Update the filename below if a newer pubmed export has been produced.
pub = pd.read_csv(RAW_PM / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                  dtype={"pmid": str})
pub = pub.dropna(subset=["abstract"])
pub["year"] = pd.to_numeric(pub["year"], errors="coerce")

print(f"Processing {len(pub):,} abstracts ({pub['year'].min():.0f}–{pub['year'].max():.0f}) …")

# ── Extract sentences ─────────────────────────────────────────────────────────
all_rows     = []
framing_rows = []

for _, row in pub.iterrows():
    pmid      = row["pmid"]
    sentences = split_sentences(row["abstract"])

    for i, sentence in enumerate(sentences):
        if not THALAMUS_PATTERN.search(sentence):
            continue

        is_framing, verb_trigger, appositive_phrase = classify_sentence(sentence)

        record = {
            "sid":               f"{pmid}_abs_{i}",
            "pmid":              pmid,
            "year":              row["year"],
            "journal":           row.get("journal"),
            "title":             row.get("title"),
            "publication_types": row.get("publication_types"),
            "sentence_index":    i,
            "sentence":          sentence,
            "verb_trigger":      verb_trigger,
            "appositive_phrase": appositive_phrase,
            "is_framing":        is_framing,
            "text_source":       "abstract",
        }
        all_rows.append(record)
        if is_framing:
            framing_rows.append(record)

all_df     = pd.DataFrame(all_rows)
framing_df = pd.DataFrame(framing_rows)

# ── Save ──────────────────────────────────────────────────────────────────────
OUT_DIR.mkdir(parents=True, exist_ok=True)
all_df.to_csv(    OUT_DIR / "thalamus_abstract_all_sentences.csv",     index=False)
framing_df.to_csv(OUT_DIR / "thalamus_abstract_framing_sentences.csv", index=False)

print(f"All thalamus sentences from abstracts : {len(all_df):,}")
print(f"Framing sentences from abstracts      : {len(framing_df):,}")
print(f"Year range of framing sentences       : "
      f"{framing_df['year'].min():.0f}–{framing_df['year'].max():.0f}")
print(f"Verb-trigger framing: {framing_df['verb_trigger'].notna().sum():,}")
print(f"Appositive framing  : {framing_df['appositive_phrase'].notna().sum():,}")
