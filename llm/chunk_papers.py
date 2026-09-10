"""
chunk_papers.py — Build the chunk manifest for LLM frame classification.

Loads the 65 full-text reviews selected by `select_influential_reviews.py`,
splits each into ~800-word chunks with 100-word overlap, attaches per-
chunk metadata (paper PMID, section if PMC, position string), and saves
the chunked text + manifest CSV.

Outputs (under `Data/processed/llm_classifications/chunks/`):
    {pmid}_{idx:03d}.json
        per-chunk record:
            pmid, chunk_idx, n_chunks, position, section, source,
            n_words, text
    chunks_manifest.csv
        flat index of every chunk: (pmid, chunk_idx, position, section,
        source, n_words, file_path)

Usage:
    python Code/llm/chunk_papers.py
    python Code/llm/chunk_papers.py --chunk-size 1000 --overlap 150
"""
from __future__ import annotations
import argparse, json, os, re, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                    errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from collections import Counter
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INTERIM = ROOT / "Data" / "interim"
# Default output dir — overridden via --out-base for non-review runs
DEFAULT_OUT_BASE = PROC / "llm_classifications"

# Known input selections — `--corpus` picks one
KNOWN_CORPORA = {
    "reviews":     PROC / "influential_reviews_selected.csv",
    "nonreviews":  PROC / "influential_nonreviews_selected.csv",
}


# ─────────────────────────────────────────────────────────────────────────
# Loading the corpus + fulltexts
# ─────────────────────────────────────────────────────────────────────────
def load_selection(selection_csv: Path) -> pd.DataFrame:
    """Top-K-per-year selection — fulltext-only filter applied here."""
    sel = pd.read_csv(selection_csv, low_memory=False)
    return sel[sel["fulltext_source"].isin(["pmc", "libkey"])].copy()


def load_pmc_text(pmid: int) -> tuple[str, list[tuple[str, str]]] | None:
    """
    Returns (full_text, list_of_(section_title, section_text)) for a PMC
    paper, or None if not found. The CSV stores the body as one big
    `full_text` string and `section_titles` as a python-list-as-string.
    """
    p = sorted((INTERIM / "fulltext").glob("pmc_fulltext_*.csv"))
    if not p:
        return None
    df = pd.read_csv(p[-1], low_memory=False,
                     usecols=["pmid", "full_text", "section_titles"])
    row = df[df["pmid"] == int(pmid)]
    if not len(row) or pd.isna(row["full_text"].iloc[0]):
        return None
    full_text = str(row["full_text"].iloc[0])
    titles_raw = row["section_titles"].iloc[0]
    titles = []
    if isinstance(titles_raw, str) and titles_raw.strip():
        # The PMC CSV stores section titles as a pipe-separated string,
        # e.g. "Introduction | Methods | Discussion".
        titles = [t.strip() for t in titles_raw.split("|") if t.strip()]

    # Map section titles back onto text by string search; fall back to
    # uniform splitting if titles can't be located in the body.
    section_spans = []
    pos = 0
    for title in titles:
        if not title or not isinstance(title, str):
            continue
        idx = full_text.find(title, pos)
        if idx == -1:
            continue
        section_spans.append((title, idx))
        pos = idx + len(title)

    sections: list[tuple[str, str]] = []
    if section_spans:
        for i, (title, start) in enumerate(section_spans):
            end = (section_spans[i+1][1]
                   if i + 1 < len(section_spans) else len(full_text))
            body = full_text[start + len(title):end].strip()
            if body:
                sections.append((title, body))
    return full_text, sections


def load_libkey_text(pmid: int) -> str | None:
    p = INTERIM / "institutional_text" / f"{int(pmid)}.txt"
    if not p.exists():
        return None
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────
# Chunking
# ─────────────────────────────────────────────────────────────────────────
_REF_BOUNDARY_RE = re.compile(
    r"\n\s*(?:references|bibliography|literature\s+cited)\s*\n",
    re.IGNORECASE,
)
_WORD_RE = re.compile(r"\S+")


def trim_trailing_references(text: str, source: str) -> str:
    """
    Drop the references / bibliography section. Use the first matching
    "References" header if found; for LibKey-extracted PDFs that lack a
    clean header, fall back to dropping the trailing 12% of words (rough
    heuristic — most refs sit at the end and a chunk of them won't carry
    metaphorical content worth analysing).
    """
    m = _REF_BOUNDARY_RE.search(text)
    if m:
        return text[:m.start()].rstrip()
    if source == "libkey":
        words = _WORD_RE.findall(text)
        if len(words) > 4000:
            cutoff = int(len(words) * 0.88)
            joined = " ".join(words[:cutoff])
            return joined
    return text


def split_into_chunks(text: str, chunk_size: int = 800,
                       overlap: int = 100) -> list[str]:
    """
    ~chunk_size word chunks, with `overlap` words shared between
    successive chunks. We split on whitespace tokens so the chunk
    boundary doesn't cut a word in half.
    """
    words = _WORD_RE.findall(text)
    if len(words) <= chunk_size:
        return [" ".join(words)] if words else []
    step = chunk_size - overlap
    chunks = []
    i = 0
    while i < len(words):
        end = min(i + chunk_size, len(words))
        chunks.append(" ".join(words[i:end]))
        if end == len(words):
            break
        i += step
    return chunks


def find_section_for_offset(sections: list[tuple[str, str]],
                             original_text: str,
                             chunk_text: str) -> Optional[str]:
    """
    Best-effort mapping from a chunk back to the section it most belongs
    to. We locate the chunk's start in the original text, then find which
    section span it sits inside.
    """
    if not sections:
        return None
    head = chunk_text[:60]   # short prefix — first 60 chars usually unique
    if not head:
        return None
    pos = original_text.find(head)
    if pos < 0:
        return None
    cursor = 0
    for title, body in sections:
        body_start = original_text.find(body, cursor)
        if body_start < 0:
            continue
        body_end = body_start + len(body)
        if body_start <= pos < body_end:
            return title
        cursor = body_end
    return None


# ─────────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────────
def process_paper(pmid: int, source: str,
                   chunk_size: int, overlap: int,
                   row: pd.Series) -> list[dict]:
    if source == "pmc":
        loaded = load_pmc_text(pmid)
        if loaded is None:
            return []
        full_text, sections = loaded
    elif source == "libkey":
        full_text = load_libkey_text(pmid)
        if full_text is None:
            return []
        sections = []
    else:
        return []

    full_text = trim_trailing_references(full_text, source)
    chunks = split_into_chunks(full_text, chunk_size, overlap)
    n = len(chunks)
    out = []
    for i, chunk_text in enumerate(chunks):
        section_title = (find_section_for_offset(sections, full_text,
                                                   chunk_text)
                         if sections else None)
        record = {
            "pmid": int(pmid),
            "chunk_idx": i,
            "n_chunks": n,
            "position": f"chunk {i+1} of {n}",
            "section": section_title,
            "source": source,
            "n_words": len(_WORD_RE.findall(chunk_text)),
            "text": chunk_text,
            # Carry paper metadata so downstream scripts don't have to
            # cross-reference the selection CSV again.
            "year": int(row["year"]),
            "journal": str(row.get("journal", "")),
            "title": str(row.get("title", "")),
        }
        out.append(record)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", choices=list(KNOWN_CORPORA),
                        default="reviews",
                        help="Which selection to chunk")
    parser.add_argument("--selection-csv", type=Path, default=None,
                        help="Override --corpus with a custom CSV path")
    parser.add_argument("--out-base", type=Path, default=None,
                        help="Override default output base "
                             "(default: Data/processed/llm_classifications/"
                             "<corpus>/ for non-default corpora, the "
                             "shared dir for reviews for back-compat)")
    parser.add_argument("--chunk-size", type=int, default=800,
                        help="Target words per chunk")
    parser.add_argument("--overlap", type=int, default=100,
                        help="Words shared between successive chunks")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process only the first N papers (for testing)")
    args = parser.parse_args()

    selection_csv = args.selection_csv or KNOWN_CORPORA[args.corpus]
    if args.out_base is None:
        # Reviews keep using the legacy shared dir for backward compat;
        # non-reviews get their own subdir.
        if args.corpus == "reviews":
            out_base = DEFAULT_OUT_BASE
        else:
            out_base = DEFAULT_OUT_BASE / args.corpus
    else:
        out_base = args.out_base
    out_dir = out_base / "chunks"
    out_dir.mkdir(parents=True, exist_ok=True)

    sel = load_selection(selection_csv)
    if args.limit:
        sel = sel.head(args.limit)
    print(f"Chunking {len(sel)} fulltext papers from {selection_csv.name} "
          f"(chunk_size={args.chunk_size} words, overlap={args.overlap})")
    print(f"Output: {out_dir}")

    manifest_rows = []
    n_chunks_total = 0
    skipped = []
    for _, row in sel.iterrows():
        pmid = int(row["pmid"])
        source = row["fulltext_source"]
        records = process_paper(pmid, source,
                                 args.chunk_size, args.overlap, row)
        if not records:
            skipped.append((pmid, source))
            continue
        for rec in records:
            fn = out_dir / f"{pmid}_{rec['chunk_idx']:03d}.json"
            fn.write_text(json.dumps(rec, ensure_ascii=False, indent=2),
                          encoding="utf-8")
            manifest_rows.append({
                "pmid": pmid,
                "chunk_idx": rec["chunk_idx"],
                "n_chunks": rec["n_chunks"],
                "position": rec["position"],
                "section": rec["section"],
                "source": source,
                "n_words": rec["n_words"],
                "year": rec["year"],
                "journal": rec["journal"],
                "title": rec["title"],
                "file_path": str(fn.relative_to(ROOT)).replace("\\", "/"),
            })
        n_chunks_total += len(records)
        if len(records) > 0:
            section_count = sum(1 for r in records if r["section"])
            print(f"  {pmid} ({source}, year={row['year']}): "
                  f"{len(records)} chunks, "
                  f"{section_count}/{len(records)} with section labels")

    manifest = pd.DataFrame(manifest_rows)
    out_csv = out_base / "chunks_manifest.csv"
    manifest.to_csv(out_csv, index=False)

    print(f"\nWrote {n_chunks_total} chunks for "
          f"{manifest['pmid'].nunique() if len(manifest) else 0} papers "
          f"to {out_dir}")
    print(f"Manifest: {out_csv}")
    if skipped:
        print(f"\n{len(skipped)} papers skipped (no fulltext located): "
              f"{skipped[:5]}{' ...' if len(skipped) > 5 else ''}")

    # Summary stats
    if len(manifest):
        print("\nChunk-count distribution:")
        sizes = manifest.groupby("pmid").size().sort_values(ascending=False)
        print(f"  papers: {len(sizes)}")
        print(f"  chunks per paper: min={sizes.min()}, "
              f"median={int(sizes.median())}, "
              f"p90={int(sizes.quantile(0.9))}, max={sizes.max()}")
        print(f"  total chunks: {sizes.sum()}")
        print(f"  PMC papers w/ section labels: "
              f"{(manifest[manifest['source']=='pmc']['section'].notna()).sum()} "
              f"of {(manifest['source']=='pmc').sum()} chunks")


if __name__ == "__main__":
    main()
