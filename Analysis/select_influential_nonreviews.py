"""
select_influential_nonreviews.py — Pick the top-K most influential
EXPERIMENTAL / MODELLING papers per year (i.e. everything that is NOT a
review, perspective, editorial, or letter) for downstream LLM frame
classification.

Parallel to `select_influential_reviews.py` — same scoring pipeline and
same output schema, just with the filter inverted.

Selection
---------
1. Candidates = papers whose PubMed `publication_types` does NOT contain
   a review flag ("Review"|"Systematic Review"|"Meta-Analysis") AND
   whose title does NOT match /perspective|opinion|hypothesis|
   viewpoint|essay|commentary/i.
   Also drop purely non-research types: "Editorial"|"Comment"|"Letter"|
   "News"|"Erratum"|"Retraction"|"Biography"|"Historical Article".

2. Influence score: identical to the review pipeline.
     z_global   = zscore(log1p(cited_by_count))
     z_internal = zscore(within_corpus_indegree)
     composite  = 0.5 * z_global + 0.5 * z_internal

3. Top-K (default 3) per publication year.

Outputs
-------
    Data/processed/influential_nonreviews.csv
        all candidates, ranked by composite
    Data/processed/influential_nonreviews_selected.csv
        top-K per year
    Data/processed/influential_nonreviews_for_llm/
        {year}_{pmid}_{slug}.txt — metadata header + body (full text or
        abstract w/ marker).

Usage:
    python analysis/select_influential_nonreviews.py
    python analysis/select_influential_nonreviews.py --top-k 5
"""
from __future__ import annotations
import argparse, os, sys, io, re
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
from collections import Counter
import datetime as _dt

import numpy as np
import pandas as pd

# Reuse the helpers from the reviews pipeline verbatim
from select_influential_reviews import (  # type: ignore  # noqa: E402
    RAW, PROC, INTERIM,
    load_pubmed, load_citations, load_internal_edges,
    load_pmc_fulltext, load_institutional_text, read_institutional_text,
    score_candidates, resolve_fulltext, slugify, write_llm_file,
    zscore,
)

OUT_CSV_ALL = PROC / "influential_nonreviews.csv"
OUT_CSV_SEL = PROC / "influential_nonreviews_selected.csv"
OUT_DIR     = PROC / "influential_nonreviews_for_llm"


# ─────────────────────────────────────────────────────────────────────────
# Inverted candidate filter — the only real difference vs the review pipeline
# ─────────────────────────────────────────────────────────────────────────
REVIEW_PT_RE = re.compile(
    r"\b(Review|Systematic Review|Meta[- ]Analysis)\b", re.IGNORECASE
)
PERSPECTIVE_TITLE_RE = re.compile(
    r"\b(perspective|opinion|hypothesis|viewpoint|essay|commentary)\b",
    re.IGNORECASE
)
NON_RESEARCH_PT_RE = re.compile(
    r"\b(Editorial|Comment|Letter|News|Erratum|Retraction|"
    r"Biography|Historical Article)\b",
    re.IGNORECASE
)


def tag_nonreviews(raw: pd.DataFrame) -> pd.DataFrame:
    pt = raw["publication_types"].fillna("")
    title = raw["title"].fillna("")
    is_review_pt    = pt.str.contains(REVIEW_PT_RE, regex=True)
    is_persp_title  = title.str.contains(PERSPECTIVE_TITLE_RE, regex=True)
    is_non_research = pt.str.contains(NON_RESEARCH_PT_RE, regex=True)

    raw = raw.copy()
    raw["is_nonreview"] = ~(is_review_pt | is_persp_title | is_non_research)
    raw["pt_research"] = pt.str.contains(r"\bJournal Article\b", case=False,
                                           regex=True)
    raw["pt_clinical_trial"] = pt.str.contains(r"\bClinical Trial\b",
                                                  case=False, regex=True)
    raw["pt_case_reports"] = pt.str.contains(r"\bCase Reports\b",
                                                case=False, regex=True)
    return raw


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-k", type=int, default=3,
                        help="Top-K candidates per year for the selected set")
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--min-citations", type=int, default=0,
                        help="Drop candidates with fewer global citations")
    parser.add_argument("--write-texts", action="store_true", default=True,
                        help="Write one .txt per selected paper (default on)")
    parser.add_argument("--no-texts", dest="write_texts", action="store_false")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    raw = load_pubmed()
    cit = load_citations()
    edges = load_internal_edges()
    pmc = load_pmc_fulltext()
    inst = load_institutional_text()

    raw = tag_nonreviews(raw)
    papers = raw[raw["is_nonreview"]].copy()
    papers = papers[(papers["year"] >= args.start_year)
                    & (papers["year"] <= args.end_year)]
    print(f"\nCandidate research papers: {len(papers):,} "
          f"(years {args.start_year}–{args.end_year})")
    print(f"  tagged \"Journal Article\": {papers['pt_research'].sum():,} | "
          f"Clinical Trial: {papers['pt_clinical_trial'].sum():,} | "
          f"Case Reports: {papers['pt_case_reports'].sum():,}")

    # Score (identical to reviews pipeline)
    current_year = max(args.end_year, _dt.date.today().year)
    scored = score_candidates(papers, cit, edges, current_year)
    if args.min_citations > 0:
        scored = scored[scored["cited_by_count"] >= args.min_citations]

    scored["fulltext_source"] = scored["pmid"].map(
        lambda p: resolve_fulltext(int(p), pmc, inst)[0]
    )

    keep_cols = ["pmid", "year", "journal", "title", "publication_types",
                  "pt_research", "pt_clinical_trial", "pt_case_reports",
                  "cited_by_count", "within_corpus_cite", "per_year_accrual",
                  "z_global", "z_internal", "composite",
                  "fulltext_source"]
    keep_cols = [c for c in keep_cols if c in scored.columns]
    scored[keep_cols].to_csv(OUT_CSV_ALL, index=False)
    print(f"\nSaved full manifest: {OUT_CSV_ALL.name} ({len(scored):,} rows)")

    sel = (scored.sort_values(["year", "composite"], ascending=[True, False])
                   .groupby("year", as_index=False).head(args.top_k)
                   .reset_index(drop=True))
    sel.to_csv(OUT_CSV_SEL, index=False)
    print(f"Saved top-{args.top_k}/year selection: {OUT_CSV_SEL.name} "
          f"({len(sel):,} rows)")

    sel["decade"] = (sel["year"] // 10) * 10
    by_dec = (sel.groupby(["decade", "fulltext_source"]).size()
                 .unstack(fill_value=0))
    print("\nCoverage by decade × fulltext source:")
    print(by_dec.to_string())

    if args.write_texts:
        print(f"\nWriting text files to {OUT_DIR} …")
        n_written = Counter()
        for _, row in sel.iterrows():
            src, body = resolve_fulltext(int(row["pmid"]), pmc, inst)
            abs_row = raw[raw["pmid"] == int(row["pmid"])]
            row_out = row.copy()
            row_out["abstract"] = abs_row["abstract"].iloc[0] \
                if len(abs_row) else ""
            write_llm_file(OUT_DIR, row_out, src, body)
            n_written[src] += 1
        print(f"  wrote {sum(n_written.values())} files: "
              f"pmc={n_written.get('pmc', 0)}, "
              f"libkey={n_written.get('libkey', 0)}, "
              f"abstract_only={n_written.get('abstract_only', 0)}")

    print("\nTop-1 composite pick per decade:")
    for d, group in sel.sort_values(["decade", "composite"],
                                      ascending=[True, False]).groupby("decade"):
        top = group.iloc[0]
        print(f"  {int(d)}s  [{int(top['year'])}] "
              f"[citations={int(top['cited_by_count'])}, "
              f"internal={int(top['within_corpus_cite'])}]  "
              f"{str(top['title'])[:95]}")


if __name__ == "__main__":
    main()
