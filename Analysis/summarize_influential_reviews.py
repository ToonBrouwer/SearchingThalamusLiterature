"""
summarize_influential_reviews.py — Build a readable summary of the
influential-reviews selection.

Consumes the CSVs produced by `select_influential_reviews.py` and emits:

    Paper/influential_reviews_summary.docx
        Methods note (what defines "review", how "global" vs "internal"
        citations differ, how the composite score is built), coverage
        statistics by decade × fulltext source, the top-K picks per
        decade, and a supplementary table of the top-N reviews ranked
        purely by global citations (OpenAlex `cited_by_count`) and
        separately by within-corpus citations.

    Data/processed/influential_reviews_by_global_citations.csv
        All candidates re-ranked by raw `cited_by_count` (desc).

    Data/processed/influential_reviews_by_internal_citations.csv
        All candidates re-ranked by raw `within_corpus_cite` (desc).

Usage:
    python analysis/summarize_influential_reviews.py
    python analysis/summarize_influential_reviews.py --top-n 50
"""
from __future__ import annotations
import argparse, os, sys, io
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
import numpy as np
import pandas as pd

from _clustering_utils import PROC

ROOT = PROC.parents[1]
PAPER = ROOT / "Paper"
PAPER.mkdir(parents=True, exist_ok=True)
OUT_DOCX = PAPER / "influential_reviews_summary.docx"


def truncate(s: str, n: int = 110) -> str:
    s = str(s or "")
    return s if len(s) <= n else s[: n - 1] + "…"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=40,
                        help="Top-N for citation-only tables in the DOCX")
    args = parser.parse_args()

    all_path = PROC / "influential_reviews.csv"
    sel_path = PROC / "influential_reviews_selected.csv"
    if not (all_path.exists() and sel_path.exists()):
        raise SystemExit(
            "Missing manifest CSVs — run select_influential_reviews.py first."
        )
    all_df = pd.read_csv(all_path, low_memory=False)
    sel_df = pd.read_csv(sel_path, low_memory=False)

    # ── Coverage by decade × fulltext source (from selection)
    sel_df["decade"] = (sel_df["year"] // 10) * 10
    coverage = (sel_df.groupby(["decade", "fulltext_source"]).size()
                      .unstack(fill_value=0))
    for c in ["pmc", "libkey", "abstract_only"]:
        if c not in coverage.columns:
            coverage[c] = 0
    coverage = coverage[["pmc", "libkey", "abstract_only"]]
    coverage["total"] = coverage.sum(axis=1)
    coverage["pct_fulltext"] = (
        (coverage["pmc"] + coverage["libkey"]) / coverage["total"] * 100
    ).round(0).astype(int)
    print(coverage)

    # ── Per-decade top-3 picks (the actual selected papers)
    sel_sorted = sel_df.sort_values(["decade", "composite"],
                                      ascending=[True, False])
    # ── Ranked by raw global citations
    by_global = (all_df.sort_values("cited_by_count", ascending=False)
                        .reset_index(drop=True))
    by_global["rank_global"] = by_global.index + 1
    by_global.to_csv(
        PROC / "influential_reviews_by_global_citations.csv", index=False)

    # ── Ranked by raw within-corpus citations
    by_internal = (all_df.sort_values("within_corpus_cite", ascending=False)
                          .reset_index(drop=True))
    by_internal["rank_internal"] = by_internal.index + 1
    by_internal.to_csv(
        PROC / "influential_reviews_by_internal_citations.csv", index=False)

    # ── DOCX assembly
    from docx import Document
    from docx.shared import Pt, Inches

    doc = Document()
    for section in doc.sections:
        section.top_margin = Inches(0.7)
        section.bottom_margin = Inches(0.7)
        section.left_margin = Inches(0.7)
        section.right_margin = Inches(0.7)
        section.page_width = Inches(11)
        section.page_height = Inches(8.5)   # landscape ≈ fits titles

    def h(txt, level=1):
        doc.add_heading(txt, level=level)

    def para(txt, bold=False, size=11):
        p = doc.add_paragraph()
        run = p.add_run(txt)
        run.font.size = Pt(size)
        if bold:
            run.bold = True
        return p

    def bullet(items):
        for item in items:
            p = doc.add_paragraph(style="List Bullet")
            p.add_run(item).font.size = Pt(10)

    def add_table(df: pd.DataFrame, cols, font_size=8, max_col_chars=110):
        tbl = doc.add_table(rows=1 + len(df), cols=len(cols))
        tbl.style = "Light Grid"
        for j, c in enumerate(cols):
            cell = tbl.rows[0].cells[j]
            cell.text = str(c)
            for run in cell.paragraphs[0].runs:
                run.bold = True
                run.font.size = Pt(font_size)
        for i, (_, row) in enumerate(df.iterrows()):
            for j, c in enumerate(cols):
                val = row[c] if c in row.index else ""
                if isinstance(val, float):
                    if pd.isna(val):
                        val = ""
                    elif c in {"composite", "z_global", "z_internal",
                                "per_year_accrual"}:
                        val = f"{val:.2f}"
                    else:
                        val = f"{val:g}"
                tbl.rows[i + 1].cells[j].text = truncate(str(val), max_col_chars)
                for run in tbl.rows[i + 1].cells[j].paragraphs[0].runs:
                    run.font.size = Pt(font_size)

    # Title + methods
    h("Influential thalamus reviews, perspectives & meta-analyses", level=1)
    para(
        "This note accompanies the outputs of "
        "select_influential_reviews.py. It describes how \"review\" was "
        "defined, explains the two citation metrics used, shows the "
        "coverage of full-text material by decade, lists the per-decade "
        "top-3 picks that are ready for LLM frame classification, and "
        "supplements those picks with two simple citation-only "
        "rankings."
    )

    h("What counts as a \"review\"?", level=2)
    para("A candidate paper is flagged as a review/perspective if either:")
    bullet([
        "Its PubMed `publication_types` field contains \"Review\", "
        "\"Systematic Review\", or \"Meta-Analysis\" (PubMed's own curation), OR",
        "Its title matches the regex /perspective|opinion|hypothesis|"
        "viewpoint|essay|commentary/i (catches editorial Perspective / "
        "Opinion articles in journals like Neuron and Nature "
        "Neuroscience that PubMed tags only as \"Journal Article\").",
    ])
    para(
        "\"Comment\", \"Editorial\", \"Letter\", \"News\", \"Erratum\", "
        "\"Retraction\", and \"Biography\" are excluded *unless* the "
        "paper also matches one of the rules above."
    )
    n_cand = len(all_df)
    para(f"Total candidates found across 1950–2026: {n_cand:,}.", bold=True)

    h("Global vs internal (within-corpus) citations", level=2)
    para(
        "Two citation metrics are used because they measure very "
        "different things."
    )
    bullet([
        "Global citations = `cited_by_count` from OpenAlex = the total "
        "number of papers anywhere in the scientific literature that "
        "cite the review. A cardiology paper citing a thalamus review "
        "counts here.",
        "Internal (within-corpus) citations = the in-degree of the "
        "review in our thalamus citation graph, i.e. how many of the "
        "~18,900 thalamus papers in our own corpus cite it. This "
        "measures how central the review is to thalamus research "
        "specifically.",
    ])
    para(
        "The composite score used to pick the top-3 per year is the "
        "average of two z-scores: z(log1p(global)) and "
        "z(within-corpus). That way neither metric dominates the "
        "other, and the ranking rewards reviews that are both "
        "broadly cited and central to this sub-literature."
    )

    # Coverage table
    h("Coverage by decade × full-text source", level=2)
    para(
        "For each selected review we recorded where its text came "
        "from: PMC (OA full text), the institutional LibKey library, "
        "or abstract-only (no full text located). Pre-2000 coverage "
        "is thin because most landmark pre-OA reviews are not in "
        "PMC."
    )
    cov_disp = coverage.reset_index().rename(columns={"decade": "Decade"})
    add_table(cov_disp, ["Decade", "pmc", "libkey", "abstract_only",
                           "total", "pct_fulltext"])

    # Top-3 per decade (illustrative)
    h("Top-3 per year, organised by decade", level=2)
    para(
        f"The selected set is {len(sel_df)} reviews — top-3 per year "
        f"1950–2026 (fewer than 3 for years with too few candidates). "
        f"Ranked within year by composite score."
    )
    disp_cols = ["year", "pmid", "journal", "title", "cited_by_count",
                 "within_corpus_cite", "composite", "fulltext_source"]
    for dec, group in sel_sorted.groupby("decade"):
        h(f"{int(dec)}s  ({len(group)} papers)", level=3)
        g = group.sort_values(["year", "composite"], ascending=[True, False])
        add_table(g, disp_cols, max_col_chars=95)

    # Top-N by GLOBAL citations
    h(f"Top-{args.top_n} reviews by global citations (OpenAlex)", level=2)
    para(
        "Ranked purely by raw `cited_by_count`. This reflects overall "
        "scientific impact — the reviews that everyone cites — "
        "regardless of whether the citing work is about thalamus. "
        "Use this table when you want the \"most famous\" reviews."
    )
    top_g_cols = ["rank_global", "year", "pmid", "journal", "title",
                  "cited_by_count", "within_corpus_cite", "per_year_accrual",
                  "fulltext_source"]
    add_table(by_global.head(args.top_n), top_g_cols, max_col_chars=95)

    # Top-N by INTERNAL citations
    h(f"Top-{args.top_n} reviews by within-corpus citations", level=2)
    para(
        "Ranked by how many thalamus papers in our corpus cite the "
        "review. This is the centrality-in-our-subfield measure — "
        "use this table when you want the reviews that shaped "
        "thalamus research from within."
    )
    top_i_cols = ["rank_internal", "year", "pmid", "journal", "title",
                  "within_corpus_cite", "cited_by_count", "fulltext_source"]
    add_table(by_internal.head(args.top_n), top_i_cols, max_col_chars=95)

    doc.save(str(OUT_DOCX))
    print(f"\nSaved: {OUT_DOCX}")
    print(f"Saved: {PROC / 'influential_reviews_by_global_citations.csv'}")
    print(f"Saved: {PROC / 'influential_reviews_by_internal_citations.csv'}")

    # Echo a quick preview of the two ranked tables to stdout
    print("\nTop 10 by GLOBAL citations:")
    for _, r in by_global.head(10).iterrows():
        print(f"  #{int(r['rank_global']):>3} [{int(r['year'])}, "
              f"cites={int(r['cited_by_count'])}, "
              f"internal={int(r['within_corpus_cite'])}]  "
              f"{truncate(r['title'], 90)}")

    print("\nTop 10 by INTERNAL citations:")
    for _, r in by_internal.head(10).iterrows():
        print(f"  #{int(r['rank_internal']):>3} [{int(r['year'])}, "
              f"internal={int(r['within_corpus_cite'])}, "
              f"cites={int(r['cited_by_count'])}]  "
              f"{truncate(r['title'], 90)}")


if __name__ == "__main__":
    main()
