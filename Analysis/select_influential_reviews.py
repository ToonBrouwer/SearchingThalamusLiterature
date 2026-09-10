"""
select_influential_reviews.py — Pick the top-K most influential review /
perspective / meta-analysis papers per year for downstream LLM frame
classification.

Inputs
------
    Data/raw/pubmed/pubmed_broad_title_only_*.csv       publication_types
    Data/processed/citation_data.csv                    global cited_by_count
    Data/processed/citation_graph.graphml               within-corpus edges
    Data/interim/fulltext/pmc_fulltext_*.csv            PMC fulltext
    Data/interim/institutional_text/{pmid}.txt          LibKey fulltext

Selection
---------
1. Candidates = any paper whose PubMed `publication_types` contains "Review",
   "Systematic Review", or "Meta-Analysis" — OR whose title matches
   /perspective|opinion|hypothesis|essay/i. Editorials and Comments are
   excluded unless they also match one of the above.

2. Influence score (z-scored across the whole candidate pool, then averaged):
     z_global   = zscore(log1p(cited_by_count))
     z_internal = zscore(within_corpus_indegree)
     composite  = 0.5 * z_global + 0.5 * z_internal

   Also computed but not used for ranking (informational):
     per_year   = cited_by_count / max(1, years_since_publication)

3. Top-K (default 3) per publication year.

Outputs
-------
    Data/processed/influential_reviews.csv
        all candidates, ranked by composite — not only the top picks
    Data/processed/influential_reviews_selected.csv
        top-K per year
    Data/processed/influential_reviews_for_llm/
        {year}_{pmid}_{slug}.txt   one file per selected paper; header
        contains the metadata block, body contains full text (if available)
        or abstract (fallback).

Usage:
    python analysis/select_influential_reviews.py
    python analysis/select_influential_reviews.py --top-k 5 --start-year 1980
"""
from __future__ import annotations
import argparse, os, sys, io, re, json
os.environ["PYTHONIOENCODING"] = "utf-8"
# Only rewrap stdout when this file is executed directly (keeps the helpers
# importable by parallel scripts like select_influential_nonreviews.py).
if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                    errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
from collections import Counter
import datetime as _dt

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "Data" / "raw" / "pubmed"
PROC = ROOT / "Data" / "processed"
INTERIM = ROOT / "Data" / "interim"

OUT_CSV_ALL = PROC / "influential_reviews.csv"
OUT_CSV_SEL = PROC / "influential_reviews_selected.csv"
OUT_DIR = PROC / "influential_reviews_for_llm"


# ─────────────────────────────────────────────────────────────────────────
# Loading
# ─────────────────────────────────────────────────────────────────────────
def load_pubmed() -> pd.DataFrame:
    """Load the main corpus CSV. Prefers the full 1950_2026 file over any
    smaller test/backup variants that share the prefix."""
    # Prefer the full-range file; fall back to any broad_title_only; never
    # pick up _BACKUP/_metadata.
    candidates = [
        p for p in RAW.glob("pubmed_broad_title_only_*.csv")
        if "_BACKUP" not in p.name and "_metadata" not in p.name
    ]
    # Order: files with "1950_2026" in name take priority, then by file size
    candidates.sort(key=lambda p: (
        0 if "1950_2026" in p.name else 1,
        -p.stat().st_size,
    ))
    if not candidates:
        raise SystemExit(f"No PubMed file in {RAW}")
    p = candidates[0]
    print(f"Loading PubMed: {p.name} ({p.stat().st_size / 1e6:.1f} MB)")
    return pd.read_csv(p, low_memory=False)


def load_citations() -> pd.DataFrame:
    p = PROC / "citation_data.csv"
    print(f"Loading citations: {p.name}")
    return pd.read_csv(p, low_memory=False)


def load_internal_edges() -> "pd.DataFrame":
    """Edges from the citation graph. Returns DataFrame (source_pmid, target_pmid).

    Uses networkx rather than python-igraph because our citation_graph.graphml
    has a duplicate-attribute quirk that trips igraph's stricter parser.
    """
    gml = PROC / "citation_graph.graphml"
    if not gml.exists():
        print(f"  [warning] no citation_graph.graphml — within-corpus score will be 0")
        return pd.DataFrame(columns=["source", "target"])
    import networkx as nx
    g = nx.read_graphml(str(gml))
    rows = []
    for u, v in g.edges():
        try:
            rows.append((int(u), int(v)))
        except (TypeError, ValueError):
            # node ids that aren't integer pmids — skip
            continue
    print(f"  loaded {len(rows):,} citation edges from graph ({g.number_of_nodes():,} nodes)")
    return pd.DataFrame(rows, columns=["source", "target"])


def load_pmc_fulltext() -> dict[int, str]:
    """Map pmid → full-text string from PMC."""
    path = sorted((INTERIM / "fulltext").glob("pmc_fulltext_*.csv"))
    if not path:
        return {}
    p = path[-1]
    print(f"Loading PMC fulltext: {p.name}")
    df = pd.read_csv(p, low_memory=False, usecols=["pmid", "full_text"])
    df = df.dropna(subset=["full_text"])
    df = df[df["full_text"].str.len() > 500]
    out = dict(zip(df["pmid"].astype(int), df["full_text"]))
    print(f"  {len(out):,} papers with PMC fulltext ≥ 500 chars")
    return out


def load_institutional_text() -> set[int]:
    """Return set of pmids that have a LibKey-extracted text file."""
    d = INTERIM / "institutional_text"
    if not d.exists():
        return set()
    pmids = set()
    for f in d.glob("*.txt"):
        try:
            pmids.add(int(f.stem))
        except ValueError:
            pass
    print(f"Found {len(pmids):,} LibKey text files in {d}")
    return pmids


def read_institutional_text(pmid: int) -> str | None:
    f = INTERIM / "institutional_text" / f"{pmid}.txt"
    if not f.exists():
        return None
    try:
        return f.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────
# Candidate filter
# ─────────────────────────────────────────────────────────────────────────
REVIEW_PT_RE = re.compile(
    r"\b(Review|Systematic Review|Meta[- ]Analysis)\b", re.IGNORECASE
)
NON_REVIEW_PT_RE = re.compile(
    r"\b(Comment|Editorial|Letter|News|Erratum|Retraction|Biography)\b",
    re.IGNORECASE
)
PERSPECTIVE_TITLE_RE = re.compile(
    r"\b(perspective|opinion|hypothesis|viewpoint|essay|commentary)\b",
    re.IGNORECASE
)


def tag_reviews(raw: pd.DataFrame) -> pd.DataFrame:
    pt = raw["publication_types"].fillna("")
    title = raw["title"].fillna("")
    is_review_pt = pt.str.contains(REVIEW_PT_RE, regex=True)
    is_persp_title = title.str.contains(PERSPECTIVE_TITLE_RE, regex=True)
    is_excluded_only = (pt.str.contains(NON_REVIEW_PT_RE, regex=True)
                        & ~is_review_pt & ~is_persp_title)

    raw = raw.copy()
    raw["is_review"] = is_review_pt | is_persp_title
    raw.loc[is_excluded_only, "is_review"] = False

    # Sub-type tags (not mutually exclusive)
    raw["pt_review"] = pt.str.contains(r"\bReview\b", case=False, regex=True)
    raw["pt_systematic"] = pt.str.contains(r"\bSystematic Review\b", case=False,
                                             regex=True)
    raw["pt_metaanalysis"] = pt.str.contains(r"\bMeta[- ]Analysis\b", case=False,
                                               regex=True)
    raw["title_perspective"] = is_persp_title
    return raw


# ─────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────
def zscore(s: pd.Series) -> pd.Series:
    mu = s.mean()
    sd = s.std(ddof=0)
    return (s - mu) / sd if sd > 0 else pd.Series(0.0, index=s.index)


def score_candidates(candidates: pd.DataFrame,
                     cit: pd.DataFrame,
                     edges: pd.DataFrame,
                     current_year: int) -> pd.DataFrame:
    # merge global citations
    df = candidates.merge(
        cit[["pmid", "cited_by_count", "openalex_id", "n_references"]],
        on="pmid", how="left")
    df["cited_by_count"] = df["cited_by_count"].fillna(0)

    # Within-corpus in-degree (how many OTHER papers in the corpus cite this one)
    if len(edges):
        corpus_pmids = set(candidates["pmid"].astype(int))
        # Edge convention: source cites target
        in_deg = Counter(edges["target"].astype(int))
        df["within_corpus_cite"] = df["pmid"].map(lambda p: in_deg.get(int(p), 0))
    else:
        df["within_corpus_cite"] = 0

    # Years since publication — guard against zero / future years
    yrs = (current_year - df["year"]).clip(lower=1)
    df["per_year_accrual"] = df["cited_by_count"] / yrs

    # Composite z-score (log-transformed for heavy tails)
    df["z_global"] = zscore(np.log1p(df["cited_by_count"]))
    df["z_internal"] = zscore(df["within_corpus_cite"].astype(float))
    df["composite"] = 0.5 * df["z_global"] + 0.5 * df["z_internal"]

    return df.sort_values("composite", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────
# Fulltext resolution + output
# ─────────────────────────────────────────────────────────────────────────
def resolve_fulltext(pmid: int, pmc_map: dict[int, str],
                      inst_pmids: set[int]) -> tuple[str, str]:
    """Returns (source_tag, text). source_tag ∈ {pmc, libkey, abstract_only}."""
    if pmid in pmc_map:
        return "pmc", pmc_map[pmid]
    if pmid in inst_pmids:
        t = read_institutional_text(pmid) or ""
        if len(t) > 500:
            return "libkey", t
    return "abstract_only", ""


def slugify(text: str, maxlen: int = 60) -> str:
    t = re.sub(r"[^\w\s-]", "", str(text).lower()).strip()
    t = re.sub(r"[\s_-]+", "-", t)
    return t[:maxlen]


def write_llm_file(out_dir: Path, row: pd.Series, source_tag: str, body: str):
    fname = f"{int(row['year'])}_{int(row['pmid'])}_{slugify(row['title'])}.txt"
    fpath = out_dir / fname
    header = [
        f"# {row['title']}",
        f"PMID: {int(row['pmid'])}",
        f"Year: {int(row['year'])}",
        f"Journal: {row.get('journal', '')}",
        f"Publication types: {row.get('publication_types', '')}",
        f"Global citations (OpenAlex): {int(row.get('cited_by_count', 0))}",
        f"Within-corpus citations: {int(row.get('within_corpus_cite', 0))}",
        f"Composite score: {row.get('composite', float('nan')):.2f}",
        f"Fulltext source: {source_tag}",
        "",
    ]
    if source_tag == "abstract_only":
        header += ["[NO FULLTEXT AVAILABLE — abstract only]", ""]
        body = str(row.get("abstract", "") or "[no abstract]")
    fpath.write_text("\n".join(header) + "\n" + body, encoding="utf-8")
    return fpath


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-k", type=int, default=3,
                        help="Top-K candidates per year")
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--min-citations", type=int, default=0,
                        help="Drop candidates with fewer global citations")
    parser.add_argument("--write-texts", action="store_true", default=True,
                        help="Write one .txt per selected paper (default on)")
    parser.add_argument("--no-texts", dest="write_texts", action="store_false")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Load everything
    raw = load_pubmed()
    cit = load_citations()
    edges = load_internal_edges()
    pmc = load_pmc_fulltext()
    inst = load_institutional_text()

    # ── Filter reviews
    raw = tag_reviews(raw)
    reviews = raw[raw["is_review"]].copy()
    reviews = reviews[(reviews["year"] >= args.start_year)
                      & (reviews["year"] <= args.end_year)]
    print(f"\nCandidate reviews: {len(reviews):,} "
          f"(years {args.start_year}–{args.end_year})")
    print(f"  PT=Review: {reviews['pt_review'].sum()}, "
          f"Systematic: {reviews['pt_systematic'].sum()}, "
          f"Meta-Analysis: {reviews['pt_metaanalysis'].sum()}, "
          f"Title=perspective/opinion/etc: {reviews['title_perspective'].sum()}")

    # ── Score
    current_year = max(args.end_year, _dt.date.today().year)
    scored = score_candidates(reviews, cit, edges, current_year)
    if args.min_citations > 0:
        scored = scored[scored["cited_by_count"] >= args.min_citations]

    # ── Resolve fulltext availability for EVERY candidate
    scored["fulltext_source"] = scored["pmid"].map(
        lambda p: resolve_fulltext(int(p), pmc, inst)[0]
    )

    # Save full ranked manifest
    keep_cols = ["pmid", "year", "journal", "title", "publication_types",
                  "pt_review", "pt_systematic", "pt_metaanalysis", "title_perspective",
                  "cited_by_count", "within_corpus_cite", "per_year_accrual",
                  "z_global", "z_internal", "composite",
                  "fulltext_source"]
    keep_cols = [c for c in keep_cols if c in scored.columns]
    scored[keep_cols].to_csv(OUT_CSV_ALL, index=False)
    print(f"\nSaved full manifest: {OUT_CSV_ALL.name} ({len(scored):,} rows)")

    # ── Select top-K per year
    sel = (scored.sort_values(["year", "composite"], ascending=[True, False])
                   .groupby("year", as_index=False)
                   .head(args.top_k)
                   .reset_index(drop=True))
    sel.to_csv(OUT_CSV_SEL, index=False)
    print(f"Saved top-{args.top_k}/year selection: {OUT_CSV_SEL.name} "
          f"({len(sel):,} rows)")

    # Coverage by decade
    sel["decade"] = (sel["year"] // 10) * 10
    by_dec = (sel.groupby(["decade", "fulltext_source"]).size()
                 .unstack(fill_value=0))
    print("\nCoverage by decade × fulltext source:")
    print(by_dec.to_string())

    # ── Write text files (with marker)
    if args.write_texts:
        print(f"\nWriting text files to {OUT_DIR} ...")
        n_written = Counter()
        for _, row in sel.iterrows():
            src, body = resolve_fulltext(int(row["pmid"]), pmc, inst)
            # Need abstract for the fallback
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

    # ── Print top picks per decade (quick-look)
    print("\nTop-3 composite pick per decade (illustrative):")
    dec = sel.copy()
    dec = dec.sort_values(["decade", "composite"], ascending=[True, False])
    for d, group in dec.groupby("decade"):
        top = group.iloc[0]
        print(f"  {int(d)}s  [{int(top['year'])}] "
              f"[citations={int(top['cited_by_count'])}, "
              f"internal={int(top['within_corpus_cite'])}]  "
              f"{top['title'][:95]}")


if __name__ == "__main__":
    main()
