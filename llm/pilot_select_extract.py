"""
pilot_select_extract.py — select the ~20-paper Tier-1 pilot sample and
extract each paper's INTRODUCTION + DISCUSSION text.

Sample design (agreed 2026-09-02):
- ~16 papers stratified across research domains and decades,
- + 4 influential reviews (from influential_reviews_selected.csv),
- all with institutional full text and detected intro+discussion headers
  (falls back to introduction+conclusion for reviews without a
  'discussion' section).

Outputs:
  Data/processed/v18/llm_pilot/<pmid>.txt      one file per paper
  Data/processed/v18/llm_pilot/manifest.csv    metadata incl. regex frames
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
sys.path.insert(0, str(ROOT / "Code"))
sys.path.insert(0, str(ROOT / "Code" / "paper"))
import v18_data as D

PROC = ROOT / "Data" / "processed"
OUT = PROC / "v18" / "llm_pilot"
OUT.mkdir(parents=True, exist_ok=True)
TXT = ROOT / "Data" / "interim" / "institutional_text"

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
    "|".join(f"(?P<{n}>{p})" for n, p in HEADER_PATTERNS) +
    r")\s*[:.]?\s*$", re.IGNORECASE)


def sections_of(text: str) -> dict:
    spans, pos = [], 0
    for line in text.splitlines(keepends=True):
        st = line.strip()
        if 0 < len(st) <= 45:
            m = HEADER_RE.match(line)
            if m:
                b = next(n for n, _ in HEADER_PATTERNS if m.group(n))
                spans.append((pos, b))
        pos += len(line)
    out = {}
    for i, (p, b) in enumerate(spans):
        end = spans[i + 1][0] if i + 1 < len(spans) else len(text)
        out[b] = out.get(b, "") + text[p:end]
    return out


def clean_words(text: str, max_words: int) -> str:
    words = re.sub(r"\s+", " ", text).split()
    return " ".join(words[:max_words])


# ── Eligibility: institutional text with intro + (discussion|conclusion) ─
secmap = pd.read_csv(PROC / "v18" / "sentence_sections_pdf.csv")
secs_by_pmid = secmap.groupby("pmid").section.agg(set)
eligible = set(secs_by_pmid[secs_by_pmid.map(
    lambda s: "introduction" in s and ("discussion" in s
                                        or "conclusion" in s))].index)
print(f"Eligible papers (institutional, intro + discussion/conclusion): "
      f"{len(eligible):,}")

dom = D.load_domains()
raw = pd.read_csv(
    D.RAW / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    usecols=["pmid", "title", "journal", "year"],
    low_memory=False).drop_duplicates("pmid")
raw["year"] = pd.to_numeric(raw.year, errors="coerce")
pool = (dom.merge(raw, on="pmid", how="left").dropna(subset=["year"]))
pool["year"] = pool.year.astype(int)
pool = pool[pool.pmid.isin(eligible)].copy()
pool["decade"] = (pool.year // 10) * 10

# regex frames per paper, for the post-hoc comparison (not used to select)
fr = pd.read_csv(PROC / "framing_full_corpus.csv", low_memory=False,
                 usecols=["pmid", "frame_type", "section"])
fr = fr[fr.section != "references"]
regex_frames = fr.groupby("pmid").frame_type.agg(
    lambda s: ";".join(sorted(s.value_counts().index[:4])))

# ── Stratified pick: major domains, spread decades ───────────────────────
target_domains = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 19]
rng_seed = 42
picks = []
used_decades: dict[int, int] = {}
for d_ in target_domains:
    sub = pool[pool.domain == d_]
    if sub.empty:
        continue
    # prefer decades not yet covered, to spread the sample over time
    sub = sub.sample(frac=1, random_state=rng_seed + d_)
    sub = sub.sort_values("decade",
                          key=lambda s: s.map(lambda x:
                                              used_decades.get(x, 0)))
    row = sub.iloc[0]
    used_decades[row.decade] = used_decades.get(row.decade, 0) + 1
    picks.append((row.pmid, "stratified"))

# ── + influential reviews ────────────────────────────────────────────────
rev = pd.read_csv(PROC / "influential_reviews_selected.csv",
                  low_memory=False)
rev_ids = [p for p in rev.pmid if p in eligible][:4]
picks += [(p, "review") for p in rev_ids]
print(f"Picked {len(picks)} papers "
      f"({sum(1 for _, k in picks if k == 'review')} reviews)")

# ── Extract and write ────────────────────────────────────────────────────
rows = []
for pmid, kind in picks:
    text = (TXT / f"{pmid}.txt").read_text(encoding="utf-8",
                                            errors="replace")
    secs = sections_of(text)
    intro = clean_words(secs.get("introduction", ""), 1100)
    disc = clean_words(secs.get("discussion", "")
                        or secs.get("conclusion", ""), 1100)
    hit = pool[pool.pmid == pmid]
    if len(hit):
        meta = hit.iloc[0]
    else:  # e.g. reviews outside the domain-clustered abstract pool
        meta = raw[raw.pmid == pmid].iloc[0].copy()
        meta["decade"] = int(meta.year) // 10 * 10
        meta["domain"] = -1
        meta["domain_name"] = "(not clustered)"
        meta["year"] = int(meta.year)
    body = (f"Title: {meta.title}\n"
            f"Year: {meta.year}\n"
            f"Journal: {meta.journal}\n\n"
            f"=== INTRODUCTION ===\n{intro}\n\n"
            f"=== DISCUSSION ===\n{disc}\n")
    (OUT / f"{pmid}.txt").write_text(body, encoding="utf-8")
    rows.append({"pmid": pmid, "kind": kind, "year": meta.year,
                 "decade": meta.decade, "domain": int(meta.domain),
                 "domain_name": meta.domain_name,
                 "journal": meta.journal,
                 "title": str(meta.title)[:120],
                 "n_words_intro": len(intro.split()),
                 "n_words_disc": len(disc.split()),
                 "regex_frames": regex_frames.get(pmid, "")})

man = pd.DataFrame(rows)
man.to_csv(OUT / "manifest.csv", index=False)
print(man[["pmid", "kind", "year", "domain_name", "n_words_intro",
           "n_words_disc"]].to_string(index=False))
