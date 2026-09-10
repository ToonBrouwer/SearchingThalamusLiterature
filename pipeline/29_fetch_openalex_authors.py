"""
29_fetch_openalex_authors.py — fetch disambiguated AUTHOR records for the
corpus from OpenAlex, for the Planck's-principle analysis.

Why OpenAlex rather than PubMed: OpenAlex supplies *disambiguated* author
IDs (A-numbers). Tracking "the same person across 50 years" from PubMed
name strings ("J Smith") is not defensible; author-ID disambiguation is
the whole basis of the cohort-vs-conversion test.

Input : Data/processed/openalex_cache.json  (pmid -> {openalex_id, ...})
Output: Data/processed/v18/paper_authors_openalex.csv
        columns: pmid, year, author_id, author_name, position, n_authors

Batched 50 works per request via filter=openalex_id:W1|W2|...,
select=id,publication_year,authorships. Checkpoints every 25 batches so
an interrupted run resumes cheaply. No mailto is sent (the polite-pool
header would mean transmitting a personal address to a third party).
"""
from __future__ import annotations

import io
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
V18 = PROC / "v18"
V18.mkdir(exist_ok=True)
OUT = V18 / "paper_authors_openalex.csv"
CKPT = V18 / "_authors_partial.json"

API = "https://api.openalex.org/works"
BATCH = 50

cache = json.loads((PROC / "openalex_cache.json").read_text(
    encoding="utf-8"))
work_to_pmid = {}
for pmid, rec in cache.items():
    if isinstance(rec, dict) and rec.get("openalex_id"):
        wid = str(rec["openalex_id"]).rsplit("/", 1)[-1]
        if wid.startswith("W"):
            work_to_pmid[wid] = int(pmid)
print(f"Works with an OpenAlex id: {len(work_to_pmid):,}")

done = {}
if CKPT.exists():
    done = json.loads(CKPT.read_text(encoding="utf-8"))
    print(f"Resuming: {len(done):,} works already fetched")

todo = [w for w in work_to_pmid if w not in done]
print(f"To fetch: {len(todo):,} works in {len(todo)//BATCH + 1:,} batches")

session = requests.Session()
session.headers.update({"User-Agent": "thalamus-framing-research/1.0"})

for bi in range(0, len(todo), BATCH):
    chunk = todo[bi:bi + BATCH]
    params = {
        "filter": "openalex_id:" + "|".join(chunk),
        "select": "id,publication_year,authorships",
        "per-page": BATCH,
    }
    for attempt in range(5):
        try:
            r = session.get(API, params=params, timeout=60)
            if r.status_code == 200:
                break
            if r.status_code in (429, 500, 502, 503):
                time.sleep(2 ** attempt)
                continue
            print(f"  HTTP {r.status_code} on batch {bi//BATCH}")
            break
        except requests.RequestException as e:
            print(f"  retry {attempt} after {type(e).__name__}")
            time.sleep(2 ** attempt)
    else:
        continue
    if r.status_code != 200:
        continue
    for w in r.json().get("results", []):
        wid = str(w["id"]).rsplit("/", 1)[-1]
        done[wid] = {
            "year": w.get("publication_year"),
            "authors": [
                {
                    "id": (a.get("author") or {}).get("id", "") or "",
                    "name": (a.get("author") or {}).get("display_name",
                                                          "") or "",
                    "pos": a.get("author_position", ""),
                }
                for a in (w.get("authorships") or [])
            ],
        }
    if (bi // BATCH) % 25 == 0:
        CKPT.write_text(json.dumps(done), encoding="utf-8")
        print(f"  batch {bi//BATCH}/{len(todo)//BATCH}  "
              f"({len(done):,} works)")
    time.sleep(0.12)

CKPT.write_text(json.dumps(done), encoding="utf-8")
print(f"\nFetched {len(done):,} works")

rows = []
for wid, rec in done.items():
    pmid = work_to_pmid.get(wid)
    if pmid is None:
        continue
    auths = rec.get("authors") or []
    n = len(auths)
    for a in auths:
        aid = str(a.get("id", "")).rsplit("/", 1)[-1]
        if not aid:
            continue
        rows.append((pmid, rec.get("year"), aid, a.get("name", ""),
                     a.get("pos", ""), n))

df = pd.DataFrame(rows, columns=["pmid", "year", "author_id",
                                  "author_name", "position", "n_authors"])
df.to_csv(OUT, index=False)
print(f"Rows: {len(df):,} | papers: {df.pmid.nunique():,} | "
      f"distinct authors: {df.author_id.nunique():,}")
print(df.position.value_counts().to_string())
print(f"Saved: {OUT}")
