"""
27_repair_institutional.py — repair the pmid attribution of mismatched
institutional full texts (see 26_verify_institutional.py: 20.8% of files
hold a different paper than their filename claims).

Strategy
--------
1. For every file whose verdict was mismatch/suspect, identify the TRUE
   paper: score all 18,891 corpus titles against the file's head text
   (fraction of the title's content words present in the head), take the
   best title if its score >= 0.70 and it beats the runner-up clearly.
2. Build a repair map old_pmid -> new_pmid (or DROP when unrecoverable).
   Collisions are resolved conservatively:
   - two files claiming the same new_pmid  -> keep the best score only;
   - new_pmid already has PMC full text    -> DROP (prefer PMC);
   - new_pmid already has its own (ok)     -> DROP (duplicate);
     institutional file
   - new_pmid currently abstract-only      -> reassign, and REMOVE the
     now-superseded abstract rows.
3. Apply the map to the canonical CSVs (pmid + year re-keyed), to
   v18/sentence_sections_pdf.csv, and rebuild sentence_sections_all.csv.
4. Report year-shift distribution and final corpus stats; delete stale
   v18 caches derived from the corpus.

Backups: framing_full_corpus.csv.bak3 / all_sentences_full_corpus.csv.bak3
"""
from __future__ import annotations

import io
import re
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
V18 = PROC / "v18"
TXT = ROOT / "Data" / "interim" / "institutional_text"

STOP = set("the a an of and or in on for to with by from at as is are was "
           "were be been its their during after before between under over "
           "not no do does effects effect study studies analysis case "
           "report review".split())


def content_words(s: str) -> set:
    return {w for w in re.findall(r"[a-z]{3,}", str(s).lower())
            if w not in STOP}


print("Loading corpus titles ...")
raw = pd.read_csv(
    ROOT / "Data" / "raw" / "pubmed" /
    "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    usecols=["pmid", "title", "year"], low_memory=False
).drop_duplicates("pmid")
raw["year"] = pd.to_numeric(raw.year, errors="coerce")
title_words = {int(r.pmid): content_words(r.title)
               for r in raw.itertuples() if isinstance(r.title, str)}
title_words = {p: w for p, w in title_words.items() if len(w) >= 3}
year_of = dict(zip(raw.pmid, raw.year))

qc = pd.read_csv(V18 / "institutional_title_check.csv")
to_fix = qc[qc.verdict.isin(["mismatch", "suspect"])].pmid.tolist()
print(f"Files to re-identify: {len(to_fix):,}")

# ── 1. identify true pmid per file ───────────────────────────────────────
candidates = list(title_words.items())
results = []
for i, old_pmid in enumerate(to_fix):
    if i % 100 == 0:
        print(f"  {i}/{len(to_fix)}")
    f = TXT / f"{old_pmid}.txt"
    if not f.exists():
        results.append((old_pmid, None, 0.0, "no_file"))
        continue
    head_words = content_words(f.read_text(encoding="utf-8",
                                            errors="replace")[:3000])
    best, best_s, second_s = None, 0.0, 0.0
    for pmid, words in candidates:
        s = len(words & head_words) / len(words)
        if s > best_s:
            best, best_s, second_s = pmid, s, best_s
        elif s > second_s:
            second_s = s
    if best_s >= 0.70 and (best == old_pmid or best_s - second_s >= 0.05
                            or best_s >= 0.9):
        results.append((old_pmid, best, best_s,
                        "confirmed" if best == old_pmid else "reassigned"))
    else:
        results.append((old_pmid, None, best_s, "unrecoverable"))

ident = pd.DataFrame(results, columns=["old_pmid", "new_pmid", "score",
                                        "status"])
print("\nIdentification outcomes:")
print(ident.status.value_counts().to_string())

# ── 2. build repair map with collision handling ──────────────────────────
sent = pd.read_csv(PROC / "all_sentences_full_corpus.csv",
                   low_memory=False)
src_of = sent.groupby("pmid").source.agg(set)

ok_inst = set(qc[qc.verdict == "ok"].pmid)
confirmed_set = set(ident[ident.status == "confirmed"].old_pmid)
reassign = ident[ident.status == "reassigned"].sort_values(
    "score", ascending=False)
repair, drops, claimed = {}, set(), set()
n_pmc_conflict = n_own_conflict = n_dupe_claim = 0
for r in reassign.itertuples():
    new = int(r.new_pmid)
    if new in claimed:
        drops.add(r.old_pmid); n_dupe_claim += 1
        continue
    tgt_src = src_of.get(new, set())
    if "fulltext" in tgt_src:
        # true paper already covered by PMC full text — prefer PMC
        drops.add(r.old_pmid); n_pmc_conflict += 1
        continue
    if new in ok_inst or new in confirmed_set:
        # true paper already has its own correct institutional file
        drops.add(r.old_pmid); n_own_conflict += 1
        continue
    repair[r.old_pmid] = new
    claimed.add(new)
for r in ident[ident.status.isin(["unrecoverable", "no_file"])].itertuples():
    drops.add(r.old_pmid)

print(f"\nRepair map: {len(repair):,} reassigned | {len(drops):,} dropped "
      f"(PMC-conflict {n_pmc_conflict}, own-file-conflict {n_own_conflict}, "
      f"duplicate-claim {n_dupe_claim})")
confirmed = set(ident[ident.status == "confirmed"].old_pmid)
print(f"Confirmed-as-correct despite QC flag: {len(confirmed):,}")

pd.DataFrame([{"old_pmid": k, "new_pmid": v} for k, v in repair.items()]
             ).to_csv(V18 / "institutional_repair_map.csv", index=False)

# year-shift diagnostics
shifts = [abs(year_of.get(new, np.nan) - year_of.get(old, np.nan))
          for old, new in repair.items()]
shifts = pd.Series([s for s in shifts if np.isfinite(s)])
print(f"\nYear shift |new-old| for reassigned papers: median "
      f"{shifts.median():.0f}, mean {shifts.mean():.1f}, "
      f"<=2y: {100 * (shifts <= 2).mean():.0f}%, "
      f">5y: {100 * (shifts > 5).mean():.0f}%")

# ── 3. apply to canonical CSVs ───────────────────────────────────────────
abstract_superseded = {new for new in repair.values()
                       if src_of.get(new) == {"abstract"}}
print(f"Papers whose abstract-only rows are superseded by recovered "
      f"full text: {len(abstract_superseded):,}")

for name in ["all_sentences_full_corpus.csv", "framing_full_corpus.csv"]:
    path = PROC / name
    shutil.copy(path, path.with_suffix(".csv.bak3"))
    df = sent if name.startswith("all_") else pd.read_csv(path,
                                                           low_memory=False)
    n0 = len(df)
    inst = df.source == "institutional"
    df = df[~(inst & df.pmid.isin(drops))]
    df = df[~((df.source == "abstract")
              & df.pmid.isin(abstract_superseded))]
    inst = df.source == "institutional"
    remap = inst & df.pmid.isin(repair)
    df.loc[remap, "pmid"] = df.loc[remap, "pmid"].map(repair)
    df.loc[remap, "year"] = df.loc[remap, "pmid"].map(year_of)
    df = df.dropna(subset=["year"])
    df["year"] = df.year.astype(int)
    df.to_csv(path, index=False)
    print(f"{name}: {n0:,} -> {len(df):,} rows "
          f"({int(remap.sum()):,} re-keyed)")

# ── 4. re-key section maps + drop stale caches ───────────────────────────
pdfmap = pd.read_csv(V18 / "sentence_sections_pdf.csv")
n0 = len(pdfmap)
pdfmap = pdfmap[~pdfmap.pmid.isin(drops)]
pdfmap["pmid"] = pdfmap.pmid.map(lambda p: repair.get(p, p))
pdfmap.to_csv(V18 / "sentence_sections_pdf.csv", index=False)
pmcmap = pd.read_csv(V18 / "sentence_sections_fulltext.csv")
pd.concat([pmcmap.assign(src="pmc"), pdfmap.assign(src="pdf")],
          ignore_index=True).to_csv(V18 / "sentence_sections_all.csv",
                                     index=False)
print(f"sentence_sections_pdf: {n0:,} -> {len(pdfmap):,}")

for stale in ["paper_techniques.csv"]:
    p = V18 / stale
    if p.exists():
        p.unlink()
        print(f"deleted stale cache v18/{stale}")

print("\nDone. Re-run v18_figures.py --figs all next.")
