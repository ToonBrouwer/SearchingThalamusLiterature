"""
28_abstract_only_robustness.py — composition-free replication of the
framing trends using ABSTRACTS ONLY.

Motivation
----------
The pooled corpus mixes text sources whose availability changes sharply
over time (1990s: 50% of sentences from abstracts; 2020s: 76% from PMC
full text). Full text yields more framing sentences per paper than
abstracts do, so BOTH the rising framing-detection rate and any frame
trend estimated on the pooled corpus are confounded by source
composition.

This script rebuilds the framing analysis on a corpus whose availability
is (near-)uniform across decades: the PubMed abstract of every corpus
paper, taken straight from the raw PubMed export and independent of the
sentence pipeline. NOTE this is deliberately NOT the `source ==
"abstract"` stream of the canonical corpus, which contains only papers
whose full-text retrieval FAILED (a biased subset).

Outputs (Data/processed/v18/):
    abstract_framing.csv          one row per abstract framing sentence
    abstract_robustness_stats.csv per-decade rates + MK trends
Prints a side-by-side comparison with the pooled-corpus estimates.
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "Code"))
sys.path.insert(0, str(ROOT / "Code" / "paper"))

import nltk
from taxonomy import detect_frame
import v18_data as D

PROC = ROOT / "Data" / "processed"
V18 = PROC / "v18"

try:
    nltk.sent_tokenize("test.")
except LookupError:
    nltk.download("punkt"); nltk.download("punkt_tab")

# ── 1. Uniform abstract corpus ───────────────────────────────────────────
raw = pd.read_csv(
    ROOT / "Data" / "raw" / "pubmed" /
    "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    usecols=["pmid", "year", "abstract"], low_memory=False
).drop_duplicates("pmid")
raw["year"] = pd.to_numeric(raw.year, errors="coerce")
raw = raw.dropna(subset=["year"])
raw["year"] = raw.year.astype(int)
raw = raw[(raw.year >= 1950) & (raw.year <= 2025)]
raw["abstract"] = raw.abstract.fillna("")
raw["has_abs"] = raw.abstract.str.len() >= 100
raw["decade"] = (raw.year // 10) * 10

print("=== Abstract availability (the uniformity check) ===")
av = raw.groupby("decade").agg(papers=("pmid", "size"),
                               with_abstract=("has_abs", "sum"))
av["pct"] = (100 * av.with_abstract / av.papers).round(0)
print(av.to_string())

# ── 2. Frame detection over abstract sentences ───────────────────────────
rows = []
sub = raw[raw.has_abs]
print(f"\nScanning {len(sub):,} abstracts ...")
for i, r in enumerate(sub.itertuples()):
    if i % 2000 == 0:
        print(f"  {i}/{len(sub)}")
    for s in nltk.sent_tokenize(re.sub(r"\s+", " ", r.abstract)):
        if "thalam" not in s.lower() or not (20 <= len(s) <= 600):
            continue
        f = detect_frame(s)
        rows.append((r.pmid, r.year, r.decade, s, f if f else ""))

sent = pd.DataFrame(rows, columns=["pmid", "year", "decade", "sentence",
                                    "frame_type"])
fr = sent[sent.frame_type != ""].copy()
fr.to_csv(V18 / "abstract_framing.csv", index=False)
print(f"\nThalamus sentences in abstracts : {len(sent):,} "
      f"({sent.pmid.nunique():,} papers)")
print(f"Framing sentences (abstracts)   : {len(fr):,} "
      f"({fr.pmid.nunique():,} papers)")

# ── 3. Framing DETECTION RATE per decade (the Fig-1 claim) ───────────────
print("\n=== Framing detection rate per decade — ABSTRACTS ONLY ===")
tot = sent.groupby("decade").size()
hit = fr.groupby("decade").size().reindex(tot.index, fill_value=0)
rate = 100 * hit / tot
lo, hi = D.wilson_ci(hit.values, tot.values)
stats = pd.DataFrame({"sentences": tot, "framing": hit,
                      "rate_pct": rate.round(2),
                      "ci_lo": (100 * lo).round(2),
                      "ci_hi": (100 * hi).round(2)})
print(stats.to_string())
keep = tot[tot >= 200].index          # decades with usable n
tau, p = D.mann_kendall(keep.values, rate.loc[keep].values)
print(f"MK trend in detection rate (decades with n>=200): "
      f"tau={tau:+.2f}, p={p:.3g}")

# pooled-corpus comparison
pooled = D.load_sentences(usecols=["pmid", "year", "source", "frame_type"])
pooled["decade"] = (pooled.year // 10) * 10
pt = pooled.groupby("decade").size()
ph = (pooled[pooled.frame_type.notna() & (pooled.frame_type != "")]
      .groupby("decade").size().reindex(pt.index, fill_value=0))
prate = 100 * ph / pt
cmp_rate = pd.DataFrame({"abstracts_only": rate.round(2),
                          "pooled_corpus": prate.round(2)}).dropna()
print("\nDetection rate: abstracts-only vs pooled corpus")
print(cmp_rate.to_string())

# ── 4. Relay vs hub trends, abstracts only ───────────────────────────────
print("\n=== Relay vs hub — ABSTRACTS ONLY (per year, n>=8 framing) ===")
yr_tot = fr.groupby("year").size()
years = yr_tot[yr_tot >= 8].index
out = []
for frame in ["relay", "hub", "gate", "clinical_target"]:
    k = (fr[fr.frame_type == frame].groupby("year").size()
         .reindex(yr_tot.index, fill_value=0))
    prop = (k / yr_tot).loc[years]
    tau, p = D.mann_kendall(years.values, prop.values)
    out.append({"frame": frame, "n": int(k.sum()),
                "tau_abstracts": round(tau, 2), "p_abstracts": p})
    print(f"  {frame:16s} n={int(k.sum()):4d}  tau={tau:+.2f}  p={p:.3g}")

print("\nSame frames on the POOLED corpus (for comparison):")
pf = D.load_framing()
pyr = pf.groupby("year").size()
pyears = pyr[pyr >= 8].index
for i, frame in enumerate(["relay", "hub", "gate", "clinical_target"]):
    k = (pf[pf.frame_type == frame].groupby("year").size()
         .reindex(pyr.index, fill_value=0))
    prop = (k / pyr).loc[pyears]
    tau, p = D.mann_kendall(pyears.values, prop.values)
    out[i]["tau_pooled"] = round(tau, 2)
    out[i]["p_pooled"] = p
    print(f"  {frame:16s} n={int(k.sum()):4d}  tau={tau:+.2f}  p={p:.3g}")

pd.DataFrame(out).to_csv(V18 / "abstract_robustness_stats.csv",
                          index=False)

# ── 5. Decade-level relay/hub composition, abstracts only ────────────────
print("\n=== Relay vs hub share of abstract framing, per decade ===")
dtot = fr.groupby("decade").size()
comp = pd.DataFrame({
    f: (fr[fr.frame_type == f].groupby("decade").size()
        .reindex(dtot.index, fill_value=0) / dtot * 100).round(1)
    for f in ["relay", "hub", "gate", "gateway", "clinical_target"]})
comp["n"] = dtot
print(comp.to_string())
print("\nSaved: v18/abstract_framing.csv, "
      "v18/abstract_robustness_stats.csv")
