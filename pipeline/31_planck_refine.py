"""
31_planck_refine.py — corrected modelling + sensitivity for the
Planck's-principle analysis.

Fixes to 30_planck_test.py:
  * The mixed model there was unidentified: cohort is a property of the
    author, so with author random effects it is a pure between-group
    term, and the mean group size is 1.2 papers (most authors contribute
    one framing paper). Replaced with a logistic regression estimated
    WITHIN a fixed publication era, where cohort is identified.
  * Explicit treatment of the age-period-cohort (APC) identification
    problem: publication year = cohort + career age exactly, so the three
    linear effects cannot be estimated simultaneously. We therefore rely
    on (a) the decomposition and (b) era-stratified cohort comparisons,
    neither of which requires the unidentified three-way model.
  * Sensitivity of the conversion/replacement decomposition to the choice
    of period split, and to using first authors instead of last.
  * Left-censoring check on cohort assignment.

Output: v18/planck_refined.txt
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "Code"))
sys.path.insert(0, str(ROOT / "Code" / "paper"))
import v18_data as D

V18 = D.PROC / "v18"
LOG = []


def say(s=""):
    print(s)
    LOG.append(str(s))


fr = D.load_framing()
rh = fr[fr.frame_type.isin(["relay", "hub"])]
paper = (rh.assign(is_hub=(rh.frame_type == "hub").astype(int))
         .groupby(["pmid", "year"])
         .agg(n_hub=("is_hub", "sum"), n_frames=("is_hub", "size"))
         .reset_index())
paper["hub_share"] = paper.n_hub / paper.n_frames
paper["hub_dominant"] = (paper.hub_share > 0.5).astype(int)

au = pd.read_csv(V18 / "paper_authors_openalex.csv").dropna(
    subset=["author_id"])
cohort = au.groupby("author_id").year.min().rename("cohort")


def build(position):
    a = au[au.position == position][["pmid", "author_id", "author_name"]]
    a = a.drop_duplicates("pmid")
    d = paper.merge(a, on="pmid").merge(cohort, left_on="author_id",
                                         right_index=True)
    d["career_age"] = d.year - d.cohort
    return d


last = build("last")
first = build("first")

# ── Left-censoring check ─────────────────────────────────────────────────
say("=" * 70)
say("0. COHORT VALIDITY — left-censoring check")
say("=" * 70)
say("Cohort = first year the author appears anywhere in the corpus. "
    "Authors already active before the corpus starts (1950) are "
    "left-censored, i.e. their true entry is earlier than recorded.")
for name, d in [("last", last), ("first", first)]:
    at_floor = (d.cohort <= 1955).mean() * 100
    say(f"  {name:5s} authors: {at_floor:.1f}% of framing papers have a "
        f"cohort at/near the 1950 floor  (n={len(d):,})")
say("Framing papers are overwhelmingly post-1990, so censoring affects a "
    "small minority; results below are also reported for 2010+ only, "
    "where censoring is negligible.")

# ── APC statement ────────────────────────────────────────────────────────
say("\n" + "=" * 70)
say("1. IDENTIFICATION — the age-period-cohort problem")
say("=" * 70)
r = np.corrcoef(last.year, last.cohort)[0, 1]
say(f"corr(publication year, author cohort) = {r:.2f} in the framing set.")
say("Because year = cohort + career_age exactly, the linear effects of "
    "period, cohort and career age are not jointly identified. We do NOT "
    "fit that model. Instead we (a) hold the publication era fixed and "
    "compare cohorts, and (b) decompose the field-level change into "
    "within-author and between-author parts.")

# ── Era-stratified cohort effect (identified) ────────────────────────────
say("\n" + "=" * 70)
say("2. COHORT EFFECT WITH PUBLICATION ERA HELD FIXED")
say("=" * 70)
import statsmodels.api as sm
for era_lo, era_hi in [(2010, 2025), (2015, 2025), (2000, 2009)]:
    d = last[(last.year >= era_lo) & (last.year <= era_hi)].copy()
    if len(d) < 60:
        continue
    d = d[d.cohort >= 1970]
    X = sm.add_constant(pd.DataFrame({
        "cohort_dec": (d.cohort - 2000) / 10.0,
        "yr_c": (d.year - (era_lo + era_hi) / 2) / 10.0}))
    try:
        fit = sm.GLM(d.hub_dominant, X,
                     family=sm.families.Binomial()).fit()
        or_ = np.exp(fit.params["cohort_dec"])
        ci = np.exp(fit.conf_int().loc["cohort_dec"])
        say(f"  Papers {era_lo}-{era_hi} (n={len(d):,}): "
            f"OR per decade of LATER author entry = {or_:.2f} "
            f"[{ci[0]:.2f}-{ci[1]:.2f}], p={fit.pvalues['cohort_dec']:.2g}")
    except Exception as e:
        say(f"  {era_lo}-{era_hi}: model failed ({e})")

d = first[(first.year >= 2010)].copy()
d = d[d.cohort >= 1970]
X = sm.add_constant(pd.DataFrame({
    "cohort_dec": (d.cohort - 2000) / 10.0,
    "yr_c": (d.year - 2017) / 10.0}))
fit = sm.GLM(d.hub_dominant, X, family=sm.families.Binomial()).fit()
say(f"  [FIRST-author robustness] papers 2010+ (n={len(d):,}): "
    f"OR per decade = {np.exp(fit.params['cohort_dec']):.2f}, "
    f"p={fit.pvalues['cohort_dec']:.2g}")

# ── Decomposition sensitivity ────────────────────────────────────────────
say("\n" + "=" * 70)
say("3. DECOMPOSITION — sensitivity to the period split and to position")
say("=" * 70)
say(f"{'split':>18} {'pos':>6} {'cont.':>6} {'total':>8} "
    f"{'convert':>9} {'replace':>9} {'%conv':>6}")
for lo1, hi1, lo2, hi2 in [(1990, 2007, 2008, 2025),
                            (1995, 2009, 2010, 2025),
                            (1990, 2004, 2005, 2019),
                            (2000, 2011, 2012, 2025)]:
    for name, d in [("last", last), ("first", first)]:
        p1 = d[(d.year >= lo1) & (d.year <= hi1)]
        p2 = d[(d.year >= lo2) & (d.year <= hi2)]
        if len(p1) < 40 or len(p2) < 40:
            continue
        a1, a2 = set(p1.author_id), set(p2.author_id)
        cont, entr, exi = a1 & a2, a2 - a1, a1 - a2
        c1 = p1[p1.author_id.isin(cont)]
        c2 = p2[p2.author_id.isin(cont)]
        e2 = p2[p2.author_id.isin(entr)]
        x1 = p1[p1.author_id.isin(exi)]
        if not (len(c1) and len(c2) and len(e2) and len(x1)):
            continue
        total = 100 * (p2.hub_dominant.mean() - p1.hub_dominant.mean())
        conv = 100 * (c2.hub_dominant.mean() - c1.hub_dominant.mean())
        repl = 100 * (e2.hub_dominant.mean() - x1.hub_dominant.mean())
        pc = 100 * abs(conv) / (abs(conv) + abs(repl)) if (
            abs(conv) + abs(repl)) else np.nan
        say(f"{lo1}-{hi1}/{lo2}-{hi2:>4} {name:>6} {len(cont):>6} "
            f"{total:>+7.1f} {conv:>+8.1f} {repl:>+8.1f} {pc:>5.0f}%")
say("\n(cont. = authors publishing framing papers in BOTH periods; the "
    "conversion estimate rests on this small group — the key limitation.)")

# ── Effect size in plain terms ───────────────────────────────────────────
say("\n" + "=" * 70)
say("4. PLAIN-LANGUAGE EFFECT SIZE (papers published 2010 or later)")
say("=" * 70)
d = last[last.year >= 2010]
for lo, hi, lab in [(1970, 1999, "entered field before 2000"),
                    (2000, 2009, "entered 2000s"),
                    (2010, 2025, "entered 2010 or later")]:
    s = d[(d.cohort >= lo) & (d.cohort <= hi)]
    if len(s):
        k = int(s.hub_dominant.sum())
        clo, chi = D.wilson_ci(k, len(s))
        say(f"  {lab:26s}: {100*s.hub_dominant.mean():5.1f}% hub-dominant "
            f"[{100*clo:.1f}-{100*chi:.1f}]  (n={len(s):,})")

(V18 / "planck_refined.txt").write_text("\n".join(LOG), encoding="utf-8")
print(f"\nSaved: {V18/'planck_refined.txt'}")
