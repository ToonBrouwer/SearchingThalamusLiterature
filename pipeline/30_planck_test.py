"""
30_planck_test.py — does the relay→hub shift happen by CONVERSION
(authors changing their framing) or by REPLACEMENT (new cohorts entering
with different framing)?

Planck's principle: "A new scientific truth does not triumph by
convincing its opponents... but rather because its opponents eventually
die and a new generation grows up that is familiar with it."

Design
------
Paper-level frame score: for every paper with >=1 relay or hub framing
sentence, hub_share = n_hub / (n_hub + n_relay); hub_dominant = share>0.5.

Author linkage (OpenAlex disambiguated IDs): LAST author is primary (the
senior/PI position — the "established figure" Planck's principle is
about); first author reported as a secondary check.

Cohort = year of the author's FIRST appearance in this corpus, i.e. entry
into the thalamus field (left-censored at 1950; acknowledged as a
limitation).

Four analyses
  1. Cohort gradient      — hub framing by author entry-cohort, controlling
                            for publication year.
  2. Within-author change — authors with framing papers >=8 years apart:
                            do individuals convert?
  3. Decomposition        — split the field-level change between periods
                            into continuing-author change (conversion) vs
                            entrant-minus-exiter difference (replacement).
  4. Mixed model          — hub_dominant ~ pub_year + cohort + (1|author).

Outputs: v18/planck_paper_frames.csv, v18/planck_results.txt
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


# ── 1. Paper-level frame scores ──────────────────────────────────────────
fr = D.load_framing()
rh = fr[fr.frame_type.isin(["relay", "hub"])]
paper = (rh.assign(is_hub=(rh.frame_type == "hub").astype(int))
         .groupby(["pmid", "year"])
         .agg(n_hub=("is_hub", "sum"), n_frames=("is_hub", "size"))
         .reset_index())
paper["hub_share"] = paper.n_hub / paper.n_frames
paper["hub_dominant"] = (paper.hub_share > 0.5).astype(int)
say(f"Papers with relay/hub framing: {len(paper):,} "
    f"({paper.hub_dominant.sum():,} hub-dominant)")

# ── 2. Author linkage ────────────────────────────────────────────────────
au = pd.read_csv(V18 / "paper_authors_openalex.csv")
au = au.dropna(subset=["author_id"])
say(f"Author rows: {len(au):,} | papers with authors: "
    f"{au.pmid.nunique():,} | distinct authors: {au.author_id.nunique():,}")

# career start = first corpus year for that author (all corpus papers,
# not just framing papers — a better estimate of field entry)
first_year = au.groupby("author_id").year.min().rename("cohort")
last_year = au.groupby("author_id").year.max().rename("last_year")
n_papers_au = au.groupby("author_id").pmid.nunique().rename("n_papers")

results_by_position = {}
for pos_name, pos_filter in [("last", "last"), ("first", "first")]:
    a = au[au.position == pos_filter][["pmid", "author_id", "author_name"]]
    a = a.drop_duplicates("pmid")
    df = paper.merge(a, on="pmid", how="inner").merge(
        first_year, left_on="author_id", right_index=True).merge(
        n_papers_au, left_on="author_id", right_index=True)
    df["career_age"] = df.year - df.cohort
    results_by_position[pos_name] = df
    say(f"\n[{pos_name} author] framing papers linked: {len(df):,} | "
        f"distinct authors: {df.author_id.nunique():,}")

df = results_by_position["last"]
df.to_csv(V18 / "planck_paper_frames.csv", index=False)

# ── 3. Cohort gradient ───────────────────────────────────────────────────
say("\n" + "=" * 68)
say("1. COHORT GRADIENT — hub framing by author entry-cohort")
say("=" * 68)
d = df[df.year >= 1990].copy()
d["cohort_dec"] = (d.cohort // 10) * 10
tab = d.groupby("cohort_dec").agg(
    papers=("pmid", "size"), hub=("hub_dominant", "sum"))
tab["hub_pct"] = (100 * tab.hub / tab.papers).round(1)
lo, hi = D.wilson_ci(tab.hub.values, tab.papers.values)
tab["ci_lo"] = (100 * lo).round(1)
tab["ci_hi"] = (100 * hi).round(1)
say("Papers published 1990+, grouped by the LAST author's entry decade:")
say(tab.to_string())

# same publication window, so publication year is roughly held constant
d2 = df[(df.year >= 2010)].copy()
d2["cohort_dec"] = (d2.cohort // 10) * 10
tab2 = d2.groupby("cohort_dec").agg(
    papers=("pmid", "size"), hub=("hub_dominant", "sum"))
tab2["hub_pct"] = (100 * tab2.hub / tab2.papers).round(1)
say("\nPapers published 2010+ ONLY (publication era held constant):")
say(tab2[tab2.papers >= 10].to_string())

# ── 4. Within-author conversion ──────────────────────────────────────────
say("\n" + "=" * 68)
say("2. WITHIN-AUTHOR CONVERSION")
say("=" * 68)
g = df.groupby("author_id")
span = g.year.agg(["min", "max", "size"])
movers = span[(span["max"] - span["min"] >= 8) & (span["size"] >= 2)]
say(f"Authors with >=2 framing papers spanning >=8 years: {len(movers):,}")
rows = []
for aid in movers.index:
    sub = df[df.author_id == aid].sort_values("year")
    half = len(sub) // 2 or 1
    early = sub.iloc[:half].hub_share.mean()
    late = sub.iloc[-half:].hub_share.mean()
    rows.append({"author_id": aid,
                 "name": sub.author_name.iloc[0],
                 "n": len(sub),
                 "yr_first": int(sub.year.min()),
                 "yr_last": int(sub.year.max()),
                 "early_hub": early, "late_hub": late,
                 "delta": late - early})
conv = pd.DataFrame(rows)
if len(conv):
    from scipy.stats import wilcoxon
    say(f"Mean within-author change in hub share: "
        f"{conv.delta.mean():+.3f} (median {conv.delta.median():+.3f})")
    say(f"  authors who shifted toward hub : {(conv.delta > 0).sum()}")
    say(f"  authors who shifted toward relay: {(conv.delta < 0).sum()}")
    say(f"  authors with no change         : {(conv.delta == 0).sum()}")
    nz = conv[conv.delta != 0]
    if len(nz) >= 6:
        stat, p = wilcoxon(nz.delta)
        say(f"  Wilcoxon signed-rank (non-zero movers, n={len(nz)}): "
            f"W={stat:.0f}, p={p:.3g}")
    say("\nLargest individual conversions:")
    say(conv.nlargest(8, "delta")[
        ["name", "n", "yr_first", "yr_last", "early_hub", "late_hub",
         "delta"]].round(2).to_string(index=False))

# ── 5. Decomposition: conversion vs replacement ──────────────────────────
say("\n" + "=" * 68)
say("3. DECOMPOSITION — conversion vs replacement")
say("=" * 68)
P1 = (1990, 2007)
P2 = (2008, 2025)
p1 = df[(df.year >= P1[0]) & (df.year <= P1[1])]
p2 = df[(df.year >= P2[0]) & (df.year <= P2[1])]
say(f"Period 1 = {P1[0]}-{P1[1]}: {len(p1):,} papers, "
    f"hub-dominant {100*p1.hub_dominant.mean():.1f}%")
say(f"Period 2 = {P2[0]}-{P2[1]}: {len(p2):,} papers, "
    f"hub-dominant {100*p2.hub_dominant.mean():.1f}%")
total_change = 100 * (p2.hub_dominant.mean() - p1.hub_dominant.mean())
say(f"TOTAL field-level change: {total_change:+.1f} pp")

a1, a2 = set(p1.author_id), set(p2.author_id)
cont = a1 & a2
entr = a2 - a1
exit_ = a1 - a2
say(f"\nAuthors: {len(a1)} in P1, {len(a2)} in P2 | "
    f"continuing {len(cont)}, entrants {len(entr)}, exiters {len(exit_)}")

cont_p1 = p1[p1.author_id.isin(cont)]
cont_p2 = p2[p2.author_id.isin(cont)]
entr_p2 = p2[p2.author_id.isin(entr)]
exit_p1 = p1[p1.author_id.isin(exit_)]
for label, s in [("continuing authors, P1", cont_p1),
                 ("continuing authors, P2", cont_p2),
                 ("entrants (new in P2)   ", entr_p2),
                 ("exiters (gone after P1)", exit_p1)]:
    if len(s):
        say(f"  {label}: n={len(s):4d}  hub-dominant "
            f"{100*s.hub_dominant.mean():5.1f}%")

if len(cont_p1) and len(cont_p2) and len(entr_p2) and len(exit_p1):
    conversion = 100 * (cont_p2.hub_dominant.mean()
                        - cont_p1.hub_dominant.mean())
    replacement = 100 * (entr_p2.hub_dominant.mean()
                         - exit_p1.hub_dominant.mean())
    say(f"\n  CONVERSION effect  (same authors, P1→P2): {conversion:+.1f} pp")
    say(f"  REPLACEMENT effect (entrants − exiters)  : {replacement:+.1f} pp")
    if abs(conversion) + abs(replacement) > 0:
        share = 100 * abs(conversion) / (abs(conversion) + abs(replacement))
        say(f"  → conversion accounts for ~{share:.0f}% of the two "
            f"components' combined magnitude")

# ── 6. Mixed model ───────────────────────────────────────────────────────
say("\n" + "=" * 68)
say("4. MIXED MODEL — hub_dominant ~ pub_year + cohort + (1|author)")
say("=" * 68)
try:
    import statsmodels.formula.api as smf
    m = df[df.year >= 1990].copy()
    m["yr_c"] = (m.year - 2005) / 10.0
    m["coh_c"] = (m.cohort - 1995) / 10.0
    fit = smf.mixedlm("hub_dominant ~ yr_c + coh_c", m,
                      groups=m["author_id"]).fit(method="lbfgs")
    say(fit.summary().as_text())
    say("\nInterpretation: yr_c = within/period effect per decade; "
        "coh_c = cohort effect per decade of later entry.")
except Exception as e:
    say(f"mixed model failed: {type(e).__name__}: {e}")
    try:
        import statsmodels.api as sm
        m = df[df.year >= 1990].copy()
        X = sm.add_constant(pd.DataFrame({
            "yr_c": (m.year - 2005) / 10.0,
            "coh_c": (m.cohort - 1995) / 10.0}))
        gee = sm.GEE(m.hub_dominant, X, groups=m.author_id,
                     family=sm.families.Binomial()).fit()
        say("GEE (binomial, clustered by author):")
        say(gee.summary().as_text())
    except Exception as e2:
        say(f"GEE also failed: {e2}")

(V18 / "planck_results.txt").write_text("\n".join(LOG), encoding="utf-8")
print(f"\nSaved: {V18/'planck_results.txt'}")
