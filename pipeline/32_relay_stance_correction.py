"""
32_relay_stance_correction.py - separate genuine relay FRAMING from relay
VOCABULARY in the 'relay' regex hits.

The 'relay' pattern in taxonomy.FRAMES_CLEAN fires on any of: relay
station/center/nucleus/for/of/between, sensory relay, way-station. That
conflates several different uses:
  - anatomical/cell-type labels   "VPM relay neurons", "relay nucleus"
  - challenges to the frame       "not a simple, passive relay"
  - reports of the old view       "traditionally considered a relay"
  - genuine characterisation      "the thalamus acts as a relay station"
and 'relay' is FIRST in the priority order, so a sentence that also says
"not merely a relay", "hub" or "gatekeeper" is still coded relay.

This script adds a NON-DESTRUCTIVE stance layer (canonical frame_type is
left untouched) and reports its effect. Categories, applied in order:
  junk              front-matter / keyword / e-mail / download-banner text
  challenge         explicit rejection or diminution of the relay frame
  historical        reports the view as past or received wisdom
  technical         'relay' as an anatomical or cell-type label
  characterization  the thalamus (or a nucleus) IS / ACTS AS a relay
  functional        remaining functional/pathway 'relay of X' descriptions
Genuine relay framing = characterization + functional (lenient) or
characterization only (strict). Each sentence is also flagged when it is
stacked with an active frame (hub, gate, gateway, integrator, filter).

Outputs: Data/processed/v18/relay_stance_classified.csv
         Data/processed/v18/relay_correction_results.txt
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
from taxonomy import FRAMES_CLEAN

V18 = D.PROC / "v18"
LOG = []


def say(s=""):
    print(s)
    LOG.append(str(s))


def clean(t):
    return re.sub(r"\s+", " ", re.sub(r"-\s+", "", str(t))).strip()


JUNK = re.compile(
    r"\bkey\s*words?\b|\bdoi\s*:|advance online publication|date received"
    r"|\breceived\s*:|\baccepted\s*:|copyright|all rights reserved"
    r"|downloaded from|\S+@\S+\.\w+", re.I)
CHALLENGE = re.compile(
    r"not\s+(a\s+|just\s+|merely\s+|simply\s+|only\s+|solely\s+"
    r"|limited\s+to\s+(a\s+)?)*(a\s+)?(simple\s+|passive\s+|mere\s+|pure\s+)?relay"
    r"|more\s+than\s+(a\s+|just\s+|simply\s+|merely\s+)*(a\s+)?"
    r"(simple\s+|passive\s+)?relay"
    r"|beyond\s+(a\s+|the\s+|simple\s+|mere\s+)*relay"
    r"|rather\s+than\s+(a\s+|simply\s+|merely\s+)*(a\s+)?"
    r"(simple\s+|passive\s+)?relay"
    r"|no\s+longer\s+(considered|viewed|regarded|seen)"
    r"|not\s+directly\s+involved\s+in\s+the\s+classical\s+relay"
    r"|outdated|supplant|challeng\w*\s.{0,60}relay|relay.{0,60}challeng"
    r"|(simple|passive|mere),?\s+(passive\s+)?relay", re.I)
HISTORICAL = re.compile(
    r"(traditionally|historically|classically|conventionally|originally"
    r"|formerly|widely|long)\s+(been\s+)?(viewed|considered|regarded|thought"
    r"|seen|described|conceived|believed|characteri[sz]ed|defined)"
    r"|has\s+(long\s+)?been\s+(viewed|considered|regarded|thought|seen"
    r"|described)\s+as"
    r"|classical\s+(view|notion|concept)|textbook", re.I)
TECHNICAL = re.compile(
    r"relay\s+(nucle\w*|neuron\w*|cell\w*)"
    r"|(first|higher|second)[\s-]+order\s+relay"
    r"|relay\s+(station|cent(er|re))s\s+(in|of|along|within)\b"
    r"|successive\s+relay|each\s+relay"
    r"|relay\s+of\s+an?\s+(independent|parallel)", re.I)
CHARACTERIZATION = re.compile(
    r"(thalamus|thalamic\s+nuclei|it|nucleus|structure)\s+(is|are|was|were"
    r"|acts?\s+as|serves?\s+as|functions?\s+as|constitutes?|represents?"
    r"|operates?\s+as)\s+(a|an|the)?\s*(\w+[\s,]+){0,5}relay"
    r"|relay\s+(station|cent(er|re))\s+(for|of|to|between)\b|way.?station",
    re.I)
ACTIVE_FRAMES = ["hub", "gate", "gateway", "integrator", "filter"]


def stance(text: str, section: str = "") -> str:
    t = text or ""
    if JUNK.search(t) or (section == "front" and len(t) < 60):
        return "junk"
    if CHALLENGE.search(t):
        return "challenge"
    if HISTORICAL.search(t):
        return "historical"
    if TECHNICAL.search(t):
        return "technical"
    if CHARACTERIZATION.search(t):
        return "characterization"
    return "functional"


def stacked_with(text: str) -> str:
    t = text or ""
    return ",".join(f for f in ACTIVE_FRAMES if FRAMES_CLEAN[f].search(t))


GENUINE_LENIENT = {"characterization", "functional"}
GENUINE_STRICT = {"characterization"}
ORDER = ["technical", "functional", "characterization", "challenge",
         "historical", "junk"]

# ─────────────────────────────────────────────── 1. classify
fr = D.load_framing()
rel = fr[fr.frame_type == "relay"].copy()
rel["relay_stance"] = [stance(s, sec) for s, sec in
                       zip(rel.sentence.fillna(""), rel.section.fillna(""))]
rel["stacked_with"] = [stacked_with(s) for s in rel.sentence.fillna("")]
rel[["pmid", "year", "section", "sentence", "relay_stance",
     "stacked_with"]].to_csv(V18 / "relay_stance_classified.csv",
                             index=False)

say("=" * 78)
say(f"1. WHAT THE {len(rel):,} 'relay' SENTENCES ACTUALLY ARE")
say("=" * 78)
vc = rel.relay_stance.value_counts()
for k in ORDER:
    n = int(vc.get(k, 0))
    say(f"  {k:17s} {n:5d}  ({100 * n / len(rel):4.1f}%)")
len_l = rel.relay_stance.isin(GENUINE_LENIENT)
len_s = rel.relay_stance.isin(GENUINE_STRICT)
say(f"\n  genuine relay framing, lenient (characterization+functional): "
    f"{len_l.sum():,} ({100 * len_l.mean():.1f}%)")
say(f"  genuine relay framing, strict  (characterization only)       : "
    f"{len_s.sum():,} ({100 * len_s.mean():.1f}%)")
stk = rel[len_l & (rel.stacked_with != "")]
say(f"  of lenient-genuine sentences, stacked with an active frame     : "
    f"{len(stk):,} ({100 * len(stk) / max(len_l.sum(), 1):.1f}%)  "
    f"{stk.stacked_with.str.split(',').explode().value_counts().to_dict()}")

rel["dec"] = (rel.year // 10) * 10
say("\nBy decade (% of relay hits in each category):")
tab = (pd.crosstab(rel.dec, rel.relay_stance, normalize="index") * 100)
tab = tab.reindex(columns=ORDER, fill_value=0).round(0)
tab["n"] = rel.groupby("dec").size()
say(tab.loc[tab.index >= 1970].to_string())

# ─────────────────────────────────────────────── 2. validation sample
say("\n" + "=" * 78)
say("2. VALIDATION SAMPLE - 7 random sentences per category (check by eye)")
say("=" * 78)
for k in ORDER:
    g = rel[rel.relay_stance == k]
    if not len(g):
        continue
    say(f"\n--- {k.upper()} (n={len(g)}) ---")
    for r in g.sample(min(7, len(g)), random_state=11).itertuples():
        tag = f" [+{r.stacked_with}]" if r.stacked_with else ""
        say(f"  [{r.year}]{tag} {clean(r.sentence)[:170]}")

# ─────────────────────────────────────────────── 3. precision vs LLM
say("\n" + "=" * 78)
say("3. PRECISION AGAINST THE LLM CODES (papers classified by both)")
say("=" * 78)
flat = pd.read_csv(V18 / "llm_full" / "frames_flat.csv")
llm_papers = set(flat.pmid)
llm_genuine = set(flat[(flat.frame_family == "relay_transmission")
                       & (flat.stance != "challenges")].pmid)


def precision(mask, label):
    p = set(rel[mask].pmid) & llm_papers
    if not p:
        say(f"  {label}: no overlap")
        return
    hit = len(p & llm_genuine)
    say(f"  {label:46s} flags {len(p):3d} papers; LLM finds relay "
        f"genuinely present in {hit:3d} = precision {100 * hit / len(p):.0f}%")


precision(rel.relay_stance.notna(), "ORIGINAL regex (any relay hit)")
precision(len_l, "CORRECTED lenient (characterization+functional)")
precision(len_s, "CORRECTED strict (characterization only)")
say("  (small validation set; the LLM sample over-represents influential "
    "reviews, which challenge relay more often)")

# ─────────────────────────────────────────────── 4. relay people
say("\n" + "=" * 78)
say("4. WHO ARE THE MODERN (2010+) RELAY PEOPLE - corrected coding")
say("=" * 78)
au = pd.read_csv(V18 / "paper_authors_openalex.csv").dropna(
    subset=["author_id"])
cohort = au.groupby("author_id").year.min().rename("cohort")
dom = D.load_domains()[["pmid", "domain_name"]]
raw = pd.read_csv(
    D.RAW / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    usecols=["pmid", "journal"], low_memory=False).drop_duplicates("pmid")
journal = dict(zip(raw.pmid, raw.journal))

gen = rel[(rel.year >= 2010) & len_l]
for pos in ["last", "first"]:
    a = au[au.position == pos][["pmid", "author_id", "author_name"]]
    j = (gen.merge(a.drop_duplicates("pmid"), on="pmid")
         .merge(cohort, left_on="author_id", right_index=True))
    per = (j.groupby(["author_id", "author_name", "cohort"])
           .agg(papers=("pmid", "nunique"), sentences=("pmid", "size"),
                strict=("relay_stance",
                        lambda s: int((s == "characterization").sum())),
                stacked=("stacked_with", lambda s: int((s != "").sum())))
           .reset_index().sort_values(["papers", "strict"],
                                      ascending=False))
    say(f"\n[{pos} author] authors with >=2 papers (2010+) containing "
        f"genuine relay framing: {(per.papers >= 2).sum()}   "
        f"(>=3 papers: {(per.papers >= 3).sum()})")
    say(per[per.papers >= 2].head(20)[
        ["author_name", "cohort", "papers", "sentences", "strict",
         "stacked"]].to_string(index=False))

say("\nExample sentences for the top last-author relay people:")
a = au[au.position == "last"][["pmid", "author_id", "author_name"]]
j = gen.merge(a.drop_duplicates("pmid"), on="pmid")
top = (j.groupby("author_name").pmid.nunique()
       .sort_values(ascending=False).head(8).index)
for name in top:
    s = j[j.author_name == name].sort_values("relay_stance")
    say(f"\n  {name}")
    for r in s.drop_duplicates("pmid").head(3).itertuples():
        jn = str(journal.get(r.pmid, ""))[:30]
        tag = f" +{r.stacked_with}" if r.stacked_with else ""
        say(f"    [{r.year} | {r.relay_stance}{tag} | {jn}] "
            f"{clean(r.sentence)[:140]}")

say("\nResearch domain of 2010+ papers with genuine relay framing (%):")
dd = gen.drop_duplicates("pmid").merge(dom, on="pmid", how="left")
say((dd.domain_name.fillna("(not clustered)").value_counts(normalize=True)
     * 100).round(1).head(10).to_string())

# Selected from the data so that no researcher is named in the code; the
# names appear only in the private results file.
say("\nThe old guard, reclassified (all their relay hits, any year; the 5 "
    "authors entering before 2000 with the most relay papers):")
rel_au = (rel[["pmid"]].drop_duplicates()
          .merge(au[["pmid", "author_id", "author_name"]], on="pmid")
          .merge(cohort, left_on="author_id", right_index=True))
guard = (rel_au[rel_au.cohort < 2000]
         .groupby(["author_id", "author_name"]).pmid.nunique()
         .sort_values(ascending=False).head(5))
for (aid, who), n_pap in guard.items():
    pm = set(au[au.author_id == aid].pmid)
    s = rel[rel.pmid.isin(pm)].relay_stance.value_counts().to_dict()
    say(f"  {who:20s} ({n_pap} relay papers) {s}")

# ─────────────────────────────────────────────── 5. robustness
say("\n" + "=" * 78)
say("5. DO THE HEADLINE RESULTS SURVIVE THE CORRECTION?")
say("=" * 78)
not_framing = rel.index[rel.relay_stance.isin({"technical", "junk"})]


def recode(keep):
    """Technical/junk hits are not framing at all -> dropped; challenge,
    historical and non-kept relay hits stay framing sentences but are no
    longer counted as relay."""
    f = fr.drop(index=not_framing).copy()
    f["relay_stance"] = rel.relay_stance.reindex(f.index)
    m = f.relay_stance.notna() & ~f.relay_stance.isin(keep)
    f.loc[m, "frame_type"] = "relay_not_endorsed"
    return f


versions = {"original": fr, "lenient": recode(GENUINE_LENIENT),
            "strict": recode(GENUINE_STRICT)}

say("\nAnnual relay / hub share of framing sentences (years n>=8), MK tau:")
for v, f in versions.items():
    yt = f.groupby("year").size()
    yrs = yt[yt >= 8].index
    parts = []
    for ft in ["relay", "hub"]:
        k = (f[f.frame_type == ft].groupby("year").size()
             .reindex(yt.index, fill_value=0))
        tau, p = D.mann_kendall(yrs.values, (k / yt).loc[yrs].values)
        parts.append(f"{ft} tau={tau:+.2f} (p={p:.1g}, n={int(k.sum())})")
    say(f"  {v:9s} " + "   ".join(parts))

say("\nRelay vs hub share of framing sentences per decade (%):")
cols = {}
for v, f in versions.items():
    f = f.assign(dec=(f.year // 10) * 10)
    t = f.groupby("dec").size()
    for ft in ["relay", "hub"]:
        cols[f"{ft}_{v}"] = (f[f.frame_type == ft].groupby("dec").size()
                             .reindex(t.index, fill_value=0) / t * 100
                             ).round(1)
say(pd.DataFrame(cols).loc[lambda x: x.index >= 1970].to_string())

say("\nPlanck decomposition (last author, 1990-2007 vs 2008-2025):")
a_last = au[au.position == "last"][["pmid", "author_id"]].drop_duplicates(
    "pmid")
for v, f in versions.items():
    rh = f[f.frame_type.isin(["relay", "hub"])]
    pap = (rh.assign(h=(rh.frame_type == "hub").astype(int))
           .groupby(["pmid", "year"])
           .agg(nh=("h", "sum"), nf=("h", "size")).reset_index())
    pap["hd"] = ((pap.nh / pap.nf) > 0.5).astype(int)
    d = (pap.merge(a_last, on="pmid")
         .merge(cohort, left_on="author_id", right_index=True))
    p1 = d[(d.year >= 1990) & (d.year <= 2007)]
    p2 = d[(d.year >= 2008) & (d.year <= 2025)]
    a1, a2 = set(p1.author_id), set(p2.author_id)
    c = a1 & a2
    conv = 100 * (p2[p2.author_id.isin(c)].hd.mean()
                  - p1[p1.author_id.isin(c)].hd.mean())
    repl = 100 * (p2[p2.author_id.isin(a2 - a1)].hd.mean()
                  - p1[p1.author_id.isin(a1 - a2)].hd.mean())
    sub = d[d.year >= 2010]
    grad = " / ".join(
        f"{100 * sub[(sub.cohort >= lo) & (sub.cohort <= hi)].hd.mean():.0f}%"
        for lo, hi in [(1970, 1999), (2000, 2009), (2010, 2025)])
    say(f"  {v:9s} papers={len(d):5d}  conversion={conv:+5.1f} pp  "
        f"replacement={repl:+5.1f} pp  cohort gradient "
        f"(<2000 / 2000s / 2010+) {grad}  [continuing={len(c)}]")

say("\nAbstract-only corpus, relay trend after correction:")
af = pd.read_csv(V18 / "abstract_framing.csv")
is_rel = af.frame_type == "relay"
af["relay_stance"] = None
af.loc[is_rel, "relay_stance"] = [stance(s) for s in
                                  af.loc[is_rel, "sentence"].fillna("")]
say("  abstract relay-hit breakdown: "
    + str(af.loc[is_rel, "relay_stance"].value_counts().to_dict()))
for v, keep in [("original", None), ("lenient", GENUINE_LENIENT),
                ("strict", GENUINE_STRICT)]:
    f = af.copy()
    if keep is not None:
        f = f[~f.relay_stance.isin({"technical", "junk"})].copy()
        m = f.frame_type.eq("relay") & ~f.relay_stance.isin(keep)
        f.loc[m, "frame_type"] = "relay_not_endorsed"
    yt = f.groupby("year").size()
    yrs = yt[yt >= 8].index
    k = (f[f.frame_type == "relay"].groupby("year").size()
         .reindex(yt.index, fill_value=0))
    tau, p = D.mann_kendall(yrs.values, (k / yt).loc[yrs].values)
    say(f"  {v:9s} relay tau={tau:+.2f} (p={p:.1g}, n={int(k.sum())})")

(V18 / "relay_correction_results.txt").write_text("\n".join(LOG),
                                                  encoding="utf-8")
print("\nSaved: v18/relay_stance_classified.csv, "
      "v18/relay_correction_results.txt")
