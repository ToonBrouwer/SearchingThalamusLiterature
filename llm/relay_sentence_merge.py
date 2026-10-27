"""
relay_sentence_merge.py - merge and validate the blind sentence-level stance
codes for the regex 'relay' hits, compare them with the regex stance layer
(and, once coded, the user's gold standard), and rebuild the "where relay
declined / where it lives" analysis on the LLM labels.

Inputs : Data/processed/v18/llm_relay/key.csv
         Data/processed/v18/llm_relay/results/batch_*.jsonl
         Data/processed/v18/llm_relay/gold_standard_coded.csv   (optional)
Outputs: Data/processed/v18/llm_relay/relay_sentence_codes.csv
         Data/processed/v18/llm_relay/relay_sentence_report.txt
"""
from __future__ import annotations

import io
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import cohen_kappa_score

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
for sub in ("Code", "Code/paper", "Code/llm"):
    sys.path.insert(0, str(ROOT / sub))
import v18_data as D
from relay_sentence_prompt import LABELS, SCOPES

BASE = D.V18 / "llm_relay"
LOG: list[str] = []


def say(s=""):
    print(s)
    LOG.append(str(s))


def rule(title):
    say("")
    say("=" * 78)
    say(title)
    say("=" * 78)


GENRES = D.GENRES
genre = D.genre_of_domain


def norm(t):
    return re.sub(r"\s+", " ", str(t)).strip()


TO_REGEX = {"characterization": "characterization",
            "mechanistic": "functional", "technical": "technical",
            "challenge": "challenge", "historical": "historical",
            "junk": "junk", "off_target": "off_target"}
GENUINE = {"characterization", "mechanistic"}
NOT_FRAMING = {"technical", "junk", "off_target"}

# ─────────────────────────────────────────────── 1. merge and validate
rule("1. MERGE AND VALIDATE")
key = pd.read_csv(BASE / "key.csv")
rows, problems = [], []
files = sorted((BASE / "results").glob("batch_*.jsonl"))
for fpath in files:
    lines = fpath.read_text(encoding="utf-8").splitlines()
    for ln, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            problems.append("%s:%d unparseable JSON" % (fpath.name, ln))
            continue
        if (r.get("label") not in LABELS or r.get("scope") not in SCOPES
                or r.get("confidence") not in {"high", "medium", "low"}):
            problems.append("%s:%d id=%s invalid field(s)"
                            % (fpath.name, ln, r.get("id")))
            continue
        rows.append({"id": r["id"], "llm_label": r["label"],
                     "llm_scope": r["scope"], "llm_conf": r["confidence"],
                     "llm_note": r.get("note", "")})
codes = pd.DataFrame(rows, columns=["id", "llm_label", "llm_scope",
                                    "llm_conf", "llm_note"])
dups = codes.id[codes.id.duplicated()].unique().tolist()
codes = codes.drop_duplicates("id", keep="last")
missing = sorted(set(key.id) - set(codes.id))
extra = sorted(set(codes.id) - set(key.id))
say("result files: %d | valid codes: %d / %d sentences"
    % (len(files), len(codes), len(key)))
say("invalid lines: %d | duplicate ids: %d | missing ids: %d | unknown ids: %d"
    % (len(problems), len(dups), len(missing), len(extra)))
for p in problems[:10]:
    say("  ! " + p)
if missing:
    say("  missing (first 20): " + ", ".join(missing[:20]))

m = key.merge(codes, on="id", how="left")
m.to_csv(BASE / "relay_sentence_codes.csv", index=False)
c = m.dropna(subset=["llm_label"]).copy()
if c.empty:
    say("No codes yet - nothing further to report.")
    (BASE / "relay_sentence_report.txt").write_text("\n".join(LOG),
                                                     encoding="utf-8")
    sys.exit(0)

# ─────────────────────────────────────────────── 2. what the LLM sees
rule("2. LLM LABELS")
vc = c.llm_label.value_counts()
for lab in LABELS:
    n = int(vc.get(lab, 0))
    say("  %-17s %5d  (%4.1f%%)" % (lab, n, 100 * n / len(c)))
say("\nconfidence by label:")
say(pd.crosstab(c.llm_label, c.llm_conf)
    .reindex(columns=["high", "medium", "low"], fill_value=0).to_string())
say("\nscope by label:")
say(pd.crosstab(c.llm_label, c.llm_scope).to_string())

# ─────────────────────────────────────────────── 3. agreement with regex
rule("3. AGREEMENT WITH THE REGEX STANCE LAYER (same sentences, same unit)")
c["llm_as_regex"] = c.llm_label.map(TO_REGEX)
say(pd.crosstab(c.regex_stance, c.llm_label, margins=True).to_string())
acc = (c.regex_stance == c.llm_as_regex).mean()
say("\nall categories: raw agreement %.1f%%, Cohen's kappa %.2f"
    % (100 * acc, cohen_kappa_score(c.regex_stance, c.llm_as_regex)))


def binary(regex_mask, llm_mask, name):
    both = (regex_mask & llm_mask).sum()
    say("%s: kappa %.2f | regex precision %.0f%% | regex recall %.0f%%"
        % (name, cohen_kappa_score(regex_mask, llm_mask),
           100 * both / max(regex_mask.sum(), 1),
           100 * both / max(llm_mask.sum(), 1)))


binary(c.regex_stance.isin(["characterization", "functional"]),
       c.llm_label.isin(GENUINE),
       "genuine relay use (char+functional vs char+mechanistic)")
binary(c.regex_stance.eq("characterization"),
       c.llm_label.eq("characterization"), "characterization only")
say("\nper regex category, share the LLM agrees with:")
for cat_, g in c.groupby("regex_stance"):
    say("  %-17s n=%4d  agree %3.0f%%   LLM says: %s"
        % (cat_, len(g), 100 * (g.regex_stance == g.llm_as_regex).mean(),
           g.llm_label.value_counts().head(3).to_dict()))

# ─────────────────────────────────────────────── 4. genre analysis
rule("4. WHERE RELAY DECLINED AND WHERE IT LIVES - on LLM labels")
fr = D.load_framing()
fr["norm"] = fr.sentence.map(norm)
lk = (c.assign(norm=c.sentence.map(norm))
      .drop_duplicates(["pmid", "norm"])
      [["pmid", "norm", "llm_label", "llm_conf", "regex_stance"]])
fr = fr.merge(lk, on=["pmid", "norm"], how="left")
is_rel = fr.frame_type.eq("relay")
say("relay framing rows matched to an LLM code: %d / %d"
    % (fr.loc[is_rel, "llm_label"].notna().sum(), is_rel.sum()))
dom = D.load_domains()[["pmid", "domain"]]
fr = fr.merge(dom, on="pmid", how="left")
fr["genre"] = fr.domain.map(genre)
f = fr[~(is_rel & fr.llm_label.isin(NOT_FRAMING))].copy()


def bucket(ft, lab):
    if ft == "relay":
        if lab == "characterization":
            return "relay_characterization"
        if lab == "mechanistic":
            return "relay_mechanistic"
        if lab in ("challenge", "historical"):
            return "relay_foil"
        return "relay_uncoded"
    return "hub" if ft == "hub" else "other"


f["c"] = [bucket(a, b) for a, b in zip(f.frame_type, f.llm_label)]
f["dec"] = (f.year // 10) * 10
SERIES = [("relay (char+mech)", ["relay_characterization",
                                 "relay_mechanistic"]),
          ("characterization", ["relay_characterization"]),
          ("mechanistic", ["relay_mechanistic"]),
          ("foil", ["relay_foil"]),
          ("hub", ["hub"])]


def trends(s, label):
    yt = s.groupby("year").size()
    yrs = yt[yt >= 8].index
    parts = []
    for name, cats in SERIES:
        k = (s[s.c.isin(cats)].groupby("year").size()
             .reindex(yt.index, fill_value=0))
        tau, p = D.mann_kendall(yrs.values, (k / yt).loc[yrs].values)
        parts.append("%s %+.2f (p=%.2g)" % (name, tau, p))
    say("  %-17s [%2d yrs] " % (label, len(yrs)) + " | ".join(parts))


say("\nAnnual Mann-Kendall tau on share of framing sentences "
    "(technical/junk/off-target relay removed):")
trends(f, "all papers")
for g in GENRES:
    trends(f[f.genre == g], g)

say("\nShare of framing sentences per decade (%):")
for g in GENRES:
    s = f[(f.genre == g) & (f.dec >= 1980)]
    t = (pd.crosstab(s.dec, s.c, normalize="index") * 100).round(1)
    t = t.reindex(columns=["relay_characterization", "relay_mechanistic",
                           "relay_foil", "hub"], fill_value=0)
    t["n_framing"] = s.groupby("dec").size()
    say("\n[%s]" % g)
    say(t.to_string())

say("\nGenre mix of framing sentences per decade (% clinical/imaging):")
mix = f[f.genre.isin(GENRES) & (f.dec >= 1980)]
say(mix.groupby("dec").genre
    .apply(lambda x: 100 * x.eq("clinical/imaging").mean())
    .round(1).to_string())


def shift_share(cats, d1, d2, name):
    s = f[f.genre.isin(GENRES)]
    a, b = s[s.dec == d1], s[s.dec == d2]
    w1 = a.genre.value_counts(normalize=True).reindex(GENRES, fill_value=0)
    w2 = b.genre.value_counts(normalize=True).reindex(GENRES, fill_value=0)
    p1 = (a.groupby("genre").c.apply(lambda x: x.isin(cats).mean())
          .reindex(GENRES, fill_value=0))
    p2 = (b.groupby("genre").c.apply(lambda x: x.isin(cats).mean())
          .reindex(GENRES, fill_value=0))
    total = 100 * ((w2 * p2).sum() - (w1 * p1).sum())
    within = 100 * (w1 * (p2 - p1)).sum()
    mixing = 100 * (p2 * (w2 - w1)).sum()
    say("  %-18s %ds->%ds  total %+5.1f pp = within-genre %+5.1f pp "
        "+ genre-mix %+5.1f pp" % (name, d1, d2, total, within, mixing))


say("\nShift-share decomposition of the pooled change "
    "(circuit/systems + clinical/imaging):")
for name, cats in SERIES:
    shift_share(cats, 2000, 2020, name)

# ─────────────────────────────────────────────── 4b. what relay hits are
rule("4b. WHAT THE RELAY HITS ARE MADE OF - share of ALL framing sentences "
     "(nothing removed from the denominator)")
USES = ["genuine", "technical", "foil", "junk/off"]
fr["use"] = np.where(
    fr.frame_type.eq("relay"),
    fr.llm_label.replace({"characterization": "genuine",
                          "mechanistic": "genuine", "challenge": "foil",
                          "historical": "foil", "junk": "junk/off",
                          "off_target": "junk/off"}),
    np.where(fr.frame_type.eq("hub"), "hub", "other"))


def use_trends(s, label):
    yt = s.groupby("year").size()
    yrs = yt[yt >= 8].index
    parts = []
    for name, mask in ([("all relay", s.frame_type.eq("relay"))]
                       + [(u, s.use.eq(u)) for u in USES]):
        k = s[mask].groupby("year").size().reindex(yt.index, fill_value=0)
        tau, p = D.mann_kendall(yrs.values, (k / yt).loc[yrs].values)
        parts.append("%s %+.2f (p=%.2g)" % (name, tau, p))
    say("  %-17s " % label + " | ".join(parts))


say("Annual Mann-Kendall tau:")
use_trends(fr, "all papers")
for g in GENRES:
    use_trends(fr[fr.genre == g], g)
for g in ["all papers"] + GENRES:
    s = fr if g == "all papers" else fr[fr.genre == g]
    s = s[s.year >= 1980]
    dec = (s.year // 10) * 10
    t = (pd.crosstab(dec, s.use, normalize="index") * 100).round(1)
    t = t.reindex(columns=USES + ["hub"], fill_value=0)
    t.insert(0, "all_relay", t[USES].sum(axis=1).round(1))
    t["n_framing"] = s.groupby(dec).size()
    say("\n[%s]" % g)
    say(t.to_string())

say("\nSensitivity to the hardest coding boundaries (relay claims = "
    "characterization + mechanistic, share of framing sentences after "
    "removing technical/junk/off-target relay; 2000s -> 2020s):")
low = fr.llm_conf.eq("low")
lowmed = fr.llm_conf.isin(["low", "medium"])
lab = fr.llm_label
variants = {
    "LLM codes (main)": lab,
    "low-conf technical -> characterization":
        lab.mask(lab.eq("technical") & low, "characterization"),
    "all technical -> characterization":
        lab.mask(lab.eq("technical"), "characterization"),
    "low-conf characterization -> technical":
        lab.mask(lab.eq("characterization") & low, "technical"),
    "low/med-conf characterization -> technical":
        lab.mask(lab.eq("characterization") & lowmed, "technical"),
    "low-conf codes dropped": lab.mask(low, "junk"),
    "regex stance layer": fr.regex_stance.replace(
        {"functional": "mechanistic"}),
}
for name, labs in variants.items():
    g_ = fr.assign(lab=labs)
    g_ = g_[~(g_.frame_type.eq("relay") & g_.lab.isin(NOT_FRAMING))].copy()
    g_["claim"] = g_.frame_type.eq("relay") & g_.lab.isin(GENUINE)
    parts = []
    for gn in GENRES:
        s = g_[g_.genre == gn]
        yt = s.groupby("year").size()
        yrs = yt[yt >= 8].index
        k = (s[s.claim].groupby("year").size()
             .reindex(yt.index, fill_value=0))
        tau, p = D.mann_kendall(yrs.values, (k / yt).loc[yrs].values)
        a = 100 * s[(s.year >= 2000) & (s.year <= 2009)].claim.mean()
        b = 100 * s[s.year >= 2020].claim.mean()
        parts.append("%s tau %+.2f (p=%.2g) %4.1f%% -> %4.1f%%"
                     % (gn.split("/")[0], tau, p, a, b))
    say("  %-44s " % name + " | ".join(parts))

# ─────────────────────────────────────────────── 4c. other denominators
rule("4c. RELAY CLAIMS AND HUB UNDER OTHER DENOMINATORS (text volume, "
     "papers)")
sent = D.load_sentences(usecols=["pmid", "year", "section"])
sent["year"] = pd.to_numeric(sent.year, errors="coerce")
sent = sent.dropna(subset=["year"])
sent["year"] = sent.year.astype(int)
sent = sent.merge(dom, on="pmid", how="left")
sent["genre"] = sent.domain.map(genre)
sent["dec"] = (sent.year // 10) * 10
fr["claim"] = fr.frame_type.eq("relay") & fr.llm_label.isin(GENUINE)
fr["technical_relay"] = fr.frame_type.eq("relay") & fr.llm_label.eq(
    "technical")
fr["hub_frame"] = fr.frame_type.eq("hub")
fr["dec"] = (fr.year // 10) * 10
RATE_COLS = ["claim", "technical_relay", "hub_frame"]
INTRO_DISC = ["introduction", "discussion"]


def rate_table(sf, ss, label):
    n = ss[ss.dec >= 1980].groupby("dec").size()
    t = pd.DataFrame({"sentences": n})
    for col in RATE_COLS:
        k = sf[sf[col]].groupby("dec").size().reindex(n.index, fill_value=0)
        t[col + "_per_1k"] = (1000 * k / n).round(2)
    yt = ss.groupby("year").size()
    yrs = yt[yt >= 500].index
    taus = []
    for col in RATE_COLS:
        k = (sf[sf[col]].groupby("year").size()
             .reindex(yt.index, fill_value=0))
        tau, p = D.mann_kendall(yrs.values, (k / yt).loc[yrs].values)
        taus.append("%s %+.2f (p=%.2g)" % (col, tau, p))
    say("\n[%s]\n  annual tau per sentence (years with >=500 sentences): %s"
        % (label, " | ".join(taus)))
    say(t.to_string())


for g in GENRES:
    rate_table(fr[fr.genre == g], sent[sent.genre == g],
               g + ", all sections")
    rate_table(fr[(fr.genre == g) & fr.section.isin(INTRO_DISC)],
               sent[(sent.genre == g) & sent.section.isin(INTRO_DISC)],
               g + ", introduction + discussion only")

say("\n% of full-text papers (>=100 sentences) with >=1 relay claim / "
    ">=1 hub frame:")
npp = sent.groupby("pmid").size()
pp = (sent[sent.pmid.isin(npp[npp >= 100].index)]
      .drop_duplicates("pmid")[["pmid", "dec", "genre"]])
pp["claim"] = pp.pmid.isin(set(fr.pmid[fr.claim]))
pp["hub"] = pp.pmid.isin(set(fr.pmid[fr.hub_frame]))
t = (pp[pp.genre.isin(GENRES) & (pp.dec >= 1980)]
     .groupby(["genre", "dec"])
     .agg(papers=("pmid", "size"), relay_claim_pct=("claim", "mean"),
          hub_pct=("hub", "mean")))
t[["relay_claim_pct", "hub_pct"]] = (100 * t[["relay_claim_pct", "hub_pct"]]
                                     ).round(1)
say(t.to_string())

say("\nPaper-weighted share of framing sentences (each paper counts once; "
    "technical/junk/off-target relay removed; 95% CI = paper bootstrap):")
fw = fr[~(fr.frame_type.eq("relay") & fr.llm_label.isin(NOT_FRAMING))]
fw = fw[fw.genre.isin(GENRES) & (fw.year >= 1980)]
per = (fw.groupby(["pmid", "genre", "year", "dec"])
       .agg(claim=("claim", "mean"), hub=("hub_frame", "mean"))
       .reset_index())
rng = np.random.default_rng(20260911)
rows = []
for (g, d), s in per.groupby(["genre", "dec"]):
    if len(s) < 10:
        continue
    arr = s[["claim", "hub"]].to_numpy(dtype=float)
    boot = arr[rng.integers(0, len(s), (1000, len(s)))].mean(axis=1)
    lo, hi = 100 * np.percentile(boot, [2.5, 97.5], axis=0)
    rows.append({"genre": g, "dec": d, "papers": len(s),
                 "relay_claim%": round(100 * arr[:, 0].mean(), 1),
                 "claim_CI": "%.1f-%.1f" % (lo[0], hi[0]),
                 "hub%": round(100 * arr[:, 1].mean(), 1),
                 "hub_CI": "%.1f-%.1f" % (lo[1], hi[1])})
say(pd.DataFrame(rows).to_string(index=False))
for g in GENRES:
    s = per[per.genre == g]
    ny = s.groupby("year").size()
    yrs = ny[ny >= 5].index
    parts = []
    for col in ["claim", "hub"]:
        tau, p = D.mann_kendall(yrs.values,
                                s.groupby("year")[col].mean().loc[yrs].values)
        parts.append("%s %+.2f (p=%.2g)" % (col, tau, p))
    say("  %-17s annual tau, paper-weighted (years with >=5 papers): %s"
        % (g, " | ".join(parts)))

# ─────────────────────────────────────────────── 5. reviews vs primary
rule("5. REVIEWS VS PRIMARY RESEARCH (2010+, LLM labels, % of relay hits)")
raw = pd.read_csv(
    D.RAW / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    usecols=["pmid", "publication_types"],
    low_memory=False).drop_duplicates("pmid")
raw["kind"] = np.where(raw.publication_types.fillna("")
                       .str.contains("Review", case=False),
                       "review", "primary research")
rv = (c[c.year >= 2010].merge(raw[["pmid", "kind"]], on="pmid", how="left")
      .dropna(subset=["kind"]))
t = (pd.crosstab(rv.kind, rv.llm_label, normalize="index") * 100).round(1)
t = t.reindex(columns=LABELS, fill_value=0)
t["n"] = rv.groupby("kind").size()
say(t.to_string())
rv = rv.merge(dom, on="pmid", how="left")
rv["genre"] = rv.domain.map(genre)
t2 = (pd.crosstab([rv.genre, rv.kind], rv.llm_label, normalize="index")
      * 100).round(1).reindex(columns=LABELS, fill_value=0)
t2["n"] = rv.groupby(["genre", "kind"]).size()
say("\nby genre x kind (2010+):")
say(t2.to_string())

# ─────────────────────────────────────────────── 6. Planck recheck
rule("6. PLANCK TEST ON LLM-CODED RELAY (last author)")
au = pd.read_csv(D.V18 / "paper_authors_openalex.csv").dropna(
    subset=["author_id"])
cohort = au.groupby("author_id").year.min().rename("cohort")
a_last = au[au.position == "last"][["pmid", "author_id"]].drop_duplicates(
    "pmid")
rh = f[f.c.isin(["relay_characterization", "relay_mechanistic",
                 "hub"])].copy()
rh["h"] = rh.c.eq("hub").astype(int)
pap = (rh.groupby(["pmid", "year"])
       .agg(nh=("h", "sum"), nf=("h", "size")).reset_index())
pap["hd"] = (pap.nh / pap.nf > 0.5).astype(int)
d = (pap.merge(a_last, on="pmid")
     .merge(cohort, left_on="author_id", right_index=True))
p1 = d[(d.year >= 1990) & (d.year <= 2007)]
p2 = d[(d.year >= 2008) & (d.year <= 2025)]
a1, a2 = set(p1.author_id), set(p2.author_id)
cont = a1 & a2
say("papers %d | hub-dominant %.1f%% (1990-2007) -> %.1f%% (2008-2025) | "
    "continuing authors %d"
    % (len(d), 100 * p1.hd.mean(), 100 * p2.hd.mean(), len(cont)))
if cont:
    conv = 100 * (p2[p2.author_id.isin(cont)].hd.mean()
                  - p1[p1.author_id.isin(cont)].hd.mean())
    say("conversion  %+.1f pp (continuing authors; unstable when few)" % conv)
repl = 100 * (p2[p2.author_id.isin(a2 - a1)].hd.mean()
              - p1[p1.author_id.isin(a1 - a2)].hd.mean())
say("replacement %+.1f pp (entrants vs exiters)" % repl)
sub = d[d.year >= 2010]
for lo, hi, lab in [(1970, 1999, "entered before 2000"),
                    (2000, 2009, "entered 2000s"),
                    (2010, 2025, "entered 2010+")]:
    s = sub[(sub.cohort >= lo) & (sub.cohort <= hi)]
    if len(s):
        k = int(s.hd.sum())
        clo, chi = D.wilson_ci(k, len(s))
        say("  %-20s %5.1f%% hub-dominant [%.1f-%.1f] (n=%d)"
            % (lab, 100 * k / len(s), 100 * clo, 100 * chi, len(s)))

# ─────────────────────────────────────────────── 7. gold standard
rule("7. GOLD STANDARD (user-coded)")
gp = BASE / "gold_standard_coded.csv"
if not gp.exists():
    say("Not coded yet. Fill gold_standard_to_code.csv (see "
        "gold_standard_codebook.md), save it as gold_standard_coded.csv in "
        "%s, and rerun this script." % BASE)
else:
    g = pd.read_csv(gp, encoding="utf-8-sig")
    g["your_label"] = g.your_label.astype(str).str.strip().str.lower()
    g = g[g.your_label.isin(LABELS)]
    g = (g.merge(c[["id", "llm_label", "regex_stance"]], on="id", how="left")
         .dropna(subset=["llm_label"]))
    g["regex_as_llm"] = g.regex_stance.replace({"functional": "mechanistic"})
    weights = key.regex_stance.value_counts() / g.regex_stance.value_counts()
    g["w"] = g.regex_stance.map(weights)
    for col, lab in [("llm_label", "LLM"), ("regex_as_llm", "regex")]:
        agree = g.your_label.eq(g[col])
        say("  human vs %-5s n=%d  kappa %.2f | raw agreement %.0f%% | "
            "corpus-weighted agreement %.0f%%"
            % (lab, len(g), cohen_kappa_score(g.your_label, g[col]),
               100 * agree.mean(), 100 * np.average(agree, weights=g.w)))
    say("\nhuman (rows) vs LLM (columns):")
    say(pd.crosstab(g.your_label, g.llm_label).to_string())
    say("genuine relay use, human vs LLM: kappa %.2f"
        % cohen_kappa_score(g.your_label.isin(GENUINE),
                            g.llm_label.isin(GENUINE)))

# ─────────────────────────────────────────────── 8. coder reliability
rule("8. LLM CODER RELIABILITY (independent re-coding of a cross-batch "
     "sample)")
rp = BASE / "reliability" / "results_rel.jsonl"
if not rp.exists():
    say("Not run yet (sample: Code/llm/relay_sentence_reliability.py).")
else:
    rr = pd.DataFrame([json.loads(x) for x in
                       rp.read_text(encoding="utf-8").splitlines()
                       if x.strip()])
    rr = (rr[rr.label.isin(LABELS)].drop_duplicates("id")
          .merge(c[["id", "llm_label", "llm_conf", "regex_stance"]],
                 on="id"))
    say("sentences re-coded: %d" % len(rr))
    say("all labels:           raw agreement %.0f%%, kappa %.2f"
        % (100 * rr.label.eq(rr.llm_label).mean(),
           cohen_kappa_score(rr.label, rr.llm_label)))
    say("genuine relay use:    raw agreement %.0f%%, kappa %.2f"
        % (100 * rr.label.isin(GENUINE).eq(rr.llm_label.isin(GENUINE)).mean(),
           cohen_kappa_score(rr.label.isin(GENUINE),
                             rr.llm_label.isin(GENUINE))))
    hi = rr[rr.llm_conf != "low"]
    say("first pass not low-confidence (n=%d): raw agreement %.0f%%, "
        "kappa %.2f" % (len(hi), 100 * hi.label.eq(hi.llm_label).mean(),
                        cohen_kappa_score(hi.label, hi.llm_label)))
    say("regex vs re-code, genuine relay use: kappa %.2f"
        % cohen_kappa_score(rr.regex_stance.isin(["characterization",
                                                  "functional"]),
                            rr.label.isin(GENUINE)))
    say("\nfirst pass (rows) vs re-code (columns):")
    say(pd.crosstab(rr.llm_label, rr.label).to_string())

(BASE / "relay_sentence_report.txt").write_text("\n".join(LOG),
                                                 encoding="utf-8")
print("\nSaved: %s, %s" % (BASE / "relay_sentence_codes.csv",
                           BASE / "relay_sentence_report.txt"))
