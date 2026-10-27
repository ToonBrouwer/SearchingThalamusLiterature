"""
v18_data.py — single source of truth for every v18 figure and statistic.

Canonical inputs (see Paper/v18_figure_plan.md):
    Data/processed/all_sentences_full_corpus.csv   (354,599 sentences)
    Data/processed/framing_full_corpus.csv         (6,087 framing sentences)
    Data/raw/pubmed/pubmed_broad_title_only_1950_2026_2026-03-26.csv
    Data/processed/clustering_stability/            (Leiden winner: mpnet__raw__r005)

Derived tables are cached under Data/processed/v18/ — delete a cache file to
force recompute. No v18 figure may read the superseded v17-era files
(all_sentences_final.csv, framing_combined_v3.csv).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW = ROOT / "Data" / "raw"
V18 = PROC / "v18"
V18.mkdir(exist_ok=True)

sys.path.insert(0, str(ROOT / "Code"))

# Winning Leiden configuration (rank 1 in model_selection_scores.csv)
LEIDEN_WINNER = "mpnet__raw__r005"

# Human-readable domain names (reviewed with user 2026-09-01; keys are
# cluster ids of LEIDEN_WINNER). Verification notes:
#  D0 also holds CTB/Fluoro-Gold/BDA/PHA-L papers (classical tracers);
#     modern viral tracing (AAV/rabies) clusters elsewhere.
#  D4 is 38% visual / 29% somatosensory — kept as a sensory umbrella.
#  D5 vs D6: both epilepsy but D5 is 92% rodent / 2% patients (mechanisms),
#     D6 is 60% patients / 32% DBS (clinical) — kept separate.
#  D8 nuclei: MD 25%, ANT 17%, PVT 13%, Reuniens 9%; memory terms 44%.
#  D15 is mostly PVT (34% of papers).
DOMAIN_NAMES = {
    0: "Classical tract tracing",
    1: "Thalamic stroke & vascular lesions",
    2: "Pain & nociception",
    3: "Tremor surgery (VIM DBS/MRgFUS)",
    4: "Sensory relay circuits (visual & somatosensory)",
    5: "Absence seizures & TRN mechanisms",
    6: "Clinical epilepsy & ANT-DBS",
    7: "Neuroimaging of thalamic structure in disease",
    8: "Learning & memory (limbic thalamus)",
    9: "Psychiatric disorders",
    10: "Development",
    11: "Classical thalamocortical electrophysiology",
    12: "Sleep",
    13: "Auditory thalamus",
    14: "Thalamic tumors & neurosurgery",
    15: "Reward, feeding & addiction (PVT)",
    16: "Brain injury & secondary degeneration",
    17: "Movement disorders (PD/HD)",
    18: "Pediatric & infectious encephalopathies",
    19: "Anesthesia & consciousness",
    20: "Head-direction & vestibular system",
    21: "Tourette & tic disorders",
}

# Two broad research genres over the domains (defined 2026-09-11), used to
# show where the relay frame declined (circuit/systems) and where it
# persists (clinical/imaging).
CIRCUIT_DOMAINS = {0, 4, 5, 8, 10, 11, 12, 13, 15, 19, 20}
CLINICAL_DOMAINS = {1, 2, 3, 6, 7, 9, 14, 16, 17, 18, 21}
GENRES = ["circuit/systems", "clinical/imaging"]


def genre_of_domain(domain):
    """'circuit/systems', 'clinical/imaging' or 'unclustered'."""
    if domain in CIRCUIT_DOMAINS:
        return "circuit/systems"
    if domain in CLINICAL_DOMAINS:
        return "clinical/imaging"
    return "unclustered"

# ── Frame taxonomy: semantic groups + fixed colours (consistent across ALL
#    v18 figures) ─────────────────────────────────────────────────────────
FRAME_GROUPS = {
    "Metaphorical": ["relay", "hub", "gate", "gateway", "switchboard",
                      "blackboard", "bridge", "bottleneck", "interface"],
    "Functional":   ["modulator", "integrator", "filter", "regulator",
                      "coordinator", "pacemaker", "amplifier", "distributor",
                      "processor"],
    "Descriptive":  ["network_node", "clinical_target", "anti_relay"],
}
GROUP_OF_FRAME = {f: g for g, fs in FRAME_GROUPS.items() for f in fs}
GROUP_COLORS = {
    "Metaphorical": "#0072B2",   # Wong blue
    "Functional":   "#009E73",   # Wong green
    "Descriptive":  "#CC79A7",   # Wong pink
}
# The 7 frames shown individually in temporal panels; the rest pool to "other"
FRAME_COLORS = {
    "relay":           "#D55E00",  # vermillion
    "hub":             "#0072B2",  # blue
    "gate":            "#009E73",  # green
    "gateway":         "#E69F00",  # orange
    "clinical_target": "#CC79A7",  # pink
    "pacemaker":       "#7B4F9E",  # purple
    "modulator":       "#56B4E9",  # cyan
    "other":           "#bbbbbb",
}
SOURCE_LABELS = {
    "institutional": "Institutional PDF (LibKey)",
    "fulltext":      "PMC open access",
    "oa_pdf":        "OA PDF",
    "abstract":      "Abstract only",
}
SOURCE_COLORS = {
    "institutional": "#0072B2",
    "fulltext":      "#56B4E9",
    "oa_pdf":        "#009E73",
    "abstract":      "#E69F00",
}


# ── Loaders ──────────────────────────────────────────────────────────────
# Reference-list sentences (section == 'references') are cited TITLES, not
# the paper's own prose — excluded by default (decided 2026-09-01).
def _drop_references(df, include_references):
    if not include_references and "section" in df.columns:
        df = df[df.section != "references"]
    return df


def load_sentences(usecols=None, include_references=False) -> pd.DataFrame:
    if usecols is not None and "section" not in usecols:
        usecols = list(usecols) + ["section"]
    df = pd.read_csv(PROC / "all_sentences_full_corpus.csv",
                     low_memory=False, usecols=usecols)
    df = _drop_references(df, include_references)
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df = df.dropna(subset=["year"])
        df["year"] = df["year"].astype(int)
    return df


def load_framing(include_references=False) -> pd.DataFrame:
    df = pd.read_csv(PROC / "framing_full_corpus.csv", low_memory=False)
    df = _drop_references(df, include_references)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)
    df["frame_group"] = df["frame_type"].map(GROUP_OF_FRAME)
    return df


def load_pubs() -> pd.DataFrame:
    """Unique PubMed records with a valid year in 1950-2026."""
    df = pd.read_csv(
        RAW / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
        usecols=["pmid", "year"], low_memory=False)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = (df.dropna(subset=["year"])
            .query("1950 <= year <= 2026")
            .drop_duplicates(subset="pmid"))
    df["year"] = df["year"].astype(int)
    return df


def load_paper_techniques() -> pd.DataFrame:
    """Per-paper boolean technique flags computed from the canonical
    sentence corpus (any sentence of the paper matches → paper flagged).
    Cached as Data/processed/v18/paper_techniques.csv."""
    cache = V18 / "paper_techniques.csv"
    if cache.exists():
        return pd.read_csv(cache)
    from technique_patterns import tag_techniques, TECHNIQUES
    sent = load_sentences(usecols=["pmid", "sentence"])
    tag_techniques(sent, text_col="sentence")
    cols = [f"has_{t}" for t in TECHNIQUES]
    per_paper = sent.groupby("pmid")[cols].any().reset_index()
    per_paper.to_csv(cache, index=False)
    return per_paper


def load_paper_techniques_abstract() -> pd.DataFrame:
    """Per-paper technique flags from TITLE + ABSTRACT only.

    Use for TEMPORAL technique trends: abstract text exists for (nearly)
    every paper in every era, so detection probability is constant over
    time. Full-text-based flags (load_paper_techniques) inflate recent
    trends because detection scales with available text volume.
    Cached as Data/processed/v18/paper_techniques_abstract.csv."""
    cache = V18 / "paper_techniques_abstract.csv"
    if cache.exists():
        return pd.read_csv(cache)
    from technique_patterns import tag_techniques, TECHNIQUES
    raw = pd.read_csv(
        RAW / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
        usecols=["pmid", "year", "title", "abstract"], low_memory=False)
    raw["year"] = pd.to_numeric(raw["year"], errors="coerce")
    raw = raw.dropna(subset=["year"]).drop_duplicates(subset="pmid")
    raw["year"] = raw["year"].astype(int)
    raw["text"] = (raw.title.fillna("") + " " + raw.abstract.fillna(""))
    raw["has_abstract"] = raw.abstract.fillna("").str.len() >= 100
    tag_techniques(raw, text_col="text")
    cols = ["pmid", "year", "has_abstract"] + [f"has_{t}" for t in TECHNIQUES]
    out = raw[cols]
    out.to_csv(cache, index=False)
    return out


def load_domains() -> pd.DataFrame:
    """Paper-level Leiden domain assignment for the winning config,
    with human-readable top-term labels."""
    pmids = np.load(PROC / "specter2_pmids.npy", allow_pickle=True)
    memb = np.load(PROC / "clustering_stability" /
                   f"memberships_{LEIDEN_WINNER}.npy")
    df = pd.DataFrame({"pmid": pmids.astype(np.int64), "domain": memb})
    labels = pd.read_csv(PROC / "clustering_stability" /
                         f"cluster_alignment_{LEIDEN_WINNER}.csv",
                         usecols=["cluster", "n_papers", "top_terms_short"])
    df = df.merge(labels.rename(columns={"cluster": "domain",
                                          "top_terms_short": "domain_label"}),
                  on="domain", how="left")
    df["domain_name"] = df.domain.map(DOMAIN_NAMES).fillna(df.domain_label)
    return df


# ── Stats helpers ────────────────────────────────────────────────────────
def wilson_ci(k, n, z=1.96):
    """Wilson score interval for a binomial proportion. Vectorised."""
    k = np.asarray(k, dtype=float)
    n = np.asarray(n, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        p = np.where(n > 0, k / n, np.nan)
        denom = 1 + z ** 2 / n
        centre = (p + z ** 2 / (2 * n)) / denom
        half = (z * np.sqrt(p * (1 - p) / n + z ** 2 / (4 * n ** 2))) / denom
    return centre - half, centre + half


def mann_kendall(years, values):
    """Kendall tau of values against years (the project's MK convention:
    scipy.stats.kendalltau on raw annual values)."""
    from scipy.stats import kendalltau
    years = np.asarray(years)
    values = np.asarray(values, dtype=float)
    ok = np.isfinite(values)
    if ok.sum() < 3:
        return np.nan, np.nan
    tau, p = kendalltau(years[ok], values[ok])
    return tau, p


def sig_stars(p):
    if not np.isfinite(p):
        return ""
    return "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else "ns"
