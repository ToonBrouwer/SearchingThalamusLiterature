"""
Framing taxonomy classifier for thalamus sentences.

Applies a keyword/regex taxonomy to three datasets:
  1. thalamus_framing_sentences.csv      (full-text framing sentences)
  2. thalamus_all_sentences.csv          (all thalamus sentences from full-text)
  3. PubMed abstracts (extracted inline)

Each sentence is assigned a primary framing label from the TAXONOMY dict and,
optionally, multiple secondary labels (multi-label output stored in
framing_categories).  The priority order for the primary label is defined by
PRIORITY.

Inputs:
    Data/interim/sentence_extraction/thalamus_framing_sentences.csv
    Data/interim/sentence_extraction/thalamus_all_sentences.csv
    Data/raw/pubmed/pubmed_broad_title_only_1950_2026_<date>.csv

Outputs (Data/processed/):
    framing_sentences_classified.csv         — full-text framing sentences + labels
    all_sentences_classified.csv             — all full-text thalamus sentences + labels
    abstract_framing_sentences.csv           — abstract framing sentences (raw)
    abstract_framing_sentences_classified.csv — abstract framing sentences + labels
    combined_framing_classified.csv          — merged, deduplicated

Outputs (Graphs/):
    01_framing_trends_fulltext.png
    02_framing_trends_abstracts.png
    03_framing_trends_combined.png
    04_framing_line_chart_combined.png

Note on paths:
    Paths are resolved relative to this script's location (ROOT = project root).
    The project-level utilities Code/config.py and Code/paths.py are not wired
    in here; a future refactor could replace ROOT with load_settings() +
    build_data_dirs() for full consistency.
"""

import sys, io, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from pathlib import Path
import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT   = _ANALYSIS_DIR.parents[1]   # project root
INTERIM = ROOT / "Data" / "interim"
RAW     = ROOT / "Data" / "raw"
PROC    = ROOT / "Data" / "processed"
GRAPHS  = ROOT / "Graphs"
PROC.mkdir(parents=True, exist_ok=True)
GRAPHS.mkdir(parents=True, exist_ok=True)

# ── Framing-detection helpers (mirrors extract_framing_sentences.py) ───────
# Note: these patterns are intentionally kept in sync with extract_framing_sentences.py
# and phase_b_extract_abstract_framing.py; any change to one should be mirrored.
THALAMUS_RE  = re.compile(r"\bthalamus\b", re.IGNORECASE)
VERB_TRIGGERS = [
    "thalamus is", "the thalamus is",
    "thalamus acts", "the thalamus acts",
    "thalamus relays", "the thalamus relays",
    "thalamus plays", "the thalamus plays",
    "thalamus serves", "the thalamus serves",
    "thalamus has", "the thalamus has",
    "thalamus may", "the thalamus may",
    "thalamus could", "the thalamus could",
    "thalamus contributes", "the thalamus contributes",
    "thalamus mediates", "the thalamus mediates",
    "thalamus provides", "the thalamus provides",
    "thalamus is associated with", "the thalamus is associated with",
    "thalamus was associated with", "the thalamus was associated with",
    "thalamus has been identified as", "the thalamus has been identified as",
    "thalamus could modulate", "the thalamus could modulate",
    "thalamus which provides", "the thalamus which provides",
    "thalamus is involved in", "the thalamus is involved in",
    "thalamus plays a", "the thalamus plays a",
    "thalamus serves as", "the thalamus serves as",
]
APPOSITIVE_RE = re.compile(
    r"\b(the\s+)?thalamus\s*,\s*([^,]{5,200})\s*,", re.IGNORECASE
)
BAD_APPOSITIVES = {"however","therefore","for example","for instance","in contrast","in turn"}

def split_sentences(text):
    if not isinstance(text, str) or not text.strip():
        return []
    text = re.sub(r"\s+", " ", text).strip()
    return re.split(r"(?<=[.!?])\s+", text)

def match_trigger(sentence):
    s = sentence.lower()
    for t in VERB_TRIGGERS:
        if t in s:
            return t
    return None

def extract_appositive(sentence):
    m = APPOSITIVE_RE.search(sentence)
    if not m:
        return None
    phrase = m.group(2).strip()
    if phrase.lower() in BAD_APPOSITIVES:
        return None
    return phrase

def is_framing_sentence(sentence):
    trigger = match_trigger(sentence)
    appositive = extract_appositive(sentence)
    return trigger is not None or appositive is not None, trigger, appositive


# ── Framing taxonomy ──────────────────────────────────────────────────────
# Each category maps to a list of regex patterns (case-insensitive).
# All matching categories are assigned (multi-label output); the primary
# label is the highest-priority match according to PRIORITY below.

TAXONOMY = {
    "relay": [
        r"relay station", r"relay center", r"relay hub", r"relay nucleus",
        r"relay for ", r"relay of ", r"relays (sensory|motor|visual|auditory|information|signal)",
        r"way.?station", r"information relay", r"sensory relay", r"relay structure",
        r"primary relay", r"major relay", r"relay between", r"relay.*to the cortex",
        r"relay.*sensory.*cortex", r"relay.*information.*cortex",
        r"(serves|acting|acts) as (a |an )?(relay|sensory relay)",
        r"thalamus relays",
    ],
    "hub_node": [
        r"\bhub\b", r"\bnexus\b",
        r"central (hub|node|relay)", r"integrative hub", r"crucial hub",
        r"key hub", r"circuit hub", r"network hub", r"relay hub",
    ],
    "gateway": [
        r"\bgateway\b", r"\bgatekeep", r"\bgate\b(?! charge)",
        r"\bportal\b", r"gating (of |mechanism|function)",
        r"sensory gating", r"information gating",
        r"filter (sensory|information|neural|of )", r"information filter",
        r"entrance (for|to) (sensory|neural|information)",
    ],
    "modulator": [
        r"modulat(es|ing|ion|or) (cortical|thalamocortical|neural|sensory|brain)",
        r"thalamus.*modulat", r"thalamus.*regulat(es|ing) (activity|flow|state|arousal)",
        r"thalamus.*controls (activity|flow|arousal|state|consciousness)",
        r"gain control", r"top.down modulation",
    ],
    "integrator": [
        r"integrat(es|ing|ive|ion) (of |information|signals|input|sensory|motor)",
        r"integration (center|hub|of|node)", r"integrative center",
        r"converge\w* (of |information|input)", r"convergence (of |center|zone)",
        r"receiving and (distribut|integrat|process)",
    ],
    "network_component": [
        r"(network|circuit) node", r"(node|hub) (in|within|of) (a |the )?(large.scale |brain |neural |cortico)",
        r"large.scale (brain |neural )?network", r"functional network",
        r"thalamocortical (network|circuit|loop)", r"cortico.thalamo",
        r"connectome", r"resting.state network", r"default mode",
    ],
    "role_player": [
        r"plays (a|an) (important|critical|crucial|key|central|major|vital|pivotal|fundamental|significant|essential|active|more active|causal) role",
        r"plays (a|an) role in",
        r"plays distinct roles",
        r"may play (a|an) (important|critical|crucial|key|central|major) role",
    ],
    "clinical_target": [
        r"(target|site) (for|of) (treatment|therapy|intervention|stimulation|surgery|deep brain)",
        r"deep brain stimulation", r"therapeutic target", r"clinical target",
        r"treatment target", r"site of (pathology|damage|injury|lesion|atrophy)",
        r"neuromodulation target",
    ],
    "structure_description": [
        r"(complex|heterogeneous|intricate|complicated) (structure|region|area|organ)",
        r"composed of (multiple|many|\d+) nuclei", r"consisting of.*nuclei",
        r"multiple (thalamic )?nuclei", r"numerous nuclei",
        r"(subcortical|forebrain|diencephalic) structure",
        r"largest (structure|region)", r"paired structure",
        r"gray matter (structure|region|mass)",
    ],
    "involved_in": [
        r"(is |was |are )involved in",
        r"(is |was |are )implicated in",
        r"participates in",
        r"plays a part in",
    ],
}

# Compiled patterns
COMPILED = {
    cat: [re.compile(p, re.IGNORECASE) for p in patterns]
    for cat, patterns in TAXONOMY.items()
}

# Priority order for primary label (when multiple match)
PRIORITY = [
    "relay", "hub_node", "gateway", "modulator", "integrator",
    "network_component", "clinical_target", "structure_description",
    "role_player", "involved_in",
]


def classify_sentence(sentence: str) -> tuple[list[str], str]:
    """Return (all_matching_categories, primary_category)."""
    s = str(sentence)
    matched = [cat for cat, pats in COMPILED.items() if any(p.search(s) for p in pats)]
    if not matched:
        primary = "other"
    else:
        # Pick highest-priority match
        primary = next((c for c in PRIORITY if c in matched), matched[0])
    return matched, primary


def apply_taxonomy(df: pd.DataFrame, sentence_col: str = "sentence") -> pd.DataFrame:
    df = df.copy()
    results = df[sentence_col].apply(lambda s: classify_sentence(s))
    df["framing_categories"] = results.apply(lambda x: "|".join(x[0]) if x[0] else "other")
    df["framing_primary"]    = results.apply(lambda x: x[1])
    return df


# ── Step 1: Classify full-text framing sentences ──────────────────────────
print("=== Loading full-text framing sentences ===")
framing_path = INTERIM / "sentence_extraction" / "thalamus_framing_sentences.csv"
framing_df   = pd.read_csv(framing_path)
print(f"  Rows: {len(framing_df)}, Year range: {framing_df['year'].min()}-{framing_df['year'].max()}")

framing_classified = apply_taxonomy(framing_df)
out_path = PROC / "framing_sentences_classified.csv"
framing_classified.to_csv(out_path, index=False)
print(f"  Saved -> {out_path}")
print("  Primary label distribution:")
print(framing_classified["framing_primary"].value_counts().to_string())
print()


# ── Step 2: Classify all full-text thalamus sentences ─────────────────────
print("=== Loading all thalamus sentences ===")
all_path = INTERIM / "sentence_extraction" / "thalamus_all_sentences.csv"
all_df   = pd.read_csv(all_path)
print(f"  Rows: {len(all_df)}, Year range: {all_df['year'].min()}-{all_df['year'].max()}")

# Classify all (not just is_framing) — adds context for non-framing sentences too
all_classified = apply_taxonomy(all_df)
out_path2 = PROC / "all_sentences_classified.csv"
all_classified.to_csv(out_path2, index=False)
print(f"  Saved -> {out_path2}")
print("  Primary label distribution (is_framing==True only):")
sub = all_classified[all_classified["is_framing"] == True]
print(sub["framing_primary"].value_counts().to_string())
print()


# ── Step 3: Extract and classify abstract framing sentences ───────────────
print("=== Extracting framing sentences from abstracts ===")
abstracts_path = RAW / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv"
abs_df = pd.read_csv(abstracts_path, dtype={"pmid": str})
print(f"  Total abstract records: {len(abs_df)}, Year range: {abs_df['year'].min()}-{abs_df['year'].max()}")
print(f"  Records with abstract text: {abs_df['abstract'].notna().sum()}")

abs_rows = []
for _, row in abs_df.iterrows():
    text = row.get("abstract")
    if not isinstance(text, str) or not text.strip():
        continue
    sentences = split_sentences(text)
    for i, sent in enumerate(sentences):
        if not THALAMUS_RE.search(sent):
            continue
        is_framing, trigger, appositive = is_framing_sentence(sent)
        if is_framing:
            abs_rows.append({
                "pmid": row.get("pmid"),
                "year": row.get("year"),
                "journal": row.get("journal"),
                "title": row.get("title"),
                "publication_types": row.get("publication_types"),
                "sentence_index": i,
                "sentence": sent,
                "verb_trigger": trigger,
                "appositive_phrase": appositive,
                "is_framing": True,
                "text_source": "abstract",
            })

abs_framing_df = pd.DataFrame(abs_rows)
print(f"  Framing sentences extracted: {len(abs_framing_df)}")
print(f"  Year range of extracted: {abs_framing_df['year'].min()}-{abs_framing_df['year'].max()}")

abs_framing_path = INTERIM / "sentence_extraction" / "abstract_framing_sentences.csv"
abs_framing_df.to_csv(abs_framing_path, index=False)
print(f"  Saved raw -> {abs_framing_path}")

# Classify
abs_classified = apply_taxonomy(abs_framing_df)
out_path3 = PROC / "abstract_framing_sentences_classified.csv"
abs_classified.to_csv(out_path3, index=False)
print(f"  Saved classified -> {out_path3}")
print("  Primary label distribution:")
print(abs_classified["framing_primary"].value_counts().to_string())
print()


# ── Step 4: Temporal trend plots ──────────────────────────────────────────
print("=== Generating temporal trend analysis ===")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import numpy as np

    CATS_ORDERED = [
        "relay", "hub_node", "gateway", "integrator", "modulator",
        "network_component", "role_player", "involved_in",
        "structure_description", "clinical_target", "other",
    ]
    COLORS = {
        "relay":                "#E07B54",
        "hub_node":             "#5B9BD5",
        "gateway":              "#70AD47",
        "integrator":           "#9B59B6",
        "modulator":            "#F4C430",
        "network_component":    "#1ABC9C",
        "role_player":          "#95A5A6",
        "involved_in":          "#BDC3C7",
        "structure_description":"#AAB7B8",
        "clinical_target":      "#E74C3C",
        "other":                "#D5D8DC",
    }

    def plot_temporal_trends(classified_df, title, filename, min_year=1980):
        df = classified_df[classified_df["year"] >= min_year].copy()
        df["year"] = df["year"].astype(int)

        # Bin into 5-year windows for smoothness
        df["period"] = (df["year"] // 5 * 5)
        counts = df.groupby(["period", "framing_primary"]).size().unstack(fill_value=0)

        # Normalize to proportions
        props = counts.div(counts.sum(axis=1), axis=0)

        # Keep only categories that appear
        cats = [c for c in CATS_ORDERED if c in props.columns]
        props = props[cats]

        fig, axes = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={"height_ratios": [3, 1]})

        # Stacked area plot
        ax = axes[0]
        bottom = np.zeros(len(props))
        for cat in cats:
            vals = props[cat].values
            ax.fill_between(props.index, bottom, bottom + vals,
                            color=COLORS.get(cat, "#ccc"), label=cat, alpha=0.85)
            bottom += vals

        ax.set_xlim(props.index.min(), props.index.max())
        ax.set_ylim(0, 1)
        ax.set_ylabel("Proportion of framing sentences", fontsize=12)
        ax.set_title(title, fontsize=14, fontweight="bold")
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False, fontsize=9)

        # Raw count bar plot
        ax2 = axes[1]
        total = counts.sum(axis=1)
        ax2.bar(total.index, total.values, width=4, color="#5B9BD5", alpha=0.7)
        ax2.set_xlabel("Year (5-year bins)", fontsize=11)
        ax2.set_ylabel("N sentences", fontsize=11)
        ax2.set_xlim(props.index.min(), props.index.max())

        plt.tight_layout()
        out = GRAPHS / filename
        plt.savefig(out, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"  Saved plot -> {out}")

    plot_temporal_trends(framing_classified,
        title="Thalamus framing over time — Full-text framing sentences",
        filename="01_framing_trends_fulltext.png")

    plot_temporal_trends(abs_classified,
        title="Thalamus framing over time — Abstract framing sentences",
        filename="02_framing_trends_abstracts.png",
        min_year=1970)

    # Combined: merge both, deduplicate by pmid+sentence
    combined = pd.concat([
        framing_classified.assign(source="fulltext"),
        abs_classified.assign(source="abstract")
    ], ignore_index=True)
    combined_dedup = combined.drop_duplicates(subset=["pmid", "sentence"])
    plot_temporal_trends(combined_dedup,
        title="Thalamus framing over time — Combined (full-text + abstracts)",
        filename="03_framing_trends_combined.png",
        min_year=1970)

    combined_dedup.to_csv(PROC / "combined_framing_classified.csv", index=False)
    print(f"  Saved combined -> {PROC / 'combined_framing_classified.csv'}")

    # ── Per-decade breakdown table ──────────────────────────────────────
    print()
    print("=== Per-decade proportions (combined, %) ===")
    combined_dedup["decade"] = (combined_dedup["year"] // 10 * 10).astype("Int64")
    pivot = combined_dedup.groupby(["decade","framing_primary"]).size().unstack(fill_value=0)
    prop_pivot = (pivot.div(pivot.sum(axis=1), axis=0) * 100).round(1)
    cats_present = [c for c in CATS_ORDERED if c in prop_pivot.columns]
    print(prop_pivot[cats_present].to_string())

    # ── Also plot line chart per category ──────────────────────────────
    fig, ax = plt.subplots(figsize=(14, 6))
    df_line = combined_dedup[combined_dedup["year"] >= 1970].copy()
    df_line["period"] = (df_line["year"] // 5 * 5)
    counts_line = df_line.groupby(["period","framing_primary"]).size().unstack(fill_value=0)
    props_line  = counts_line.div(counts_line.sum(axis=1), axis=0)
    cats_line   = [c for c in CATS_ORDERED if c in props_line.columns]
    for cat in cats_line:
        ax.plot(props_line.index, props_line[cat], marker="o", markersize=4,
                color=COLORS.get(cat,"#ccc"), label=cat, linewidth=2)
    ax.set_xlabel("Year (5-year bins)", fontsize=11)
    ax.set_ylabel("Proportion", fontsize=11)
    ax.set_title("Thalamus framing trajectories (combined sources)", fontsize=13, fontweight="bold")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), frameon=False, fontsize=9)
    ax.set_ylim(0, 1)
    plt.tight_layout()
    plt.savefig(GRAPHS / "04_framing_line_chart_combined.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved line chart -> {GRAPHS / '04_framing_line_chart_combined.png'}")

except ImportError as e:
    print(f"  matplotlib not available, skipping plots: {e}")

print()
print("=== Done ===")
