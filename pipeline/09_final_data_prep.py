"""
09_final_data_prep.py — Final data preparation for paper v10.

1. Fix mega-theme assignments (reduce "Other" from 56% to <20%)
2. Remove garbage topics (naacr, vibratome)
3. Create framing_combined.csv (merge abstract + full-text, 2,536 sentences)
4. Merge frame_type into all_sentences_final.csv
5. Recreate conclusion themes from scratch
"""
import sys, io, os, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW  = ROOT / "Data" / "raw" / "pubmed"

# ═══════════════════════════════════════════════════════════════════════════
# 1. FIX MEGA-THEMES
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("1. FIXING MEGA-THEME ASSIGNMENTS")
print("=" * 70)

df = pd.read_csv(PROC / "all_sentences_final.csv")
df["topic_clean_v2"] = df["topic_clean_v2"].fillna("unassigned")
print(f"Loaded {len(df):,} sentences")

# Comprehensive mega-theme assignment using keyword matching on topic labels
def assign_mega_theme(topic):
    t = topic.lower()

    # Clinical / disease
    if any(kw in t for kw in [
        "seizure", "epilepsy", "stroke", "infarct", "ischemi", "tremor", "dbs",
        "sclerosis", "atrophy", "tumor", "glioma", "migraine", "ptsd", "trauma",
        "depression", "depressive", "mdd", "anxiety", "alzheimer", "dementia",
        "ocd", "harm", "cocaine", "addiction", "nicoti", "alcohol",
        "injury", "damage", "tbi", "microglia", "preterm", "infant", "birth",
        "pain", "nocicepti", "neuropath", "chronic pain",
        "schizophren", "psychos", "psychotic",
        "consciousness", "anaesthe", "propofol",
        "fatigue", "disability",
        "hemorrhag", "thrombos",
    ]):
        return "Clinical / disease"

    # Sensory / perception
    if any(kw in t for kw in [
        "visual", "retina", "pulvinar", "lgn", "geniculate",
        "auditory", "sound", "tinnitus", "acoustic",
        "somatosens", "whisker", "barrel", "nocicepti",
        "taste", "gustatory", "chemosens", "olfact",
        "speech", "language", "aphasia",
    ]):
        return "Sensory / perception"

    # Motor / basal ganglia
    if any(kw in t for kw in [
        "motor", "ganglia", "basal", "cerebellum", "locomot",
        "dystonia", "parkinson",
    ]):
        return "Motor"

    # Cognitive / behavioral
    if any(kw in t for kw in [
        "fear", "conditioning", "extinction",
        "reward", "motivation", "incentive",
        "emotion", "emotional",
        "memory", "hippocamp", "learning",
        "attention", "executive",
        "decision", "choice",
        "paraventricular", "stress",
    ]):
        return "Cognitive / behavioral"

    # Sleep / oscillations
    if any(kw in t for kw in [
        "sleep", "wakefulness", "insomnia", "nrem",
        "spindle", "ripple", "oscillat",
        "arousal",
    ]):
        return "Sleep / oscillations"

    # Circuit / cellular
    if any(kw in t for kw in [
        "trn", "reticular", "inhibitory", "gabaergic", "gaba",
        "habenula", "lateral habenula",
        "interneuron",
        "synap", "glutamat", "receptor",
        "ketamine", "gamma",
        "calcium", "channel",
    ]):
        return "Circuit / cellular"

    # Development / anatomy
    if any(kw in t for kw in [
        "axon", "thalamocortical", "development", "embryo", "prenatal",
        "progenitor", "neurogenesis", "patterning",
        "tcf7l2", "lef1", "gene", "pax6", "shh", "fgf",
        "virus", "viral", "aav", "injected",  # tracing
        "reunion",  # nucleus reuniens anatomy
    ]):
        return "Development / anatomy"

    # Methodology / imaging
    if any(kw in t for kw in [
        "volume", "grey matter", "voxel", "segment", "freesurfer",
        "putamen", "caudate",  # segmentation
        "age", "males", "women", "sex differ",
        "naacr", "chocr", "spectroscop",
        "cut", "vibratome", "leica",
        "artery", "arteries", "vascular",
        "atlas", "parcellat",
        "tractograph", "dti", "diffusion",
        "tfus", "ultrasound",
    ]):
        return "Methodology / imaging"

    return "Other"

df["mega_theme"] = df["topic_clean_v2"].apply(assign_mega_theme)

# Remove garbage topics
GARBAGE_TOPICS = ["naacr chocr naa tn", "cut containing vibratome leica"]
for gt in GARBAGE_TOPICS:
    mask = df["topic_clean_v2"].str.contains(gt[:15], case=False, na=False)
    n = mask.sum()
    if n > 0:
        df.loc[mask, "bertopic_id_v2"] = -1
        df.loc[mask, "topic_clean_v2"] = "unassigned"
        df.loc[mask, "mega_theme"] = "unassigned"
        print(f"  Removed garbage topic '{gt}' ({n} sentences)")

# Check distribution
assigned = df[df.bertopic_id_v2 != -1]
mt = assigned.groupby("mega_theme").size().sort_values(ascending=False)
print(f"\nMega-theme distribution (assigned only, n={len(assigned):,}):")
for t, c in mt.items():
    print(f"  {c:5d} ({c/len(assigned)*100:5.1f}%)  {t}")

other_pct = mt.get("Other", 0) / len(assigned) * 100
print(f"\n  'Other' is now {other_pct:.1f}% (target: <20%)")

# Show what's left in "Other"
if "Other" in mt.index:
    other_topics = assigned[assigned.mega_theme == "Other"].groupby("topic_clean_v2").size().sort_values(ascending=False)
    print(f"\n  Remaining 'Other' topics ({len(other_topics)}):")
    for t, c in other_topics.head(15).items():
        print(f"    {c:4d}  {t}")

# ═══════════════════════════════════════════════════════════════════════════
# 2. COMBINE FRAMING DATA
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. COMBINING FRAMING DATA")
print("=" * 70)

ft = pd.read_csv(PROC / "framing_clean.csv")
ab = pd.read_csv(PROC / "framing_clean_abstracts.csv")

ft["source"] = "fulltext"
if "source" not in ab.columns:
    ab["source"] = "abstract"

# Align columns
common_cols = ["pmid", "year", "sentence", "frame_type", "frame_all", "source"]
for col in common_cols:
    if col not in ft.columns:
        ft[col] = ""
    if col not in ab.columns:
        ab[col] = ""

combined = pd.concat([ft[common_cols], ab[common_cols]], ignore_index=True)
combined = combined.drop_duplicates(subset="sentence")
combined["year"] = pd.to_numeric(combined["year"], errors="coerce")

print(f"  Full-text framing: {len(ft):,}")
print(f"  Abstract framing: {len(ab):,}")
print(f"  Combined (dedup): {len(combined):,}")
print(f"\n  Frame distribution:")
for f, c in combined["frame_type"].value_counts().items():
    print(f"    {f:20s}  {c:4d} ({c/len(combined)*100:4.1f}%)")

combined.to_csv(PROC / "framing_combined.csv", index=False)
print(f"\n  Saved framing_combined.csv")

# ═══════════════════════════════════════════════════════════════════════════
# 3. MERGE FRAME_TYPE INTO ALL_SENTENCES_FINAL
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. MERGING FRAME_TYPE INTO ALL_SENTENCES_FINAL")
print("=" * 70)

# The frame_type column in all_sentences_final may already exist from pipeline 07
# but let's ensure it's correct by re-detecting frames
from collections import OrderedDict

FRAMES = OrderedDict([
    ("relay", re.compile(r"relay\s*(station|center|centre|hub|nucleus|for\b|of\s|between|structure)|way.?station|sensory\s+relay|information\s+relay|(thalamus|it)\s+(relays|relayed)\s+(sensory|motor|visual|auditory|information|signal)", re.I)),
    ("hub", re.compile(r"\bhub\b|\bnexus\b", re.I)),
    ("gateway", re.compile(r"\bgateway\b|\bgatekeep|portal|gating\s+(of|mechanism|function|information)", re.I)),
    ("gate", re.compile(r"\bgate\b(?!\s*(charge|voltage|current|ion))|\bgating\b(?!\s*(current|variable|channel|ring))", re.I)),
    ("modulator", re.compile(r"thalamus\s+(modulat|is\s+a\s+modulat)|thalamic\s+modulat|modulat(es|ing|or)\s+(cortical|neural|brain|sensory)\s+(activity|state|response)", re.I)),
    ("integrator", re.compile(r"integrat(es|ive|ion)\s+(center|centre|hub|of\s+information|signals?|sensory|input)", re.I)),
    ("filter", re.compile(r"(sensory|information|neural)\s+filter|\bfilter(s|ing)?\b\s+(sensory|information)", re.I)),
    ("regulator", re.compile(r"regulat(es|or|ory)\s+(cortical|brain|neural|arousal|sleep|wake|consciousness|state|activity)", re.I)),
    ("coordinator", re.compile(r"coordinat(es|or|ion)\s+(of\s+)?(cortical|brain|neural|network|activity|information)", re.I)),
    ("pacemaker", re.compile(r"pacemaker|oscillat(ory|ion)\s+(generator|source|center|centre)", re.I)),
    ("switchboard", re.compile(r"switchboard|switch\s*(board|station)|routing\s+station", re.I)),
    ("amplifier", re.compile(r"amplif(ier|ies|y|ying|ication)\s+(of\s+)?(cortical|neural|signal|sensory)", re.I)),
    ("distributor", re.compile(r"distribut(es|or|ing|ion)\s+(of\s+)?(information|signal|input|sensory)", re.I)),
    ("processor", re.compile(r"process(es|or|ing)\s+(sensory|information|visual|auditory|signal)", re.I)),
    ("bridge", re.compile(r"\bbridge\b\s+(between|connecting|linking)", re.I)),
    ("bottleneck", re.compile(r"\bbottleneck\b", re.I)),
    ("blackboard", re.compile(r"\bblackboard\b", re.I)),
    ("interface", re.compile(r"\binterface\b\s+(between|for|of)", re.I)),
    ("network_node", re.compile(r"(network|circuit)\s+node|node\s+(in|within|of)\s+(a\s+|the\s+)?(large.scale|brain|neural|functional|cortical)", re.I)),
    ("clinical_target", re.compile(r"(therapeutic|treatment|clinical|stimulation|DBS|neuromodulation)\s+target|target\s+(for|of)\s+(treatment|therapy|intervention|stimulation|DBS)", re.I)),
    ("anti_relay", re.compile(r"not\s+(merely|just|simply|only)\s+(a\s+)?relay|more\s+than\s+(a\s+|just\s+)?(a\s+)?relay|beyond\s+(the\s+)?relay", re.I)),
])

# Detect frames
sent_text = df["sentence"].fillna("")
frame_type = pd.Series("", index=df.index)
for fname, pat in FRAMES.items():
    mask = sent_text.str.contains(pat, na=False)
    frame_type[mask & (frame_type == "")] = fname

df["frame_type"] = frame_type
n_framed = (df["frame_type"] != "").sum()
print(f"  Sentences with frame: {n_framed:,} out of {len(df):,}")

# Save
df.to_csv(PROC / "all_sentences_final.csv", index=False)
print(f"  Saved all_sentences_final.csv")

# ═══════════════════════════════════════════════════════════════════════════
# 4. RECREATE CONCLUSION THEMES
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. RECREATING CONCLUSION THEMES")
print("=" * 70)

# Get conclusion-section sentences from full-text data
fulltext_path = ROOT / "Data" / "interim" / "fulltext" / "pmc_fulltext_2026-03-26.csv"
if fulltext_path.exists():
    print("  Loading full-text data for conclusion extraction...")
    ft_data = pd.read_csv(fulltext_path, usecols=["pmid", "year", "section_titles", "full_text"])
    ft_data["full_text"] = ft_data["full_text"].fillna("")

    # Extract conclusion sections
    conclusion_sents = []
    for _, row in ft_data.iterrows():
        text = str(row["full_text"])
        # Find conclusion/discussion sections
        patterns = [
            r"(?:conclusion|concluding|summary|discussion)\s*\n(.*?)(?:\n\s*(?:reference|acknowledg|supplementar|appendix|conflict)|$)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.I | re.DOTALL)
            if m:
                section = m.group(1)
                # Split into sentences
                sents_list = re.split(r"(?<=[.!?])\s+", section)
                for s in sents_list:
                    if len(s) > 30 and len(s) < 500:
                        if re.search(r"thalam", s, re.I):
                            conclusion_sents.append({
                                "pmid": row["pmid"],
                                "year": row["year"],
                                "sentence": s.strip(),
                                "section": "conclusion"
                            })
                break

    conc_df = pd.DataFrame(conclusion_sents)
    print(f"  Conclusion thalamus sentences: {len(conc_df):,}")

    if len(conc_df) > 50:
        # Classify conclusion themes using keyword patterns
        CONCLUSION_THEMES = {
            "Network / connectivity": r"network|connectiv|functional\s+connect|resting.state|default\s+mode|large.scale",
            "Clinical / therapeutic": r"treatment|therapy|therapeutic|clinical|patient|DBS|stimulat|intervention|disease",
            "Cognitive / attention": r"cognit|attention|executive|working\s+memory|decision|consciousness|awareness",
            "Sensory / circuit": r"sensory|visual|auditory|somatosens|circuit|pathway|project|afferent|efferent",
            "Memory / learning": r"memory|learning|hippocamp|spatial|navigation|consolidat|encoding|retrieval",
            "Sleep / oscillations": r"sleep|wake|arousal|spindle|oscillat|rhythm|NREM|REM|tonic|burst",
            "Pain / nociception": r"pain|nocicepti|analges|hyperalgesia|allodynia|chronic\s+pain",
            "Motor / movement": r"motor|movement|tremor|dyskinesia|locomot|basal\s+ganglia|cerebellum",
            "Development / plasticity": r"develop|plastic|maturat|critical\s+period|neurogenesis|progenitor",
            "Emotion / stress": r"emotion|fear|anxiety|stress|reward|motivat|depression|affect",
        }

        for theme, pat in CONCLUSION_THEMES.items():
            conc_df[theme] = conc_df["sentence"].str.contains(pat, case=False, na=False)

        # Also detect frames in conclusions
        conc_frame = pd.Series("", index=conc_df.index)
        for fname, fpat in FRAMES.items():
            mask = conc_df["sentence"].str.contains(fpat, na=False)
            conc_frame[mask & (conc_frame == "")] = fname
        conc_df["frame_type"] = conc_frame

        conc_df.to_csv(PROC / "conclusion_themes_v2.csv", index=False)
        print(f"  Saved conclusion_themes_v2.csv ({len(conc_df):,} rows)")

        # Theme distribution
        theme_cols = list(CONCLUSION_THEMES.keys())
        print("\n  Conclusion theme distribution:")
        for theme in theme_cols:
            n = conc_df[theme].sum()
            print(f"    {theme:30s}  {n:4d} ({n/len(conc_df)*100:4.1f}%)")
    else:
        print("  Too few conclusion sentences, skipping theme analysis")
else:
    print("  Full-text data not found, skipping conclusion extraction")

print("\n Done. All data prepared for v10 figures.")
