"""
07_clean_framing_and_topics.py — Create definitive clean datasets:
  1. Clean framing: detect genuine metaphorical frames across ALL sentences,
     no "other" category, key_structure excluded (emphasis, not metaphor)
  2. Clean topics: fix BERTopic garbage topics, reduce noise
  3. Also run frame detection on PubMed abstracts for longer temporal coverage
  4. Save clean CSVs ready for figure generation
"""
import sys, io, os, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
from pathlib import Path
from collections import OrderedDict

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW  = ROOT / "Data" / "raw" / "pubmed"

# ═══════════════════════════════════════════════════════════════════════════
# PART 1: CLEAN FRAMING — genuine metaphorical frames only
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("PART 1: HONEST FRAME DETECTION")
print("=" * 70)

# Frame patterns: each captures a CONCEPTUAL/METAPHORICAL description of what
# the thalamus IS or DOES. Anatomical facts, methods, findings = excluded.
FRAMES = OrderedDict([
    ("relay", re.compile(
        r"relay\s*(station|center|centre|hub|nucleus|for\b|of\s|between|structure)"
        r"|way.?station|sensory\s+relay|information\s+relay"
        r"|(thalamus|it)\s+(relays|relayed)\s+(sensory|motor|visual|auditory|information|signal)",
        re.I)),
    ("hub", re.compile(r"\bhub\b|\bnexus\b", re.I)),
    ("gateway", re.compile(
        r"\bgateway\b|\bgatekeep|portal|gating\s+(of|mechanism|function|information)",
        re.I)),
    ("gate", re.compile(
        r"\bgate\b(?!\s*(charge|voltage|current|ion))|\bgating\b(?!\s*(current|variable|channel|ring))",
        re.I)),
    ("modulator", re.compile(
        r"thalamus\s+(modulat|is\s+a\s+modulat)|thalamic\s+modulat"
        r"|modulat(es|ing|or)\s+(cortical|neural|brain|sensory)\s+(activity|state|response)",
        re.I)),
    ("integrator", re.compile(
        r"integrat(es|ive|ion)\s+(center|centre|hub|of\s+information|signals?|sensory|input)",
        re.I)),
    ("filter", re.compile(
        r"(sensory|information|neural)\s+filter|\bfilter(s|ing)?\b\s+(sensory|information)",
        re.I)),
    ("regulator", re.compile(
        r"regulat(es|or|ory)\s+(cortical|brain|neural|arousal|sleep|wake|consciousness|state|activity)",
        re.I)),
    ("coordinator", re.compile(
        r"coordinat(es|or|ion)\s+(of\s+)?(cortical|brain|neural|network|activity|information)",
        re.I)),
    ("pacemaker", re.compile(
        r"pacemaker|oscillat(ory|ion)\s+(generator|source|center|centre)",
        re.I)),
    ("switchboard", re.compile(r"switchboard|switch\s*(board|station)|routing\s+station", re.I)),
    ("amplifier", re.compile(
        r"amplif(ier|ies|y|ying|ication)\s+(of\s+)?(cortical|neural|signal|sensory)",
        re.I)),
    ("distributor", re.compile(
        r"distribut(es|or|ing|ion)\s+(of\s+)?(information|signal|input|sensory)",
        re.I)),
    ("processor", re.compile(
        r"process(es|or|ing)\s+(sensory|information|visual|auditory|signal)",
        re.I)),
    ("bridge", re.compile(r"\bbridge\b\s+(between|connecting|linking)", re.I)),
    ("bottleneck", re.compile(r"\bbottleneck\b", re.I)),
    ("blackboard", re.compile(r"\bblackboard\b", re.I)),
    ("interface", re.compile(r"\binterface\b\s+(between|for|of)", re.I)),
    ("network_node", re.compile(
        r"(network|circuit)\s+node|node\s+(in|within|of)\s+(a\s+|the\s+)?"
        r"(large.scale|brain|neural|functional|cortical)",
        re.I)),
    ("clinical_target", re.compile(
        r"(therapeutic|treatment|clinical|stimulation|DBS|neuromodulation)\s+target"
        r"|target\s+(for|of)\s+(treatment|therapy|intervention|stimulation|DBS)",
        re.I)),
    ("anti_relay", re.compile(
        r"not\s+(merely|just|simply|only)\s+(a\s+)?relay"
        r"|more\s+than\s+(a\s+|just\s+)?(a\s+)?relay"
        r"|beyond\s+(the\s+)?relay",
        re.I)),
])

def detect_frames(text_series):
    """Detect all frames in a text series. Returns frame_type (primary) and frame_all (list)."""
    frame_type = pd.Series("", index=text_series.index)
    frame_all = pd.Series("", index=text_series.index)

    for fname, pat in FRAMES.items():
        mask = text_series.str.contains(pat, na=False)
        # Primary: first match wins (priority order)
        frame_type = frame_type.where(frame_type != "", other="")
        frame_type[mask & (frame_type == "")] = fname
        # All: accumulate
        frame_all[mask] = frame_all[mask] + "," + fname

    frame_all = frame_all.str.strip(",")
    return frame_type, frame_all


# ── Apply to ALL PMC+Unpaywall sentences ──
print("\nLoading all sentences...")
sents = pd.read_csv(PROC / "all_sentences_bertopic.csv")
sents["sentence"] = sents["sentence"].fillna("")
sents["year"] = pd.to_numeric(sents["year"], errors="coerce")
print(f"  Total sentences: {len(sents):,}")

print("Detecting frames in sentences...")
sents["frame_type"], sents["frame_all"] = detect_frames(sents["sentence"])

framed = sents[sents["frame_type"] != ""].copy()
print(f"  Framed sentences: {len(framed):,} ({len(framed)/len(sents)*100:.1f}%)")

# Distribution
print("\n  Frame distribution:")
fd = framed["frame_type"].value_counts()
for cat, n in fd.items():
    print(f"    {cat:20s}  {n:5d}")

# Save
framed.to_csv(PROC / "framing_clean.csv", index=False)
print(f"\n  Saved framing_clean.csv ({len(framed):,} rows)")

# ── Apply to PubMed abstracts ──
print("\nDetecting frames in abstracts...")
raw_files = list(RAW.glob("pubmed_broad_title_only_1950_2026*.csv"))
if raw_files:
    raw = pd.read_csv(raw_files[0])
    raw["year"] = pd.to_numeric(raw["year"], errors="coerce")
    raw["abstract"] = raw["abstract"].fillna("")
    print(f"  PubMed abstracts: {len(raw):,}")

    # Split abstracts into sentences for frame detection
    import nltk
    try:
        nltk.data.find("tokenizers/punkt_tab")
    except LookupError:
        nltk.download("punkt_tab", quiet=True)

    abstract_sents = []
    for _, row in raw.iterrows():
        if len(row["abstract"]) < 30:
            continue
        try:
            sentences = nltk.sent_tokenize(row["abstract"])
        except:
            sentences = re.split(r"(?<=[.!?])\s+", row["abstract"])
        for s in sentences:
            if re.search(r"thalam", s, re.I) and len(s) > 20:
                abstract_sents.append({
                    "pmid": row["pmid"],
                    "year": row["year"],
                    "sentence": s,
                    "source": "abstract"
                })

    abs_df = pd.DataFrame(abstract_sents)
    print(f"  Abstract thalamus sentences: {len(abs_df):,}")

    abs_df["frame_type"], abs_df["frame_all"] = detect_frames(abs_df["sentence"])
    abs_framed = abs_df[abs_df["frame_type"] != ""].copy()
    print(f"  Framed abstract sentences: {len(abs_framed):,}")

    # Distribution
    afd = abs_framed["frame_type"].value_counts()
    print("\n  Abstract frame distribution:")
    for cat, n in afd.items():
        print(f"    {cat:20s}  {n:5d}")

    abs_framed.to_csv(PROC / "framing_clean_abstracts.csv", index=False)
    print(f"\n  Saved framing_clean_abstracts.csv ({len(abs_framed):,} rows)")
else:
    print("  No PubMed abstracts found.")
    abs_framed = pd.DataFrame()

# ═══════════════════════════════════════════════════════════════════════════
# PART 2: CLEAN BERTOPIC TOPICS
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("PART 2: CLEAN BERTOPIC TOPICS")
print("=" * 70)

# Identify garbage topics
bt = sents.copy()  # already has bertopic_id from all_sentences_bertopic.csv

# Topic 0 is catch-all (11k+ sentences, very generic)
# Topic 6 is PDF garbage (merged text, broken references)
# Let's check which topics are garbage by looking at avg sentence quality
print("\nChecking topic quality...")

topic_stats = []
for tid in bt["bertopic_id"].unique():
    if tid == -1:
        continue
    sub = bt[bt["bertopic_id"] == tid]
    # Garbage indicators:
    # 1. Very long avg sentence (PDF artifacts)
    avg_len = sub["sentence"].str.len().mean()
    # 2. Contains lots of non-alpha characters (broken text)
    non_alpha_ratio = sub["sentence"].apply(
        lambda s: sum(1 for c in str(s) if not c.isalpha() and not c.isspace()) / max(len(str(s)), 1)
    ).mean()
    # 3. Contains reference patterns (merged citations)
    ref_pct = sub["sentence"].str.contains(r"\(\d{4}[a-z]?\)", na=False).mean()

    topic_stats.append({
        "topic_id": tid,
        "label": sub["bertopic_label"].iloc[0] if "bertopic_label" in sub.columns else f"topic_{tid}",
        "count": len(sub),
        "avg_len": avg_len,
        "non_alpha_ratio": non_alpha_ratio,
        "ref_pct": ref_pct
    })

ts = pd.DataFrame(topic_stats).sort_values("count", ascending=False)

# Flag garbage: high non_alpha or high ref_pct
garbage_mask = (ts["non_alpha_ratio"] > 0.20) | (ts["ref_pct"] > 0.25) | (ts["avg_len"] > 500)
garbage_topics = set(ts[garbage_mask]["topic_id"].tolist())

# Also flag Topic 0 if it's a catch-all (>10% of all sentences)
if 0 in ts["topic_id"].values:
    t0_count = ts[ts["topic_id"] == 0]["count"].iloc[0]
    if t0_count > len(bt) * 0.10:
        garbage_topics.add(0)
        print(f"  Topic 0 flagged as catch-all (n={t0_count:,})")

print(f"\n  Garbage topics identified: {len(garbage_topics)}")
for gid in sorted(garbage_topics):
    row = ts[ts["topic_id"] == gid].iloc[0]
    print(f"    Topic {gid}: {row['label'][:50]:50s} n={row['count']:5d} "
          f"avg_len={row['avg_len']:.0f} non_alpha={row['non_alpha_ratio']:.2f} ref_pct={row['ref_pct']:.2f}")

# Reassign garbage topic sentences to noise (-1)
bt.loc[bt["bertopic_id"].isin(garbage_topics), "bertopic_id"] = -1
bt.loc[bt["bertopic_id"].isin(garbage_topics), "bertopic_label"] = "noise"

# Clean topic labels: remove numeric prefix, underscores
def clean_label(s):
    s = str(s)
    s = re.sub(r"^\d+_", "", s)  # remove "0_", "1_" prefix
    s = s.replace("_", " ")
    return s.strip()

bt["topic_clean"] = bt["bertopic_label"].apply(clean_label)
bt.loc[bt["bertopic_id"] == -1, "topic_clean"] = "unassigned"

# Stats
n_topics = bt[bt["bertopic_id"] != -1]["bertopic_id"].nunique()
n_noise = (bt["bertopic_id"] == -1).sum()
print(f"\n  Clean topics: {n_topics}")
print(f"  Unassigned: {n_noise:,} ({n_noise/len(bt)*100:.1f}%)")

# Save
bt.to_csv(PROC / "all_sentences_clean.csv", index=False)
print(f"  Saved all_sentences_clean.csv ({len(bt):,} rows)")

# Top 20 clean topics
print("\n  Top 20 clean topics:")
tc = bt[bt["bertopic_id"] != -1].groupby("topic_clean").size().sort_values(ascending=False)
for i, (name, count) in enumerate(tc.head(20).items()):
    print(f"    {i+1:2d}. {name:50s} n={count:5d}")

# ═══════════════════════════════════════════════════════════════════════════
# PART 3: COMBINE — each sentence has topic + frame (if any)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("PART 3: SUMMARY")
print("=" * 70)

# Sentences with BOTH a topic AND a frame
has_topic = bt["bertopic_id"] != -1
has_frame = bt["frame_type"] != ""

print(f"  Total sentences: {len(bt):,}")
print(f"  With topic: {has_topic.sum():,} ({has_topic.mean()*100:.1f}%)")
print(f"  With frame: {has_frame.sum():,} ({has_frame.mean()*100:.1f}%)")
print(f"  With both: {(has_topic & has_frame).sum():,}")

# Frame distribution across topics
print("\n  Frames by topic (top 10 topics):")
top_topics = bt[has_topic]["topic_clean"].value_counts().head(10).index
for topic in top_topics:
    sub = bt[(bt["topic_clean"] == topic) & has_frame]
    if len(sub) > 0:
        fd = sub["frame_type"].value_counts().head(3)
        top_frames = ", ".join(f"{f}({n})" for f, n in fd.items())
        print(f"    {topic:40s} frames: {top_frames}")

print("\n Done.")
