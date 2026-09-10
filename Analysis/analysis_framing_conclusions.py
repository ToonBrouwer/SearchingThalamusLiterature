"""
Analysis 4 — Does framing co-occur with certain types of conclusions?

Strategy:
  1. Extract 'conclusion sentences' from thalamus_all_sentences.csv using
     linguistic markers (e.g. "these results suggest", "we conclude", "taken
     together").
  2. For each paper that has at least one relay or hub framing sentence, collect
     its conclusion sentences.
  3. Compare the frequency of 10 topic keywords across relay-framed vs
     hub-framed papers.
  4. Analyse the relative position of framing sentences within the paper
     (sentence_index normalised to 0–1) to test whether relay framing appears
     more often in introductions and hub framing in discussions.

Inputs (Data/processed/):
    framing_embedded_clustered.csv   — cluster_id, sentence, pmid, sentence_index

Inputs (Data/interim/sentence_extraction/):
    thalamus_all_sentences.csv       — for conclusion-sentence extraction

Outputs (Data/processed/):
    framing_conclusion_topics.csv    — topic % for relay vs hub papers

Outputs (Graphs/):
    fig10_framing_conclusion_topics.png   — diverging bar chart of topic %
    fig11_framing_position_in_paper.png   — histogram of relative sentence
                                            position for relay vs hub

Note on paths:
    PROC, INTERIM, and GRAPH are resolved from __file__; Code/config.py /
    paths.py are not wired in here.  Relay-cluster IDs {55, 56, 46} and
    hub-cluster IDs {53, 54} must match apply_cluster_labels.py.
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import re
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from collections import Counter
from pathlib import Path

_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT   = _ANALYSIS_DIR.parents[1]
PROC   = ROOT / "Data" / "processed"
INTERIM= ROOT / "Data" / "interim"
GRAPH  = ROOT / "Graphs"

# ── Load data ────────────────────────────────────────────────────────────────
# Load framing sentences (clustered)
fr = pd.read_csv(PROC / "framing_embedded_clustered.csv", dtype={"pmid": str})
fr = fr[fr["cluster_id"] != -1].copy()

# Load all thalamus sentences
all_s = pd.read_csv(INTERIM / "sentence_extraction" / "thalamus_all_sentences.csv",
                    dtype={"pmid": str})
all_s["year"] = pd.to_numeric(all_s["year"], errors="coerce")

# ── Conclusion sentence markers ───────────────────────────────────────────────
CONCL_PAT = re.compile(
    r"\b(these\s+(?:results|findings|data|observations)\s+(?:suggest|indicate|demonstrate|show|reveal|support)|"
    r"we\s+(?:conclude|demonstrate|show|report|propose|suggest|found\s+that|provide\s+evidence)|"
    r"our\s+(?:results|findings|data|study)\s+(?:suggest|indicate|show|demonstrate|reveal)|"
    r"in\s+(?:summary|conclusion)|taken\s+together|collectively[,\s]|"
    r"this\s+study\s+(?:demonstrates?|shows?|provides?|reveals?|suggests?)|"
    r"(?:overall|together)[,\s]+(?:these|our)|"
    r"the\s+(?:present|current)\s+(?:study|results?|findings?)\s+(?:suggest|demonstrate|show))\b",
    re.IGNORECASE
)

def is_conclusion(sentence):
    return bool(CONCL_PAT.search(str(sentence)))

# Tag conclusion sentences
all_s["is_conclusion"] = all_s["sentence"].apply(is_conclusion)
concl = all_s[all_s["is_conclusion"]].copy()
print(f"Conclusion sentences: {len(concl)} from {concl['pmid'].nunique()} papers")

# ── Assign broad framing group per paper ─────────────────────────────────────
relay_ids = [55, 56, 46]
hub_ids   = [53, 54]

def broad_frame(cid):
    if cid in relay_ids: return "relay"
    if cid in hub_ids:   return "hub"
    return None

fr["broad"] = fr["cluster_id"].apply(broad_frame)
fr_typed = fr[fr["broad"].notna()]

# One dominant framing per paper (most common)
paper_frame = (fr_typed.groupby(["pmid","broad"])
                       .size()
                       .reset_index(name="n")
                       .sort_values("n", ascending=False)
                       .drop_duplicates("pmid", keep="first")
                       .set_index("pmid")["broad"])

print(f"\nPapers with relay framing: {(paper_frame=='relay').sum()}")
print(f"Papers with hub framing:   {(paper_frame=='hub').sum()}")

# Attach framing to conclusion sentences
concl = concl[concl["pmid"].isin(paper_frame.index)].copy()
concl["broad"] = concl["pmid"].map(paper_frame)

# ── Keyword extraction from conclusion sentences ──────────────────────────────
CONCLUSION_TOPICS = {
    "relay / transmission":   r"\b(relay|transmit|transmission|project\s+to|send\s+signal)\b",
    "hub / integration":      r"\b(hub|integrat|coordinat|orchestrat|central\s+node)\b",
    "gating / filtering":     r"\b(gat(e|ing)|filter|modulate\s+flow|control\s+(?:of\s+)?(?:information|signal))\b",
    "sensory processing":     r"\b(sensory\s+(?:process|information|signal|input)|perception|somatosensory|auditory|visual)\b",
    "cognitive function":     r"\b(cogniti|working\s+memory|attention|executive|prefrontal)\b",
    "oscillation / rhythm":   r"\b(oscillat|rhythm|synchron|slow.wave|spindle|alpha|theta|delta)\b",
    "connectivity":           r"\b(connect(?:ivity|ed)|circuit|network|pathway|thalamocortical)\b",
    "pathology / disease":    r"\b(patholog|disease|disorder|dysfunction|abnormal|deficit|impair)\b",
    "consciousness / arousal":r"\b(conscious|arousal|awareness|wakefulness|sleep)\b",
    "clinical / treatment":   r"\b(clinic|treat(?:ment)?|therap|dbs|stimulat|surgical)\b",
}

for topic, pat in CONCLUSION_TOPICS.items():
    concl[topic] = concl["sentence"].str.contains(pat, case=False, na=False)

# Compare relay vs hub papers
relay_concl = concl[concl["broad"] == "relay"]
hub_concl   = concl[concl["broad"] == "hub"]

print("\n── Conclusion topics: relay vs hub papers ─────────────────────────────")
print(f"{'Topic':<30} {'relay%':>8} {'hub%':>8} {'diff':>8}")
rows = []
for topic in CONCLUSION_TOPICS:
    r_pct = relay_concl[topic].mean() * 100 if len(relay_concl) > 0 else 0
    h_pct = hub_concl[topic].mean()   * 100 if len(hub_concl)   > 0 else 0
    diff  = h_pct - r_pct
    print(f"  {topic:<28} {r_pct:>8.1f} {h_pct:>8.1f} {diff:>+8.1f}")
    rows.append({"topic": topic, "relay_%": r_pct, "hub_%": h_pct, "diff": diff})

result_df = pd.DataFrame(rows).sort_values("diff")
result_df.to_csv(PROC / "framing_conclusion_topics.csv", index=False)

# ── Plot: diverging bar chart ─────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(10, 6))
y = range(len(result_df))
relay_vals = result_df["relay_%"].values
hub_vals   = result_df["hub_%"].values
topics     = result_df["topic"].values

ax.barh(list(y), relay_vals, color="#E8342A", alpha=0.8, label="relay papers", height=0.4,
        align="center")
ax.barh([yi + 0.45 for yi in y], hub_vals, color="#2E86AB", alpha=0.8,
        label="hub papers", height=0.4, align="center")
ax.set_yticks([yi + 0.225 for yi in y])
ax.set_yticklabels(topics, fontsize=9)
ax.set_xlabel("% of conclusion sentences containing topic")
ax.set_title("Conclusion sentence topics by dominant paper framing")
ax.legend()
plt.tight_layout()
fig.savefig(GRAPH / "fig10_framing_conclusion_topics.png", dpi=150)
plt.close()

# ── Framing position: where in the paper do framing sentences appear? ─────────
# Normalize sentence index to 0-1 (position within paper)
fr2 = fr[fr["pmid"].isin(paper_frame.index)].copy()
fr2["broad"] = fr2["pmid"].map(paper_frame)

# Get max sentence_index per paper from all_s
max_idx = all_s.groupby("pmid")["sentence_index"].max().rename("max_idx")
fr2 = fr2.join(max_idx, on="pmid")
fr2["rel_pos"] = fr2["sentence_index"] / (fr2["max_idx"] + 1)
fr2 = fr2[fr2["rel_pos"].notna() & fr2["max_idx"] > 5]

fig, ax = plt.subplots(figsize=(9, 4))
for label, color in [("relay","#E8342A"), ("hub","#2E86AB")]:
    vals = fr2[fr2["broad"] == label]["rel_pos"].dropna()
    ax.hist(vals, bins=20, range=(0,1), density=True, alpha=0.5,
            color=color, label=f"{label} (n={len(vals)})")
ax.set_xlabel("Relative position in paper (0=start, 1=end)")
ax.set_ylabel("Density")
ax.set_title("Where in the paper do relay vs hub framing sentences appear?")
ax.legend()
plt.tight_layout()
fig.savefig(GRAPH / "fig11_framing_position_in_paper.png", dpi=150)
plt.close()

print("\nSaved fig10_framing_conclusion_topics.png")
print("Saved fig11_framing_position_in_paper.png")
print("Saved framing_conclusion_topics.csv")
