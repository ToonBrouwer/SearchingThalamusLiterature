"""
Analysis 1 — Does framing differ by thalamic nucleus mentioned?

Tags each framing sentence with zero or more thalamic nucleus labels using
regex patterns, then compares the relay-vs-hub framing balance across nuclei.
Only nuclei with >= 10 tagged sentences are retained in the plot.

Inputs (Data/processed/):
    framing_embedded_clustered.csv   — must include cluster_id, sentence, year

Outputs (Data/processed/):
    nucleus_framing_pct.csv          — relay/hub/other % per nucleus

Outputs (Graphs/):
    fig6_framing_by_nucleus.png      — horizontal stacked bar chart

Note on paths:
    PROC and GRAPH are resolved from __file__; Code/config.py / paths.py are
    not wired in here.  Relay-cluster IDs {55, 56, 46} and hub-cluster IDs
    {53, 54} are hardcoded and must be kept in sync with apply_cluster_labels.py.
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import re
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT  = _ANALYSIS_DIR.parents[1]
PROC  = ROOT / "Data" / "processed"
GRAPH = ROOT / "Graphs"

# ── Load data ────────────────────────────────────────────────────────────────
df = pd.read_csv(PROC / "framing_embedded_clustered.csv")
df = df[df["cluster_id"] != -1].copy()

# ── Nucleus detection patterns ────────────────────────────────────────────────
NUCLEI = {
    "mediodorsal (MD)":      r"\b(mediodorsal|MD\s+thal|MD\s+nucl|dorsomedial\s+thal)\b",
    "ventroposterior (VP)":  r"\b(ventroposterior|VPL|VPM|ventrobasal|VB\s+thal|VP\s+thal)\b",
    "lateral geniculate (LGN)": r"\b(lateral\s+geniculate|LGN|dLGN|vLGN)\b",
    "medial geniculate (MGB)":  r"\b(medial\s+geniculate|MGB|MGN|MGv|MGd|MGm)\b",
    "pulvinar":              r"\b(pulvinar|Pu[lL])\b",
    "anterior (ANT)":        r"\b(anterior\s+thal|ANT\b|anteroventral|anterodorsal|anteromedial|AV\s+nucl|AD\s+nucl)\b",
    "intralaminar (IL)":     r"\b(intralaminar|centromedian|CM.Pf|parafascicular|central\s+lateral|CL\s+nucl|CM\s+nucl)\b",
    "ventrolateral (VL)":    r"\b(ventrolateral\s+thal|VL\s+thal|VL\s+nucl|VIM\b|ventral\s+intermediate)\b",
    "reticular (TRN)":       r"\b(reticular\s+nucl|TRN\b|thalamic\s+reticular)\b",
    "midline / paraventricular": r"\b(midline\s+thal|paraventricular\s+thal|PVT\b|nucleus\s+reuniens|rhomboid)\b",
    "posterior / LP":        r"\b(posterior\s+thal|lateral\s+posterior|LP\s+nucl|POm\b|posterior\s+nucl)\b",
    "centromedian (CM)":     r"\b(centromedian|CM\s+nucl|CM\b(?!\s*-\s*Pf))\b",
}

def tag_nucleus(sentence):
    s = str(sentence)
    found = []
    for name, pat in NUCLEI.items():
        if re.search(pat, s, re.IGNORECASE):
            found.append(name)
    return found if found else ["unspecified"]

df["nuclei"] = df["sentence"].apply(tag_nucleus)

# Explode so one row per nucleus mention
df_nuc = df.explode("nuclei")

# ── Broad framing groups ──────────────────────────────────────────────────────
relay_ids = [55, 56, 46]
hub_ids   = [53, 54]
topic_ids = {
    "relay / gateway":    relay_ids,
    "hub / integration":  hub_ids,
    "oscillations":       [31],
    "sensory (pain/soma)":[2, 46],
    "cognition / memory": [39, 40],
    "clinical / DBS":     [0, 8, 33],
}
def broad_frame(cid):
    for label, ids in topic_ids.items():
        if cid in ids:
            return label
    return "other"
df_nuc["broad_frame"] = df_nuc["cluster_id"].apply(broad_frame)

# Count by nucleus × framing
ct = (df_nuc[df_nuc["nuclei"] != "unspecified"]
      .groupby(["nuclei", "broad_frame"])
      .size().unstack(fill_value=0))

# % per nucleus
ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100
ct_pct["_total"] = ct.sum(axis=1)
ct_pct = ct_pct[ct_pct["_total"] >= 10].drop(columns="_total")
ct_pct = ct_pct.sort_values("relay / gateway", ascending=False)

# ── Save tabular result ───────────────────────────────────────────────────────
ct_pct.to_csv(PROC / "nucleus_framing_pct.csv")

# ── Plot ──────────────────────────────────────────────────────────────────────
cols = [c for c in ct_pct.columns if c in topic_ids]
colors = ["#E8342A","#2E86AB","#9B59B6","#59B069","#F7A541","#7F8C8D"]
fig, ax = plt.subplots(figsize=(13, 6))
bottom = np.zeros(len(ct_pct))
for col, color in zip(cols, colors):
    if col in ct_pct.columns:
        ax.barh(ct_pct.index, ct_pct[col], left=bottom, label=col, color=color, height=0.6)
        bottom += ct_pct[col].values
ax.set_xlabel("% of framing sentences")
ax.set_title("Thalamus framing by nucleus mentioned (≥10 sentences)")
ax.legend(fontsize=9, loc="lower right")
plt.tight_layout()
fig.savefig(GRAPH / "fig6_framing_by_nucleus.png", dpi=150)
plt.close()

# Also: relay vs hub % per nucleus
rh = pd.DataFrame({
    "relay_%": (ct.get("relay / gateway", 0) / ct.sum(axis=1) * 100).round(1),
    "hub_%":   (ct.get("hub / integration", 0) / ct.sum(axis=1) * 100).round(1),
    "n":        ct.sum(axis=1),
}).sort_values("relay_%", ascending=False)

print("\n── Relay vs Hub % by nucleus ────────────────────────────")
print(rh[rh["n"] >= 10].to_string())
print("\nSaved fig6_framing_by_nucleus.png")
