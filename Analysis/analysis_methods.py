"""
Analysis 2 — Does framing differ by method used?

Tags each framing sentence with the experimental method(s) detected in the
corresponding abstract (joined with the framing sentence itself).  The method
taxonomy covers 12 technique categories (fMRI, electrophysiology, optogenetics,
DBS, etc.).  Compares relay vs hub framing balance across methods and plots
method prevalence over time.

Inputs (Data/processed/):
    framing_embedded_clustered.csv   — must include cluster_id, sentence, pmid, year

Inputs (Data/raw/pubmed/):
    pubmed_broad_title_only_1950_2026_<date>.csv   — for abstract text

Outputs (Data/processed/):
    method_framing_pct.csv      — relay/hub/other % per method
    method_keyword_pct.csv      — keyword framing % per method

Outputs (Graphs/):
    fig7_framing_by_method.png             — stacked bar: framing % by method
    fig8_method_prevalence_over_time.png   — 5-yr rolling method % over time

Note on paths:
    PROC, RAW, and GRAPH are resolved from __file__; Code/config.py /
    paths.py are not wired in here.  The pubmed CSV filename is hardcoded and
    must be updated if a newer export is produced.  Relay-cluster IDs
    {55, 56, 46} and hub-cluster IDs {53, 54} must match apply_cluster_labels.py.
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
ROOT   = _ANALYSIS_DIR.parents[1]
PROC   = ROOT / "Data" / "processed"
RAW    = ROOT / "Data" / "raw" / "pubmed"
GRAPH  = ROOT / "Graphs"

# ── Load data ────────────────────────────────────────────────────────────────
df = pd.read_csv(PROC / "framing_embedded_clustered.csv")
df = df[df["cluster_id"] != -1].copy()

# Abstracts are used for method detection (paper-level context).
# Update the filename if a newer pubmed export has been produced.
ab = pd.read_csv(RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                 dtype={"pmid": str})
ab = ab[["pmid","abstract"]].dropna(subset=["abstract"])
ab_dict = dict(zip(ab["pmid"], ab["abstract"]))

# ── Method detection patterns ────────────────────────────────────────────────
METHODS = {
    "fMRI / resting-state":     r"\b(fmri|functional\s+mri|bold\b|resting.state\s+f|functional\s+connectivity|functional\s+magnetic)\b",
    "structural MRI / VBM":     r"\b(vbm|voxel.based\s+morph|structural\s+mri|gray\s+matter\s+vol|cortical\s+thick|morphometry|t1.weighted|t1w)\b",
    "diffusion MRI":            r"\b(dti\b|diffusion\s+tensor|tractography|diffusion.weighted|dwi\b|dki\b|white\s+matter\s+tract)\b",
    "electrophysiology":        r"\b(electrophysiol|patch.clamp|single.unit|local\s+field\s+potential|lfp\b|spike\s+(?:rate|firing)|firing\s+rate|extracellular\s+record|unit\s+record|multi.electrode)\b",
    "optogenetics":             r"\b(optogenet|channelrhodopsin|halorhodopsin|photostimul)\b",
    "DBS / neurostimulation":   r"\b(deep\s+brain\s+stim|dbs\b|thalamotomy|stereotactic\s+surg|neurostimul)\b",
    "tract tracing / anatomy":  r"\b(tract\s+trac|anterograde|retrograde\s+trac|viral\s+trac|axonal\s+trac|fluorogold|ctb\b|aav\b|bartha|bda\b)\b",
    "histology / immunohisto":  r"\b(immunohisto|ihc\b|immunofluor|cytoarchitect|nissl\b|golgi\b|electron\s+microscop|immuno.label)\b",
    "EEG / MEG":                r"\b(eeg\b|electroencephalog|meg\b|magnetoencephalog)\b",
    "lesion / stroke":          r"\b(lesion\s+stud|ablation|excitotoxic|electrolytic\s+lesion|ibotenic|infarct|stroke\b|thalamic\s+lesion)\b",
    "pharmacology":             r"\b(pharmaco|drug\s+(?:effect|treat)|receptor\s+(?:agonist|antagonist)|microinfusion|intracerebral\s+injec)\b",
    "calcium imaging":          r"\b(calcium\s+imag|two.photon|gcamp|fiber\s+photometr)\b",
}

def detect_methods(text):
    if not isinstance(text, str):
        return []
    t = text.lower()
    return [m for m, pat in METHODS.items() if re.search(pat, t, re.IGNORECASE)]

# Tag from abstract + framing sentence combined
def get_text(row):
    ab_text = ab_dict.get(str(row["pmid"]), "")
    sent    = str(row.get("sentence",""))
    return (ab_text + " " + sent)

df["methods"] = df.apply(lambda r: detect_methods(get_text(r)), axis=1)
df_m = df.explode("methods").dropna(subset=["methods"])
df_m = df_m[df_m["methods"] != ""]

# Broad framing groups
relay_ids = [55, 56, 46]
hub_ids   = [53, 54]
def broad_frame(cid):
    if cid in relay_ids:   return "relay / gateway"
    if cid in hub_ids:     return "hub / integration"
    return "other"
df_m["broad_frame"] = df_m["cluster_id"].apply(broad_frame)

# Count
ct = (df_m.groupby(["methods","broad_frame"])
          .size().unstack(fill_value=0))
ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100
ct_pct["n"] = ct.sum(axis=1)
ct_pct = ct_pct.sort_values("relay / gateway", ascending=False)
ct_pct.to_csv(PROC / "method_framing_pct.csv")

# ── Keyword framing × method ──────────────────────────────────────────────────
kw_ct = (df_m.groupby(["methods","framing_primary"])
              .size().unstack(fill_value=0))
kw_pct = kw_ct.div(kw_ct.sum(axis=1), axis=0) * 100
kw_pct["n"] = kw_ct.sum(axis=1)
kw_pct.to_csv(PROC / "method_keyword_pct.csv")

# ── Plot ──────────────────────────────────────────────────────────────────────
plot_df = ct_pct.drop(columns="n")
colors  = {"relay / gateway":"#E8342A","hub / integration":"#2E86AB","other":"#CCCCCC"}
fig, ax = plt.subplots(figsize=(12, 6))
bottom = np.zeros(len(plot_df))
for col in plot_df.columns:
    ax.barh(plot_df.index, plot_df[col], left=bottom,
            label=col, color=colors.get(col,"#aaa"), height=0.6)
    bottom += plot_df[col].values
ax.set_xlabel("% of framing sentences")
ax.set_title("Thalamus framing by method detected")
ax.legend(fontsize=9)
plt.tight_layout()
fig.savefig(GRAPH / "fig7_framing_by_method.png", dpi=150)
plt.close()

# ── Method prevalence over time ───────────────────────────────────────────────
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df_m2 = df.explode("methods")
df_m2 = df_m2[df_m2["methods"].notna() & (df_m2["methods"] != "")]
df_m2 = df_m2[df_m2["year"].between(1980, 2026)]

meth_yr = (df_m2.groupby(["year","methods"])
                .size().unstack(fill_value=0))
meth_yr_pct = meth_yr.div(meth_yr.sum(axis=1), axis=0) * 100
meth_smooth  = meth_yr_pct.rolling(5, min_periods=2, center=True).mean()

fig, ax = plt.subplots(figsize=(13, 5))
for col in meth_smooth.columns:
    ax.plot(meth_smooth.index, meth_smooth[col], label=col, lw=1.5)
ax.set_xlabel("Year"); ax.set_ylabel("% of method-tagged sentences (5-yr rolling)")
ax.set_title("Method prevalence in framing-sentence papers over time")
ax.legend(fontsize=7, bbox_to_anchor=(1.01,1), loc="upper left")
plt.tight_layout()
fig.savefig(GRAPH / "fig8_method_prevalence_over_time.png", dpi=150, bbox_inches="tight")
plt.close()

print("\n── Relay vs Hub % by method ────────────────────────────────")
print(ct_pct[["relay / gateway","hub / integration","n"]].to_string())
print("\nSaved fig7_framing_by_method.png")
print("Saved fig8_method_prevalence_over_time.png")
