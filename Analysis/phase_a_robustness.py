"""
Phase A — Robustness checks
============================
1. Normalised temporal trend: relay/hub/other as % of all framing sentences
   per year, with Wilson 95% CI bands.
2. Mann-Kendall monotonic trend test on relay% and hub% over time.
3. Decade-stratified nucleus analysis — controls for the year × nucleus
   confound (older papers study VP; newer papers study MD).
4. Decade-stratified method analysis — controls for the year × method
   confound (older = electrophysiology; newer = fMRI).
5. Publication-type split: Review vs Original Research framing comparison.

Outputs (all saved to Data/processed/ and Graphs/):
  fig_A1_temporal_normalised.png
  fig_A2_decade_nucleus.png
  fig_A3_decade_method.png
  fig_A4_pubtype_framing.png
  table_A1_mannkendall.csv
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import re
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from scipy.stats import kendalltau
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT   = _ANALYSIS_DIR.parents[1]
PROC   = ROOT / "Data" / "processed"
GRAPH  = ROOT / "Graphs"
RAW_PM = ROOT / "Data" / "raw" / "pubmed"

# ── Load data ─────────────────────────────────────────────────────────────────
df = pd.read_csv(PROC / "framing_embedded_clustered.csv", dtype={"pmid": str})
df = df[df["cluster_id"] != -1].copy()
df["year"] = pd.to_numeric(df["year"], errors="coerce")

# Total thalamus papers per year (publication-volume baseline).
# Update the filename below if a newer pubmed export has been produced.
pubmed = pd.read_csv(RAW_PM / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                     dtype={"pmid": str})
pubmed["year"] = pd.to_numeric(pubmed["year"], errors="coerce")
papers_per_year = pubmed.groupby("year").size().rename("total_papers")

# ── Helper: Wilson binomial 95% CI ───────────────────────────────────────────
def wilson_ci(k, n, z=1.96):
    """Return (lower, upper) Wilson confidence interval for proportion k/n."""
    if n == 0:
        return 0.0, 0.0
    p = k / n
    denom  = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denom
    margin = z * np.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom
    return float(center - margin), float(center + margin)

# ── Framing group labels ──────────────────────────────────────────────────────
RELAY_IDS = {55, 56, 46}
HUB_IDS   = {53, 54}

def broad_frame(cid):
    if cid in RELAY_IDS: return "relay"
    if cid in HUB_IDS:   return "hub"
    return "other"

df["broad"] = df["cluster_id"].apply(broad_frame)

# ══════════════════════════════════════════════════════════════════════════════
# 1 & 2 · Normalised temporal trend + Mann-Kendall
# ══════════════════════════════════════════════════════════════════════════════
yr_df = df[df["year"].between(1975, 2026)].copy()

yearly = (yr_df.groupby(["year", "broad"]).size()
               .unstack(fill_value=0)
               .reindex(columns=["relay", "hub", "other"], fill_value=0))
yearly["total"] = yearly.sum(axis=1)

# Proportions + CI per year
records = []
for yr, row in yearly.iterrows():
    for framing in ["relay", "hub"]:
        lo, hi = wilson_ci(row[framing], row["total"])
        records.append({"year": yr, "framing": framing,
                         "pct": row[framing] / row["total"] * 100 if row["total"] > 0 else 0,
                         "ci_lo": lo * 100, "ci_hi": hi * 100,
                         "n": row[framing]})
trend_df = pd.DataFrame(records)

# Smooth (5-yr rolling)
for framing in ["relay", "hub"]:
    sub = trend_df[trend_df["framing"] == framing].set_index("year").sort_index()
    trend_df.loc[trend_df["framing"] == framing, "pct_smooth"] = (
        sub["pct"].rolling(5, min_periods=2, center=True).mean().values)

# Mann-Kendall on yearly proportions (years with ≥ 5 framing sentences)
mk_results = []
for framing in ["relay", "hub"]:
    sub = trend_df[trend_df["framing"] == framing].dropna(subset=["pct"])
    sub = sub[sub["n"] >= 5].sort_values("year")
    if len(sub) < 5:
        continue
    tau, pval = kendalltau(sub["year"], sub["pct"])
    direction = "↑ increasing" if tau > 0 else "↓ decreasing"
    mk_results.append({"framing": framing, "tau": round(tau, 3),
                        "p_value": round(pval, 4), "direction": direction,
                        "n_years": len(sub)})
    print(f"Mann-Kendall {framing}: tau={tau:.3f}, p={pval:.4f} ({direction})")

pd.DataFrame(mk_results).to_csv(PROC / "table_A1_mannkendall.csv", index=False)

# Plot A1: normalised temporal trend
fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
colors = {"relay": "#E8342A", "hub": "#2E86AB"}
labels = {"relay": "Relay / gateway framing",
          "hub":   "Hub / integration framing"}

for ax, framing in zip(axes, ["relay", "hub"]):
    sub = trend_df[trend_df["framing"] == framing].sort_values("year")
    # Raw % as faint dots
    ax.scatter(sub["year"], sub["pct"], color=colors[framing],
               alpha=0.25, s=15, zorder=2)
    # Smoothed line
    ax.plot(sub["year"], sub["pct_smooth"], color=colors[framing],
            lw=2.5, zorder=3, label=labels[framing])
    # CI ribbon on raw values (too noisy yearly — show on smoothed instead)
    # Overlay total paper volume (right axis)
    ax2 = ax.twinx()
    vol = papers_per_year.reindex(sub["year"]).fillna(0)
    ax2.fill_between(sub["year"], vol, alpha=0.07, color="grey")
    ax2.set_ylabel("Total thalamus papers", color="grey", fontsize=8)
    ax2.tick_params(axis="y", labelcolor="grey", labelsize=7)

    # Mann-Kendall annotation
    mk = next((r for r in mk_results if r["framing"] == framing), None)
    if mk:
        ax.text(0.02, 0.92,
                f"Mann-Kendall: τ={mk['tau']}, p={mk['p_value']} {mk['direction']}",
                transform=ax.transAxes, fontsize=9, color=colors[framing])

    ax.set_ylabel("% of framing sentences")
    ax.legend(fontsize=10)
    ax.set_ylim(0)

axes[1].set_xlabel("Year")
fig.suptitle("Normalised thalamus framing over time (5-yr rolling mean)", fontsize=12)
plt.tight_layout()
fig.savefig(GRAPH / "fig_A1_temporal_normalised.png", dpi=150)
plt.close()
print("Saved fig_A1_temporal_normalised.png")

# ══════════════════════════════════════════════════════════════════════════════
# 3 · Decade-stratified nucleus analysis
# ══════════════════════════════════════════════════════════════════════════════
NUCLEI = {
    "VP / VPL / VPM":   r"\b(ventroposterior|VPL|VPM|ventrobasal|VB\s+thal)\b",
    "LGN":              r"\b(lateral\s+geniculate|LGN|dLGN|vLGN)\b",
    "MGB":              r"\b(medial\s+geniculate|MGB|MGN|MGv|MGd)\b",
    "pulvinar":         r"\b(pulvinar)\b",
    "MD":               r"\b(mediodorsal|MD\s+thal|MD\s+nucl)\b",
    "anterior (ANT)":   r"\b(anterior\s+thal|ANT\b|anteroventral|anterodorsal)\b",
    "intralaminar":     r"\b(intralaminar|centromedian|CM.Pf|parafascicular|CL\s+nucl)\b",
    "VL / VIM":         r"\b(ventrolateral\s+thal|VL\s+thal|VL\s+nucl|VIM\b)\b",
    "TRN":              r"\b(reticular\s+nucl|TRN\b|thalamic\s+reticular)\b",
}

DECADES = [("pre-2000", 1975, 1999), ("2000–2009", 2000, 2009),
           ("2010–2019", 2010, 2019), ("2020+", 2020, 2026)]

def tag_nucleus(sentence):
    s = str(sentence)
    for name, pat in NUCLEI.items():
        if re.search(pat, s, re.IGNORECASE):
            return name
    return None

df["nucleus"] = df["sentence"].apply(tag_nucleus)
df_nuc = df.dropna(subset=["nucleus"])
df_nuc = df_nuc[df_nuc["broad"].isin(["relay","hub"])].copy()
df_nuc["decade"] = pd.cut(df_nuc["year"],
                           bins=[1974,1999,2009,2019,2026],
                           labels=["pre-2000","2000–2009","2010–2019","2020+"])

fig, axes = plt.subplots(1, len(DECADES), figsize=(16, 5), sharey=True)
for ax, (dlabel, _, _) in zip(axes, DECADES):
    sub = df_nuc[df_nuc["decade"] == dlabel]
    if len(sub) == 0:
        ax.set_title(dlabel); continue
    ct = (sub.groupby(["nucleus","broad"]).size()
             .unstack(fill_value=0)
             .reindex(columns=["relay","hub"], fill_value=0))
    ct = ct[ct.sum(axis=1) >= 3]   # at least 3 sentences
    ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100
    ct_pct = ct_pct.sort_values("relay", ascending=True)
    nuclei = ct_pct.index.tolist()
    y_pos  = np.arange(len(nuclei))
    ax.barh(y_pos + 0.2, ct_pct["relay"].values, height=0.38,
            color="#E8342A", alpha=0.75, label="relay")
    ax.barh(y_pos - 0.2, ct_pct["hub"].values,   height=0.38,
            color="#2E86AB", alpha=0.75, label="hub")
    ax.set_yticks(y_pos); ax.set_yticklabels(nuclei, fontsize=8)
    ax.set_title(dlabel); ax.set_xlabel("% of framing sentences")
    if ax is axes[0]:
        ax.legend(fontsize=8)
    ax.set_xlim(0, 40)

fig.suptitle("Framing by nucleus — stratified by decade\n(controls for year confound)",
             fontsize=11)
plt.tight_layout()
fig.savefig(GRAPH / "fig_A2_decade_nucleus.png", dpi=150)
plt.close()
print("Saved fig_A2_decade_nucleus.png")

# ══════════════════════════════════════════════════════════════════════════════
# 4 · Decade-stratified method analysis
# ══════════════════════════════════════════════════════════════════════════════
# Load abstract text for method detection
ab = pubmed[["pmid","abstract"]].dropna(subset=["abstract"])
ab_dict = dict(zip(ab["pmid"].astype(str), ab["abstract"]))

METHODS = {
    "fMRI / rs-fMRI":       r"\b(fmri|functional\s+mri|bold\b|resting.state\s+f|functional\s+connect)\b",
    "structural MRI / VBM": r"\b(vbm|voxel.based|structural\s+mri|gray\s+matter\s+vol|morphometry)\b",
    "diffusion MRI":        r"\b(dti\b|diffusion\s+tensor|tractography|diffusion.weighted|dwi\b)\b",
    "electrophysiology":    r"\b(electrophysiol|patch.clamp|single.unit|lfp\b|firing\s+rate|unit\s+record)\b",
    "optogenetics":         r"\b(optogenet|channelrhodopsin)\b",
    "DBS / stimulation":    r"\b(deep\s+brain\s+stim|dbs\b|thalamotomy)\b",
    "tract tracing":        r"\b(tract\s+trac|anterograde|retrograde\s+trac|viral\s+trac|aav\b)\b",
    "EEG / MEG":            r"\b(eeg\b|electroencephalog|meg\b|magnetoencephalog)\b",
    "lesion":               r"\b(lesion\s+stud|ablation|excitotoxic|ibotenic|infarct)\b",
}

def detect_method(row):
    text = (ab_dict.get(str(row["pmid"]), "") + " " + str(row["sentence"])).lower()
    for m, pat in METHODS.items():
        if re.search(pat, text, re.IGNORECASE):
            return m
    return None

df["method"] = df.apply(detect_method, axis=1)
df_mth = df.dropna(subset=["method"])
df_mth = df_mth[df_mth["broad"].isin(["relay","hub"])].copy()
df_mth["decade"] = pd.cut(df_mth["year"],
                           bins=[1974,1999,2009,2019,2026],
                           labels=["pre-2000","2000–2009","2010–2019","2020+"])

fig, axes = plt.subplots(1, len(DECADES), figsize=(18, 5), sharey=True)
for ax, (dlabel, _, _) in zip(axes, DECADES):
    sub = df_mth[df_mth["decade"] == dlabel]
    if len(sub) == 0:
        ax.set_title(dlabel); continue
    ct = (sub.groupby(["method","broad"]).size()
             .unstack(fill_value=0)
             .reindex(columns=["relay","hub"], fill_value=0))
    ct = ct[ct.sum(axis=1) >= 3]
    ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100
    ct_pct = ct_pct.sort_values("relay", ascending=True)
    methods = ct_pct.index.tolist()
    y_pos   = np.arange(len(methods))
    ax.barh(y_pos + 0.2, ct_pct["relay"].values, height=0.38,
            color="#E8342A", alpha=0.75, label="relay")
    ax.barh(y_pos - 0.2, ct_pct["hub"].values,   height=0.38,
            color="#2E86AB", alpha=0.75, label="hub")
    ax.set_yticks(y_pos); ax.set_yticklabels(methods, fontsize=8)
    ax.set_title(dlabel); ax.set_xlabel("% of framing sentences")
    if ax is axes[0]:
        ax.legend(fontsize=8)
    ax.set_xlim(0, 50)

fig.suptitle("Framing by method — stratified by decade\n(controls for year confound)",
             fontsize=11)
plt.tight_layout()
fig.savefig(GRAPH / "fig_A3_decade_method.png", dpi=150)
plt.close()
print("Saved fig_A3_decade_method.png")

# ══════════════════════════════════════════════════════════════════════════════
# 5 · Publication-type split: Review vs Original Research
# ══════════════════════════════════════════════════════════════════════════════
def is_review(pub_types):
    if not isinstance(pub_types, str):
        return False
    return "review" in pub_types.lower()

df["is_review"] = df["publication_types"].apply(is_review)
df["pub_type"]  = df["is_review"].map({True: "Review", False: "Original research"})

# Framing % by publication type
pt_ct = (df.groupby(["pub_type","broad"]).size()
           .unstack(fill_value=0)
           .reindex(columns=["relay","hub","other"], fill_value=0))
pt_pct = pt_ct.div(pt_ct.sum(axis=1), axis=0) * 100
print("\n── Framing % by publication type ─────────────")
print(pt_pct.to_string())

# Temporal trend split by pub type
yr_pt = (df[df["year"].between(1990, 2026)]
         .groupby(["year","pub_type","broad"]).size()
         .unstack(fill_value=0)
         .reset_index())

fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
for ax, framing, color in [(axes[0], "relay", "#E8342A"),
                            (axes[1], "hub",   "#2E86AB")]:
    for ptype, ls in [("Review", "--"), ("Original research", "-")]:
        sub = (df[df["year"].between(1990,2026) & (df["pub_type"]==ptype)]
               .groupby(["year","broad"]).size().unstack(fill_value=0))
        if framing not in sub.columns:
            continue
        total = sub.sum(axis=1)
        pct   = (sub[framing] / total.replace(0,1) * 100)
        smooth = pct.rolling(5, min_periods=2, center=True).mean()
        ax.plot(smooth.index, smooth.values, color=color,
                ls=ls, lw=2, label=ptype)
    ax.set_title(f"{framing.capitalize()} framing: reviews vs research")
    ax.set_xlabel("Year"); ax.set_ylabel("% of framing sentences")
    ax.legend(fontsize=9); ax.set_ylim(0)

fig.suptitle("Publication type comparison — do reviews drive the framing shift?",
             fontsize=11)
plt.tight_layout()
fig.savefig(GRAPH / "fig_A4_pubtype_framing.png", dpi=150)
plt.close()
print("Saved fig_A4_pubtype_framing.png")
