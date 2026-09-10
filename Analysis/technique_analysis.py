"""
technique_analysis.py -- Analyse framing & topic differences by recording technique.

Uses the CLEAN 21-frame detection (from pipeline/07_clean_framing_and_topics.py),
NOT the old taxonomy with its giant "other" bucket.

Generates Figure 6 (2x3, 6 panels):
  A: Technique prevalence over time (abstracts, 1990-2026)
  B: Clean 21-frame distribution per technique (stacked horizontal bar)
  C: Topic x technique heatmap (from all_sentences_clean.csv)
  D: Technique x nucleus (relative, row-normalised)
  E: Technique-frame temporal evolution (small multiples 2x4)
  F: Mann-Kendall trend bars for technique prevalence
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path
from collections import OrderedDict
from scipy.stats import chi2_contingency

from plotting import apply_style, save_fig, panel_label, despine, WONG, mk_bar, heatmap
from technique_patterns import TECHNIQUES, tag_techniques
from nucleus_patterns import NUCLEI, tag_nuclei
from stats import mann_kendall, rolling_mean

apply_style()

# ── Paths ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW  = ROOT / "Data" / "raw"
PUB  = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

# ── 21 clean frame regexes (same as 07_clean_framing_and_topics.py) ──────
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

FRAME_NAMES = list(FRAMES.keys())


def detect_frames_series(text_series):
    """Detect primary frame for a text series using the 21 clean patterns."""
    frame_type = pd.Series("", index=text_series.index)
    for fname, pat in FRAMES.items():
        mask = text_series.str.contains(pat, na=False)
        frame_type[mask & (frame_type == "")] = fname
    return frame_type


# ── Load data ────────────────────────────────────────────────────────────
print("Loading data...")

# 1. All sentences (with clean topics)
sents = pd.read_csv(PROC / "all_sentences_clean.csv")
sents["sentence"] = sents["sentence"].fillna("")
sents["year"] = pd.to_numeric(sents["year"], errors="coerce")
print(f"  All sentences (clean): {len(sents):,}")

# 2. Combined framing (PMC + abstract sentences with a frame) – more data
framing = pd.read_csv(PROC / "framing_combined.csv")
framing["sentence"] = framing["sentence"].fillna("")
framing["year"] = pd.to_numeric(framing["year"], errors="coerce")
print(f"  Framing combined: {len(framing):,}")

# 3. PubMed abstracts
raw_files = list((RAW / "pubmed").glob("pubmed_broad_title_only_1950_2026*.csv"))
if raw_files:
    raw = pd.read_csv(raw_files[0])
    raw["year"] = pd.to_numeric(raw["year"], errors="coerce")
    raw["abstract"] = raw["abstract"].fillna("")
    print(f"  PubMed abstracts: {len(raw):,}")
else:
    raw = pd.DataFrame()
    print("  No PubMed abstracts found.")

# 4. Abstract frames
abs_frames_path = PROC / "framing_clean_abstracts.csv"
if abs_frames_path.exists():
    abs_frames = pd.read_csv(abs_frames_path)
    abs_frames["sentence"] = abs_frames["sentence"].fillna("")
    abs_frames["year"] = pd.to_numeric(abs_frames["year"], errors="coerce")
    print(f"  Abstract frames: {len(abs_frames):,}")
else:
    abs_frames = pd.DataFrame()
    print("  No abstract frames found.")

# ── Tag techniques ───────────────────────────────────────────────────────
print("\nTagging recording techniques...")
sents = tag_techniques(sents, text_col="sentence")
framing = tag_techniques(framing, text_col="sentence")
if len(raw) > 0:
    raw = tag_techniques(raw, text_col="abstract")
if len(abs_frames) > 0:
    abs_frames = tag_techniques(abs_frames, text_col="sentence")

# Tag nuclei on all sentences
print("Tagging nuclei...")
sents = tag_nuclei(sents, text_col="sentence")

# Count technique coverage
for tid in TECHNIQUES:
    col = f"has_{tid}"
    n_sent = sents[col].sum() if col in sents.columns else 0
    n_abs = raw[col].sum() if col in raw.columns and len(raw) > 0 else 0
    n_framed = framing[col].sum() if col in framing.columns else 0
    print(f"  {TECHNIQUES[tid]['label']:25s}: {n_sent:6,} sents, {n_abs:6,} abstracts, {n_framed:5,} framed")

# ── Determine top 8 techniques by abstract count ────────────────────────
if len(raw) > 0:
    tech_counts = {tid: int(raw[f"has_{tid}"].sum()) for tid in TECHNIQUES
                   if f"has_{tid}" in raw.columns}
else:
    tech_counts = {tid: int(sents[f"has_{tid}"].sum()) for tid in TECHNIQUES
                   if f"has_{tid}" in sents.columns}
top_techs = sorted(tech_counts, key=tech_counts.get, reverse=True)[:8]
print(f"\nTop 8 techniques: {[TECHNIQUES[t]['label'] for t in top_techs]}")

# ── Panel A data: technique prevalence over time (abstracts) ─────────────
print("\nComputing technique prevalence over time...")
yr_range = range(1990, 2027)
tech_ts = {}
if len(raw) > 0:
    total_per_yr = raw.groupby("year").size()
    for tid in top_techs:
        col = f"has_{tid}"
        if col in raw.columns:
            yearly = raw[raw[col]].groupby("year").size()
            pct = (yearly / total_per_yr * 100).reindex(yr_range, fill_value=0)
            pct = pct[(pct.index >= 1990) & (pct.index <= 2026)]
            tech_ts[tid] = pct

# Mann-Kendall on each technique
tech_mk = {}
for tid, pct in tech_ts.items():
    valid = pct.dropna()
    if len(valid) >= 10:
        tau, p, sig, label = mann_kendall(valid.index.tolist(), valid.values.tolist())
        tech_mk[tid] = {"tau": tau, "p": p, "sig": sig, "label": label,
                        "n": tech_counts.get(tid, 0)}

# ── Panel B data: clean 21-frame distribution per technique ──────────────
print("Computing clean framing per technique...")
framing_by_tech = {}
for tid in top_techs:
    col = f"has_{tid}"
    if col not in framing.columns:
        continue
    sub = framing[framing[col]]
    if len(sub) < 5:
        continue
    fc = sub["frame_type"].value_counts(normalize=True)
    # Keep only clean 21 frames (no "other", no old taxonomy)
    fc = fc[fc.index.isin(FRAME_NAMES)]
    if len(fc) > 0:
        # Re-normalise after filtering
        fc = fc / fc.sum()
        framing_by_tech[tid] = fc

# Determine frame display order: by overall frequency
overall_frame_counts = framing["frame_type"].value_counts()
overall_frame_counts = overall_frame_counts[overall_frame_counts.index.isin(FRAME_NAMES)]
frame_order = overall_frame_counts.index.tolist()

# Colour palette for the 21 frames
_frame_palette_base = [
    "#D55E00", "#0072B2", "#E69F00", "#56B4E9", "#009E73", "#CC79A7",
    "#F0E442", "#7B4F9E", "#999999", "#FFB347", "#4DC5A4", "#A0522D",
    "#B22222", "#6A5ACD", "#2E8B57", "#CD853F", "#8B008B", "#20B2AA",
    "#FF6B6B", "#4ECDC4", "#45B7D1",
]
FRAME_COLORS = {f: _frame_palette_base[i % len(_frame_palette_base)]
                for i, f in enumerate(FRAME_NAMES)}

# ── Panel C data: topic x technique heatmap ──────────────────────────────
print("Computing topic x technique heatmap...")
# Use all_sentences_clean which has bertopic_id_v2 and topic_clean_v2
sents_with_topic = sents[sents["bertopic_id_v2"] != -1].copy()

# Top 10 topics by count
top_topics = sents_with_topic["topic_clean_v2"].value_counts().head(10).index.tolist()

# Build matrix: rows = top 8 techniques, cols = top 10 topics
topic_tech_matrix = []
topic_tech_rows = []
for tid in top_techs:
    col = f"has_{tid}"
    if col not in sents_with_topic.columns:
        continue
    sub = sents_with_topic[sents_with_topic[col]]
    row = []
    for topic in top_topics:
        row.append((sub["topic_clean_v2"] == topic).sum())
    if sum(row) > 0:
        topic_tech_rows.append(TECHNIQUES[tid]["label"])
        topic_tech_matrix.append(row)

# Column-normalise (percentage of each topic's sentences in each technique)
topic_tech_arr = np.array(topic_tech_matrix, dtype=float)
col_sums = topic_tech_arr.sum(axis=0, keepdims=True)
col_sums[col_sums == 0] = 1
topic_tech_norm = topic_tech_arr / col_sums * 100

# ── Panel D data: technique x nucleus (RELATIVE) ────────────────────────
print("Computing technique x nucleus (relative)...")
tech_nuc = {}
for tid in top_techs:
    tcol = f"has_{tid}"
    if tcol not in sents.columns:
        continue
    sub = sents[sents[tcol]]
    if len(sub) < 20:
        continue
    row = {}
    for nid in NUCLEI:
        ncol = f"has_{nid}"
        if ncol in sub.columns:
            row[nid] = sub[ncol].sum()
    total_tech = len(sub)
    # Normalise: percentage of this technique's sentences that mention each nucleus
    row_pct = {nid: (cnt / total_tech * 100) if total_tech > 0 else 0
               for nid, cnt in row.items()}
    tech_nuc[tid] = row_pct

# Top 12 nuclei by overall count
nuc_overall = {}
for nid in NUCLEI:
    ncol = f"has_{nid}"
    if ncol in sents.columns:
        nuc_overall[nid] = sents[ncol].sum()
top_nuclei = sorted(nuc_overall, key=nuc_overall.get, reverse=True)[:12]

# ── Panel E data: technique-frame temporal evolution ─────────────────────
print("Computing technique-frame temporal evolution...")
# For each technique: relay%, hub%, gate% over time using framing_clean.csv
# Use 5-year rolling window for stability
KEY_FRAMES_E = ["relay", "hub", "gate"]
FRAME_COLORS_E = {
    "relay": "#D55E00", "hub": "#0072B2", "gate": "#009E73",
}
FRAME_LSTYLES_E = {
    "relay": "-", "hub": "--", "gate": ":",
}

# We need temporal data: use framing_clean which has frame_type + year + sentence
# Also merge with abstract frames for longer coverage
all_framed = pd.concat([framing, abs_frames], ignore_index=True) if len(abs_frames) > 0 else framing.copy()
all_framed = tag_techniques(all_framed, text_col="sentence")

tech_frame_temporal = {}
for tid in top_techs:
    col = f"has_{tid}"
    if col not in all_framed.columns:
        continue
    sub = all_framed[all_framed[col] & all_framed["year"].notna()].copy()
    if len(sub) < 10:
        continue
    # For each year: count frames
    yearly_data = {}
    for yr in range(1990, 2027):
        yr_sub = sub[(sub["year"] >= yr - 2) & (sub["year"] <= yr + 2)]  # 5-year window
        if len(yr_sub) < 3:
            continue
        total = len(yr_sub)
        row = {}
        for fname in KEY_FRAMES_E:
            count = (yr_sub["frame_type"] == fname).sum()
            row[fname] = count / total * 100
        yearly_data[yr] = row
    if yearly_data:
        tech_frame_temporal[tid] = pd.DataFrame(yearly_data).T

# ═════════════════════════════════════════════════════════════════════════
# Figure 6: 2x3 multi-panel (14 x 12)
# ═════════════════════════════════════════════════════════════════════════
print("\nGenerating Figure 6...")

fig = plt.figure(figsize=(14, 12))
gs = gridspec.GridSpec(2, 3, hspace=0.40, wspace=0.40)

# ── Panel A: technique prevalence over time ──────────────────────────────
ax_a = fig.add_subplot(gs[0, 0])
panel_label(ax_a, "A")

LINE_STYLES_A = ["-", "--", "-.", ":", "-", "--", "-.", ":"]
for i_tech, tid in enumerate(top_techs):
    if tid not in tech_ts:
        continue
    pct = tech_ts[tid]
    smoothed = rolling_mean(pct, window=3)
    color = TECHNIQUES[tid]["color"]
    label = TECHNIQUES[tid]["label"]
    mk_info = tech_mk.get(tid, {})
    sig = mk_info.get("sig", "")
    ax_a.plot(smoothed.index, smoothed.values, color=color,
              linestyle=LINE_STYLES_A[i_tech % len(LINE_STYLES_A)],
              label=f"{label} {sig}", linewidth=1.8)

ax_a.set_xlabel("Year")
ax_a.set_ylabel("% of thalamus papers")
ax_a.set_title("Technique prevalence (abstracts)")
ax_a.legend(fontsize=6, ncol=2, loc="upper left")
ax_a.set_xlim(1990, 2026)
despine(ax_a)

# ── Panel B: clean 21-frame distribution per technique ───────────────────
ax_b = fig.add_subplot(gs[0, 1])
panel_label(ax_b, "B")

y_pos = []
y_labels = []
for i, tid in enumerate(top_techs):
    if tid not in framing_by_tech:
        continue
    y_pos.append(i)
    y_labels.append(TECHNIQUES[tid]["label"])
    left = 0
    fc = framing_by_tech[tid]
    for cat in frame_order:
        val = fc.get(cat, 0) * 100
        if val > 0:
            ax_b.barh(i, val, left=left, height=0.7,
                      color=FRAME_COLORS.get(cat, "#cccccc"),
                      edgecolor="white", linewidth=0.3)
            if val > 8:
                display = cat.replace("_", "\n")
                ax_b.text(left + val / 2, i, display, ha="center", va="center",
                          fontsize=5, color="black")
            left += val

ax_b.set_yticks(y_pos)
ax_b.set_yticklabels(y_labels, fontsize=8)
ax_b.set_xlabel("% of framed sentences")
ax_b.set_title("Frame distribution by technique (21 frames)")
ax_b.set_xlim(0, 100)
despine(ax_b)

# ── Panel C: topic x technique heatmap ───────────────────────────────────
ax_c = fig.add_subplot(gs[0, 2])
panel_label(ax_c, "C")

if len(topic_tech_matrix) > 0:
    col_labs_c = [t[:25] if len(t) > 25 else t for t in top_topics]
    heatmap(ax_c, topic_tech_norm, topic_tech_rows, col_labs_c,
            cmap="YlOrRd", fmt=".0f")
    ax_c.set_title("Topic x technique (col-normalised %)")
else:
    ax_c.text(0.5, 0.5, "Insufficient data", ha="center", va="center",
              transform=ax_c.transAxes)
    ax_c.set_title("Topic x technique")
despine(ax_c)

# ── Panel D: technique x nucleus (RELATIVE) ─────────────────────────────
ax_d = fig.add_subplot(gs[1, 0])
panel_label(ax_d, "D")

if tech_nuc:
    tech_ids_d = [t for t in top_techs if t in tech_nuc]
    matrix_d = []
    row_labs_d = []
    for tid in tech_ids_d:
        row = [tech_nuc[tid].get(nid, 0) for nid in top_nuclei]
        if sum(row) > 0:
            row_labs_d.append(TECHNIQUES[tid]["label"])
            matrix_d.append(row)

    if matrix_d:
        col_labs_d = [NUCLEI[n]["label"] for n in top_nuclei]
        im_d = heatmap(ax_d, np.array(matrix_d), row_labs_d, col_labs_d,
                        cmap="YlGnBu", fmt=".1f")
        cbar_d = fig.colorbar(im_d, ax=ax_d, fraction=0.046, pad=0.04)
        cbar_d.set_label("% of technique's sentences\nmentioning nucleus",
                         fontsize=7)
        cbar_d.ax.tick_params(labelsize=7)
    ax_d.set_title("Technique x nucleus (row-normalised %)")
else:
    ax_d.text(0.5, 0.5, "Insufficient data", ha="center", va="center",
              transform=ax_d.transAxes)
despine(ax_d)

# ── Panel E: technique-frame temporal evolution (2x2, top 4 techniques) ──
TOP4_TECHS_E = ["fMRI", "electrophysiology", "optogenetics", "DBS"]
gs_e = gs[1, 1].subgridspec(2, 2, hspace=0.55, wspace=0.45)
ax_e_label = fig.add_subplot(gs[1, 1])
ax_e_label.set_axis_off()
panel_label(ax_e_label, "E", x=-0.08, y=1.10)

for idx, tid in enumerate(TOP4_TECHS_E):
    row_e = idx // 2
    col_e = idx % 2
    ax_sub = fig.add_subplot(gs_e[row_e, col_e])

    if tid in tech_frame_temporal:
        df_tf = tech_frame_temporal[tid]
        for fname in KEY_FRAMES_E:
            if fname in df_tf.columns:
                vals = df_tf[fname].dropna()
                if len(vals) > 2:
                    ax_sub.plot(vals.index, vals.values,
                                color=FRAME_COLORS_E[fname],
                                linestyle=FRAME_LSTYLES_E[fname],
                                linewidth=1.8, label=f"{fname}%")
    ax_sub.set_title(TECHNIQUES[tid]["label"], fontsize=9, pad=4)
    ax_sub.tick_params(labelsize=7)
    ax_sub.set_xlim(1990, 2026)
    ax_sub.set_ylabel("% of framed", fontsize=7)
    despine(ax_sub)

    # Add legend in every subplot for clarity (only if lines were plotted)
    handles, labels = ax_sub.get_legend_handles_labels()
    if handles:
        ax_sub.legend(fontsize=7, ncol=1, loc="best",
                      handlelength=1.5, framealpha=0.8)

# Title for the panel E region
fig.text(0.52, 0.455, "Frame evolution per technique (5-yr rolling)",
         ha="center", fontsize=10, fontweight="bold")

# ── Panel F: Mann-Kendall trend bars ─────────────────────────────────────
ax_f = fig.add_subplot(gs[1, 2])
panel_label(ax_f, "F")

if tech_mk:
    mk_items = []
    for tid in top_techs:
        if tid in tech_mk:
            label = TECHNIQUES[tid]["label"]
            tau = tech_mk[tid]["tau"]
            sig = tech_mk[tid]["sig"]
            display = f"{label} {sig}"
            mk_items.append((display, tau))
    mk_bar(ax_f, mk_items, sort=True)
    ax_f.set_title("Technique prevalence trends\n(Mann-Kendall tau)", pad=8)
else:
    ax_f.text(0.5, 0.5, "Insufficient data", ha="center", va="center",
              transform=ax_f.transAxes)
despine(ax_f)

save_fig(fig, PUB / "fig6_techniques_v2")
plt.close(fig)

# ═════════════════════════════════════════════════════════════════════════
# Summary statistics
# ═════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("SUMMARY STATISTICS")
print("=" * 70)

print("\n-- Technique Mann-Kendall Trends --")
for tid in sorted(tech_mk, key=lambda t: tech_mk[t]["tau"], reverse=True):
    mk = tech_mk[tid]
    print(f"  {TECHNIQUES[tid]['label']:25s}  n={mk['n']:5d}  "
          f"tau={mk['tau']:+.3f}  {mk['sig']:3s}  {mk['label']}")

print("\n-- Clean frame distribution per technique --")
for tid in top_techs:
    if tid not in framing_by_tech:
        continue
    fc = framing_by_tech[tid]
    top3 = fc.head(3)
    desc = ", ".join(f"{f}={v:.0%}" for f, v in top3.items())
    print(f"  {TECHNIQUES[tid]['label']:25s}: {desc}")

# Chi-squared test: technique x frame independence (clean frames only)
print("\n-- Chi-squared: technique x frame (clean 21 frames) --")
contingency = []
tech_labels_chi = []
# Use counts (not proportions) for chi-squared
for tid in top_techs:
    col = f"has_{tid}"
    if col not in framing.columns:
        continue
    sub = framing[framing[col]]
    if len(sub) < 5:
        continue
    fc = sub["frame_type"].value_counts()
    fc = fc[fc.index.isin(FRAME_NAMES)]
    if len(fc) == 0:
        continue
    # Use top frames that appear across techniques
    top_chi_frames = overall_frame_counts.head(8).index.tolist()
    row = [int(fc.get(f, 0)) for f in top_chi_frames]
    if sum(row) > 0:
        tech_labels_chi.append(TECHNIQUES[tid]["label"])
        contingency.append(row)

if len(contingency) >= 2:
    arr = np.array(contingency)
    # Remove columns with all zeros
    col_sums_chi = arr.sum(axis=0)
    keep = col_sums_chi > 0
    arr = arr[:, keep]
    frames_kept = [f for f, k in zip(top_chi_frames, keep) if k]

    chi2, p, dof, expected = chi2_contingency(arr)
    print(f"  Techniques: {tech_labels_chi}")
    print(f"  Frames: {frames_kept}")
    print(f"  chi2 = {chi2:.1f}, df = {dof}, p = {p:.2e}")
    if p < 0.001:
        print("  --> Technique and framing are significantly associated (p < 0.001)")
    elif p < 0.05:
        print(f"  --> Significant association (p = {p:.4f})")
    else:
        print(f"  --> No significant association (p = {p:.4f})")
else:
    print("  Insufficient data for chi-squared test.")

print("\nDone -- Figure 6 saved.")
