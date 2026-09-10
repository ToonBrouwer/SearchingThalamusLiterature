"""
relay_decline_drivers.py — What drives the decline of relay framing?

Investigates:
  1. Which techniques are associated with relay vs hub, and how does this change?
  2. Which nuclei are relay-framed, and is their prevalence declining?
  3. Cross-correlation: does technique/nucleus decline precede relay decline?
  4. Decomposition: how much of relay's decline is explained by technique vs nucleus shifts?
"""
import sys, io, os, re, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
from collections import Counter

from plotting import apply_style, save_fig, panel_label, despine, WONG
from technique_patterns import TECHNIQUES, TECHNIQUE_SUBCATEGORIES
from nucleus_patterns import NUCLEI

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
df = pd.read_csv(PROC / "all_sentences_final.csv", low_memory=False)
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df = df[df["year"].between(1985, 2025)].copy()

# Load enriched paper techniques
paper_tech = pd.read_csv(PROC / "paper_techniques.csv", low_memory=False)
paper_tech["year"] = pd.to_numeric(paper_tech["year"], errors="coerce")
paper_tech_idx = paper_tech.set_index("pmid")

# Framing subset
framing = df[df["frame_type"].notna() & (df["frame_type"] != "")].copy()
print(f"  Sentences: {len(df):,}, Framing: {len(framing):,}")

years = np.arange(1990, 2026)

# ═══════════════════════════════════════════════════════════════════════════
# 1. RELAY RATE PER TECHNIQUE OVER TIME
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("1. RELAY RATE PER TECHNIQUE OVER TIME")
print("=" * 70)

# For each framing paper, look up its techniques
framing_papers = framing.groupby("pmid").agg(
    year=("year", "first"),
    frame_types=("frame_type", lambda x: list(x)),
    n_relay=("frame_type", lambda x: (x == "relay").sum()),
    n_hub=("frame_type", lambda x: (x == "hub").sum()),
    n_gate=("frame_type", lambda x: (x == "gate").sum()),
    n_total=("frame_type", "count"),
).reset_index()

# Merge technique tags
tech_cols = [f"has_{t}" for t in list(TECHNIQUES.keys()) + list(TECHNIQUE_SUBCATEGORIES.keys())]
available_tech = [c for c in tech_cols if c in paper_tech_idx.columns]

for col in available_tech:
    framing_papers[col] = framing_papers["pmid"].map(
        lambda p: paper_tech_idx.loc[p, col] if p in paper_tech_idx.index else False
    )

# Compute relay fraction per technique per 5-year bin
framing_papers["half_decade"] = (framing_papers["year"] // 5) * 5
framing_papers["relay_frac"] = framing_papers["n_relay"] / framing_papers["n_total"]
framing_papers["hub_frac"] = framing_papers["n_hub"] / framing_papers["n_total"]

print("\n  Relay rate by technique (overall):")
key_techs = ["fMRI", "electrophysiology", "optogenetics", "DBS", "lesion",
             "tract_tracing", "EEG_MEG", "structural_MRI",
             "functional_connectivity", "resting_state_fMRI",
             "single_unit", "patch_clamp", "histology"]

for tid in key_techs:
    col = f"has_{tid}"
    if col not in framing_papers.columns:
        continue
    with_tech = framing_papers[framing_papers[col] == True]
    without_tech = framing_papers[framing_papers[col] == False]
    if len(with_tech) < 10:
        continue
    r_with = with_tech["relay_frac"].mean()
    r_without = without_tech["relay_frac"].mean()
    h_with = with_tech["hub_frac"].mean()
    print(f"    {tid:25s}: relay={r_with:.2f} (n={len(with_tech)}), "
          f"hub={h_with:.2f}, vs no-{tid[:10]} relay={r_without:.2f}")

# Temporal: relay rate per technique per half-decade
print("\n  Relay rate per technique over time (5yr bins):")
HALF_DECADES = [1990, 1995, 2000, 2005, 2010, 2015, 2020]
tech_relay_temporal = {}

for tid in ["electrophysiology", "fMRI", "lesion", "DBS", "tract_tracing", "structural_MRI"]:
    col = f"has_{tid}"
    if col not in framing_papers.columns:
        continue
    temporal = {}
    for hd in HALF_DECADES:
        subset = framing_papers[(framing_papers["half_decade"] == hd) & (framing_papers[col] == True)]
        if len(subset) >= 5:
            temporal[hd] = subset["relay_frac"].mean()
    if temporal:
        tech_relay_temporal[tid] = temporal
        vals = [f"{temporal.get(hd, float('nan')):.2f}" for hd in HALF_DECADES]
        print(f"    {tid:25s}: {' → '.join(vals)}")

# ═══════════════════════════════════════════════════════════════════════════
# 2. RELAY RATE PER NUCLEUS OVER TIME
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("2. RELAY RATE PER NUCLEUS OVER TIME")
print("=" * 70)

nuc_cols = [f"has_{nid}" for nid in NUCLEI if f"has_{nid}" in framing.columns]

# Per nucleus: what fraction of framing sentences are relay?
print("\n  Relay dominance per nucleus (overall):")
nuc_relay_rate = {}
for nid in NUCLEI:
    col = f"has_{nid}"
    if col not in framing.columns:
        continue
    nuc_framing = framing[framing[col] == True]
    if len(nuc_framing) < 20:
        continue
    relay_rate = (nuc_framing["frame_type"] == "relay").mean()
    hub_rate = (nuc_framing["frame_type"] == "hub").mean()
    gate_rate = (nuc_framing["frame_type"] == "gate").mean()
    nuc_relay_rate[nid] = relay_rate
    print(f"    {nid:15s}: relay={relay_rate:.2f}, hub={hub_rate:.2f}, "
          f"gate={gate_rate:.2f} (n={len(nuc_framing)})")

# Temporal: relay rate per nucleus per 5-year bin
framing["half_decade"] = (framing["year"] // 5) * 5
nuc_relay_temporal = {}

print("\n  Relay rate per nucleus over time:")
for nid in ["VP", "LGN", "MGN", "VL", "MD", "TRN", "Pulvinar", "ANT", "PVT"]:
    col = f"has_{nid}"
    if col not in framing.columns:
        continue
    temporal = {}
    for hd in HALF_DECADES:
        subset = framing[(framing["half_decade"] == hd) & (framing[col] == True)]
        if len(subset) >= 5:
            temporal[hd] = (subset["frame_type"] == "relay").mean()
    if temporal:
        nuc_relay_temporal[nid] = temporal
        vals = [f"{temporal.get(hd, float('nan')):.2f}" for hd in HALF_DECADES]
        print(f"    {nid:15s}: {' → '.join(vals)}")

# ═══════════════════════════════════════════════════════════════════════════
# 3. NUCLEUS PREVALENCE VS RELAY DECLINE
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("3. NUCLEUS PREVALENCE TRENDS")
print("=" * 70)

# Per nucleus: prevalence (% of all sentences) per year
nuc_prevalence = {}
for nid in NUCLEI:
    col = f"has_{nid}"
    if col not in df.columns:
        continue
    yearly = df.groupby("year")[col].mean() * 100
    nuc_prevalence[nid] = yearly.reindex(years, fill_value=0)

# Overall relay rate per year
relay_yearly = framing[framing["frame_type"] == "relay"].groupby("year").size()
total_framing_yearly = framing.groupby("year").size()
relay_pct = (relay_yearly / total_framing_yearly * 100).reindex(years, fill_value=0)

# Cross-correlation: does nucleus prevalence decline predict relay decline?
from statsmodels.tsa.stattools import grangercausalitytests

print("\n  Nucleus prevalence → relay framing (Granger causality):")
for nid in ["VP", "LGN", "MGN", "VL", "MD", "TRN", "Pulvinar", "ANT", "PVT"]:
    if nid not in nuc_prevalence:
        continue
    nuc_ts = nuc_prevalence[nid].values
    relay_ts = relay_pct.values

    # Trim to where both have signal
    valid = (nuc_ts > 0.01) | (relay_ts > 0.01)
    first = np.argmax(valid) if valid.any() else 0
    nuc_ts = nuc_ts[first:]
    relay_ts = relay_ts[first:]

    if len(nuc_ts) < 10 or np.std(nuc_ts) < 0.001 or np.std(relay_ts) < 0.001:
        continue

    data = np.column_stack([relay_ts, nuc_ts])
    try:
        gc = grangercausalitytests(data, maxlag=3, verbose=False)
        best_p = min(gc[lag][0]['ssr_ftest'][1] for lag in [1, 2, 3])
        best_f = max(gc[lag][0]['ssr_ftest'][0] for lag in [1, 2, 3])
        sig = "***" if best_p < 0.001 else "**" if best_p < 0.01 else "*" if best_p < 0.05 else "ns"
        print(f"    {nid:15s} → relay: F={best_f:.2f}, p={best_p:.4f} {sig}")
    except Exception as e:
        print(f"    {nid:15s} → relay: FAILED ({e})")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE: 6-panel
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("GENERATING FIGURES")
print("=" * 70)

fig, axes = plt.subplots(3, 2, figsize=(16, 18))
fig.suptitle("Drivers of Relay Framing Decline", fontsize=15, fontweight="bold", y=0.98)

# Panel A: Relay rate per technique (bar chart)
ax = axes[0, 0]
tech_data = []
for tid in key_techs:
    col = f"has_{tid}"
    if col not in framing_papers.columns:
        continue
    with_tech = framing_papers[framing_papers[col] == True]
    if len(with_tech) < 10:
        continue
    tech_data.append({
        "technique": tid,
        "relay_rate": with_tech["relay_frac"].mean(),
        "hub_rate": with_tech["hub_frac"].mean(),
        "n": len(with_tech),
    })

td = pd.DataFrame(tech_data).sort_values("relay_rate", ascending=True)
y_pos = np.arange(len(td))
ax.barh(y_pos, td["relay_rate"], color=WONG["blue"], alpha=0.8, label="Relay rate")
ax.barh(y_pos, -td["hub_rate"], color=WONG["red"], alpha=0.8, label="Hub rate")
ax.set_yticks(y_pos)
ax.set_yticklabels(td["technique"], fontsize=8)
ax.axvline(0, color="grey", linewidth=0.5)
ax.set_xlabel("← Hub rate | Relay rate →")
ax.set_title("A. Relay vs hub rate per technique")
ax.legend(fontsize=8)
despine(ax)
panel_label(ax, "A")

# Panel B: Relay rate per nucleus (bar chart)
ax = axes[0, 1]
nuc_data = sorted(nuc_relay_rate.items(), key=lambda x: x[1])
nuc_names = [n for n, _ in nuc_data]
nuc_vals = [v for _, v in nuc_data]
colors_nuc = [WONG["blue"] if v > 0.5 else WONG["red"] if v < 0.2 else WONG["orange"]
              for v in nuc_vals]
y_pos = np.arange(len(nuc_names))
ax.barh(y_pos, nuc_vals, color=colors_nuc, alpha=0.8)
ax.axvline(0.5, color="grey", linestyle="--", alpha=0.5)
ax.set_yticks(y_pos)
ax.set_yticklabels(nuc_names, fontsize=8)
ax.set_xlabel("Relay fraction of framing sentences")
ax.set_title("B. Relay dominance per nucleus\n(blue >50%, red <20%)")
despine(ax)
panel_label(ax, "B")

# Panel C: Relay rate temporal trajectory per technique
ax = axes[1, 0]
tech_colors = {"electrophysiology": WONG["blue"], "fMRI": WONG["red"],
               "lesion": WONG["orange"], "DBS": WONG["grey"],
               "tract_tracing": WONG["green"], "structural_MRI": WONG["purple"]}
for tid, temporal in tech_relay_temporal.items():
    hds = sorted(temporal.keys())
    vals = [temporal[h] for h in hds]
    ax.plot(hds, vals, "o-", color=tech_colors.get(tid, "black"),
            label=tid, linewidth=2, markersize=5)
ax.set_xlabel("5-year bin")
ax.set_ylabel("Relay fraction within technique")
ax.set_title("C. Relay rate over time per technique")
ax.legend(fontsize=7)
ax.axhline(0.5, color="grey", linestyle="--", alpha=0.3)
despine(ax)
panel_label(ax, "C")

# Panel D: Relay rate temporal trajectory per nucleus
ax = axes[1, 1]
nuc_colors = {"VP": WONG["blue"], "LGN": WONG["green"], "MGN": WONG["orange"],
              "VL": WONG["grey"], "MD": WONG["red"], "TRN": WONG["purple"],
              "Pulvinar": "#8c564b", "ANT": "#17becf", "PVT": "#bcbd22"}
for nid, temporal in nuc_relay_temporal.items():
    hds = sorted(temporal.keys())
    vals = [temporal[h] for h in hds]
    ax.plot(hds, vals, "o-", color=nuc_colors.get(nid, "black"),
            label=nid, linewidth=2, markersize=5)
ax.set_xlabel("5-year bin")
ax.set_ylabel("Relay fraction within nucleus")
ax.set_title("D. Relay rate over time per nucleus")
ax.legend(fontsize=7, ncol=2)
ax.axhline(0.5, color="grey", linestyle="--", alpha=0.3)
despine(ax)
panel_label(ax, "D")

# Panel E: Nucleus prevalence vs relay decline (scatter)
ax = axes[2, 0]
# For each nucleus: plot change in prevalence vs change in relay rate
for nid in nuc_relay_temporal:
    temporal = nuc_relay_temporal[nid]
    hds = sorted(temporal.keys())
    if len(hds) < 3:
        continue
    # Change from first to last available
    relay_change = temporal[hds[-1]] - temporal[hds[0]]

    # Prevalence change
    if nid in nuc_prevalence:
        prev = nuc_prevalence[nid]
        prev_early = prev[(prev.index >= hds[0]) & (prev.index < hds[0]+5)].mean()
        prev_late = prev[(prev.index >= hds[-1]) & (prev.index < hds[-1]+5)].mean()
        prev_change = prev_late - prev_early

        ax.scatter(prev_change, relay_change, s=80,
                   color=nuc_colors.get(nid, "black"), zorder=5)
        ax.annotate(nid, (prev_change, relay_change), fontsize=8,
                    textcoords="offset points", xytext=(5, 5))

ax.axhline(0, color="grey", linestyle="--", alpha=0.3)
ax.axvline(0, color="grey", linestyle="--", alpha=0.3)
ax.set_xlabel("Change in nucleus prevalence (% of sentences)")
ax.set_ylabel("Change in relay fraction")
ax.set_title("E. Nucleus prevalence change vs relay rate change")
despine(ax)
panel_label(ax, "E")

# Panel F: Technique prevalence vs relay decline
ax = axes[2, 1]
for tid in tech_relay_temporal:
    temporal = tech_relay_temporal[tid]
    hds = sorted(temporal.keys())
    if len(hds) < 3:
        continue
    relay_change = temporal[hds[-1]] - temporal[hds[0]]

    # Technique prevalence change (from paper_tech)
    pt = paper_tech[paper_tech["year"].between(1985, 2025)]
    col = f"has_{tid}"
    if col not in pt.columns:
        continue
    prev_early = pt[pt["year"].between(hds[0], hds[0]+4)][col].mean() * 100
    prev_late = pt[pt["year"].between(hds[-1], hds[-1]+4)][col].mean() * 100
    prev_change = prev_late - prev_early

    ax.scatter(prev_change, relay_change, s=80,
               color=tech_colors.get(tid, "black"), zorder=5)
    ax.annotate(tid, (prev_change, relay_change), fontsize=7,
                textcoords="offset points", xytext=(5, 5))

ax.axhline(0, color="grey", linestyle="--", alpha=0.3)
ax.axvline(0, color="grey", linestyle="--", alpha=0.3)
ax.set_xlabel("Change in technique prevalence (%)")
ax.set_ylabel("Change in relay fraction within technique")
ax.set_title("F. Technique prevalence change vs relay rate change")
despine(ax)
panel_label(ax, "F")

plt.tight_layout(rect=[0, 0, 1, 0.96])
save_fig(fig, PUB / "fig_relay_decline_drivers.png")
print("  Saved fig_relay_decline_drivers.png")

print("\nDone.")
