"""
Confound analysis — Test whether frame-type changes remain significant
after controlling for recording technique.

Uses logistic regression (statsmodels GLM with binomial family) to model
hub vs relay framing as a function of year + technique indicators.

Outputs:
    Graphs/pub/fig_confound_lme.{png,svg}
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

from plotting import apply_style, save_fig, panel_label, despine, WONG, heatmap
from nucleus_patterns import NUCLEI, tag_nuclei
from technique_patterns import TECHNIQUES, tag_techniques
from stats import mann_kendall

apply_style()

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT  = Path(__file__).resolve().parents[2]
PROC  = ROOT / "Data" / "processed"
PUB   = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

# ── Load and combine framing data ────────────────────────────────────────────
print("Loading framing data ...")
framing_sent = pd.read_csv(PROC / "framing_clean.csv")
framing_abs  = pd.read_csv(PROC / "framing_clean_abstracts.csv")

# Harmonise columns
if "source" not in framing_sent.columns:
    framing_sent["source"] = "fulltext"
framing_all = pd.concat([framing_sent, framing_abs], ignore_index=True)
print(f"  Combined: {len(framing_all):,} framing sentences")
print(f"  Frame types: {sorted(framing_all['frame_type'].dropna().unique())}")

# ── Tag techniques ───────────────────────────────────────────────────────────
print("Tagging techniques ...")
framing_all = tag_techniques(framing_all, text_col="sentence")

# ── Try statsmodels; fall back to sklearn ────────────────────────────────────
USE_STATSMODELS = True
try:
    import statsmodels.api as sm
    from statsmodels.formula.api import logit as sm_logit
    print("Using statsmodels for logistic regression")
except ImportError:
    USE_STATSMODELS = False
    from sklearn.linear_model import LogisticRegression
    print("statsmodels not available; falling back to sklearn LogisticRegression")

# ── 1) Hub vs Relay logistic regression ──────────────────────────────────────
print("\n=== Hub vs Relay analysis ===")
relay_hub = framing_all[framing_all["frame_type"].isin(["relay", "hub"])].copy()
relay_hub["is_hub"] = (relay_hub["frame_type"] == "hub").astype(int)

# Centre year for interpretability
relay_hub["year_c"] = relay_hub["year"] - relay_hub["year"].median()

# Key technique predictors
tech_predictors = ["has_fMRI", "has_electrophysiology", "has_optogenetics",
                   "has_calcium_imaging", "has_chemogenetics", "has_lesion"]
# Keep only those that have enough variance
tech_used = []
for tp in tech_predictors:
    if tp in relay_hub.columns and relay_hub[tp].sum() >= 5:
        relay_hub[tp] = relay_hub[tp].astype(int)
        tech_used.append(tp)
    elif tp in relay_hub.columns:
        relay_hub[tp] = relay_hub[tp].astype(int)

if not tech_used:
    # Fall back to any technique with enough counts
    for tid in TECHNIQUES:
        col = f"has_{tid}"
        if col in relay_hub.columns and relay_hub[col].sum() >= 5:
            relay_hub[col] = relay_hub[col].astype(int)
            tech_used.append(col)

print(f"  Relay: {(relay_hub['is_hub']==0).sum()}, Hub: {(relay_hub['is_hub']==1).sum()}")
print(f"  Technique predictors used: {tech_used}")

results_table = []

if USE_STATSMODELS:
    # Model 1: Year only
    formula_year = "is_hub ~ year_c"
    try:
        model_year = sm_logit(formula_year, data=relay_hub).fit(disp=0)
        print("\n--- Model 1: Year only ---")
        print(model_year.summary2().tables[1].to_string())

        for param in model_year.params.index:
            results_table.append({
                "model": "year_only",
                "predictor": param,
                "coef": model_year.params[param],
                "SE": model_year.bse[param],
                "z": model_year.tvalues[param],
                "p": model_year.pvalues[param],
                "OR": np.exp(model_year.params[param]),
            })
    except Exception as e:
        print(f"  Model 1 failed: {e}")

    # Model 2: Year + techniques
    if tech_used:
        formula_full = "is_hub ~ year_c + " + " + ".join(tech_used)
        try:
            model_full = sm_logit(formula_full, data=relay_hub).fit(disp=0)
            print("\n--- Model 2: Year + techniques ---")
            print(model_full.summary2().tables[1].to_string())

            for param in model_full.params.index:
                results_table.append({
                    "model": "year_plus_tech",
                    "predictor": param,
                    "coef": model_full.params[param],
                    "SE": model_full.bse[param],
                    "z": model_full.tvalues[param],
                    "p": model_full.pvalues[param],
                    "OR": np.exp(model_full.params[param]),
                })
        except Exception as e:
            print(f"  Model 2 failed: {e}")

    # ── Per-frame-type binary models ─────────────────────────────────────────
    print("\n=== Per-frame-type year trends (controlling for techniques) ===")
    major_frames = ["relay", "hub", "gate", "integrator", "modulator",
                    "regulator", "filter", "pacemaker", "network_node"]
    # Only include frames present in data
    major_frames = [f for f in major_frames if f in framing_all["frame_type"].values]

    frame_year_coefs = {}  # {frame: (coef_yearonly, coef_controlled)}

    for ft in major_frames:
        framing_all[f"is_{ft}"] = (framing_all["frame_type"] == ft).astype(int)
        framing_all["year_c"] = framing_all["year"] - framing_all["year"].median()

        # Ensure technique columns are int
        for tp in tech_used:
            if tp in framing_all.columns:
                framing_all[tp] = framing_all[tp].astype(int)

        # Year-only model
        try:
            m1 = sm_logit(f"is_{ft} ~ year_c", data=framing_all).fit(disp=0)
            coef_year = m1.params.get("year_c", np.nan)
        except Exception:
            coef_year = np.nan

        # Year + techniques
        if tech_used:
            try:
                formula = f"is_{ft} ~ year_c + " + " + ".join(tech_used)
                m2 = sm_logit(formula, data=framing_all).fit(disp=0)
                coef_controlled = m2.params.get("year_c", np.nan)
            except Exception:
                coef_controlled = np.nan
        else:
            coef_controlled = np.nan

        frame_year_coefs[ft] = (coef_year, coef_controlled)
        print(f"  {ft:20s}  year_only={coef_year:+.4f}  controlled={coef_controlled:+.4f}")

else:
    # sklearn fallback
    print("  Running sklearn LogisticRegression ...")
    X_year = relay_hub[["year_c"]].values
    y = relay_hub["is_hub"].values

    lr1 = LogisticRegression(max_iter=1000)
    lr1.fit(X_year, y)
    coef_year_only = lr1.coef_[0][0]
    print(f"  Year-only coef: {coef_year_only:.4f}")

    results_table.append({
        "model": "year_only", "predictor": "year_c",
        "coef": coef_year_only, "SE": np.nan, "z": np.nan, "p": np.nan,
        "OR": np.exp(coef_year_only),
    })

    if tech_used:
        X_full = relay_hub[["year_c"] + tech_used].values
        lr2 = LogisticRegression(max_iter=1000)
        lr2.fit(X_full, y)
        coef_year_controlled = lr2.coef_[0][0]
        print(f"  Year+tech coef: {coef_year_controlled:.4f}")

        results_table.append({
            "model": "year_plus_tech", "predictor": "year_c",
            "coef": coef_year_controlled, "SE": np.nan, "z": np.nan, "p": np.nan,
            "OR": np.exp(coef_year_controlled),
        })

    # Per-frame models with sklearn
    major_frames = ["relay", "hub", "gate", "integrator", "modulator",
                    "regulator", "filter", "pacemaker", "network_node"]
    major_frames = [f for f in major_frames if f in framing_all["frame_type"].values]
    framing_all["year_c"] = framing_all["year"] - framing_all["year"].median()
    for tp in tech_used:
        if tp in framing_all.columns:
            framing_all[tp] = framing_all[tp].astype(int)

    frame_year_coefs = {}
    for ft in major_frames:
        framing_all[f"is_{ft}"] = (framing_all["frame_type"] == ft).astype(int)
        y_ft = framing_all[f"is_{ft}"].values

        try:
            lr_a = LogisticRegression(max_iter=1000)
            lr_a.fit(framing_all[["year_c"]].values, y_ft)
            c1 = lr_a.coef_[0][0]
        except Exception:
            c1 = np.nan

        try:
            lr_b = LogisticRegression(max_iter=1000)
            lr_b.fit(framing_all[["year_c"] + tech_used].values, y_ft)
            c2 = lr_b.coef_[0][0]
        except Exception:
            c2 = np.nan

        frame_year_coefs[ft] = (c1, c2)

# ── Print results table ─────────────────────────────────────────────────────
if results_table:
    print("\n=== Regression results table ===")
    rt = pd.DataFrame(results_table)
    print(rt.to_string(index=False))

# ── FIGURE ───────────────────────────────────────────────────────────────────
print("\nPlotting ...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
fig.subplots_adjust(wspace=0.4)

# --- Panel A: Forest plot of odds ratios (year effect per frame type) ---
if frame_year_coefs:
    frames_sorted = sorted(frame_year_coefs.keys(),
                           key=lambda f: frame_year_coefs[f][1]
                           if np.isfinite(frame_year_coefs[f][1]) else 0)

    y_pos = np.arange(len(frames_sorted))
    ors = [np.exp(frame_year_coefs[f][1]) if np.isfinite(frame_year_coefs[f][1]) else 1.0
           for f in frames_sorted]

    colours = [WONG["red"] if or_val > 1 else WONG["blue"] for or_val in ors]
    ax1.barh(y_pos, ors, color=colours, edgecolor="none", height=0.6, alpha=0.8)
    ax1.axvline(1.0, color="#444444", linewidth=0.8, linestyle="--")
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(frames_sorted, fontsize=8)
    ax1.set_xlabel("Odds ratio (year effect, controlling for technique)")
    ax1.set_title("Year effect on frame type (OR)")
    despine(ax1)
    ax1.invert_yaxis()
    panel_label(ax1, "A")
else:
    ax1.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax1.transAxes)
    panel_label(ax1, "A")

# --- Panel B: Year coefficient comparison (with vs without controls) ---
if frame_year_coefs:
    frames_sorted2 = sorted(frame_year_coefs.keys(),
                            key=lambda f: abs(frame_year_coefs[f][0])
                            if np.isfinite(frame_year_coefs[f][0]) else 0,
                            reverse=True)

    x = np.arange(len(frames_sorted2))
    width = 0.35
    coefs_yearonly = [frame_year_coefs[f][0] if np.isfinite(frame_year_coefs[f][0]) else 0
                      for f in frames_sorted2]
    coefs_controlled = [frame_year_coefs[f][1] if np.isfinite(frame_year_coefs[f][1]) else 0
                        for f in frames_sorted2]

    bars1 = ax2.bar(x - width/2, coefs_yearonly, width, label="Year only",
                    color=WONG["blue"], alpha=0.8, edgecolor="none")
    bars2 = ax2.bar(x + width/2, coefs_controlled, width, label="Year + techniques",
                    color=WONG["orange"], alpha=0.8, edgecolor="none")

    ax2.set_xticks(x)
    ax2.set_xticklabels(frames_sorted2, rotation=45, ha="right", fontsize=8)
    ax2.axhline(0, color="#444444", linewidth=0.6)
    ax2.set_ylabel("Year coefficient (logistic regression)")
    ax2.set_title("Effect of controlling for technique")
    ax2.legend(fontsize=8, frameon=False)
    despine(ax2)
    panel_label(ax2, "B")
else:
    ax2.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax2.transAxes)
    panel_label(ax2, "B")

# ── Save ─────────────────────────────────────────────────────────────────────
save_fig(fig, PUB / "fig_confound_lme.png", dpi=300)
save_fig(fig, PUB / "fig_confound_lme.svg")
plt.close(fig)

print(f"\nFigure saved to {PUB / 'fig_confound_lme.png'}")
print("Done.")
