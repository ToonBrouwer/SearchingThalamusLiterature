"""
confound_lme_v2.py -- Test whether frame-type changes remain significant
after controlling for recording technique.

Uses logistic regression (statsmodels GLM with binomial family) to model
hub vs relay framing as a function of year + technique indicators.

Changes from v1:
  - Loads framing_combined_v2.csv
  - annotate_n() on all panels
  - Panel title/subtitle: "Does the relay->hub shift survive after
    controlling for fMRI adoption?"
  - New Panel C: predicted probability curves
  - Text annotation explaining purpose

Outputs:
    Graphs/pub/fig_confound_lme_v2.{png,svg}
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
from taxonomy import FRAMES_CLEAN, detect_frame, FRAME_NAMES
from stats import mann_kendall

apply_style()


# ── annotate_n helper ──────────────────────────────────────────────────────
def annotate_n(ax, n, loc="upper right", fontsize=8):
    ha = "right" if "right" in loc else "left"
    va = "top" if "upper" in loc else "bottom"
    x = 0.97 if "right" in loc else 0.03
    y = 0.97 if "upper" in loc else 0.03
    ax.text(x, y, f"n = {n:,}", transform=ax.transAxes,
            fontsize=fontsize, ha=ha, va=va,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white",
                      alpha=0.8, edgecolor="grey", linewidth=0.5))


# ── Paths ────────────────────────────────────────────────────────────────────
ROOT  = Path(__file__).resolve().parents[2]
PROC  = ROOT / "Data" / "processed"
PUB   = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

# ── Load framing data (v2) ──────────────────────────────────────────────────
print("Loading framing data ...")
framing_all = pd.read_csv(PROC / "framing_combined_v2.csv")
framing_all["sentence"] = framing_all["sentence"].fillna("")
framing_all["year"] = pd.to_numeric(framing_all["year"], errors="coerce")

# Harmonise source column
if "source" not in framing_all.columns:
    framing_all["source"] = "combined"

print(f"  Combined v2: {len(framing_all):,} framing sentences")
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
tech_used = []
for tp in tech_predictors:
    if tp in relay_hub.columns and relay_hub[tp].sum() >= 5:
        relay_hub[tp] = relay_hub[tp].astype(int)
        tech_used.append(tp)
    elif tp in relay_hub.columns:
        relay_hub[tp] = relay_hub[tp].astype(int)

if not tech_used:
    for tid in TECHNIQUES:
        col = f"has_{tid}"
        if col in relay_hub.columns and relay_hub[col].sum() >= 5:
            relay_hub[col] = relay_hub[col].astype(int)
            tech_used.append(col)

print(f"  Relay: {(relay_hub['is_hub']==0).sum()}, Hub: {(relay_hub['is_hub']==1).sum()}")
print(f"  Technique predictors used: {tech_used}")

results_table = []
model_year = None
model_full = None

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
    major_frames = [f for f in major_frames if f in framing_all["frame_type"].values]

    frame_year_coefs = {}

    for ft in major_frames:
        framing_all[f"is_{ft}"] = (framing_all["frame_type"] == ft).astype(int)
        framing_all["year_c"] = framing_all["year"] - framing_all["year"].median()

        for tp in tech_used:
            if tp in framing_all.columns:
                framing_all[tp] = framing_all[tp].astype(int)

        try:
            m1 = sm_logit(f"is_{ft} ~ year_c", data=framing_all).fit(disp=0)
            coef_year = m1.params.get("year_c", np.nan)
        except Exception:
            coef_year = np.nan

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
fig, axes = plt.subplots(1, 3, figsize=(16, 5))
fig.subplots_adjust(wspace=0.40, top=0.85)

# Suptitle
fig.suptitle("Does the relay \u2192 hub shift survive after controlling for fMRI adoption?",
             fontsize=13, fontweight="bold", y=0.97)

n_total = len(framing_all)
n_relay_hub = len(relay_hub)

# --- Panel A: Forest plot of odds ratios (year effect per frame type) ---
ax1 = axes[0]
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
    annotate_n(ax1, n_total, loc="lower right")
else:
    ax1.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax1.transAxes)
    panel_label(ax1, "A")

# --- Panel B: Year coefficient comparison (with vs without controls) ---
ax2 = axes[1]
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
    annotate_n(ax2, n_total, loc="upper right")
else:
    ax2.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax2.transAxes)
    panel_label(ax2, "B")

# --- Panel C: Predicted probability curves ───────────────────────────────────
ax3 = axes[2]
panel_label(ax3, "C")

# Generate predicted P(hub) over years 1980-2025
year_range_pred = np.arange(1980, 2026)
year_median = relay_hub["year"].median()
year_c_pred = year_range_pred - year_median

if USE_STATSMODELS and model_year is not None:
    # Year-only predicted probabilities
    pred_df_year = pd.DataFrame({"year_c": year_c_pred})
    # Add dummy technique columns (set to 0)
    for tp in tech_used:
        pred_df_year[tp] = 0

    p_year = model_year.predict(pred_df_year[["year_c"]])
    ax3.plot(year_range_pred, p_year, color=WONG["blue"],
             linewidth=2.0, label="Year only", linestyle="-")

    if model_full is not None:
        # Year + techniques: set all techniques to 0 (baseline)
        pred_df_full = pd.DataFrame({"year_c": year_c_pred})
        for tp in tech_used:
            pred_df_full[tp] = 0
        # Use predict with the formula interface
        p_full = model_full.predict(pred_df_full)
        ax3.plot(year_range_pred, p_full, color=WONG["orange"],
                 linewidth=2.0, label="Year + technique", linestyle="--")

    ax3.set_xlabel("Year")
    ax3.set_ylabel("Predicted P(hub)")
    ax3.set_title("Predicted probability of hub framing")
    ax3.legend(fontsize=8, frameon=False)
    ax3.set_xlim(1980, 2025)
    ax3.set_ylim(0, 1)
    despine(ax3)
    annotate_n(ax3, n_relay_hub, loc="upper left")

else:
    # sklearn fallback: compute predicted probabilities manually
    if 'lr1' in dir() or 'lr1' in locals():
        try:
            from scipy.special import expit
            p_year_sk = expit(lr1.intercept_[0] + lr1.coef_[0][0] * year_c_pred)
            ax3.plot(year_range_pred, p_year_sk, color=WONG["blue"],
                     linewidth=2.0, label="Year only", linestyle="-")

            if 'lr2' in dir() or 'lr2' in locals():
                # All technique indicators = 0
                X_pred = np.zeros((len(year_c_pred), 1 + len(tech_used)))
                X_pred[:, 0] = year_c_pred
                p_full_sk = expit(lr2.intercept_[0] + X_pred @ lr2.coef_[0])
                ax3.plot(year_range_pred, p_full_sk, color=WONG["orange"],
                         linewidth=2.0, label="Year + technique", linestyle="--")
        except Exception as e:
            print(f"  Prediction curves failed: {e}")

    ax3.set_xlabel("Year")
    ax3.set_ylabel("Predicted P(hub)")
    ax3.set_title("Predicted probability of hub framing")
    ax3.legend(fontsize=8, frameon=False)
    ax3.set_xlim(1980, 2025)
    ax3.set_ylim(0, 1)
    despine(ax3)
    annotate_n(ax3, n_relay_hub, loc="upper left")

# ── Text annotation explaining the purpose ───────────────────────────────
fig.text(0.5, 0.02,
         "If the shift were purely methodological, the year coefficient "
         "would become non-significant after controlling for technique.",
         ha="center", fontsize=8, fontstyle="italic", color="#555555",
         wrap=True)

# ── Save ─────────────────────────────────────────────────────────────────────
save_fig(fig, PUB / "fig_confound_lme_v2.png", dpi=300)
save_fig(fig, PUB / "fig_confound_lme_v2.svg")
plt.close(fig)

print(f"\nFigure saved to {PUB / 'fig_confound_lme_v2.png'}")
print("Done.")
