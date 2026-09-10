"""
syntactic_framing.py — Analysis 3 enhancement: Syntactic role of thalamus.

Classifies thalamus's grammatical role in each sentence:
  - SUBJECT: thalamus modulates / gates / drives (active framing)
  - OBJECT: cortex projects to thalamus / stimulates thalamus (passive framing)
  - PREPOSITIONAL: via thalamus / through the thalamus (conduit framing)
  - NONE: thalamus mentioned but not in SVO relation

Also tracks:
  - Active vs passive voice for thalamus sentences
  - Verb class analysis (what verbs co-occur with each role)
  - Ratio of subject:object:PP per decade

Uses existing spaCy + SVO infrastructure from advanced_nlp_analysis.py.

Output:
  Data/processed/syntactic_roles.csv
  Graphs/pub/fig_syntactic_roles.png
"""
import sys, io, os, re, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import Counter

from plotting import apply_style, save_fig, panel_label, despine, WONG
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
df["decade"] = (df["year"] // 10) * 10
print(f"  {len(df):,} sentences")

# ═══════════════════════════════════════════════════════════════════════════
# THALAMUS TERMS
# ═══════════════════════════════════════════════════════════════════════════
THAL_TERMS = {"thalamus", "thalamic", "thalamocortical", "thalamocortial"}
for nid, info in NUCLEI.items():
    label = info["label"].split("(")[0].strip().lower()
    THAL_TERMS.add(label)
    THAL_TERMS.add(nid.lower())

def text_contains_thal(text):
    return any(t in text.lower() for t in THAL_TERMS)

# ═══════════════════════════════════════════════════════════════════════════
# SPACY SETUP
# ═══════════════════════════════════════════════════════════════════════════
import spacy
try:
    nlp = spacy.load("en_core_web_md", disable=["ner"])
except:
    nlp = spacy.load("en_core_web_sm", disable=["ner"])
print(f"  spaCy model: {nlp.meta['name']}")


def classify_thalamus_role(doc):
    """Classify the grammatical role of thalamus in a sentence.

    Returns dict with:
      role: SUBJECT | OBJECT | PREPOSITIONAL | NONE
      is_passive: True/False (passive voice)
      verb: the main verb if thalamus is subj/obj
      voice: 'active' | 'passive' | 'unknown'
    """
    result = {"role": "NONE", "is_passive": False, "verb": "", "voice": "unknown"}

    for token in doc:
        token_text = token.text.lower()
        # Check if this token or its head noun phrase mentions thalamus
        subtree_text = " ".join(t.text.lower() for t in token.subtree)

        # --- SUBJECT ---
        if token.dep_ == "nsubj" and text_contains_thal(subtree_text):
            result["role"] = "SUBJECT"
            result["verb"] = token.head.lemma_.lower()
            result["voice"] = "active"
            result["is_passive"] = False
            return result

        # --- PASSIVE SUBJECT (nsubjpass) ---
        if token.dep_ == "nsubjpass" and text_contains_thal(subtree_text):
            result["role"] = "SUBJECT"
            result["verb"] = token.head.lemma_.lower()
            result["voice"] = "passive"
            result["is_passive"] = True
            return result

        # --- DIRECT OBJECT ---
        if token.dep_ in ("dobj", "attr") and text_contains_thal(subtree_text):
            result["role"] = "OBJECT"
            result["verb"] = token.head.lemma_.lower()
            result["voice"] = "active"
            return result

        # --- PREPOSITIONAL OBJECT ---
        if token.dep_ == "pobj" and text_contains_thal(subtree_text):
            prep = token.head.text.lower() if token.head.dep_ == "prep" else ""
            result["role"] = "PREPOSITIONAL"
            result["verb"] = f"{prep} {token.head.head.lemma_.lower()}".strip()
            result["voice"] = "unknown"
            return result

    return result


# ═══════════════════════════════════════════════════════════════════════════
# CLASSIFY ALL SENTENCES (subsample for speed)
# ═══════════════════════════════════════════════════════════════════════════
print("\nClassifying thalamus syntactic role...")
t0 = time.time()

# Subsample: all framing sentences + random non-framing (max 30K total)
framing_mask = df["frame_type"].notna() & (df["frame_type"] != "")
framing_idx = df[framing_mask].index.values
non_framing_idx = np.random.RandomState(42).choice(
    df[~framing_mask].index.values,
    size=min(25000, (~framing_mask).sum()),
    replace=False,
)
sample_idx = np.concatenate([framing_idx, non_framing_idx])
sample = df.loc[sample_idx].copy()
print(f"  Subsample: {len(sample):,} sentences ({len(framing_idx):,} framing + "
      f"{len(non_framing_idx):,} non-framing)")

roles = []
batch_size = 500
sentences = sample["sentence"].tolist()

for batch_start in range(0, len(sentences), batch_size):
    batch = sentences[batch_start:batch_start + batch_size]
    docs = list(nlp.pipe(batch, batch_size=batch_size))
    for doc in docs:
        roles.append(classify_thalamus_role(doc))

    if (batch_start + batch_size) % 5000 < batch_size:
        elapsed = time.time() - t0
        print(f"  {batch_start + batch_size}/{len(sentences)} ({elapsed:.0f}s)")

roles_df = pd.DataFrame(roles)
sample["thal_role"] = roles_df["role"].values
sample["thal_voice"] = roles_df["voice"].values
sample["thal_is_passive"] = roles_df["is_passive"].values
sample["thal_verb"] = roles_df["verb"].values

print(f"\n  Role distribution:")
for role, count in sample["thal_role"].value_counts().items():
    pct = count / len(sample) * 100
    print(f"    {role:15s}: {count:>6,} ({pct:.1f}%)")

print(f"\n  Voice distribution (SUBJECT only):")
subj = sample[sample["thal_role"] == "SUBJECT"]
for voice, count in subj["thal_voice"].value_counts().items():
    pct = count / len(subj) * 100
    print(f"    {voice:15s}: {count:>6,} ({pct:.1f}%)")

# ═══════════════════════════════════════════════════════════════════════════
# TEMPORAL ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════
print("\n  Role ratio per decade:")
DECADES = [1980, 1990, 2000, 2010, 2020]
role_by_decade = sample[sample["decade"].isin(DECADES)].groupby(
    ["decade", "thal_role"]).size().unstack(fill_value=0)
role_pct = (role_by_decade.T / role_by_decade.sum(axis=1) * 100).T
print(role_pct.round(1).to_string())

# Top verbs per role
print("\n  Top 15 verbs — SUBJECT (thalamus does X):")
subj_verbs = sample[sample["thal_role"] == "SUBJECT"]["thal_verb"].value_counts()
for v, c in subj_verbs.head(15).items():
    print(f"    {v:25s}: {c}")

print("\n  Top 15 verbs — OBJECT (X does to thalamus):")
obj_verbs = sample[sample["thal_role"] == "OBJECT"]["thal_verb"].value_counts()
for v, c in obj_verbs.head(15).items():
    print(f"    {v:25s}: {c}")

print("\n  Top 15 preps — PREPOSITIONAL (via/in/through thalamus):")
pp_verbs = sample[sample["thal_role"] == "PREPOSITIONAL"]["thal_verb"].value_counts()
for v, c in pp_verbs.head(15).items():
    print(f"    {v:25s}: {c}")

# ═══════════════════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════════════════
sample.to_csv(PROC / "syntactic_roles.csv", index=False)
print(f"\n  Saved syntactic_roles.csv ({len(sample):,} rows)")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE: 4-panel syntactic role analysis
# ═══════════════════════════════════════════════════════════════════════════
print("\nGenerating figure...")

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("Syntactic Role of the Thalamus in Scientific Text",
             fontsize=14, fontweight="bold", y=0.98)

role_colors = {"SUBJECT": WONG["blue"], "OBJECT": WONG["red"],
               "PREPOSITIONAL": WONG["orange"], "NONE": WONG["grey"]}

# Panel A: Role ratio per decade (stacked bar)
ax = axes[0, 0]
role_order = ["SUBJECT", "OBJECT", "PREPOSITIONAL", "NONE"]
bottom = np.zeros(len(DECADES))
for role in role_order:
    if role in role_pct.columns:
        vals = [role_pct.loc[d, role] if d in role_pct.index else 0 for d in DECADES]
        ax.bar(DECADES, vals, bottom=bottom, width=8, label=role,
               color=role_colors[role], alpha=0.85)
        bottom += vals
ax.set_xlabel("Decade")
ax.set_ylabel("% of sentences")
ax.set_title("A. Thalamus grammatical role per decade")
ax.legend(fontsize=8)
despine(ax)
panel_label(ax, "A")

# Panel B: Subject:Object ratio over time
ax = axes[0, 1]
for dec in DECADES:
    if dec in role_by_decade.index:
        n_subj = role_by_decade.loc[dec, "SUBJECT"] if "SUBJECT" in role_by_decade.columns else 0
        n_obj = role_by_decade.loc[dec, "OBJECT"] if "OBJECT" in role_by_decade.columns else 0
        ratio = n_subj / max(n_obj, 1)
        ax.bar(dec, ratio, width=8, color=WONG["blue"], alpha=0.85)
ax.axhline(1.0, color="grey", linestyle="--", linewidth=1, alpha=0.5)
ax.set_xlabel("Decade")
ax.set_ylabel("Subject:Object ratio")
ax.set_title("B. Thalamus agency ratio\n(>1 = more active, <1 = more passive)")
despine(ax)
panel_label(ax, "B")

# Panel C: Top verbs when thalamus is SUBJECT (active)
ax = axes[1, 0]
top_subj = subj_verbs.head(12)
y_pos = np.arange(len(top_subj))
ax.barh(y_pos, top_subj.values, color=WONG["blue"], alpha=0.8)
ax.set_yticks(y_pos)
ax.set_yticklabels(top_subj.index, fontsize=8)
ax.invert_yaxis()
ax.set_xlabel("Count")
ax.set_title("C. What the thalamus DOES (subject verbs)")
despine(ax)
panel_label(ax, "C")

# Panel D: Top verbs when thalamus is OBJECT (passive)
ax = axes[1, 1]
top_obj = obj_verbs.head(12)
y_pos = np.arange(len(top_obj))
ax.barh(y_pos, top_obj.values, color=WONG["red"], alpha=0.8)
ax.set_yticks(y_pos)
ax.set_yticklabels(top_obj.index, fontsize=8)
ax.invert_yaxis()
ax.set_xlabel("Count")
ax.set_title("D. What is DONE TO the thalamus (object verbs)")
despine(ax)
panel_label(ax, "D")

plt.tight_layout(rect=[0, 0, 1, 0.96])
save_fig(fig, PUB / "fig_syntactic_roles.png")
print(f"  Saved fig_syntactic_roles.png")

elapsed = time.time() - t0
print(f"\nDone in {elapsed/60:.1f} minutes.")
