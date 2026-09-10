"""
Exploratory analysis of non-framing thalamus sentences.

Reads thalamus_all_sentences.csv (all sentences mentioning 'thalamus', not
just the framing subset), adds a sentence-ID column (sid), then investigates
what conceptual language was missed by the verb-trigger and appositive
detectors in extract_framing_sentences.py.

The script:
  1. Adds sid ({pmid}_f{sentence_index}) to every row and saves an enriched
     copy to Data/processed/all_sentences_with_sid.csv.
  2. Counts the words immediately following 'thalamus' in non-framing
     sentences to identify vocabulary gaps.
  3. Tests a set of MISSED_PATTERNS (e.g. "thalamus.*role in") against
     non-framing sentences to estimate how many additional framing sentences
     could be recovered by extending the triggers.
  4. Flags and saves non-framing sentences that match any MISSED_PATTERN to
     Data/processed/non_framing_sentences_flagged.csv.
  5. Prints a breakdown of verb_trigger usage per decade across framing
     sentences.

Inputs:
    Data/interim/sentence_extraction/thalamus_all_sentences.csv

Outputs (Data/processed/):
    all_sentences_with_sid.csv          — full corpus with sid column added
    non_framing_sentences_flagged.csv   — non-framing sentences with
                                          potential_framing flag set

Note on paths:
    ROOT is resolved from __file__; Code/config.py and Code/paths.py are not
    used here.
"""

import sys, io, re, warnings
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
warnings.filterwarnings("ignore")

from pathlib import Path
from collections import Counter
import pandas as pd

_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT    = _ANALYSIS_DIR.parents[1]   # project root
INTERIM = ROOT / "Data" / "interim"
PROC    = ROOT / "Data" / "processed"

# ── Load ───────────────────────────────────────────────────────────────────
print("Loading all_sentences...")
df = pd.read_csv(INTERIM / "sentence_extraction" / "thalamus_all_sentences.csv",
                 dtype={"pmid": str, "pmcid": str})
print(f"  Shape: {df.shape}")
print(f"  is_framing breakdown:\n{df['is_framing'].value_counts().to_string()}")

# ── Add sid ────────────────────────────────────────────────────────────────
df["sentence_index"] = df["sentence_index"].fillna(-1).astype(int)
df["sid"] = df["pmid"].astype(str) + "_f" + df["sentence_index"].astype(str)

# ── Save enriched file ─────────────────────────────────────────────────────
df.to_csv(PROC / "all_sentences_with_sid.csv", index=False)
print(f"\n  Saved all_sentences with sid -> {PROC / 'all_sentences_with_sid.csv'}")

# ── Split framing vs non-framing ───────────────────────────────────────────
framing_df     = df[df["is_framing"] == True].copy()
non_framing_df = df[df["is_framing"] == False].copy()
print(f"\n  Framing sentences:     {len(framing_df)}")
print(f"  Non-framing sentences: {len(non_framing_df)}")

# ── What's in non-framing sentences? ──────────────────────────────────────
# Extract the 1-2 words immediately after "thalamus"
thal_re = re.compile(r"\bthalamus\b\s+(\w+)(?:\s+(\w+))?", re.IGNORECASE)

next_word_counter  = Counter()
next_2word_counter = Counter()

for sent in non_framing_df["sentence"].dropna():
    for m in thal_re.finditer(sent):
        w1 = m.group(1).lower()
        w2 = m.group(2).lower() if m.group(2) else ""
        next_word_counter[w1] += 1
        if w2:
            next_2word_counter[f"{w1} {w2}"] += 1

print("\n=== Top words immediately after 'thalamus' in NON-FRAMING sentences ===")
for word, cnt in next_word_counter.most_common(40):
    print(f"  {word:<25} {cnt}")

print("\n=== Top 2-word phrases after 'thalamus' in NON-FRAMING sentences ===")
for phrase, cnt in next_2word_counter.most_common(30):
    print(f"  {phrase:<35} {cnt}")

# ── Sentences that look like framings but weren't caught ──────────────────
# Patterns that express conceptual framing but aren't in our triggers
MISSED_PATTERNS = [
    (r"thalamus.*function(s)?\s+as",            "functions as"),
    (r"thalamus.*act(s)?\s+as",                 "acts as"),
    (r"thalamus.*known\s+to\s+be",              "known to be"),
    (r"thalamus.*thought\s+to\s+(be|play|serve)","thought to"),
    (r"thalamus.*consider(ed)?\s+(a|an|to\s+be)","considered"),
    (r"thalamus.*regarded\s+(as|to\s+be)",      "regarded as"),
    (r"thalamus.*described\s+as",               "described as"),
    (r"thalamus.*defined\s+as",                 "defined as"),
    (r"thalamus.*role\s+in",                    "role in"),
    (r"thalamus.*function\s+in",                "function in"),
    (r"thalamus.*critical\s+for",               "critical for"),
    (r"thalamus.*important\s+for",              "important for"),
    (r"thalamus.*essential\s+for",              "essential for"),
    (r"thalamus.*necessary\s+for",              "necessary for"),
    (r"thalamus.*required\s+for",               "required for"),
    (r"thalamus.*responsible\s+for",            "responsible for"),
    (r"thalamus.*capable\s+of",                 "capable of"),
    (r"thalamus.*key\s+(structure|region|area|node|hub|target)",  "key structure/region"),
    (r"thalamus.*central\s+(structure|region|role|hub|node)",     "central structure/role"),
]

print("\n=== Potentially missed framing patterns in non-framing sentences ===")
missed_counts = {}
for pattern, label in MISSED_PATTERNS:
    re_compiled = re.compile(pattern, re.IGNORECASE)
    matches = non_framing_df["sentence"].str.contains(re_compiled, na=False)
    missed_counts[label] = matches.sum()
    print(f"  {label:<35} {matches.sum()}")

# Show examples of the most common missed patterns
print("\n=== Examples of most-missed pattern: 'role in' ===")
role_in = non_framing_df[non_framing_df["sentence"].str.contains(
    r"thalamus.*role\s+in", flags=re.IGNORECASE, na=False, regex=True)]
for _, row in role_in.sample(min(8, len(role_in)), random_state=42).iterrows():
    print(f"  [{int(row['year']) if pd.notna(row['year']) else '?'}] {str(row['sentence'])[:200]}")

print("\n=== Examples of 'critical/important for' ===")
crit = non_framing_df[non_framing_df["sentence"].str.contains(
    r"thalamus.*(critical|important|essential)\s+for", flags=re.IGNORECASE, na=False, regex=True)]
for _, row in crit.sample(min(8, len(crit)), random_state=42).iterrows():
    print(f"  [{int(row['year']) if pd.notna(row['year']) else '?'}] {str(row['sentence'])[:200]}")

print("\n=== Examples of 'thought/known/considered to be' ===")
thought = non_framing_df[non_framing_df["sentence"].str.contains(
    r"thalamus.*(thought|known|considered|regarded)\s+(as|to\s+be)",
    flags=re.IGNORECASE, na=False, regex=True)]
for _, row in thought.sample(min(8, len(thought)), random_state=42).iterrows():
    print(f"  [{int(row['year']) if pd.notna(row['year']) else '?'}] {str(row['sentence'])[:200]}")

# ── Flag and save potentially missed framing sentences ─────────────────────
any_missed = re.compile(
    "|".join(p for p, _ in MISSED_PATTERNS), re.IGNORECASE
)
non_framing_df = non_framing_df.copy()
non_framing_df["potential_framing"] = non_framing_df["sentence"].str.contains(
    any_missed, na=False
)
n_potential = non_framing_df["potential_framing"].sum()
print(f"\n=== Potentially missed framing sentences: {n_potential} ===")
print("  (These are non-framing sentences matching missed-pattern heuristics)")
print(f"  This is {n_potential/len(non_framing_df)*100:.1f}% of non-framing sentences")

# Save flagged non-framing sentences
non_framing_df.to_csv(PROC / "non_framing_sentences_flagged.csv", index=False)
print(f"  Saved -> {PROC / 'non_framing_sentences_flagged.csv'}")

# ── Topic / verb analysis on framing sentences by decade ──────────────────
print("\n=== Framing sentence trigger breakdown by decade ===")
framing_df["decade"] = (framing_df["year"] // 10 * 10).astype("Int64")
pivot = framing_df.groupby(["decade", "verb_trigger"]).size().unstack(fill_value=0)
top_triggers = framing_df["verb_trigger"].value_counts().head(8).index.tolist()
top_triggers = [t for t in top_triggers if pd.notna(t)]
available = [t for t in top_triggers if t in pivot.columns]
if available:
    print((pivot[available].div(pivot.sum(axis=1), axis=0) * 100).round(1).to_string())

print("\n=== Done ===")
