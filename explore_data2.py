import sys
import io
import re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pandas as pd

from pathlib import Path
DATA = (Path(__file__).resolve().parents[1] / "Data").as_posix()
df = pd.read_csv(f"{DATA}/interim/sentence_extraction/thalamus_framing_sentences.csv")

# ---- Focus on "thalamus is" sentences: what follows "is a/the/an"? ----
is_sents = df[df['verb_trigger'] == 'thalamus is'].copy()
print(f"'thalamus is' sentences: {len(is_sents)}")
print()

# Extract what follows "is a/an/the"
pattern = re.compile(r'thalamus\b[^.]*?\bis\s+(a|an|the)\s+(\w[\w\s,\-]{3,60})', re.IGNORECASE)

extractions = []
for _, row in is_sents.iterrows():
    m = pattern.search(str(row['sentence']))
    if m:
        extractions.append({'year': row['year'], 'article': m.group(1), 'description': m.group(2).strip()})

ext_df = pd.DataFrame(extractions)
print(f"Extracted 'thalamus is a/an/the X' descriptions: {len(ext_df)}")
print()
print("Sample descriptions:")
for _, r in ext_df.sample(min(40, len(ext_df)), random_state=42).iterrows():
    print(f"  [{int(r['year'])}] is {r['article']} {r['description']}")

print()

# ---- "serves as" pattern ----
print("=== 'serves as' phrases (from 3-gram) ===")
ph = pd.read_csv(f"{DATA}/interim/phrase_analysis/thalamus_next_5_words.csv")
serves = ph[ph['phrase'].str.startswith('serves as')]
print(serves.head(20).to_string())

# ---- "plays X role" breakdown ----
print()
print("=== 'plays X role' phrases ===")
plays = ph[ph['phrase'].str.contains('plays.*role', na=False)]
print(plays.head(20).to_string())

# ---- "thalamus is involved in" what? ----
print()
involved = df[df['sentence'].str.contains('thalamus is involved in', case=False, na=False)]
print(f"'thalamus is involved in' sentences: {len(involved)}")
for _, row in involved.sample(min(15, len(involved)), random_state=42).iterrows():
    s = str(row['sentence'])
    m = re.search(r'involved in\s+(.{10,100})', s, re.IGNORECASE)
    if m:
        print(f"  [{int(row['year'])}] involved in: {m.group(1)[:100]}")

# ---- "thalamus is a key/important/critical X" ----
print()
key_sents = df[df['sentence'].str.contains(r'thalamus is (a |an )?(key|important|critical|crucial|essential|central|major|vital)', case=False, na=False, regex=True)]
print(f"'thalamus is [key/important/...] X' sentences: {len(key_sents)}")
for _, row in key_sents.sample(min(20, len(key_sents)), random_state=1).iterrows():
    m = re.search(r'thalamus\s+is\s+(a|an)?\s*(key|important|critical|crucial|essential|central|major|vital)\s+(\w[\w\s,]{3,80})', str(row['sentence']), re.IGNORECASE)
    if m:
        print(f"  [{int(row['year'])}] is {m.group(0)[10:100]}")
