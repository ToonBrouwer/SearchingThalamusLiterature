import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pandas as pd

from pathlib import Path
DATA = (Path(__file__).resolve().parents[1] / "Data").as_posix()

# ---- Framing sentences ----
df = pd.read_csv(f"{DATA}/interim/sentence_extraction/thalamus_framing_sentences.csv")
print(f"Framing sentences shape: {df.shape}")
print(f"Year range: {df['year'].min()} - {df['year'].max()}")
print()

print("=== verb_trigger counts ===")
print(df['verb_trigger'].value_counts().head(20))
print()

print("=== Appositive phrases (sample 20) ===")
apps = df[df['appositive_phrase'].notna()][['year','appositive_phrase']].dropna()
print(f"Total with appositives: {len(apps)}")
for _, row in apps.sample(min(20, len(apps)), random_state=42).iterrows():
    print(f"  [{int(row['year'])}] {row['appositive_phrase']}")
print()

print("=== Sample framing sentences ===")
for _, row in df.sample(20, random_state=7).iterrows():
    yr = int(row['year']) if pd.notna(row['year']) else '?'
    s = str(row['sentence'])[:250]
    print(f"[{yr}] {s}")
    if pd.notna(row.get('verb_trigger')):
        print(f"  trigger: {row['verb_trigger']}")
    if pd.notna(row.get('appositive_phrase')):
        print(f"  appositive: {row['appositive_phrase']}")
    print()

# ---- Phrases ----
print("=== Top N-grams (next 3 words after 'thalamus') ===")
ph = pd.read_csv(f"{DATA}/interim/phrase_analysis/thalamus_next_3_words.csv")
print(ph.head(30).to_string())

print()
print("=== Top N-grams (next 5 words after 'thalamus') ===")
ph5 = pd.read_csv(f"{DATA}/interim/phrase_analysis/thalamus_next_5_words.csv")
print(ph5.head(30).to_string())

# ---- Per-decade counts ----
print()
print("=== Framing sentences per decade ===")
df['decade'] = (df['year'] // 10 * 10).astype('Int64')
print(df.groupby('decade').size())

# ---- verb trigger over time ----
print()
print("=== Top verb triggers per decade ===")
top_triggers = ['thalamus is', 'thalamus has', 'thalamus may', 'thalamus plays', 'thalamus could', 'thalamus serves']
sub = df[df['verb_trigger'].isin(top_triggers)]
pivot = sub.groupby(['decade','verb_trigger']).size().unstack(fill_value=0)
print(pivot)
