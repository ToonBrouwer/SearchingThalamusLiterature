"""
relay_sentence_setup.py - prepare BLIND sentence-level stance coding of the
regex 'relay' sentences (pipeline/32), plus a human gold-standard sheet.

Input : Data/processed/v18/relay_stance_classified.csv
Output: Data/processed/v18/llm_relay/
          key.csv                    id -> pmid, year, section, regex_stance,
                                     stacked_with, sentence
          batches/batch_XX.jsonl     {"id", "sentence"} ONLY - no year,
                                     authors, journal, section or regex label
          results/                   coders write batch_XX.jsonl here
          gold_standard_to_code.csv  100 sentences for the user to code blind
Order is shuffled so every batch mixes eras, genres and regex categories.
"""
import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
V18 = ROOT / "Data" / "processed" / "v18"
OUT = V18 / "llm_relay"
(OUT / "batches").mkdir(parents=True, exist_ok=True)
(OUT / "results").mkdir(exist_ok=True)

BATCH = 145
GOLD_QUOTA = {"technical": 20, "functional": 25, "characterization": 25,
              "challenge": 12, "historical": 10, "junk": 8}

rs = pd.read_csv(V18 / "relay_stance_classified.csv")
rs = rs.sample(frac=1, random_state=20260911).reset_index(drop=True)
rs["id"] = ["r%04d" % i for i in range(1, len(rs) + 1)]
rs["sentence_clean"] = rs.sentence.fillna("").map(
    lambda t: re.sub(r"\s+", " ", t).strip())

rs[["id", "pmid", "year", "section", "relay_stance", "stacked_with",
    "sentence_clean"]].rename(columns={"relay_stance": "regex_stance",
                                       "sentence_clean": "sentence"}).to_csv(
    OUT / "key.csv", index=False)

n_batches = 0
for b, start in enumerate(range(0, len(rs), BATCH)):
    chunk = rs.iloc[start:start + BATCH]
    with open(OUT / "batches" / ("batch_%02d.jsonl" % b), "w",
              encoding="utf-8") as fh:
        for r in chunk.itertuples():
            fh.write(json.dumps({"id": r.id, "sentence": r.sentence_clean},
                                ensure_ascii=False) + "\n")
    n_batches += 1

parts = []
for k, q in GOLD_QUOTA.items():
    pool = rs[rs.relay_stance == k]
    parts.append(pool.sample(min(q, len(pool)), random_state=7))
gold = pd.concat(parts).sample(frac=1, random_state=8)
sheet = gold[["id", "sentence_clean"]].rename(
    columns={"sentence_clean": "sentence"})
sheet["your_label"] = ""
sheet["your_scope"] = ""
sheet["notes"] = ""
sheet.to_csv(OUT / "gold_standard_to_code.csv", index=False,
             encoding="utf-8-sig")

print("sentences: %d | batches: %d (up to %d each)"
      % (len(rs), n_batches, BATCH))
print("gold standard: %d sentences, stratified by regex category %s"
      % (len(gold), gold.relay_stance.value_counts().to_dict()))
