"""
relay_sentence_reliability.py - draw a cross-batch re-coding sample to
estimate LLM coder consistency (independent coder instances, same codebook).

145 sentences are sampled at random from all 12 batches (about 12 per batch)
and shuffled, so the re-coder sees a mix that no first-pass coder saw. Ids
are unchanged; they carry no information about the paper.

Output: Data/processed/v18/llm_relay/reliability/batch_rel.jsonl
        (codes go to reliability/results_rel.jsonl; compared in
        relay_sentence_merge.py)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "Code" / "paper"))
import v18_data as D

BASE = D.V18 / "llm_relay"
OUT = BASE / "reliability"
OUT.mkdir(exist_ok=True)

key = pd.read_csv(BASE / "key.csv")
sample = key.sample(n=145, random_state=20260912)
with open(OUT / "batch_rel.jsonl", "w", encoding="utf-8") as fh:
    for i, s in zip(sample.id, sample.sentence):
        fh.write(json.dumps({"id": i, "sentence": s}, ensure_ascii=False)
                 + "\n")
print("reliability sample: %d sentences from %d first-pass batches"
      % (len(sample), sample.id.str[1:].astype(int).floordiv(145).nunique()))
