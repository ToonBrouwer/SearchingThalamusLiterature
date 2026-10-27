"""
relay_paper_level_genre.py - the paper-level (Tier-1) LLM reading of relay
and hub frames by research genre and era.

Complements the sentence-level coding (relay_sentence_merge.py) with
whole-paper frames carrying centrality (central/peripheral) and stance
(assumes/endorses/challenges). Mismatch-flagged papers are excluded.
Influential papers are over-sampled, so the stratified stratum is the
representative view; cells are small (roughly 10-40 papers).

Inputs : Data/processed/v18/llm_full/{frames_flat.csv, manifest.csv,
         paper_framing_results.jsonl}
Output : printed tables
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                              errors="replace")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "Code"))
sys.path.insert(0, str(ROOT / "Code" / "paper"))
import v18_data as D

B = D.V18 / "llm_full"
flat = pd.read_csv(B / "frames_flat.csv")
man = pd.read_csv(B / "manifest.csv")
with open(B / "paper_framing_results.jsonl", encoding="utf-8") as fh:
    recs = [json.loads(line) for line in fh if line.strip()]
mism = {r["pmid"] for r in recs
        if "MISMATCH" in str(r.get("notes", "")).upper()}
man = man[~man.pmid.isin(mism)].copy()
# domain id where assigned, otherwise recovered from the metadata fallback name
name_to_id = {v: k for k, v in D.DOMAIN_NAMES.items()}
man["genre"] = (man.domain.fillna(man.domain_name.map(name_to_id))
                .map(D.genre_of_domain))
man["era"] = np.where(man.year < 2000, "1_pre-2000",
                      np.where(man.year < 2010, "2_2000s", "3_2010+"))
flat = (flat[flat.pmid.isin(man.pmid)]
        .drop(columns=["domain_name", "decade", "kind"], errors="ignore")
        .merge(man[["pmid", "genre", "era", "kind"]], on="pmid"))

print("PAPER-LEVEL LLM VIEW BY GENRE (mismatch-flagged papers excluded: %d)"
      % len(mism))
print("\nSample composition (papers): kind x genre")
print(pd.crosstab(man.kind, man.genre, margins=True).to_string())


def table(m, f, title):
    print("\n--- " + title + " ---")
    rows = []
    for (g, e), mm in m.groupby(["genre", "era"]):
        ff = f[f.pmid.isin(mm.pmid)]
        rel = ff[ff.frame_family == "relay_transmission"]
        hub = ff[ff.frame_family == "hub_integrator"]
        n = len(mm)
        rows.append({
            "genre": g, "era": e[2:], "papers": n,
            "relay_any%": round(100 * rel.pmid.nunique() / n),
            "relay_central%": round(
                100 * rel[rel.centrality == "central"].pmid.nunique() / n),
            "relay A/E/C": "%d/%d/%d" % ((rel.stance == "assumes").sum(),
                                         (rel.stance == "endorses").sum(),
                                         (rel.stance == "challenges").sum()),
            "relay_periph": int((rel.centrality == "peripheral").sum()),
            "hub_any%": round(100 * hub.pmid.nunique() / n),
            "hub_central%": round(
                100 * hub[hub.centrality == "central"].pmid.nunique() / n),
        })
    print(pd.DataFrame(rows).to_string(index=False))


table(man, flat, "ALL sampled papers (relay A/E/C = assumes/endorses/"
      "challenges frame counts)")
table(man[man.kind == "stratified"], flat,
      "STRATIFIED sample only (the representative stratum)")

rel = flat[flat.frame_family == "relay_transmission"]
print("\n--- relay frames pooled over eras, by genre and kind ---")
print(pd.crosstab([rel.genre, rel.kind], rel.stance).to_string())
print(pd.crosstab(rel.genre, rel.centrality).to_string())
