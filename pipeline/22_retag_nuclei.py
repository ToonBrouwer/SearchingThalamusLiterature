"""
22_retag_nuclei.py — recompute the has_<nucleus> columns of the canonical
v18 corpus files from the current nucleus_patterns.NUCLEI.

Run after any change to nucleus patterns (2026-09: VIM added). Prints
before/after counts per nucleus so unintended pattern changes are visible,
then overwrites the canonical CSVs in place (framing file gets a .bak).
"""
from __future__ import annotations

import io
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "Code"))

from nucleus_patterns import NUCLEI, tag_nuclei

PROC = ROOT / "Data" / "processed"
TARGETS = ["framing_full_corpus.csv", "all_sentences_full_corpus.csv"]

for name in TARGETS:
    path = PROC / name
    print(f"\n=== {name} ===")
    df = pd.read_csv(path, low_memory=False)
    old = {nid: int(df[f"has_{nid}"].sum()) for nid in NUCLEI
           if f"has_{nid}" in df.columns}
    tag_nuclei(df, text_col="sentence")
    for nid in NUCLEI:
        new = int(df[f"has_{nid}"].sum())
        o = old.get(nid, "—")
        flag = "" if o == new else "   <-- changed"
        print(f"  {nid:10s} old={o!s:>8}  new={new:>8}{flag}")
    if name == "framing_full_corpus.csv":
        shutil.copy(path, path.with_suffix(".csv.bak"))
    df.to_csv(path, index=False)
    print(f"  written: {path.name} ({len(df):,} rows)")
