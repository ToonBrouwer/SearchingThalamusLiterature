"""
Print cluster contents for manual labelling.

Reads the HDBSCAN clusters produced by embed_and_cluster.py and prints a
formatted summary for every cluster so that a human analyst can assign
descriptive labels.  For each cluster the script shows:
  - cluster ID, size, and year range
  - top keyword-taxonomy labels (from framing_primary)
  - 5 representative sentences (closest to the cluster centroid)

Output is to stdout only — pipe to a file or copy-paste into a notes document
to record label assignments, then enter them into apply_cluster_labels.py.

Inputs (Data/processed/):
    cluster_examples.csv             — from embed_and_cluster.py
    framing_embedded_clustered.csv   — from embed_and_cluster.py

No files are written.
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT = _ANALYSIS_DIR.parents[1]
PROC = ROOT / "Data" / "processed"

# ── Load data ─────────────────────────────────────────────────────────────────
ex = pd.read_csv(PROC / "cluster_examples.csv")
main = pd.read_csv(PROC / "framing_embedded_clustered.csv")

# ── Build keyword and year summaries per cluster ───────────────────────────────
kw_dist = (main.groupby(["cluster_id", "framing_primary"])
               .size()
               .unstack(fill_value=0))

# Year range per cluster
yr = main.groupby("cluster_id")["year"].agg(["min","max"])

# ── Print one block per cluster ───────────────────────────────────────────────
for cid in sorted(ex["cluster_id"].unique()):
    sub = ex[ex["cluster_id"] == cid].head(5)
    size = sub["cluster_size"].iloc[0]
    label = "NOISE" if cid == -1 else f"Cluster {cid}"

    # keyword breakdown
    if cid in kw_dist.index:
        row = kw_dist.loc[cid]
        top_kw = row[row > 0].sort_values(ascending=False).head(4)
        kw_str = ", ".join(f"{k}:{v}" for k, v in top_kw.items())
    else:
        kw_str = ""

    ymin = int(yr.loc[cid, "min"]) if cid in yr.index and pd.notna(yr.loc[cid, "min"]) else "?"
    ymax = int(yr.loc[cid, "max"]) if cid in yr.index and pd.notna(yr.loc[cid, "max"]) else "?"

    print(f"\n{'='*80}")
    print(f"  {label}  (n={size}, years={ymin}-{ymax})")
    print(f"  Keywords: {kw_str}")
    print(f"{'='*80}")
    for _, row in sub.iterrows():
        yr_s = int(row["year"]) if pd.notna(row["year"]) else "?"
        sent = str(row["sentence"])
        # Trim to ~300 chars
        sent = sent[:300] + ("..." if len(sent) > 300 else "")
        print(f"\n  [{yr_s}] {sent}")
    print(f"\n  >>> LABEL: _______________________________________________")
