"""
merge_validate_results.py — merge the per-batch Tier-1 classification
outputs, validate them against the schema, join sample metadata, and
produce analysis-ready tables.

Inputs : Data/processed/v18/llm_full/results/batch_*.jsonl
         Data/processed/v18/llm_full/manifest.csv
Outputs: Data/processed/v18/llm_full/paper_framing_results.jsonl
         Data/processed/v18/llm_full/frames_flat.csv   (one row per frame)
Prints a validation + coverage report and first-pass statistics.
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "Data" / "processed" / "v18" / "llm_full"

VALID = {"centrality": {"central", "peripheral"},
         "agency": {"active", "passive", "mixed"},
         "explicitness": {"explicit", "implicit"},
         "stance": {"endorses", "assumes", "challenges"},
         "confidence": {"high", "medium", "low"}}

records, errors = {}, []
for f in sorted((BASE / "results").glob("batch_*.jsonl")):
    for ln, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            errors.append(f"{f.name}:{ln} JSON error: {e}")
            continue
        problems = []
        if not isinstance(rec.get("pmid"), int):
            problems.append("pmid")
        if rec.get("confidence") not in VALID["confidence"]:
            problems.append("confidence")
        for fr in rec.get("frames", []):
            for k in ["centrality", "agency", "explicitness", "stance"]:
                if fr.get(k) not in VALID[k]:
                    problems.append(f"frame.{k}={fr.get(k)}")
            if not fr.get("frame_name") or not fr.get("evidence"):
                problems.append("frame incomplete")
        if problems:
            errors.append(f"{f.name}:{ln} pmid={rec.get('pmid')}: "
                          + ", ".join(problems[:4]))
            continue
        records[rec["pmid"]] = rec  # last write wins on duplicates

man = pd.read_csv(BASE / "manifest.csv").set_index("pmid")
missing = [p for p in man.index if p not in records]
extra = [p for p in records if p not in man.index]
print(f"Valid papers: {len(records):,} / {len(man):,} in manifest")
print(f"Invalid/skipped lines: {len(errors)}")
for e in errors[:12]:
    print("  !", e)
if missing:
    print(f"Missing pmids ({len(missing)}): {missing[:20]}")
if extra:
    print(f"Unexpected pmids ({len(extra)}): {extra[:10]}")

# ── write merged JSONL (manifest metadata attached) ──────────────────────
with open(BASE / "paper_framing_results.jsonl", "w",
          encoding="utf-8") as out:
    for pmid, rec in sorted(records.items()):
        if pmid in man.index:
            m = man.loc[pmid]
            rec["kind"] = m.kind
            rec["domain_name"] = m.domain_name
            rec["decade"] = int(m.decade)
        out.write(json.dumps(rec, ensure_ascii=False) + "\n")

# ── flat frames table ────────────────────────────────────────────────────
rows = []
for pmid, rec in records.items():
    meta = man.loc[pmid] if pmid in man.index else None
    for rank, fr in enumerate(rec.get("frames", [])):
        rows.append({
            "pmid": pmid, "year": rec.get("year"),
            "kind": meta.kind if meta is not None else "?",
            "domain_name": (meta.domain_name if meta is not None else "?"),
            "decade": (int(meta.decade) if meta is not None else None),
            "rank": rank, "frame_name": fr["frame_name"],
            "centrality": fr["centrality"], "agency": fr["agency"],
            "explicitness": fr["explicitness"], "stance": fr["stance"],
            "confidence": rec["confidence"],
            "mismatch": "MISMATCH" in str(rec.get("notes", "")).upper(),
        })
flat = pd.DataFrame(rows)
flat.to_csv(BASE / "frames_flat.csv", index=False)

print(f"\nFrames total: {len(flat):,} "
      f"({len(flat) / max(len(records), 1):.1f} per paper)")
print(f"no_framing papers: "
      f"{sum(1 for r in records.values() if r.get('no_framing'))}")
print(f"mismatch-flagged papers: {int(flat.groupby('pmid').mismatch.any().sum())}")

print("\nTop 25 frame names:")
print(flat.frame_name.value_counts().head(25).to_string())
print("\nAgency x decade (central frames only, %):")
cen = flat[(flat.centrality == "central") & flat.decade.notna()]
tab = (pd.crosstab(cen.decade, cen.agency, normalize="index") * 100).round(0)
print(tab.to_string())
print("\nStance counts:")
print(flat.stance.value_counts().to_string())
print("\nExplicitness:")
print(flat.explicitness.value_counts().to_string())
