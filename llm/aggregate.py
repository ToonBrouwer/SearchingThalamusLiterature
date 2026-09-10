"""
aggregate.py — Roll up chunk-level frame classifications into:

    Data/processed/llm_classifications/per_paper/{pmid}.json
        deduplicated metaphors and frames at the paper level

    Data/processed/llm_classifications/frame_evidence.csv
        one row per metaphor / conceptual-metaphor / frame /
        explicit-analogy occurrence — the canonical flat table for
        downstream temporal / cluster / topic joins

    Data/processed/llm_classifications/aggregate_summary.csv
        one row per paper: counts of each evidence type + cost summary

Resume-friendly: re-run any time. Reads what's currently in
`per_chunk/` and `manifest.csv`.

Usage:
    python Code/llm/aggregate.py
"""
from __future__ import annotations
import argparse, json, os, sys, io, re
os.environ["PYTHONIOENCODING"] = "utf-8"
if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                    errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
LLM_DIR = ROOT / "Data" / "processed" / "llm_classifications"
PER_CHUNK_DIR = LLM_DIR / "per_chunk"
PER_PAPER_DIR = LLM_DIR / "per_paper"
PER_PAPER_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────
# Load all chunk-level outputs
# ─────────────────────────────────────────────────────────────────────────
def load_chunks() -> list[dict]:
    chunks = []
    for f in sorted(PER_CHUNK_DIR.glob("*.json")):
        try:
            chunks.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception as e:
            print(f"  [skip {f.name}] {e}")
    return chunks


# ─────────────────────────────────────────────────────────────────────────
# Build flat evidence table
# ─────────────────────────────────────────────────────────────────────────
def build_evidence_rows(chunks: list[dict]) -> list[dict]:
    rows = []
    for ch in chunks:
        pmid = ch["pmid"]; idx = ch["chunk_idx"]
        section = ch.get("section")
        position = ch.get("position", "")
        year = ch.get("year")
        journal = ch.get("journal", "")
        title = ch.get("title", "")
        cls = ch.get("classification", {})
        common = dict(pmid=pmid, chunk_idx=idx, section=section,
                       position=position, year=year, journal=journal,
                       title=title)
        for lm in cls.get("linguistic_metaphors", []):
            rows.append(dict(common, kind="linguistic_metaphor",
                              span=lm.get("span", ""),
                              source_domain=lm.get("source_domain", ""),
                              target_domain=lm.get("target_domain", ""),
                              textual_support=lm.get("textual_support", ""),
                              lexicalization=lm.get("lexicalization", ""),
                              technicalized=lm.get("technicalized", False),
                              detail=""))
        for cm in cls.get("conceptual_metaphors", []):
            rows.append(dict(common, kind="conceptual_metaphor",
                              span="", source_domain="",
                              target_domain="",
                              textual_support="", lexicalization="",
                              technicalized=False,
                              name=cm.get("name", ""),
                              detail=" | ".join(cm.get("supporting_spans", []))))
        for fr in cls.get("frames", []):
            rows.append(dict(common, kind="frame",
                              span="", source_domain="", target_domain="",
                              textual_support="", lexicalization="",
                              technicalized=False,
                              name=fr.get("name", ""),
                              detail=" | ".join(fr.get("cue_words", []))))
        for an in cls.get("explicit_analogies", []):
            rows.append(dict(common, kind="explicit_analogy",
                              span=an.get("span", ""),
                              source_domain="", target_domain="",
                              textual_support="", lexicalization="",
                              technicalized=False,
                              proposed_analogy=an.get("proposed_analogy", ""),
                              author_stance=an.get("author_stance", ""),
                              detail=an.get("proposed_analogy", "")))
    return rows


# ─────────────────────────────────────────────────────────────────────────
# Per-paper rollup with deduplication
# ─────────────────────────────────────────────────────────────────────────
def _norm(text: str) -> str:
    """Loose normalisation: lowercase, collapse whitespace, strip punct."""
    t = text.lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[\W_]+$", "", t)
    return t


def per_paper_rollup(chunks: list[dict]) -> dict[int, dict]:
    by_paper: dict[int, list[dict]] = defaultdict(list)
    for ch in chunks:
        by_paper[ch["pmid"]].append(ch)

    out = {}
    for pmid, paper_chunks in by_paper.items():
        meta = paper_chunks[0]
        # Linguistic metaphors deduped by normalised span
        lm_seen: dict[str, dict] = {}
        for ch in paper_chunks:
            for lm in ch["classification"].get("linguistic_metaphors", []):
                key = _norm(lm.get("span", ""))
                if not key:
                    continue
                if key not in lm_seen:
                    lm_seen[key] = {**lm, "occurrences": 1,
                                     "chunks": [ch["chunk_idx"]]}
                else:
                    lm_seen[key]["occurrences"] += 1
                    lm_seen[key]["chunks"].append(ch["chunk_idx"])

        # Conceptual metaphors deduped by normalised NAME
        cm_seen: dict[str, dict] = {}
        for ch in paper_chunks:
            for cm in ch["classification"].get("conceptual_metaphors", []):
                key = _norm(cm.get("name", ""))
                if not key:
                    continue
                if key not in cm_seen:
                    cm_seen[key] = {**cm, "occurrences": 1,
                                     "chunks": [ch["chunk_idx"]]}
                else:
                    cm_seen[key]["occurrences"] += 1
                    cm_seen[key]["chunks"].append(ch["chunk_idx"])
                    cm_seen[key]["supporting_spans"] = list(set(
                        cm_seen[key]["supporting_spans"]
                        + cm.get("supporting_spans", [])))

        # Frames deduped by normalised name
        fr_seen: dict[str, dict] = {}
        for ch in paper_chunks:
            for fr in ch["classification"].get("frames", []):
                key = _norm(fr.get("name", ""))
                if not key:
                    continue
                if key not in fr_seen:
                    fr_seen[key] = {**fr, "occurrences": 1,
                                     "chunks": [ch["chunk_idx"]]}
                else:
                    fr_seen[key]["occurrences"] += 1
                    fr_seen[key]["chunks"].append(ch["chunk_idx"])
                    fr_seen[key]["cue_words"] = list(set(
                        fr_seen[key]["cue_words"] + fr.get("cue_words", [])))

        # Explicit analogies are kept verbatim — they're rare enough that
        # collisions are unlikely and nuance matters
        analogies = []
        for ch in paper_chunks:
            for an in ch["classification"].get("explicit_analogies", []):
                analogies.append({**an, "chunk_idx": ch["chunk_idx"]})

        out[pmid] = {
            "pmid": pmid,
            "year": meta.get("year"),
            "journal": meta.get("journal", ""),
            "title": meta.get("title", ""),
            "n_chunks": len(paper_chunks),
            "linguistic_metaphors": list(lm_seen.values()),
            "conceptual_metaphors": list(cm_seen.values()),
            "frames": list(fr_seen.values()),
            "explicit_analogies": analogies,
            "notes": [ch["classification"].get("notes", "")
                      for ch in paper_chunks],
        }
    return out


# ─────────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-evidence-csv", action="store_true",
                        help="Skip writing the flat frame_evidence.csv")
    parser.add_argument("--no-per-paper", action="store_true",
                        help="Skip writing per-paper JSON rollups")
    args = parser.parse_args()

    chunks = load_chunks()
    print(f"Loaded {len(chunks)} chunk classifications")
    if not chunks:
        return

    rollup = per_paper_rollup(chunks)
    if not args.no_per_paper:
        for pmid, obj in rollup.items():
            (PER_PAPER_DIR / f"{pmid}.json").write_text(
                json.dumps(obj, ensure_ascii=False, indent=2),
                encoding="utf-8")
        print(f"  Wrote {len(rollup)} per-paper rollups to "
              f"{PER_PAPER_DIR.relative_to(ROOT)}")

    if not args.no_evidence_csv:
        rows = build_evidence_rows(chunks)
        df = pd.DataFrame(rows)
        out = LLM_DIR / "frame_evidence.csv"
        df.to_csv(out, index=False)
        print(f"  Wrote {len(df):,} evidence rows to "
              f"{out.relative_to(ROOT)}")

    # Per-paper summary table
    summary_rows = []
    for pmid, obj in rollup.items():
        summary_rows.append({
            "pmid": pmid, "year": obj["year"],
            "journal": obj["journal"],
            "title": obj["title"][:100],
            "n_chunks": obj["n_chunks"],
            "n_linguistic_metaphors": len(obj["linguistic_metaphors"]),
            "n_conceptual_metaphors": len(obj["conceptual_metaphors"]),
            "n_frames": len(obj["frames"]),
            "n_explicit_analogies": len(obj["explicit_analogies"]),
            "n_live_metaphors": sum(
                1 for lm in obj["linguistic_metaphors"]
                if lm.get("lexicalization") == "live"),
            "n_conventional_metaphors": sum(
                1 for lm in obj["linguistic_metaphors"]
                if lm.get("lexicalization") == "conventional"),
        })
    summary = pd.DataFrame(summary_rows).sort_values(["year", "pmid"])
    summary.to_csv(LLM_DIR / "aggregate_summary.csv", index=False)
    print(f"  Wrote aggregate_summary.csv ({len(summary)} papers)")

    # Print top-10 most common conceptual metaphors corpus-wide
    cm_counter = Counter()
    for obj in rollup.values():
        for cm in obj["conceptual_metaphors"]:
            cm_counter[cm["name"]] += cm.get("occurrences", 1)
    print("\nTop-10 conceptual metaphors across the corpus so far:")
    for name, n in cm_counter.most_common(10):
        print(f"  {n:3d}  {name}")

    fr_counter = Counter()
    for obj in rollup.values():
        for fr in obj["frames"]:
            fr_counter[fr["name"]] += fr.get("occurrences", 1)
    print("\nTop-10 frames across the corpus so far:")
    for name, n in fr_counter.most_common(10):
        print(f"  {n:3d}  {name}")


if __name__ == "__main__":
    main()
