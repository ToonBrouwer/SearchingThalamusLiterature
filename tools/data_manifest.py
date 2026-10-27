"""
data_manifest.py - inventory and verify the project files that live OUTSIDE
git (Data/, Graphs/, Paper/, root files), so the project can be moved to
another computer by drive and checked on arrival.

    python Code/tools/data_manifest.py build             # write MANIFEST.csv
    python Code/tools/data_manifest.py summary           # sizes per tier
    python Code/tools/data_manifest.py verify            # check this copy
    python Code/tools/data_manifest.py verify --root E:/LiteratureQueeryThalamusTime
    python Code/tools/data_manifest.py verify --quick    # sizes only, no hashing

MANIFEST.csv sits in the project root: path, tier, bytes, sha256, note.

Tiers
  core      what the v18 work needs: every input of v18_figures.py, the
            relay analysis and the w2v category cache (traced on 2026-10-27
            with an open() audit hook), plus all of Data/processed/v18/
            (LLM runs, Planck, section labels, caches). Enough to rebuild
            every v18 figure and statistic.
  pipeline  everything else under Data/: raw downloads, interim text,
            embeddings, models and pre-v18 outputs. Needed only to re-run
            earlier pipeline stages.
  outputs   Graphs/ and Paper/ (figures, drafts, the private decision log).
  root      loose files in the project root (README, CLAUDE.md, logs).

Never inventoried or copied: Code/ (travels by git), .env (API key),
.claude/ (machine-local settings), Data/interim/chrome_profile_copy/ (a copy
of a Chrome browser profile, which can hold login cookies), caches and
Office lock files.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = "MANIFEST.csv"

EXCLUDE_TOP = {"Code", ".claude", ".git", ".env", MANIFEST}
EXCLUDE_DIRS = {"__pycache__", ".ipynb_checkpoints", "chrome_profile_copy"}
EXCLUDE_FILE = re.compile(r"^~\$|\.pyc$|^Thumbs\.db$|^\.DS_Store$")

# Inputs outside Data/processed/v18 that the v18 code reads (traced).
CORE_FILES = {
    "Data/raw/pubmed/pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    "Data/processed/all_sentences_full_corpus.csv",
    "Data/processed/framing_full_corpus.csv",
    "Data/processed/paper_abstracts.csv",
    "Data/processed/specter2_pmids.npy",
    "Data/processed/syntactic_roles.csv",
    "Data/processed/cooccurrence_temporal.csv",
}
CORE_DIRS = ("Data/processed/v18/", "Data/processed/clustering_stability/")
CORE_PATTERN = re.compile(r"^Data/models/w2v_(19|20)\d0\.model$")

SUPERSEDED = {
    "Data/processed/all_sentences_final.csv": "v17-era corpus, superseded",
    "Data/processed/framing_combined_v3.csv": "v17-era framing, superseded",
}
BACKUP = re.compile(r"\.bak\d*$|_old_backup|_backup\.")


def tier_of(rel: str) -> str:
    if rel in CORE_FILES or rel.startswith(CORE_DIRS) or CORE_PATTERN.match(rel):
        return "core"
    if rel.startswith("Data/"):
        return "pipeline"
    if rel.startswith(("Graphs/", "Paper/")):
        return "outputs"
    return "root"


def note_of(rel: str) -> str:
    if rel in SUPERSEDED:
        return SUPERSEDED[rel]
    if BACKUP.search(rel):
        return "backup copy"
    if rel == "Paper/v18_figure_plan.md":
        return "PRIVATE decision log - never commit or publish"
    return ""


def iter_files(root: Path):
    for top in sorted(root.iterdir()):
        if top.name in EXCLUDE_TOP:
            continue
        paths = [top] if top.is_file() else sorted(top.rglob("*"))
        for p in paths:
            if not p.is_file():
                continue
            rel = p.relative_to(root)
            if EXCLUDE_DIRS & set(rel.parts[:-1]) or EXCLUDE_FILE.search(p.name):
                continue
            yield rel.as_posix(), p


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def build(args):
    rows = []
    files = list(iter_files(ROOT))
    for i, (rel, p) in enumerate(files, 1):
        rows.append({"path": rel, "tier": tier_of(rel),
                     "bytes": p.stat().st_size, "sha256": sha256(p),
                     "note": note_of(rel)})
        if i % 500 == 0:
            print(f"  hashed {i:,} / {len(files):,}", flush=True)
    with open(ROOT / MANIFEST, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["path", "tier", "bytes", "sha256",
                                           "note"])
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {ROOT / MANIFEST} ({len(rows):,} files)")
    summarize(rows)


def load_manifest(root: Path):
    with open(root / MANIFEST, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def summarize(rows):
    size, count = defaultdict(int), defaultdict(int)
    for r in rows:
        size[r["tier"]] += int(r["bytes"])
        count[r["tier"]] += 1
    print(f"\n{'tier':10s} {'files':>8s} {'size':>10s}")
    for t in ("core", "pipeline", "outputs", "root"):
        print(f"{t:10s} {count[t]:8,d} {human(size[t]):>10s}")
    print(f"{'total':10s} {sum(count.values()):8,d} "
          f"{human(sum(size.values())):>10s}")


def summary(args):
    summarize(load_manifest(ROOT))


def verify(args):
    root = Path(args.root).resolve() if args.root else ROOT
    rows = load_manifest(root if (root / MANIFEST).exists() else ROOT)
    tiers = set(args.tiers.split(",")) if args.tiers else None
    missing, wrong_size, wrong_hash, ok = [], [], [], 0
    for i, r in enumerate(rows, 1):
        if tiers and r["tier"] not in tiers:
            continue
        p = root / r["path"]
        if not p.is_file():
            missing.append(r)
        elif p.stat().st_size != int(r["bytes"]):
            wrong_size.append(r)
        elif not args.quick and sha256(p) != r["sha256"]:
            wrong_hash.append(r)
        else:
            ok += 1
        if i % 1000 == 0:
            print(f"  checked {i:,} / {len(rows):,}", flush=True)
    print(f"\nverified {root}: {ok:,} ok, {len(missing):,} missing, "
          f"{len(wrong_size):,} wrong size, {len(wrong_hash):,} wrong checksum"
          + (" (sizes only)" if args.quick else ""))
    for label, bad in [("MISSING", missing), ("WRONG SIZE", wrong_size),
                       ("WRONG CHECKSUM", wrong_hash)]:
        for r in bad[:25]:
            print(f"  {label:14s} [{r['tier']}] {r['path']}")
        if len(bad) > 25:
            print(f"  ... and {len(bad) - 25} more {label.lower()}")
    known = {r["path"] for r in rows}
    extra = [rel for rel, _ in iter_files(root) if rel not in known]
    if extra:
        print(f"\n{len(extra):,} file(s) not in the manifest (new since it "
              f"was built), e.g. {extra[0]}")
    sys.exit(1 if (missing or wrong_size or wrong_hash) else 0)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build", help="hash all files and write MANIFEST.csv")
    sub.add_parser("summary", help="print sizes per tier")
    v = sub.add_parser("verify", help="check a copy against MANIFEST.csv")
    v.add_argument("--root", help="project copy to check (default: this one)")
    v.add_argument("--tiers", help="comma-separated tiers, e.g. core")
    v.add_argument("--quick", action="store_true",
                   help="compare sizes only (no hashing)")
    args = ap.parse_args()
    {"build": build, "summary": summary, "verify": verify}[args.cmd](args)


if __name__ == "__main__":
    main()
