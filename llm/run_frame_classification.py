"""
run_frame_classification.py — Orchestrator for the frame-classification
pipeline. Three modes:

    --mode pilot          run sync on a small sample, print outputs
                          alongside the source passages (default 5 chunks)
    --mode sync           run every queued chunk synchronously, in
                          parallel with a small thread pool
    --mode batch          submit all queued chunks via the Batches API
                          (50% cost), poll until done, write results

Resume is automatic in every mode: chunks whose output JSON already
exists with a matching prompt-version hash are skipped.

Outputs (under `Data/processed/llm_classifications/`):
    per_chunk/{pmid}_{idx:03d}.json   parsed FrameClassification
    raw/{pmid}_{idx:03d}.json         raw API response (audit trail)
    manifest.csv                      one row per chunk (status, tokens,
                                       cost, prompt_hash, model, request_id)

Usage:
    # Build chunks first, then:
    python Code/llm/run_frame_classification.py --mode pilot --limit 5
    python Code/llm/run_frame_classification.py --mode sync
    python Code/llm/run_frame_classification.py --mode batch
"""
from __future__ import annotations
import argparse, json, os, sys, io, time, traceback
os.environ["PYTHONIOENCODING"] = "utf-8"
if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                    errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

# Allow `python Code/llm/run_frame_classification.py` to find the package
_pkg_parent = Path(__file__).resolve().parents[1]
if str(_pkg_parent) not in sys.path:
    sys.path.insert(0, str(_pkg_parent))
from llm.frame_prompt import PROMPT_VERSION_HASH
from llm.frame_schema import FrameClassification
from llm.llm_client import (  # noqa: E402
    MODEL_ID, build_batch_request, classify_chunk, estimate_cost, make_client,
)


ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
DEFAULT_BASE = PROC / "llm_classifications"

# These get re-bound at the top of main() once --base-dir is parsed
LLM_DIR = DEFAULT_BASE
CHUNK_DIR = LLM_DIR / "chunks"
OUT_PARSED = LLM_DIR / "per_chunk"
OUT_RAW = LLM_DIR / "raw"
MANIFEST_CSV = LLM_DIR / "manifest.csv"


def _bind_paths(base: Path):
    """Re-bind module-level paths once the user has chosen a base dir."""
    global LLM_DIR, CHUNK_DIR, OUT_PARSED, OUT_RAW, MANIFEST_CSV
    LLM_DIR = base
    CHUNK_DIR = LLM_DIR / "chunks"
    OUT_PARSED = LLM_DIR / "per_chunk"
    OUT_RAW = LLM_DIR / "raw"
    MANIFEST_CSV = LLM_DIR / "manifest.csv"
    OUT_PARSED.mkdir(parents=True, exist_ok=True)
    OUT_RAW.mkdir(parents=True, exist_ok=True)


_bind_paths(DEFAULT_BASE)


# ─────────────────────────────────────────────────────────────────────────
# Manifest helpers
# ─────────────────────────────────────────────────────────────────────────
def load_chunks_manifest() -> pd.DataFrame:
    p = LLM_DIR / "chunks_manifest.csv"
    if not p.exists():
        raise SystemExit(
            f"Missing {p}. Run `python Code/llm/chunk_papers.py` first.")
    return pd.read_csv(p)


def out_path_parsed(pmid: int, idx: int) -> Path:
    return OUT_PARSED / f"{pmid}_{idx:03d}.json"


def out_path_raw(pmid: int, idx: int) -> Path:
    return OUT_RAW / f"{pmid}_{idx:03d}.json"


def chunk_done(pmid: int, idx: int, prompt_hash: str) -> bool:
    """A chunk is 'done' if its parsed file exists and matches the prompt hash."""
    p = out_path_parsed(pmid, idx)
    if not p.exists():
        return False
    try:
        existing = json.loads(p.read_text(encoding="utf-8"))
        return existing.get("prompt_hash") == prompt_hash
    except Exception:
        return False


def append_manifest(rows: list[dict]):
    if not rows:
        return
    new = pd.DataFrame(rows)
    if MANIFEST_CSV.exists():
        old = pd.read_csv(MANIFEST_CSV)
        # de-dupe: keep newest by (pmid, chunk_idx, prompt_hash)
        full = pd.concat([old, new], ignore_index=True)
        full = full.drop_duplicates(
            subset=["pmid", "chunk_idx", "prompt_hash"], keep="last")
    else:
        full = new
    full.to_csv(MANIFEST_CSV, index=False)


# ─────────────────────────────────────────────────────────────────────────
# Single-chunk runner (sync)
# ─────────────────────────────────────────────────────────────────────────
def run_one_sync(client, row: pd.Series, *, field: str, effort: str,
                  thinking: str) -> dict:
    pmid = int(row["pmid"])
    idx = int(row["chunk_idx"])
    chunk_path = ROOT / row["file_path"]
    chunk = json.loads(chunk_path.read_text(encoding="utf-8"))
    t0 = time.time()
    try:
        parsed, usage = classify_chunk(
            client, chunk, field=field, effort=effort, thinking=thinking)
    except Exception as e:
        return {
            "pmid": pmid, "chunk_idx": idx,
            "status": "error",
            "error": f"{type(e).__name__}: {str(e)[:300]}",
            "elapsed_s": round(time.time() - t0, 2),
            "prompt_hash": PROMPT_VERSION_HASH,
            "model": MODEL_ID,
        }

    # Save parsed output (with metadata)
    out_obj = {
        "pmid": pmid, "chunk_idx": idx,
        "section": chunk.get("section"),
        "position": chunk.get("position", ""),
        "year": chunk.get("year"),
        "journal": chunk.get("journal", ""),
        "title": chunk.get("title", ""),
        "model": MODEL_ID,
        "prompt_hash": PROMPT_VERSION_HASH,
        "request_id": usage.get("request_id"),
        "stop_reason": usage.get("stop_reason"),
        "classification": json.loads(parsed.model_dump_json()),
    }
    out_path_parsed(pmid, idx).write_text(
        json.dumps(out_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    out_path_raw(pmid, idx).write_text(
        json.dumps({"usage": usage}, indent=2), encoding="utf-8")

    return {
        "pmid": pmid, "chunk_idx": idx, "status": "ok",
        "input_tokens": usage["input_tokens"],
        "output_tokens": usage["output_tokens"],
        "cache_creation_input_tokens": usage["cache_creation_input_tokens"],
        "cache_read_input_tokens": usage["cache_read_input_tokens"],
        "estimated_cost_usd": round(estimate_cost(usage), 5),
        "elapsed_s": round(time.time() - t0, 2),
        "prompt_hash": PROMPT_VERSION_HASH,
        "model": MODEL_ID,
        "request_id": usage.get("request_id"),
    }


# ─────────────────────────────────────────────────────────────────────────
# Mode: pilot
# ─────────────────────────────────────────────────────────────────────────
def mode_pilot(args):
    chunks = load_chunks_manifest()
    if args.papers:
        chunks = chunks[chunks["pmid"].isin([int(p) for p in args.papers])]
    if args.limit:
        chunks = chunks.head(args.limit)
    queued = chunks[~chunks.apply(
        lambda r: chunk_done(int(r.pmid), int(r.chunk_idx),
                             PROMPT_VERSION_HASH), axis=1)]
    print(f"Pilot: {len(queued)} chunks (of {len(chunks)} requested)")

    if not len(queued):
        print("Nothing to do — all requested chunks already classified.")
        return

    client = make_client()
    rows = []
    for _, row in queued.iterrows():
        result = run_one_sync(client, row, field=args.field,
                               effort=args.effort, thinking=args.thinking)
        rows.append(result)
        if result["status"] == "ok":
            parsed = json.loads(out_path_parsed(
                int(row.pmid), int(row.chunk_idx)).read_text(encoding="utf-8"))
            cls = parsed["classification"]
            print(f"\n=== {row['pmid']} chunk {row['chunk_idx']+1}/"
                  f"{row['n_chunks']}  ({row.get('section') or 'no-section'}, "
                  f"{row['n_words']} words, {row['year']}) ===")
            print(f"  cost=${result['estimated_cost_usd']:.4f}, "
                  f"tokens in={result['input_tokens']}/"
                  f"out={result['output_tokens']}, "
                  f"cache hit={result['cache_read_input_tokens']}")
            print(f"  linguistic_metaphors: {len(cls['linguistic_metaphors'])}")
            for lm in cls['linguistic_metaphors'][:3]:
                print(f"    - \"{lm['span']}\"  "
                      f"({lm['source_domain']} → {lm['target_domain']}, "
                      f"{lm['lexicalization']}, "
                      f"{'multi' if lm['textual_support']=='multiple' else 'single'})")
            if cls['conceptual_metaphors']:
                print(f"  conceptual_metaphors: "
                      f"{[c['name'] for c in cls['conceptual_metaphors']]}")
            if cls['frames']:
                print(f"  frames: {[f['name'] for f in cls['frames']]}")
            if cls['explicit_analogies']:
                print(f"  explicit_analogies: {len(cls['explicit_analogies'])}")
            if cls['notes']:
                print(f"  notes: {cls['notes'][:200]}")
        else:
            print(f"  ERROR: {result['error']}")

    append_manifest(rows)
    ok = sum(1 for r in rows if r["status"] == "ok")
    err = sum(1 for r in rows if r["status"] == "error")
    total_cost = sum(r.get("estimated_cost_usd", 0)
                     for r in rows if r["status"] == "ok")
    print(f"\nPilot summary: {ok} ok, {err} errored. "
          f"Total cost ≈ ${total_cost:.3f}")


# ─────────────────────────────────────────────────────────────────────────
# Mode: sync (parallel)
# ─────────────────────────────────────────────────────────────────────────
def mode_sync(args):
    chunks = load_chunks_manifest()
    queued = chunks[~chunks.apply(
        lambda r: chunk_done(int(r.pmid), int(r.chunk_idx),
                             PROMPT_VERSION_HASH), axis=1)]
    print(f"Sync run: {len(queued)} chunks queued (of {len(chunks)} total). "
          f"Concurrency = {args.concurrency}")
    if not len(queued):
        print("Nothing to do.")
        return

    client = make_client()
    rows: list[dict] = []
    t0 = time.time()
    flushed_at = 0

    def _process(row):
        return run_one_sync(client, row, field=args.field,
                            effort=args.effort, thinking=args.thinking)

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futs = {pool.submit(_process, row): row for _, row in queued.iterrows()}
        for i, f in enumerate(as_completed(futs)):
            r = f.result()
            rows.append(r)
            if (i + 1) % 10 == 0:
                cost = sum(x.get("estimated_cost_usd", 0) for x in rows)
                eta = (time.time()-t0)/(i+1) * (len(queued)-i-1)
                print(f"  {i+1}/{len(queued)}  "
                      f"cost=${cost:.3f}  eta={eta:.0f}s")
            # Periodic manifest flush so a crash doesn't lose progress
            if len(rows) - flushed_at >= 25:
                append_manifest(rows[flushed_at:])
                flushed_at = len(rows)

    append_manifest(rows[flushed_at:])
    ok = sum(1 for r in rows if r["status"] == "ok")
    err = sum(1 for r in rows if r["status"] == "error")
    cost = sum(r.get("estimated_cost_usd", 0) for r in rows)
    print(f"\nSync run complete: {ok} ok, {err} errored. "
          f"Cost ≈ ${cost:.2f}, elapsed {(time.time()-t0)/60:.1f} min.")


# ─────────────────────────────────────────────────────────────────────────
# Mode: batch (Batches API)
# ─────────────────────────────────────────────────────────────────────────
def mode_batch(args):
    from anthropic.types.message_create_params import \
        MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request

    chunks = load_chunks_manifest()
    queued = chunks[~chunks.apply(
        lambda r: chunk_done(int(r.pmid), int(r.chunk_idx),
                             PROMPT_VERSION_HASH), axis=1)]
    print(f"Batch run: {len(queued)} chunks (of {len(chunks)} total)")
    if not len(queued):
        print("Nothing to do.")
        return

    client = make_client()
    requests = []
    for _, row in queued.iterrows():
        chunk = json.loads((ROOT / row["file_path"]).read_text(encoding="utf-8"))
        req = build_batch_request(
            chunk,
            custom_id=f"{int(row.pmid)}_{int(row.chunk_idx):03d}",
            field=args.field, effort=args.effort, thinking=args.thinking,
        )
        requests.append(Request(
            custom_id=req["custom_id"],
            params=MessageCreateParamsNonStreaming(**req["params"]),
        ))

    print(f"Submitting batch of {len(requests)} requests…")
    batch = client.messages.batches.create(requests=requests)
    print(f"  batch.id = {batch.id}")
    (LLM_DIR / "last_batch_id.txt").write_text(batch.id)

    # Poll until done
    while True:
        status = client.messages.batches.retrieve(batch.id)
        if status.processing_status == "ended":
            break
        c = status.request_counts
        print(f"  status={status.processing_status} "
              f"(processing={c.processing}, succeeded={c.succeeded}, "
              f"errored={c.errored})")
        time.sleep(60)

    # Fetch results
    rows: list[dict] = []
    for result in client.messages.batches.results(batch.id):
        cid = result.custom_id
        pmid_str, idx_str = cid.split("_")
        pmid, idx = int(pmid_str), int(idx_str)
        if result.result.type == "succeeded":
            msg = result.result.message
            # tool-output / structured output: parse text block as JSON
            text_block = next((b.text for b in msg.content if b.type == "text"),
                              None)
            try:
                cls_dict = json.loads(text_block) if text_block else {}
                parsed = FrameClassification.model_validate(cls_dict)
                chunk_path = ROOT / chunks.loc[
                    (chunks.pmid == pmid) & (chunks.chunk_idx == idx),
                    "file_path"].iloc[0]
                chunk = json.loads(chunk_path.read_text(encoding="utf-8"))
                out_obj = {
                    "pmid": pmid, "chunk_idx": idx,
                    "section": chunk.get("section"),
                    "position": chunk.get("position", ""),
                    "year": chunk.get("year"),
                    "journal": chunk.get("journal", ""),
                    "title": chunk.get("title", ""),
                    "model": MODEL_ID,
                    "prompt_hash": PROMPT_VERSION_HASH,
                    "request_id": getattr(msg, "id", None),
                    "stop_reason": msg.stop_reason,
                    "classification": json.loads(parsed.model_dump_json()),
                }
                out_path_parsed(pmid, idx).write_text(
                    json.dumps(out_obj, ensure_ascii=False, indent=2),
                    encoding="utf-8")
                usage = {
                    "input_tokens": msg.usage.input_tokens,
                    "output_tokens": msg.usage.output_tokens,
                    "cache_creation_input_tokens":
                        getattr(msg.usage, "cache_creation_input_tokens", 0) or 0,
                    "cache_read_input_tokens":
                        getattr(msg.usage, "cache_read_input_tokens", 0) or 0,
                }
                # Batch pricing: 50% off — adjust the cost estimate
                cost = estimate_cost(usage) * 0.5
                rows.append({
                    "pmid": pmid, "chunk_idx": idx, "status": "ok",
                    **usage,
                    "estimated_cost_usd": round(cost, 5),
                    "prompt_hash": PROMPT_VERSION_HASH,
                    "model": MODEL_ID,
                })
            except Exception as e:
                rows.append({
                    "pmid": pmid, "chunk_idx": idx,
                    "status": "parse_error",
                    "error": f"{type(e).__name__}: {str(e)[:300]}",
                    "prompt_hash": PROMPT_VERSION_HASH,
                    "model": MODEL_ID,
                })
        else:
            rows.append({
                "pmid": pmid, "chunk_idx": idx,
                "status": result.result.type,
                "prompt_hash": PROMPT_VERSION_HASH,
                "model": MODEL_ID,
            })

    append_manifest(rows)
    ok = sum(1 for r in rows if r["status"] == "ok")
    err = sum(1 for r in rows if r["status"] != "ok")
    cost = sum(r.get("estimated_cost_usd", 0) for r in rows)
    print(f"\nBatch complete: {ok} ok, {err} errored. "
          f"Cost ≈ ${cost:.2f} (50% batch discount applied)")


# ─────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["pilot", "sync", "batch"],
                        required=True)
    parser.add_argument("--corpus", choices=["reviews", "nonreviews"],
                        default="reviews",
                        help="Resolves --base-dir if not set explicitly")
    parser.add_argument("--base-dir", type=Path, default=None,
                        help="Override the LLM-output base directory")
    parser.add_argument("--limit", type=int, default=None,
                        help="(pilot) Max chunks")
    parser.add_argument("--papers", nargs="+", default=None,
                        help="(pilot) Restrict to these PMIDs")
    parser.add_argument("--concurrency", type=int, default=5,
                        help="(sync) Parallel API calls")
    parser.add_argument("--field", type=str, default="",
                        help="Optional `field` value for the prompt")
    parser.add_argument("--effort", type=str, default="medium",
                        choices=["low", "medium", "high"])
    parser.add_argument("--thinking", type=str, default="adaptive",
                        choices=["adaptive", "disabled"])
    args = parser.parse_args()

    # Resolve base dir: explicit --base-dir wins; otherwise reviews uses
    # the legacy shared dir for back-compat, non-reviews gets a subdir.
    if args.base_dir is not None:
        _bind_paths(args.base_dir)
    elif args.corpus == "nonreviews":
        _bind_paths(DEFAULT_BASE / "nonreviews")
    # else: reviews → keep DEFAULT_BASE bound at import time

    if args.mode == "pilot":
        if args.limit is None:
            args.limit = 5
        mode_pilot(args)
    elif args.mode == "sync":
        mode_sync(args)
    elif args.mode == "batch":
        mode_batch(args)


if __name__ == "__main__":
    main()
