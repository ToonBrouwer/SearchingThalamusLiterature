"""
llm_client.py — Thin wrapper around `anthropic.Anthropic` for the
frame-classification pipeline.

What it provides
----------------
* `make_client()` — returns an `anthropic.Anthropic` with the API key
  loaded from `.env` and a sensible default timeout. Raises a clear
  error if the key is missing.
* `classify_chunk(client, chunk)` — runs one synchronous call against
  Sonnet 4.6 with prompt caching, adaptive thinking, and structured
  output validated against the `FrameClassification` Pydantic model.
* `build_batch_request(chunk, custom_id)` — builds a batch-API request
  dict for the same payload, used by the full-run orchestrator.
* `estimate_cost(usage)` — converts the API's usage object into a USD
  estimate using current Sonnet 4.6 pricing.

Caching strategy
----------------
The full instruction text lives in the `system` parameter; we mark a
single cache breakpoint on the last system block so tools+system are
cached together. The variable per-call content (`field`, `section`,
`position`, `passage`) sits in the `user` message, after the cache
breakpoint, so it never invalidates the prefix.
"""
from __future__ import annotations
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .frame_prompt import SYSTEM_MESSAGE, build_user_message
from .frame_schema import FrameClassification, empty


MODEL_ID = "claude-sonnet-4-6"
# Public Sonnet 4.6 pricing (per million tokens) — used for budget estimates.
PRICE_INPUT_PER_M = 3.00
PRICE_OUTPUT_PER_M = 15.00
PRICE_CACHE_WRITE_5MIN_PER_M = PRICE_INPUT_PER_M * 1.25  # ~$3.75
PRICE_CACHE_READ_PER_M = PRICE_INPUT_PER_M * 0.10        # ~$0.30


# ─────────────────────────────────────────────────────────────────────────
# Client construction
# ─────────────────────────────────────────────────────────────────────────
def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def make_client():
    """Construct an Anthropic client with the API key loaded from .env.

    Uses ``override=True`` because some environments have an empty
    ``ANTHROPIC_API_KEY`` baked into the user-level env vars (e.g., via a
    prior ``setx`` invocation), which would otherwise win silently and
    cause an "auth not provided" error despite a populated .env file.
    """
    import anthropic
    load_dotenv(_project_root() / ".env", override=True)
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not set. Add it to "
            f"{_project_root() / '.env'}"
        )
    return anthropic.Anthropic(api_key=key, timeout=120.0)


# ─────────────────────────────────────────────────────────────────────────
# Single-chunk synchronous call
# ─────────────────────────────────────────────────────────────────────────
def classify_chunk(client, chunk: dict[str, Any], *,
                   field: str = "",
                   max_tokens: int = 4000,
                   effort: str = "medium",
                   thinking: str = "adaptive",
                   ) -> tuple[FrameClassification, dict[str, Any]]:
    """
    Run the frame-classification prompt against one chunk synchronously.

    Returns
    -------
    (parsed, usage) — the Pydantic-validated output and a dict of the
    response's usage / metadata fields.
    """
    user_msg = build_user_message(
        passage=chunk["text"],
        field=field,
        section=chunk.get("section"),
        position=chunk.get("position", ""),
    )

    kwargs: dict[str, Any] = dict(
        model=MODEL_ID,
        max_tokens=max_tokens,
        system=[
            {
                "type": "text",
                "text": SYSTEM_MESSAGE,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_msg}],
        output_format=FrameClassification,
    )
    if thinking == "adaptive":
        kwargs["thinking"] = {"type": "adaptive"}
        kwargs["output_config"] = {"effort": effort}
    elif thinking == "disabled":
        kwargs["thinking"] = {"type": "disabled"}

    resp = client.messages.parse(**kwargs)
    parsed: FrameClassification = (
        resp.parsed_output if resp.parsed_output is not None else empty()
    )
    usage = {
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
        "cache_creation_input_tokens":
            getattr(resp.usage, "cache_creation_input_tokens", 0) or 0,
        "cache_read_input_tokens":
            getattr(resp.usage, "cache_read_input_tokens", 0) or 0,
        "stop_reason": resp.stop_reason,
        "model": resp.model,
        "request_id": getattr(resp, "_request_id", None),
    }
    return parsed, usage


# ─────────────────────────────────────────────────────────────────────────
# Batch-API request builder
# ─────────────────────────────────────────────────────────────────────────
def build_batch_request(chunk: dict[str, Any],
                         custom_id: str,
                         field: str = "",
                         max_tokens: int = 4000,
                         effort: str = "medium",
                         thinking: str = "adaptive") -> dict:
    """
    Returns a dict matching the Batches API request shape (a
    `Request(custom_id=, params=...)` payload).
    """
    user_msg = build_user_message(
        passage=chunk["text"],
        field=field,
        section=chunk.get("section"),
        position=chunk.get("position", ""),
    )
    params: dict[str, Any] = dict(
        model=MODEL_ID,
        max_tokens=max_tokens,
        system=[
            {
                "type": "text",
                "text": SYSTEM_MESSAGE,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_msg}],
    )
    if thinking == "adaptive":
        params["thinking"] = {"type": "adaptive"}
        params["output_config"] = {"effort": effort}
    elif thinking == "disabled":
        params["thinking"] = {"type": "disabled"}
    return {"custom_id": custom_id, "params": params}


# ─────────────────────────────────────────────────────────────────────────
# Cost helpers
# ─────────────────────────────────────────────────────────────────────────
def estimate_cost(usage: dict[str, int]) -> float:
    """Approximate USD cost of one classification call."""
    inp = usage.get("input_tokens", 0)
    out = usage.get("output_tokens", 0)
    cw = usage.get("cache_creation_input_tokens", 0)
    cr = usage.get("cache_read_input_tokens", 0)
    return (
        (inp - cr) * PRICE_INPUT_PER_M
        + cw * PRICE_CACHE_WRITE_5MIN_PER_M
        + cr * PRICE_CACHE_READ_PER_M
        + out * PRICE_OUTPUT_PER_M
    ) / 1_000_000
