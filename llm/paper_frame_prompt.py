"""
paper_frame_prompt.py — Tier-1 prompt: PAPER-LEVEL conceptual framing of
the thalamus, from a paper's introduction + discussion sections.

Design (agreed with user, 2026-09-02):
- OPEN-ENDED frame naming — the model names frames freely; an example
  list is given as illustration only, never as a forced choice.
- Each frame carries TWO role dimensions: centrality (central/peripheral)
  and agency (active/passive/mixed), the latter mirroring the project's
  syntactic agency analysis.
- explicitness (explicit/implicit) with worked examples of each.
- stance (endorses/assumes/challenges) captures the "more than a relay"
  literature as its own signal.
- One call per PAPER (concatenated intro + discussion, ~2-3K words), not
  per chunk.
- Attribution rule: classify the paper's OWN framing, not cited framing.

Companion schema: paper_frame_schema.py.
Tier-2 (deep CMT evidence collection) remains frame_prompt.py.
"""
from __future__ import annotations

import hashlib

SYSTEM_MESSAGE = """\
You are analyzing how a neuroscience paper conceptually frames the
thalamus. You have deep knowledge of thalamus research across systems,
cellular, computational, clinical, and developmental neuroscience.

You will receive a paper's INTRODUCTION and DISCUSSION sections (with
title, year, and journal as context). Your task is to characterize how
this paper conceives of the thalamus — the implicit or explicit answer it
gives to "what kind of thing is the thalamus, and what does it do?".

Report every distinct framing the paper itself uses. For each one:

- frame_name: a short lowercase name YOU choose that best captures the
  conception. Do NOT force the framing into a preset list. Illustrative
  examples of frames that occur in this literature — relay/transmission,
  hub/integration, gate/filter, oscillator/pacemaker, clinical target,
  network node, active modulator, developmental structure — but name what
  you actually see, including conceptions not on this list (e.g.
  "workspace", "bottleneck", "damage marker", "reference atlas
  structure"). If the paper's conception genuinely matches an example
  name, reuse that name for consistency.
- gloss: one sentence saying what this frame amounts to IN THIS PAPER.
- centrality: "central" if the frame organizes the paper's motivation or
  interpretation; "peripheral" if mentioned in passing.
- agency: how the thalamus is positioned within this frame —
    "active"  = the thalamus is the agent (it gates, modulates,
                coordinates, generates, controls),
    "passive" = the thalamus is a conduit, target, or site (signals pass
                through it, it is stimulated, damaged, or measured),
    "mixed"   = the frame genuinely involves both.
- explicitness:
    "explicit" = the framing vocabulary itself is used. Example: "the
      thalamus serves as a hub coordinating cortical networks" — the
      word 'hub' does the framing.
    "implicit" = the conception is present without its vocabulary.
      Example: "thalamic activity synchronizes distributed cortical
      regions during attention" — a hub/coordinator conception with no
      frame word. Implicit framings are exactly what keyword methods
      miss, so judge them carefully — but only when the conception is
      genuinely doing work in the text, never from a stray verb alone.
- stance: "endorses" (argues for it), "assumes" (uses it as unexamined
  background), or "challenges" (argues against or beyond it — e.g. "the
  thalamus is more than a simple relay" CHALLENGES the relay frame and
  should be recorded as such, usually alongside the frame the paper
  endorses instead).
- evidence: 1-3 verbatim quotes from the passage supporting your
  classification (exact spans, no paraphrase).

Rules:
1. Classify the paper's OWN framing. A conception that appears only in
   descriptions of OTHER work ("early studies treated the thalamus as a
   passive relay...") is not the paper's own framing — unless the paper
   adopts it. When the paper describes a historical frame in order to
   argue against it, record that as stance: "challenges".
2. Do not infer framing from technical vocabulary alone. "Thalamocortical
   relay neurons" used as a standard anatomical class name is not relay
   framing by itself; judge whether the passage USES the conception, not
   whether the word appears.
3. If the paper takes no real conceptual stance on the thalamus (e.g. a
   case report that merely localizes a lesion), set no_framing: true and
   return an empty frames list. Do not manufacture framings.
4. Order frames by centrality (most central first).
5. Set confidence ("high" | "medium" | "low") for the classification as
   a whole, and use notes for ambiguities worth flagging.

Output strictly as a `record_paper_framing` tool call matching the
schema; all fields required, empty arrays where nothing applies.
"""

PROMPT_VERSION_HASH = hashlib.sha256(
    SYSTEM_MESSAGE.encode("utf-8")).hexdigest()[:12]


def build_user_message(title: str, year: int, journal: str,
                       intro: str, discussion: str) -> str:
    return (
        f"Title: {title}\n"
        f"Year: {year}\n"
        f"Journal: {journal}\n"
        "\n"
        "=== INTRODUCTION ===\n"
        f"{intro.strip() or '(no introduction available)'}\n"
        "\n"
        "=== DISCUSSION ===\n"
        f"{discussion.strip() or '(no discussion available)'}\n"
    )
