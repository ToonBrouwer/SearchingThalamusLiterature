"""
frame_schema.py — Pydantic models matching the JSON schema from the
frame-classification prompt. Used both for `messages.parse()` validation
and for downstream serialisation.

Mirror of the schema documented in the prompt. Field order, names, and
types are deliberately identical so the model produces output we can
parse into these classes without translation.
"""
from __future__ import annotations
from typing import List, Literal
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────────────────
# Inner records
# ─────────────────────────────────────────────────────────────────────────
class LinguisticMetaphor(BaseModel):
    span: str = Field(description="Exact quoted phrase from the passage")
    source_domain: str
    target_domain: str
    textual_support: Literal["multiple", "single"]
    lexicalization: Literal["live", "conventional"]
    technicalized: bool


class ConceptualMetaphor(BaseModel):
    name: str = Field(description='"TARGET IS SOURCE" form')
    supporting_spans: List[str]
    independence_check: str = Field(
        description="Brief note explaining why the spans count as "
                     "independent expressions"
    )


class Frame(BaseModel):
    name: str
    cue_words: List[str]
    roles_and_entailments: str


class ExplicitAnalogy(BaseModel):
    span: str
    proposed_analogy: str
    author_stance: Literal["proposing", "endorsing", "critiquing"]


# ─────────────────────────────────────────────────────────────────────────
# Top-level output for one passage
# ─────────────────────────────────────────────────────────────────────────
class FrameClassification(BaseModel):
    """One LLM call's output. Matches the prompt's JSON schema 1-to-1."""
    linguistic_metaphors: List[LinguisticMetaphor]
    conceptual_metaphors: List[ConceptualMetaphor]
    frames: List[Frame]
    explicit_analogies: List[ExplicitAnalogy]
    notes: str


# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────
def empty() -> FrameClassification:
    """A sentinel empty record for chunks that fail validation."""
    return FrameClassification(
        linguistic_metaphors=[],
        conceptual_metaphors=[],
        frames=[],
        explicit_analogies=[],
        notes="",
    )
