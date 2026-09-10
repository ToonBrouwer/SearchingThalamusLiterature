"""
paper_frame_schema.py — Pydantic models for the Tier-1 paper-level
framing classification (see paper_frame_prompt.py).
"""
from __future__ import annotations

from typing import List, Literal

from pydantic import BaseModel, Field


class PaperFrame(BaseModel):
    frame_name: str = Field(description="short lowercase name, freely "
                                         "chosen (open-ended)")
    gloss: str
    centrality: Literal["central", "peripheral"]
    agency: Literal["active", "passive", "mixed"]
    explicitness: Literal["explicit", "implicit"]
    stance: Literal["endorses", "assumes", "challenges"]
    evidence: List[str] = Field(description="1-3 verbatim quotes")


class PaperFraming(BaseModel):
    """One LLM call's output for one paper."""
    frames: List[PaperFrame]
    no_framing: bool
    confidence: Literal["high", "medium", "low"]
    notes: str


def empty() -> PaperFraming:
    return PaperFraming(frames=[], no_framing=True, confidence="low",
                        notes="")
