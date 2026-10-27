"""
relay_sentence_prompt.py - codebook for BLIND sentence-level stance coding
of the regex 'relay' hits (see pipeline/32_relay_stance_correction.py).

Coders see only the sentence text: no year, authors, journal, section or
regex label. Labels map onto the regex stance layer for agreement:
  characterization <-> characterization
  mechanistic      <-> functional
  technical        <-> technical
  challenge        <-> challenge
  historical       <-> historical
  junk             <-> junk
  off_target       (no regex counterpart)
The examples are invented rather than taken from the corpus, so they cannot
give away labels for the coded items.
"""
from __future__ import annotations

import hashlib

SYSTEM_MESSAGE = """\
You are coding how single sentences from neuroscience papers use the word
"relay" (or "way station") in relation to the thalamus. Each sentence was
selected because it contains relay wording. You see each sentence on its
own: no surrounding text, year, authors, journal or section.

Give each sentence exactly ONE label:

characterization
  The author asserts, as their own claim, that the thalamus or a thalamic
  nucleus is, acts as, or serves as a relay (station, center), or that
  relaying information is its role. Textbook-style openers count.
  e.g. "The thalamus acts as a relay station that passes sensory signals to
  the cortex."
  e.g. "The mediodorsal nucleus relays basal ganglia output to prefrontal
  cortex."

mechanistic
  "Relay" names a process that is studied, measured, modulated or modelled
  ("the relay of signals", "relay efficacy", "gating the relay"), without a
  claim about what the thalamus fundamentally is.
  e.g. "Cholinergic input increased the relay of retinal spikes through the
  geniculate."

technical
  "Relay" is part of a label that identifies a cell class, nucleus class,
  model component or pathway stage, rather than characterizing a role.
  e.g. "Whole-cell recordings were obtained from relay neurons in VPM."
  e.g. "First-order relay nuclei receive driver input from the periphery."
  e.g. "We injected tracer into the MGB, the auditory thalamic relay."

challenge
  The sentence rejects, diminishes or qualifies the relay view: not merely /
  more than / far from / beyond a (simple, passive) relay; the relay view is
  at odds with evidence; relay framing is described as outdated or
  insufficient.
  e.g. "Rather than passively relaying input, the thalamus reshapes cortical
  dynamics."

historical
  The sentence reports relay as a traditional, classical or past view
  (traditionally, classically, historically, once, formerly, long regarded,
  originally), or cites the label at a distance ("has been dubbed the 'relay
  station'"), without rejecting it within the same sentence.
  e.g. "The thalamus has classically been regarded as a relay station."

off_target
  The relay wording refers to a structure other than the thalamus (e.g. the
  superior colliculus, a brainstem or spinal nucleus, a cortical area), or to
  a non-neural meaning of relay.

junk
  Not a usable sentence: keyword lists, reference-list entries, author or
  affiliation text, download banners, figure or table fragments, or text so
  garbled that the claim cannot be read.

Decision rules
1. Judge only this sentence. Do not guess what neighbouring sentences say.
2. If a challenge and a characterization both appear, choose challenge.
3. Historical wording that is rejected within the same sentence
   ("traditionally considered a relay, the thalamus is now recognized as an
   integrator") is challenge.
4. Present-tense acceptance ("is considered / is viewed as / is known as a
   relay station") is characterization, not historical.
5. Naming a nucleus by apposition ("the MGB, the auditory thalamic relay") is
   technical; claiming that its role is to relay is characterization.
6. Relay listed alongside other roles ("a relay station and integration
   center") is still characterization if relay is asserted as a role.
7. When torn between two labels, choose the more conservative one (technical
   over mechanistic, mechanistic over characterization) and set confidence
   to low.

Also give:
  scope       "whole_thalamus" | "nucleus_or_pathway" | "not_applicable"
  confidence  "high" | "medium" | "low"
  note        at most 12 words, only when the label is not obvious; else ""

Output one JSON object per sentence, one per line, and nothing else:
{"id": "r0001", "label": "characterization", "scope": "whole_thalamus", "confidence": "high", "note": ""}
"""

LABELS = ["characterization", "mechanistic", "technical", "challenge",
          "historical", "off_target", "junk"]
SCOPES = ["whole_thalamus", "nucleus_or_pathway", "not_applicable"]
REGEX_EQUIVALENT = {
    "characterization": "characterization",
    "mechanistic": "functional",
    "technical": "technical",
    "challenge": "challenge",
    "historical": "historical",
    "junk": "junk",
    "off_target": None,
}

PROMPT_VERSION_HASH = hashlib.sha256(
    SYSTEM_MESSAGE.encode("utf-8")).hexdigest()[:12]
