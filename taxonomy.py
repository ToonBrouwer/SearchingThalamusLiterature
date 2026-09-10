"""
taxonomy.py -- Single source of truth for the thalamus framing taxonomy.

All framing-detection helpers, category definitions, regex patterns, and
classification functions live here.  Every other module that needs framing
labels should ``from taxonomy import ...`` rather than redefining patterns.

Exported constants
------------------
VERB_TRIGGERS       List of lowercase trigger phrases ("thalamus is", ...)
APPOSITIVE_RE       Compiled regex for appositive detection
BAD_APPOSITIVES     Set of phrases to reject as appositives
TAXONOMY            Dict[str, List[str]]  -- category -> raw pattern strings
COMPILED            Dict[str, List[re.Pattern]] -- pre-compiled version
PRIORITY            List of category names in descending priority
OTHER_SUBTYPES      List of (label, colour, raw_regex) for 'other' decomposition
FRAME_CATS_SIMPLE   List of (label, colour, raw_regex) for simplified classifier

Exported functions
------------------
split_sentences(text)             -> List[str]
match_trigger(sentence)           -> str | None
extract_appositive(sentence)      -> str | None
is_framing(sentence)              -> (bool, trigger_or_None, appositive_or_None)
classify_sentence(sentence)       -> (List[str], str)   -- all matches + primary
classify_primary(sentence)        -> str                -- primary label only
classify_all(sentence)            -> List[str]          -- all matching categories
classify_other_subtype(sentence)  -> str                -- sub-label within 'other'
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Framing-detection primitives
# ═══════════════════════════════════════════════════════════════════════════════

VERB_TRIGGERS: list[str] = [
    "thalamus is", "the thalamus is",
    "thalamus acts", "the thalamus acts",
    "thalamus relays", "the thalamus relays",
    "thalamus plays", "the thalamus plays",
    "thalamus serves", "the thalamus serves",
    "thalamus has", "the thalamus has",
    "thalamus may", "the thalamus may",
    "thalamus could", "the thalamus could",
    "thalamus contributes", "the thalamus contributes",
    "thalamus mediates", "the thalamus mediates",
    "thalamus provides", "the thalamus provides",
    "thalamus is associated with", "the thalamus is associated with",
    "thalamus was associated with", "the thalamus was associated with",
    "thalamus has been identified as", "the thalamus has been identified as",
    "thalamus could modulate", "the thalamus could modulate",
    "thalamus which provides", "the thalamus which provides",
    "thalamus is involved in", "the thalamus is involved in",
    "thalamus plays a", "the thalamus plays a",
    "thalamus serves as", "the thalamus serves as",
]

APPOSITIVE_RE: re.Pattern = re.compile(
    r"\b(the\s+)?thalamus\s*,\s*([^,]{5,200})\s*,", re.IGNORECASE
)

BAD_APPOSITIVES: set[str] = {
    "however", "therefore", "for example", "for instance",
    "in contrast", "in turn",
}

# Pre-compiled sentence splitter (avoid re-compiling each call)
_SENTENCE_SPLIT_RE: re.Pattern = re.compile(r"(?<=[.!?])\s+")
_WHITESPACE_RE: re.Pattern = re.compile(r"\s+")


def split_sentences(text: str) -> list[str]:
    """Split *text* into sentences on .!? boundaries."""
    if not isinstance(text, str) or not text.strip():
        return []
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return _SENTENCE_SPLIT_RE.split(text)


def match_trigger(sentence: str) -> Optional[str]:
    """Return the first matching verb trigger, or *None*."""
    s = sentence.lower()
    for t in VERB_TRIGGERS:
        if t in s:
            return t
    return None


def extract_appositive(sentence: str) -> Optional[str]:
    """Return the appositive phrase following 'thalamus, ...,' or *None*."""
    m = APPOSITIVE_RE.search(sentence)
    if not m:
        return None
    phrase = m.group(2).strip()
    if phrase.lower() in BAD_APPOSITIVES:
        return None
    return phrase


def is_framing(sentence: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """Check whether *sentence* is a framing sentence.

    Returns
    -------
    (is_framing_bool, trigger_or_None, appositive_or_None)
    """
    trigger = match_trigger(sentence)
    appositive = extract_appositive(sentence)
    return (trigger is not None or appositive is not None), trigger, appositive


# Keep the old name available as an alias
is_framing_sentence = is_framing


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Main framing taxonomy  (multi-label, priority-ordered)
# ═══════════════════════════════════════════════════════════════════════════════

TAXONOMY: dict[str, list[str]] = {
    "relay": [
        r"relay station", r"relay center", r"relay hub", r"relay nucleus",
        r"relay for ", r"relay of ", r"relays (sensory|motor|visual|auditory|information|signal)",
        r"way.?station", r"information relay", r"sensory relay", r"relay structure",
        r"primary relay", r"major relay", r"relay between", r"relay.*to the cortex",
        r"relay.*sensory.*cortex", r"relay.*information.*cortex",
        r"(serves|acting|acts) as (a |an )?(relay|sensory relay)",
        r"thalamus relays",
    ],
    "hub_node": [
        r"\bhub\b", r"\bnexus\b",
        r"central (hub|node|relay)", r"integrative hub", r"crucial hub",
        r"key hub", r"circuit hub", r"network hub", r"relay hub",
    ],
    "gateway": [
        r"\bgateway\b", r"\bgatekeep", r"\bgate\b(?! charge)",
        r"\bportal\b", r"gating (of |mechanism|function)",
        r"sensory gating", r"information gating",
        r"filter (sensory|information|neural|of )", r"information filter",
        r"entrance (for|to) (sensory|neural|information)",
    ],
    "modulator": [
        r"modulat(es|ing|ion|or) (cortical|thalamocortical|neural|sensory|brain)",
        r"thalamus.*modulat", r"thalamus.*regulat(es|ing) (activity|flow|state|arousal)",
        r"thalamus.*controls (activity|flow|arousal|state|consciousness)",
        r"gain control", r"top.down modulation",
    ],
    "integrator": [
        r"integrat(es|ing|ive|ion) (of |information|signals|input|sensory|motor)",
        r"integration (center|hub|of|node)", r"integrative center",
        r"converge\w* (of |information|input)", r"convergence (of |center|zone)",
        r"receiving and (distribut|integrat|process)",
    ],
    "network_component": [
        r"(network|circuit) node", r"(node|hub) (in|within|of) (a |the )?(large.scale |brain |neural |cortico)",
        r"large.scale (brain |neural )?network", r"functional network",
        r"thalamocortical (network|circuit|loop)", r"cortico.thalamo",
        r"connectome", r"resting.state network", r"default mode",
    ],
    "role_player": [
        r"plays (a|an) (important|critical|crucial|key|central|major|vital|pivotal|fundamental|significant|essential|active|more active|causal) role",
        r"plays (a|an) role in",
        r"plays distinct roles",
        r"may play (a|an) (important|critical|crucial|key|central|major) role",
    ],
    "clinical_target": [
        r"(target|site) (for|of) (treatment|therapy|intervention|stimulation|surgery|deep brain)",
        r"deep brain stimulation", r"therapeutic target", r"clinical target",
        r"treatment target", r"site of (pathology|damage|injury|lesion|atrophy)",
        r"neuromodulation target",
    ],
    "structure_description": [
        r"(complex|heterogeneous|intricate|complicated) (structure|region|area|organ)",
        r"composed of (multiple|many|\d+) nuclei", r"consisting of.*nuclei",
        r"multiple (thalamic )?nuclei", r"numerous nuclei",
        r"(subcortical|forebrain|diencephalic) structure",
        r"largest (structure|region)", r"paired structure",
        r"gray matter (structure|region|mass)",
    ],
    "involved_in": [
        r"(is |was |are )involved in",
        r"(is |was |are )implicated in",
        r"participates in",
        r"plays a part in",
    ],
}

# Pre-compiled patterns for performance
COMPILED: dict[str, list[re.Pattern]] = {
    cat: [re.compile(p, re.IGNORECASE) for p in patterns]
    for cat, patterns in TAXONOMY.items()
}

# Priority order for choosing the primary label when multiple categories match
PRIORITY: list[str] = [
    "relay", "hub_node", "gateway", "modulator", "integrator",
    "network_component", "clinical_target", "structure_description",
    "role_player", "involved_in",
]


# ═══════════════════════════════════════════════════════════════════════════════
# 3. 'Other' sub-type taxonomy  (from phase_l_other_deep_dive.py)
# ═══════════════════════════════════════════════════════════════════════════════

OTHER_SUBTYPES: list[tuple[str, str, str]] = [
    # (label, colour, regex_pattern)
    (
        "Structural / anatomical description",
        "#7B4F9E",
        r"(composed of|consists of|comprising|subdivided into|made up of|"
        r"heterogeneous|heterogenous|parcellated|parcellation|"
        r"distinct nuclei|specific nuclei|group of nuclei|anatomically|"
        r"thalamic nuclei|nuclear mass|subnucl)",
    ),
    (
        "Important structure (key / critical / essential)",
        "#D55E00",
        r"(is\s+(a\s+)?(key|critical|essential|crucial|vital|important|"
        r"fundamental|central|pivotal|indispensable)\s+(brain\s+)?"
        r"(structure|region|area|node|component|hub|center|part|organ)|"
        r"plays?\s+a\s+(key|critical|essential|crucial|vital|important|"
        r"fundamental|central|pivotal)\s+role|"
        r"thalamus\s+is\s+(critical|essential|crucial|vital|fundamental|"
        r"indispensable|imperative)\s+for)",
    ),
    (
        "Research evidence / implication",
        "#56B4E9",
        r"(has\s+been\s+(shown|demonstrated|found|reported|implicated|"
        r"linked|associated|suggested|proposed|identified|considered|"
        r"documented|noted|recognised|recognized|established|confirmed|"
        r"observed|described|hypothes)|"
        r"thalamus\s+(may|could|might|appears\s+to|seems\s+to|is\s+thought\s+to|"
        r"is\s+believed\s+to|is\s+likely)\s+(be|play|have|contribute|provide|serve))",
    ),
    (
        "Connectivity / circuit membership",
        "#0072B2",
        r"(thalamocortical|corticothalamic|thalamo.cortical|"
        r"connected\s+to|connections\s+(with|to|between)|"
        r"reciprocal\s+connect|axonal\s+project|efferent|afferent|"
        r"cortico.thalamo|thalamo.striatal|thalamo.limbic|"
        r"is\s+(connected|interconnected|linked)\s+(with|to)|"
        r"connectivity\s+(between|with|of))",
    ),
    (
        "Sensory / motor processing",
        "#E69F00",
        r"(sensory|somatosensory|nocicepti|pain\s+(signal|process|transmit)|"
        r"auditory|visual\s+(process|thalamus|cortex)|"
        r"motor\s+(thalamus|control|function|circuit|output)|"
        r"propriocepti|vestibular|relay.+sensory|sensory.+relay)",
    ),
    (
        "Cognitive / memory function",
        "#009E73",
        r"(cognit|working\s+memory|episodic\s+memory|attention|"
        r"executive\s+function|conscious|awareness|"
        r"decision.making|learning|mnemonic|prefrontal|"
        r"hippocampal|hippocampo.thalamic)",
    ),
    (
        "Sleep / arousal / oscillation",
        "#F0E442",
        r"(sleep|wake|arousal|NREM|REM|slow.wave|spindle|"
        r"oscillat|pacemaker|synchroni|EEG|desynchroni|"
        r"thalamo.cortical\s+oscillat|burst\s+(mode|firing)|"
        r"tonic\s+(mode|firing)|up.state|down.state)",
    ),
    (
        "Clinical / disease / pathology",
        "#CC79A7",
        r"(patient|disease|disorder|patholog|lesion|infarct|stroke|"
        r"stimulat|DBS|deep\s+brain\s+stimul|thermocoagulat|ablat|"
        r"treatment|therapeutic|clinical|surgery|seizure|epilep|"
        r"schizophren|depression|Parkinson|tremor|atrophy|"
        r"dementia|Alzheimer|trauma|injury|damage)",
    ),
    (
        "General functional involvement",
        "#aaaaaa",
        r"(involved\s+in|participates?\s+in|contributes?\s+to|"
        r"plays?\s+(a\s+)?(role|part)\s+in|"
        r"is\s+(responsible|important|relevant|necessary)\s+for|"
        r"is\s+(related|linked)\s+to|"
        r"has\s+(multiple\s+functions|been\s+associated|an\s+important|"
        r"a\s+role|important\s+effects|direct\s+impact))",
    ),
]

# Compiled version for the 'other' sub-classifier
_COMPILED_SUBTYPES: list[tuple[str, re.Pattern]] = [
    (label, re.compile(pat, re.IGNORECASE))
    for label, _colour, pat in OTHER_SUBTYPES
]


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Simplified framing categories  (from phase_m_nucleus_wording.py)
# ═══════════════════════════════════════════════════════════════════════════════

FRAME_CATS_SIMPLE: list[tuple[str, str, str]] = [
    # (label, colour, regex_pattern)
    ("relay / gateway",    "#D55E00", r"\brelay\b|\bgateway\b|\btransmit\b|\bforward\b"),
    ("hub / network",      "#0072B2", r"\bhub\b|\bnetwork\s+node\b|\bconnector\b|\bcentrality\b|\bconnectome\b"),
    ("modulator",          "#E69F00", r"\bmodulat\b|\bmodulatory\b"),
    ("integrator",         "#F0E442", r"\bintegrat\b|\bconverge\b|\bconvergence\b"),
    ("cognitive / memory", "#009E73", r"\bcognit\b|\bworking\s+memory\b|\battention\b|\bexecutive\b|\bmemory\b"),
    ("clinical / DBS",     "#CC79A7", r"\bDBS\b|\bdeep\s+brain\s+stimul\b|\bstimulat\b|\btherapy\b|\btreatment\b|\bsurgery\b"),
    ("sensory / motor",    "#56B4E9", r"\bsensory\b|\bmotor\b|\bsomatosensory\b|\bpain\b|\bauditory\b|\bvisual\b"),
    ("structural desc.",   "#7B4F9E", r"\bcomposed\b|\bsubdivided\b|\bheterogeneous\b|\bnuclei\b|\bsubnucl\b"),
]

_COMPILED_FRAME_CATS_SIMPLE: list[tuple[str, re.Pattern]] = [
    (label, re.compile(pat, re.IGNORECASE))
    for label, _colour, pat in FRAME_CATS_SIMPLE
]


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Classification functions
# ═══════════════════════════════════════════════════════════════════════════════

def classify_sentence(sentence: str) -> Tuple[List[str], str]:
    """Return ``(all_matching_categories, primary_category)``."""
    s = str(sentence)
    matched = [cat for cat, pats in COMPILED.items() if any(p.search(s) for p in pats)]
    if not matched:
        primary = "other"
    else:
        primary = next((c for c in PRIORITY if c in matched), matched[0])
    return matched, primary


def classify_primary(sentence: str) -> str:
    """Return only the primary (highest-priority) framing label."""
    _, primary = classify_sentence(sentence)
    return primary


def classify_all(sentence: str) -> List[str]:
    """Return all matching taxonomy categories (empty list -> 'other')."""
    matched, _ = classify_sentence(sentence)
    return matched if matched else ["other"]


def classify_other_subtype(sentence: str) -> str:
    """Classify a sentence within the 'other' sub-taxonomy.

    Intended for sentences whose ``classify_primary`` result is ``"other"``.
    Returns the first matching sub-type label, or ``"Other / unclassified"``.
    """
    s = str(sentence)
    for label, pat in _COMPILED_SUBTYPES:
        if pat.search(s):
            return label
    return "Other / unclassified"


def classify_frame_simple(sentence: str) -> str:
    """Simplified framing classifier (FRAME_CATS_SIMPLE).

    Returns the first matching simplified label or ``"other/generic"``.
    """
    s = str(sentence)
    for label, pat in _COMPILED_FRAME_CATS_SIMPLE:
        if pat.search(s):
            return label
    return "other/generic"


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Clean 21-frame detection (FRAMES_CLEAN) — single source of truth
# ═══════════════════════════════════════════════════════════════════════════════
# These 21 regex patterns detect explicit metaphorical/conceptual descriptions
# of thalamic function. Applied in priority order — first match wins.
# Groups: Metaphorical (9), Functional (9), Descriptive (3).

from collections import OrderedDict

FRAMES_CLEAN = OrderedDict([
    # ── Metaphorical frames (9) ──
    ("relay", re.compile(
        r"relay\s*(station|center|centre|hub|nucleus|for\b|of\s|between|structure)"
        r"|way.?station|sensory\s+relay|information\s+relay"
        r"|(thalamus|it)\s+(relays|relayed)\s+(sensory|motor|visual|auditory|information|signal)",
        re.I)),
    ("hub", re.compile(r"\bhub\b|\bnexus\b", re.I)),
    ("gateway", re.compile(
        r"\bgateway\b|\bgatekeep|portal|gating\s+(of|mechanism|function|information)", re.I)),
    ("gate", re.compile(
        r"\bgate\b(?!\s*(charge|voltage|current|ion))"
        r"|\bgating\b(?!\s*(current|variable|channel|ring))", re.I)),
    ("switchboard", re.compile(r"switchboard|switch\s*(board|station)|routing\s+station", re.I)),
    ("blackboard", re.compile(r"\bblackboard\b", re.I)),
    ("bridge", re.compile(r"\bbridge\b\s+(between|connecting|linking)", re.I)),
    ("bottleneck", re.compile(r"\bbottleneck\b", re.I)),
    ("interface", re.compile(r"\binterface\b\s+(between|for|of)", re.I)),
    # ── Functional frames (9) ──
    ("modulator", re.compile(
        r"thalamus\s+(modulat|is\s+a\s+modulat)|thalamic\s+modulat"
        r"|modulat(es|ing|or)\s+(cortical|neural|brain|sensory)\s+(activity|state|response)",
        re.I)),
    ("integrator", re.compile(
        r"integrat(es|ive|ion)\s+(center|centre|hub|of\s+information|signals?|sensory|input)", re.I)),
    ("filter", re.compile(
        r"(sensory|information|neural)\s+filter|\bfilter(s|ing)?\b\s+(sensory|information)", re.I)),
    ("regulator", re.compile(
        r"regulat(es|or|ory)\s+(cortical|brain|neural|arousal|sleep|wake|consciousness|state|activity)",
        re.I)),
    ("coordinator", re.compile(
        r"coordinat(es|or|ion)\s+(of\s+)?(cortical|brain|neural|network|activity|information)", re.I)),
    ("pacemaker", re.compile(
        r"pacemaker|oscillat(ory|ion)\s+(generator|source|center|centre)", re.I)),
    ("amplifier", re.compile(
        r"amplif(ier|ies|y|ying|ication)\s+(of\s+)?(cortical|neural|signal|sensory)", re.I)),
    ("distributor", re.compile(
        r"distribut(es|or|ing|ion)\s+(of\s+)?(information|signal|input|sensory)", re.I)),
    ("processor", re.compile(
        r"process(es|or|ing)\s+(sensory|information|visual|auditory|signal)", re.I)),
    # ── Descriptive frames (3) ──
    ("network_node", re.compile(
        r"(network|circuit)\s+node|node\s+(in|within|of)\s+(a\s+|the\s+)?"
        r"(large.scale|brain|neural|functional|cortical)", re.I)),
    ("clinical_target", re.compile(
        r"(therapeutic|treatment|clinical|stimulation|DBS|neuromodulation)\s+target"
        r"|target\s+(for|of)\s+(treatment|therapy|intervention|stimulation|DBS)", re.I)),
    ("anti_relay", re.compile(
        r"not\s+(merely|just|simply|only)\s+(a\s+)?relay"
        r"|more\s+than\s+(a\s+|just\s+)?(a\s+)?relay|beyond\s+(the\s+)?relay", re.I)),
])

# Pre-compiled list for iteration
_FRAMES_CLEAN_LIST = list(FRAMES_CLEAN.items())

# All frame type names
FRAME_NAMES = list(FRAMES_CLEAN.keys())


def detect_frame(sentence: str) -> str:
    """Detect the primary frame type of a sentence.

    Applies the 21 FRAMES_CLEAN patterns in priority order.
    Returns the first matching frame name, or '' if none match.
    """
    s = str(sentence)
    for fname, pat in _FRAMES_CLEAN_LIST:
        if pat.search(s):
            return fname
    return ''


def detect_all_frames(sentence: str) -> list[str]:
    """Detect ALL matching frame types (not just the first).

    Returns a list of all matching frame names (may be empty).
    """
    s = str(sentence)
    return [fname for fname, pat in _FRAMES_CLEAN_LIST if pat.search(s)]
