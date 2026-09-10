"""
Thalamic nucleus regex patterns and tagging utilities.

Provides the canonical NUCLEI dictionary (19 nuclei) and the
``tag_nuclei`` function for adding boolean ``has_<ID>`` columns to a
DataFrame.

Colour palette: Wong colourblind-safe palette (+ a few extras for the
extended set).
"""

import re
import pandas as pd

# ── Wong colorblind-safe palette (extended) ──────────────────────────────────
_WONG = [
    "#E69F00",  # orange
    "#D55E00",  # vermilion
    "#CC79A7",  # reddish purple
    "#56B4E9",  # sky blue
    "#999999",  # grey
    "#009E73",  # bluish green
    "#7B4F9E",  # purple (extension)
    "#0072B2",  # blue
    "#F0E442",  # yellow
    "#888888",  # dark grey
    "#FFB347",  # light orange
    "#4DC5A4",  # teal
    "#A0522D",  # sienna (extension)
    "#B22222",  # firebrick (extension)
    "#6A5ACD",  # slate blue (extension)
    "#2E8B57",  # sea green (extension)
    "#CD853F",  # peru (extension)
    "#8B008B",  # dark magenta (extension)
    "#20B2AA",  # light sea green (extension)
]

# ── Nucleus definitions ──────────────────────────────────────────────────────
#
# 11 original entries from phase_m (minus "IL/Intralaminar" which is a group,
# not a single nucleus — its members CM and CL are listed individually),
# plus Reuniens, Habenula, and MGN.
#
# Each entry carries:
#   label    – display name for figures
#   color    – hex colour from the Wong palette
#   pat_sent – regex used to tag individual sentences
#   pat_abs  – regex used to tag full abstracts

NUCLEI = {
    # ── Original 11 ──────────────────────────────────────────────────────────
    "MD": {
        "label": "MD (Mediodorsal)",
        "color": "#E69F00",
        "pat_sent": r"mediodorsal|MD\s+(nucleus|thalamus|neurons?|cells?|unit)\b",
        "pat_abs":  r"mediodorsal|MD\s+(nucleus|thalamus|neurons?|cells?|unit)\b",
    },
    "VP": {
        "label": "VP/VPM (Ventroposterior)",
        "color": "#D55E00",
        "pat_sent": r"ventroposterior|ventral\s+poster|VPL\b|VPM\b|\bVP\s+(nucleus|thalamus|neurons?)",
        "pat_abs":  r"ventroposterior|ventral\s+poster|VPL\b|VPM\b|\bVP\s+(nucleus|thalamus|neurons?)",
    },
    "VL": {
        "label": "VL (Ventrolateral)",
        "color": "#CC79A7",
        "pat_sent": r"ventrolateral|ventral\s+lateral\s+(nucleus|thalamus)|VL\s+(nucleus|thalamus|neurons?)\b",
        "pat_abs":  r"ventrolateral|ventral\s+lateral\s+(nucleus|thalamus)|VL\s+(nucleus|thalamus|neurons?)\b",
    },
    "Pulvinar": {
        "label": "Pulvinar/LP",
        "color": "#56B4E9",
        "pat_sent": r"\bpulvinar\b|lateral\s+posterior\s+(nucleus|thalamus|thalamic)|\bLP\s+(nucleus|thalamus|neurons?)\b",
        "pat_abs":  r"\bpulvinar\b|lateral\s+posterior\s+(nucleus|thalamus|thalamic)|\bLP\s+(nucleus|thalamus|neurons?)\b",
    },
    "LGN": {
        "label": "dLGN (Lat. Geniculate)",
        "color": "#999999",
        "pat_sent": r"lateral\s+geniculate|LGN\b|dLGN\b",
        "pat_abs":  r"lateral\s+geniculate|LGN\b|dLGN\b",
    },
    "ANT": {
        "label": "ANT (Anterior)",
        "color": "#009E73",
        "pat_sent": (r"anterior\s+(thalamic|nucleus|nuclear)\s*(group|complex)?|"
                     r"\bANT\s+(nucleus|thalamus|neurons?)\b"),
        "pat_abs":  (r"anterior\s+(thalamic|nucleus|nuclear)\s*(group|complex)?|"
                     r"\bANT\s+(nucleus|thalamus|neurons?)\b"),
    },
    "CM": {
        "label": "CM (Centromedian)",
        "color": "#7B4F9E",
        "pat_sent": r"centromedian|centromedial|CM\s+(nucleus|thalamus)\b|CM-Pf|Pf\s+nucleus",
        "pat_abs":  r"centromedian|centromedial|CM\s+(nucleus|thalamus)\b|CM-Pf|Pf\s+nucleus",
    },
    "PVT": {
        "label": "PVT (Paraventricular)",
        "color": "#0072B2",
        "pat_sent": r"paraventricular\s+(thalamus|nucleus|thalamic)|\bPVT\b",
        "pat_abs":  r"paraventricular\s+(thalamus|nucleus|thalamic)|\bPVT\b",
    },
    "TRN": {
        "label": "TRN (Reticular)",
        "color": "#888888",
        "pat_sent": r"reticular\s+(nucleus|thalamus)|thalamic\s+reticular|\bTRN\b",
        "pat_abs":  r"reticular\s+(nucleus|thalamus)|thalamic\s+reticular|\bTRN\b",
    },
    "CL": {
        "label": "CL (Central Lateral)",
        "color": "#FFB347",
        "pat_sent": r"central[- ]?lateral|centrolateral|\bCL\s+(nucleus|thalamus|neurons?|cells?|unit)\b",
        "pat_abs":  r"central[- ]?lateral|centrolateral|\bCL\s+(nucleus|thalamus|neurons?|cells?|unit)\b",
    },
    "PO": {
        "label": "PO/POm (Posterior)",
        "color": "#4DC5A4",
        "pat_sent": r"\bPOm\b|\bPO\s+(nucleus|thalamus|thalamic|neurons?)\b|postero[- ]?medial\s+(nucleus|thalamus|thalamic)|posterior\s+medial\s+(nucleus|thalamus|thalamic)|posterior\s+thalamic\s+nuclear\s+group",
        "pat_abs":  r"\bPOm\b|\bPO\s+(nucleus|thalamus|thalamic|neurons?)\b|postero[- ]?medial\s+(nucleus|thalamus|thalamic)|posterior\s+medial\s+(nucleus|thalamus|thalamic)|posterior\s+thalamic\s+nuclear\s+group",
    },

    # ── Additional nuclei ────────────────────────────────────────────────────
    "Reuniens": {
        "label": "Reuniens (RE)",
        "color": "#A0522D",
        "pat_sent": r"\breuniens\b|\bnucleus\s+reuniens\b|\bRe\s+(nucleus|thalamus|neurons?)\b",
        "pat_abs":  r"\breuniens\b|\bnucleus\s+reuniens\b|\bRe\s+(nucleus|thalamus|neurons?)\b",
    },
    # Habenula removed — epithalamus, not thalamus proper
    "MGN": {
        "label": "MGN/MGB (Med. Geniculate)",
        "color": "#F0E442",
        "pat_sent": r"medial\s+geniculate|MGN\b|MGB\b|medial\s+geniculate\s+body",
        "pat_abs":  r"medial\s+geniculate|MGN\b|MGB\b|medial\s+geniculate\s+body",
    },

    # ── User-requested additions ────────────────────────────────────────────
    "AD": {
        "label": "AD (Anterodorsal)",
        "color": "#6A5ACD",
        "pat_sent": r"anterodorsal|\bAD\s+(nucleus|thalamus|neurons?|cells?)\b",
        "pat_abs":  r"anterodorsal|\bAD\s+(nucleus|thalamus|neurons?|cells?)\b",
    },
    # LP merged into Pulvinar/LP above (rodent homolog)
    # PO entry merged into PO/POm above
    "PF": {
        "label": "PF (Parafascicular)",
        "color": "#8B008B",
        "pat_sent": r"parafascicular|\bPf\s+(nucleus|thalamus|neurons?)\b|\bPF\s+(nucleus|thalamus|neurons?)\b",
        "pat_abs":  r"parafascicular|\bPf\s+(nucleus|thalamus|neurons?)\b|\bPF\s+(nucleus|thalamus|neurons?)\b",
    },
    "VM": {
        "label": "VM (Ventromedial)",
        "color": "#20B2AA",
        "pat_sent": r"ventromedial\s+(nucleus|thalamus|thalamic)|\bVM\s+(nucleus|thalamus|neurons?)\b",
        "pat_abs":  r"ventromedial\s+(nucleus|thalamus|thalamic)|\bVM\s+(nucleus|thalamus|neurons?)\b",
    },
    "VIM": {
        # Ventral intermediate nucleus — the VIM-DBS / MRgFUS tremor target.
        # Added 2026-09 after the nuclei×domain heatmap showed the tremor-
        # surgery domain under-lighting (VIM was untagged). \bvim\b is safe
        # from 'vimentin' via the word boundary.
        "label": "VIM (Ventral intermediate)",
        "color": "#B22222",
        "pat_sent": r"ventral(is)?\s+intermedi(ate|us)|\bvim\b",
        "pat_abs":  r"ventral(is)?\s+intermedi(ate|us)|\bvim\b",
    },
}


# ── Tagging utility ──────────────────────────────────────────────────────────

def tag_nuclei(df, text_col="sentence"):
    """Add boolean ``has_<ID>`` columns for every nucleus in :data:`NUCLEI`.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with a text column to search.
    text_col : str, optional
        Name of the column containing text to match against.
        Default ``"sentence"``.

    Returns
    -------
    pd.DataFrame
        The same DataFrame (modified in place) with new boolean columns
        ``has_MD``, ``has_VP``, ``has_VL``, ... for each key in NUCLEI.
    """
    pat_key = "pat_sent" if text_col == "sentence" else "pat_abs"
    text = df[text_col].fillna("")
    for nid, info in NUCLEI.items():
        df[f"has_{nid}"] = text.str.contains(
            info[pat_key], case=False, regex=True, na=False
        )
    return df
