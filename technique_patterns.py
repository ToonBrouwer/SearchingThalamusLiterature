"""
Recording-technique regex patterns and tagging utilities.

Provides the canonical TECHNIQUES dictionary (11 technique categories) and
the ``tag_techniques`` function for adding boolean ``has_<ID>`` columns to
a DataFrame.

Colour palette: Wong colourblind-safe palette (extended to 11 entries).
"""

import re
import pandas as pd

# ── Technique definitions ────────────────────────────────────────────────────
#
# Each entry carries:
#   label   – display name for figures
#   color   – hex colour (Wong colourblind-safe palette, extended)
#   pattern – compiled-ready regex (case-insensitive matching applied at
#             search time)

TECHNIQUES = {
    "fMRI": {
        "label": "fMRI",
        "color": "#E69F00",   # orange
        "pattern": (
            r"\bfMRI\b|\bfunctional\s+MRI\b|\bfunctional\s+magnetic\s+resonance\b"
            r"|\bBOLD\b|\bresting[- ]state\b|\btask[- ]related\b|\btask[- ]based\b"
        ),
    },
    "electrophysiology": {
        "label": "Electrophysiology",
        "color": "#56B4E9",   # sky blue
        "pattern": (
            r"\belectrophysiol\w+\b|\bsingle[- ]unit\b|\bmulti[- ]unit\b"
            r"|\btetrode\b|\bextracellular\s+record\w*\b|\bin\s+vivo\s+record\w*\b"
            r"|\bpatch[- ]clamp\b|\bwhole[- ]cell\b|\bsharp\s+electrode\b"
            r"|\blocal\s+field\s+potent\w+\b|\bLFP\b|\bspike\s+sort\w*\b"
            r"|\bneuropixel\w*\b|\bsilicon\s+probe\b"
        ),
    },
    "calcium_imaging": {
        "label": "Calcium imaging",
        "color": "#009E73",   # bluish green
        "pattern": (
            r"\bcalcium\s+imag\w+\b|\bGCaMP\w*\b|\btwo[- ]photon\b"
            r"|\bfiber\s+photometry\b|\bfibre\s+photometry\b"
            r"|\bminiScope\b|\bminiscope\b|\b2[- ]?photon\b"
        ),
    },
    "optogenetics": {
        "label": "Optogenetics",
        "color": "#D55E00",   # vermilion
        "pattern": (
            r"\boptogenet\w+\b|\bChR2\b|\bchannelrhodopsin\b"
            r"|\bNpHR\b|\bhalorhodopsin\b|\bphoto[- ]?stimulat\w+\b"
            r"|\bArchT?\b|\beNpHR\b|\bChRmine\b|\bChrimson\b"
        ),
    },
    "chemogenetics": {
        "label": "Chemogenetics",
        "color": "#CC79A7",   # reddish purple
        "pattern": (
            r"\bchemogenet\w+\b|\bDREADD\w*\b|\bhM[34]D[iq]?\b"
            r"|\bCNO\b|\bclozapine[- ]N[- ]oxide\b"
        ),
    },
    "EEG_MEG": {
        "label": "EEG / MEG",
        "color": "#0072B2",   # blue
        "pattern": (
            r"\bEEG\b|\belectroencephalogra\w+\b|\bMEG\b"
            r"|\bmagnetoencephalogra\w+\b|\bevent[- ]related\s+potent\w+\b"
            r"|\bERP\b|\bECoG\b|\belectrocorticogra\w+\b"
        ),
    },
    "PET": {
        "label": "PET",
        "color": "#F0E442",   # yellow
        "pattern": (
            r"\bPET\b|\bpositron\s+emission\b|\bSPECT\b"
            r"|\bradioli?gand\b|\btracer\s+uptake\b"
        ),
    },
    "tract_tracing": {
        "label": "Tract tracing",
        "color": "#7B4F9E",   # purple
        "pattern": (
            r"\btract[- ]trac\w+\b|\banterograde\b|\bretrograde\b"
            r"|\bBDA\b|\bFluoroGold\b|\bfluoro[- ]?gold\b|\bCTB\b"
            r"|\bcholera\s+toxin\b|\bAAV[- ]?tracer\b|\brAAV\b"
            r"|\btrans[- ]?synaptic\b|\bmonosynaptic\s+trac\w+\b"
        ),
    },
    "DBS": {
        "label": "DBS",
        "color": "#999999",   # grey
        "pattern": (
            r"\bdeep\s+brain\s+stimulat\w+\b|\bDBS\b"
            r"|\bthalamic\s+stimulat\w+\b|\bthalamo\w*\s+stimulat\w+\b"
        ),
    },
    "lesion": {
        "label": "Lesion studies",
        "color": "#FFB347",   # light orange
        "pattern": (
            r"\blesion\w*\b|\bablat\w+\b|\bibotenic\s+acid\b"
            r"|\bexcitotoxic\b|\binactivat\w+\b|\bmuscimol\b"
            r"|\bthalamic\s+stroke\b|\bthalamotomy\b"
        ),
    },
    "structural_MRI": {
        "label": "Structural MRI",
        "color": "#4DC5A4",   # teal
        "pattern": (
            r"\bstructural\s+MRI\b|\bvoxel[- ]based\s+morpho\w+\b|\bVBM\b"
            r"|\bdiffusion\s+tensor\b|\bDTI\b|\btractography\b"
            r"|\bfreesurfer\b|\bcortical\s+thickness\b|\bwhite\s+matter\s+tract\b"
        ),
    },
}

# ── Subcategories for finer-grained temporal analysis ───────────────────────
TECHNIQUE_SUBCATEGORIES = {
    # fMRI subtypes
    "resting_state_fMRI": {
        "label": "Resting-state fMRI",
        "parent": "fMRI",
        "pattern": r"\bresting[- ]state\b|\brsFC\b|\bintrinsic\s+connectivity\b",
    },
    "task_fMRI": {
        "label": "Task-based fMRI",
        "parent": "fMRI",
        "pattern": r"\btask[- ](?:related|based|evoked)\b|\btask\s+fMRI\b|\bevent[- ]related\s+fMRI\b",
    },
    "functional_connectivity": {
        "label": "Functional connectivity",
        "parent": "fMRI",
        "pattern": (
            r"\bfunctional\s+connectivity\b|\brsFC\b|\bseed[- ]based\b"
            r"|\bconnectome\b|\bconnectomic\b|\beffective\s+connectivity\b"
            r"|\bdynamic\s+causal\b|\bDCM\b"
        ),
    },
    # Electrophysiology subtypes
    "single_unit": {
        "label": "Single-unit recording",
        "parent": "electrophysiology",
        "pattern": r"\bsingle[- ]unit\b|\bsingle[- ]neuron\b|\bsingle\s+cell\s+record\w*\b",
    },
    "multi_electrode": {
        "label": "Multi-electrode / high-density",
        "parent": "electrophysiology",
        "pattern": (
            r"\bmulti[- ]electrode\b|\bMEA\b|\bneuropixel\w*\b"
            r"|\bsilicon\s+probe\b|\bhigh[- ]density\s+record\w*\b"
        ),
    },
    "patch_clamp": {
        "label": "Patch clamp",
        "parent": "electrophysiology",
        "pattern": r"\bpatch[- ]clamp\b|\bwhole[- ]cell\b|\bintracellular\s+record\w*\b",
    },
    # Imaging subtypes
    "diffusion_MRI": {
        "label": "Diffusion MRI / tractography",
        "parent": "structural_MRI",
        "pattern": r"\bDTI\b|\bdiffusion\s+tensor\b|\btractograph\w*\b|\bdiffusion\s+weighted\b|\bDWI\b",
    },
    "volumetric_MRI": {
        "label": "Volumetric MRI",
        "parent": "structural_MRI",
        "pattern": (
            r"\bVBM\b|\bvoxel[- ]based\s+morpho\w+\b|\bfreesurfer\b"
            r"|\bcortical\s+thickness\b|\bgrey\s+matter\s+volume\b"
            r"|\bgray\s+matter\s+volume\b"
        ),
    },
    # Computational
    "computational_model": {
        "label": "Computational modeling",
        "parent": None,
        "pattern": (
            r"\bcomputational\s+model\w*\b|\bbiophysical\s+model\w*\b"
            r"|\bneural\s+network\s+model\w*\b|\bsimulat\w+\s+(?:of|the)\b"
            r"|\bmean[- ]field\s+model\b|\bconductance[- ]based\s+model\b"
        ),
    },
    # Histology / anatomy methods (large gap in current detection)
    "histology": {
        "label": "Histology / immunostaining",
        "parent": None,
        "pattern": (
            r"\bimmunohistochem\w+\b|\bimmunofluoresc\w+\b|\bimmunostain\w+\b"
            r"|\bin\s+situ\s+hybridiz\w+\b|\belectron\s+microscop\w+\b"
            r"|\bconfocal\s+microscop\w+\b|\blight\s+microscop\w+\b"
            r"|\bNissl\b|\bGolgi\s+stain\w*\b|\bcresyl\s+violet\b"
            r"|\bimmunocy\w+\b|\bhistolog\w+\b"
        ),
    },
    # Clinical study designs
    "clinical_study": {
        "label": "Clinical study",
        "parent": None,
        "pattern": (
            r"\bcohort\s+stud\w*\b|\bretrospective\b|\bprospective\b"
            r"|\bcase[- ]control\b|\bclinical\s+trial\b"
            r"|\bmeta[- ]analysis\b|\bsystematic\s+review\b"
            r"|\bneuropsycholog\w+\s+(?:test|assess|evaluat)\w*\b"
        ),
    },
    # Molecular / genomic
    "molecular": {
        "label": "Molecular / genomic",
        "parent": None,
        "pattern": (
            r"\bRT[- ]?PCR\b|\bqPCR\b|\bwestern\s+blot\b"
            r"|\bRNA[- ]?seq\w*\b|\btranscriptom\w+\b|\bsingle[- ]cell\s+RNA\b"
            r"|\bproteom\w+\b|\bGWAS\b|\bgene\s+expression\b"
        ),
    },
    # CT / structural clinical imaging
    "CT_imaging": {
        "label": "CT imaging",
        "parent": None,
        "pattern": (
            r"\bCT\s+scan\w*\b|\bcomputed\s+tomograph\w+\b"
            r"|\bCT\s+angiograph\w+\b"
        ),
    },
}


# ── Tagging utility ──────────────────────────────────────────────────────────

def tag_techniques(df, text_col="sentence"):
    """Add boolean ``has_<ID>`` columns for every technique in :data:`TECHNIQUES`.

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
        ``has_fMRI``, ``has_electrophysiology``, ... for each key in
        TECHNIQUES.
    """
    text = df[text_col].fillna("")
    for tid, info in TECHNIQUES.items():
        df[f"has_{tid}"] = text.str.contains(
            info["pattern"], case=False, regex=True, na=False
        )
    return df


def tag_technique_subcategories(df, text_col="sentence"):
    """Add boolean columns for technique subcategories."""
    text = df[text_col].fillna("")
    for tid, info in TECHNIQUE_SUBCATEGORIES.items():
        df[f"has_{tid}"] = text.str.contains(
            info["pattern"], case=False, regex=True, na=False
        )
    return df
