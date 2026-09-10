"""
Apply human-assigned labels to HDBSCAN clusters.

Adds a cluster_label column to the two main output files from
embed_and_cluster.py by mapping the integer cluster_id values through the
manually curated CLUSTER_LABELS dictionary below.

The labels were assigned after reviewing the cluster examples printed by
print_clusters_for_labelling.py.  Key framing clusters are:
    53  hub / integration framing
    54  information flow / cortico-thalamic mediator
    55  relay / gateway framing
    56  sensory filtering / higher-order relay / gating

Inputs (Data/processed/):
    framing_embedded_clustered.csv   — from embed_and_cluster.py
    cluster_examples.csv             — from embed_and_cluster.py

Outputs (Data/processed/):
    framing_embedded_clustered.csv   — updated in-place with cluster_label column
    cluster_examples.csv             — updated in-place with cluster_label column

Note:
    This script overwrites both input files.  Re-run embed_and_cluster.py
    followed by this script if cluster assignments change.
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT = _ANALYSIS_DIR.parents[1]
PROC = ROOT / "Data" / "processed"

# ── Manually assigned cluster labels ─────────────────────────────────────────
CLUSTER_LABELS = {
    -1: "noise/mixed",
     0: "DBS – tremor (VL/VIM)",
     1: "dystonia",
     2: "pain / nociception",
     3: "migraine",
     4: "fear conditioning / emotional learning",
     5: "auditory thalamus – general (MGB)",
     6: "auditory thalamus – MGB subdivisions",
     7: "addiction / reward / CSTC circuits",
     8: "DBS surgical outcomes",
     9: "epilepsy",
    10: "language processing",
    11: "schizophrenia / psychosis",
    12: "depression / MDD",
    13: "multiple sclerosis",
    14: "anaesthesia",
    15: "sleep / wake / NREM",
    16: "arousal / reticular activating system",
    17: "consciousness",
    18: "TRN / inhibitory interneurons / circuit anatomy",
    19: "GABAergic transmission / neurotransmitters",
    20: "neurodegeneration / Alzheimer's",
    21: "burst/tonic firing modes",
    22: "thalamus development – cell lineage",
    23: "thalamus development – gene expression",
    24: "axonal connectivity development",
    25: "molecular markers / gene expression (adult)",
    26: "cholinergic projections / basal forebrain",
    27: "MRI signal abnormalities / basal ganglia lesions",
    28: "thalamic stroke / infarction",
    29: "receptor binding / mixed",
    30: "visual thalamus / LGN / pulvinar",
    31: "brain oscillations / slow waves / pacemaker",
    32: "midline & intralaminar nuclei / organisation",
    33: "thalamic nucleus targeting / DBS imaging",
    34: "gustatory / taste thalamus",
    35: "ASD / sensory filtering",
    36: "TBI / inflammation / injury",
    37: "atrophy / degeneration / lesion studies",
    38: "attention / alertness",
    39: "cognition – general",
    40: "working memory / anterior thalamus",
    41: "MRI methods / voxel / segmentation",
    42: "somatosensory tract tracing",
    43: "thalamic nuclei anatomy & connectivity",
    44: "brain volumetrics / MRI morphometry",
    45: "PD / FM / structural abnormalities",
    46: "somatosensory thalamus / first- vs higher-order relay",
    47: "cerebellar & VL thalamus / motor",
    48: "fMRI / brain activation measurements",
    49: "VBM / gray matter volume studies",
    50: "structural & functional connectivity",
    51: "basal ganglia–thalamocortical motor circuits",
    52: "functional networks / resting-state connectivity",
    53: "hub / integration framing",
    54: "information flow / cortico-thalamic mediator",
    55: "relay / gateway framing",
    56: "sensory filtering / higher-order relay / gating",
}

# ── Load and label ───────────────────────────────────────────────────────────
df = pd.read_csv(PROC / "framing_embedded_clustered.csv")
df["cluster_label"] = df["cluster_id"].map(CLUSTER_LABELS).fillna("unknown")

# ── Also apply labels to cluster examples file ────────────────────────────────
ex = pd.read_csv(PROC / "cluster_examples.csv")
ex["cluster_label"] = ex["cluster_id"].map(CLUSTER_LABELS).fillna("unknown")

# ── Save ──────────────────────────────────────────────────────────────────────
df.to_csv(PROC / "framing_embedded_clustered.csv", index=False)
ex.to_csv(PROC / "cluster_examples.csv", index=False)

print(f"Tagged {len(df)} sentences across {df['cluster_id'].nunique()} clusters.")
print(f"\nLabel distribution (top 20 by size):")
print(df.groupby(["cluster_id","cluster_label"])
        .size()
        .reset_index(name="n")
        .sort_values("n", ascending=False)
        .head(20)
        .to_string(index=False))
