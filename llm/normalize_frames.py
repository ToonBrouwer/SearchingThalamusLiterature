"""
normalize_frames.py — collapse the open-coded frame names (489 unique)
into ~15 frame FAMILIES via ordered keyword rules (first match wins).

Outputs:
  v18/llm_full/frames_flat.csv        (adds frame_family column, in place)
  v18/llm_full/frame_family_map.csv   (name -> family, for user review)
Prints family statistics: prevalence, family x decade, stance timeline.
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path

import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                               errors="replace")
ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "Data" / "processed" / "v18" / "llm_full"

# Ordered rules — first regex that matches the frame name wins.
RULES = [
    ("driver_modulator", r"driver.modulator|driver/modulator"),
    ("gate_filter", r"gate|gating|filter|gatekeeper|searchlight"),
    ("hub_integrator", r"hub|integrat|convergence|blackboard|workspace"),
    ("oscillator_pacemaker", r"oscillat|pacemaker|rhythm|spindle|"
                              r"dysrhythm|synchroniz|generator"),
    ("state_arousal_regulator", r"arousal|consciousness|sleep|wake|"
                                 r"vigilance|state_regul|anesthetic|"
                                 r"hypnotic|state_switch|firing_mode|"
                                 r"mode_switch"),
    ("clinical_target", r"target|stimulat|dbs\b|neuromodulat|thalamotomy|"
                         r"stereotactic|surgical|access_point|therapeutic"),
    ("damage_site", r"lesion|damage|degenerat|atroph|infarct|vulnerab|"
                     r"tumor|patholog|abscess|hemorrhage|injur|encephal|"
                     r"disease|stroke|ischem|seizure_focus|epileptogenic"),
    ("biomarker", r"biomarker|marker"),
    ("reference_structure", r"atlas|reference|nomenclature|parcellat|"
                             r"segmentation|model_system|anatomical_"),
    ("map_representation", r"\bmap\b|representation|topograph|somatotop|"
                            r"mirror|internal_copy|fingerprint"),
    ("memory_cognitive", r"memory|mnemonic|cognit|attention|decision|"
                          r"reward|emotion|fear|stress|mood|behavioral"),
    ("developmental", r"develop|embryo|axon_source|axon_guidance|"
                       r"regenerat|wiring|differentiat|progenitor"),
    ("modulator_regulator", r"modulat|regulat|gain|amplif|suppress|"
                             r"control(ler)?\b|command"),
    ("processor_computation", r"process|comput|transform|encod|logic|"
                               r"predict|signal_to_noise"),
    ("network_node", r"node|network|circuit|loop|partner|participant|"
                      r"interface|link|bridge|member|subnetwork|"
                      r"communication"),
    ("relay_transmission", r"relay|transmission|transmit|station|conduit|"
                            r"channel|route|passage|forward"),
    ("projection_pathway", r"projection|afferent|efferent|input|output|"
                            r"source|pathway|terminus|territory|origin|"
                            r"tract|fiber|innervation|connect"),
]


def family_of(name: str) -> str:
    n = str(name).lower().replace(" ", "_")
    for fam, pat in RULES:
        if re.search(pat, n):
            return fam
    return "other"


flat = pd.read_csv(BASE / "frames_flat.csv")
if "frame_family" in flat.columns:
    flat = flat.drop(columns=["frame_family"])
flat["frame_family"] = flat.frame_name.map(family_of)
flat.to_csv(BASE / "frames_flat.csv", index=False)

fmap = (flat.groupby(["frame_name", "frame_family"]).size()
        .reset_index(name="n").sort_values(["frame_family", "n"],
                                            ascending=[True, False]))
fmap.to_csv(BASE / "frame_family_map.csv", index=False)

print("Family prevalence (all frames):")
print(flat.frame_family.value_counts().to_string())

print("\n'other' names (unmapped, check):")
print(flat.loc[flat.frame_family == "other", "frame_name"]
      .value_counts().head(20).to_string())

cen = flat[flat.centrality == "central"].copy()
print(f"\nCentral frames: {len(cen)}")
print("\n% of papers per decade with a CENTRAL frame in family "
      "(top families):")
papers_per_dec = flat.groupby("decade").pmid.nunique()
top_fams = cen.frame_family.value_counts().head(9).index
tab = pd.DataFrame({
    fam: cen[cen.frame_family == fam].groupby("decade").pmid.nunique()
    / papers_per_dec * 100
    for fam in top_fams}).round(0)
print(tab.to_string())

print("\nStance = challenges: which families get challenged, by decade:")
ch = flat[flat.stance == "challenges"]
print(pd.crosstab(ch.decade, ch.frame_family).to_string())

print("\nAgency by family (central frames, %):")
ag = (pd.crosstab(cen.frame_family, cen.agency, normalize="index")
      * 100).round(0)
print(ag.loc[top_fams].to_string())

print("\nKind comparison (frames per paper):")
print(flat.groupby("kind").apply(
    lambda g: round(len(g) / g.pmid.nunique(), 1)).to_string())
