"""
08_clean_topics.py — Clean and merge BERTopic topics for publication.

Issues to fix:
1. Merge vision-related topics into one "Visual processing" topic
2. Relabel nucleus-centric topics (MGN→Auditory processing, PVT→Paraventricular/stress)
3. Create missing mega-themes: Somatosensory, Cognitive, Perception
4. Clean garbled topic names (fgf15, casastorremo, etc.)
5. Merge duplicates
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"

df = pd.read_csv(PROC / "all_sentences_clean.csv")
df["topic_clean_v2"] = df["topic_clean_v2"].fillna("unassigned")
print(f"Loaded {len(df):,} sentences")

# ── Topic relabeling map ──
# Format: old_topic_id → (new_label, new_mega_theme)
RELABEL = {
    # Vision-related → merge into "Visual processing"
    37: "Visual processing / pulvinar",
    43: "Visual cortex development",
    51: "Retinal / visual interneurons",
    80: "Visual stimulation (TUS/tFUS)",
    130: "Retinal convergence",

    # Somatosensory
    30: "Somatosensory / whisker barrel",
    173: "Whisker / neostriatum",

    # Auditory (relabel from MGN-centric)
    6: "Auditory processing",  # was "auditory mgb geniculate sound"

    # Cognitive/memory/attention
    52: "Consciousness / anaesthesia",
    59: "Language / aphasia",
    155: "Speech production",
    106: "Reward / motivation",
    101: "Emotion processing",
    32: "Fear conditioning",

    # Clinical - relabel nucleus-centric ones
    13: "Paraventricular thalamus / stress",  # was "pvt kirouac"

    # Development - clean garbled names
    9: "Thalamocortical axon development",  # was "axons axon dii telencephalon"

    # Clean other garbled topics
    # Topic with "casastorremo..." is already removed (was topic 4 in v2)
}

# Apply relabeling
for tid, new_label in RELABEL.items():
    mask = df["bertopic_id_v2"] == tid
    n = mask.sum()
    if n > 0:
        old = df.loc[mask, "topic_clean_v2"].iloc[0]
        df.loc[mask, "topic_clean_v2"] = new_label
        print(f"  Relabeled topic {tid}: '{old}' -> '{new_label}' ({n} sentences)")

# ── Create mega-theme column for grouping ──
MEGA_THEMES = {
    "Clinical / disease": [
        "seizures seizure epilepsy", "stroke infarction", "tremor vim essential dbs",
        "ms sclerosis atrophy", "tumor tumors glioma", "migraine aura",
        "ptsd rsfc trauma", "depression depressive mdd", "injury damage tbi",
        "cocaine addiction", "Consciousness / anaesthesia",
    ],
    "Sensory / motor": [
        "Auditory processing", "Somatosensory / whisker barrel",
        "Visual processing / pulvinar", "Retinal / visual interneurons",
        "Visual cortex development", "Retinal convergence",
        "ganglia basal motor cerebellum", "Visual stimulation",
        "Whisker / neostriatum", "Speech production",
    ],
    "Cognitive / behavioral": [
        "Fear conditioning", "Language / aphasia", "Reward / motivation",
        "Emotion processing", "Paraventricular thalamus / stress",
    ],
    "Sleep / oscillations": [
        "sleep wakefulness insomnia", "spindles spindle ripples",
    ],
    "Methodology / anatomy": [
        "Thalamocortical axon development", "virus viral injected aav",
        "microglia microglial", "volume matter grey",
        "age volume males", "segmentation putamen caudate",
        "naacr chocr naa", "artery arteries supply",
    ],
    "Circuit / cellular": [
        "trn inhibitory gabaergic reticular", "nucleus th habenula",
        "hippocampus hippocampal", "gaba gabaergic gabaa",
    ],
}

def assign_mega_theme(topic_name):
    topic_lower = topic_name.lower()
    for theme, keywords in MEGA_THEMES.items():
        for kw in keywords:
            if kw.lower() in topic_lower:
                return theme
    return "Other"

df["mega_theme"] = df["topic_clean_v2"].apply(assign_mega_theme)

# ── Merge small visual topics into one ──
VISION_MERGE = [37, 43, 51, 80, 130]
vision_mask = df["bertopic_id_v2"].isin(VISION_MERGE)
df.loc[vision_mask, "topic_clean_v2"] = "Visual processing"
n_vision = vision_mask.sum()
print(f"\n  Merged {n_vision} visual sentences into 'Visual processing'")

# ── Summary ──
tc = df[df.bertopic_id_v2 != -1].groupby("topic_clean_v2").size().sort_values(ascending=False)
print(f"\nTop 25 cleaned topics:")
for i, (name, count) in enumerate(tc.head(25).items()):
    print(f"  {i+1:2d}. {count:5d}  {name}")

# Mega-theme distribution
mt = df[df.bertopic_id_v2 != -1].groupby("mega_theme").size().sort_values(ascending=False)
print(f"\nMega-theme distribution:")
for theme, count in mt.items():
    print(f"  {theme:30s}  {count:5d}")

# Save
df.to_csv(PROC / "all_sentences_final.csv", index=False)
print(f"\nSaved all_sentences_final.csv ({len(df):,} rows)")
