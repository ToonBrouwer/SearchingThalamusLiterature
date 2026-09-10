"""
19_enrich_and_rerun.py — Enrich the new full corpus with mega_theme,
create backward-compatible CSV files, then rerun all analyses.
"""
import sys, os, time, subprocess
os.environ["PYTHONIOENCODING"] = "utf-8"
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
CODE = ROOT / "Code"
PYTHON = sys.executable

t0 = time.time()

# ═══════════════════════════════════════════════════════════════════════════
# 1. ASSIGN MEGA_THEME TO NEW CORPUS
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("1. ENRICHING CORPUS WITH MEGA_THEME")
print("=" * 70)

df = pd.read_csv(PROC / "all_sentences_full_corpus.csv", low_memory=False)
print(f"Loaded: {len(df):,} sentences, {df['pmid'].nunique():,} papers")


def assign_mega_theme_from_sentence(sent):
    """Assign mega_theme based on sentence content (keyword matching)."""
    t = sent.lower()

    if any(kw in t for kw in [
        "seizure", "epilepsy", "stroke", "infarct", "ischemi", "tremor", "dbs",
        "sclerosis", "atrophy", "tumor", "glioma", "migraine", "ptsd", "trauma",
        "depression", "depressive", "mdd", "anxiety", "alzheimer", "dementia",
        "ocd", "cocaine", "addiction", "nicoti", "alcohol",
        "injury", "damage", "tbi", "microglia", "preterm", "infant",
        "pain", "nocicepti", "neuropath", "chronic pain",
        "schizophren", "psychos", "psychotic",
        "consciousness", "anaesthe", "propofol",
        "hemorrhag", "thrombos", "lesion",
        "patient", "clinical", "diagnosis", "symptom", "treatment",
        "surgery", "surgical", "outcome", "prognos",
    ]):
        return "Clinical / disease"

    if any(kw in t for kw in [
        "visual", "retina", "pulvinar", "lgn", "geniculate",
        "auditory", "sound", "tinnitus", "acoustic",
        "somatosens", "whisker", "barrel",
        "taste", "gustatory", "chemosens", "olfact",
        "speech", "language", "aphasia",
        "sensory", "percept",
    ]):
        return "Sensory / perception"

    if any(kw in t for kw in [
        "motor", "ganglia", "basal", "cerebellum", "locomot",
        "dystonia", "parkinson", "movement",
    ]):
        return "Motor"

    if any(kw in t for kw in [
        "fear", "conditioning", "extinction",
        "reward", "motivation", "incentive",
        "emotion", "emotional", "affective",
        "memory", "hippocamp", "learning",
        "attention", "executive",
        "decision", "choice",
        "paraventricular", "stress",
        "cognitive", "cognition", "behavior",
    ]):
        return "Cognitive / behavioral"

    if any(kw in t for kw in [
        "sleep", "wakefulness", "insomnia", "nrem",
        "spindle", "ripple", "oscillat",
        "arousal", "circadian",
    ]):
        return "Sleep / oscillations"

    if any(kw in t for kw in [
        "trn", "reticular nucleus", "inhibitory", "gabaergic", "gaba",
        "interneuron",
        "synap", "glutamat", "receptor",
        "calcium", "channel",
        "neuron", "firing", "burst",
        "dendrit", "axon terminal",
    ]):
        return "Circuit / cellular"

    if any(kw in t for kw in [
        "axon", "thalamocortical project", "development", "embryo", "prenatal",
        "progenitor", "neurogenesis", "patterning",
        "tcf7l2", "gene", "pax6", "shh", "fgf",
        "virus", "viral", "aav", "tracer", "tracing",
        "reunion", "connectom",
        "anatomy", "morpholog",
    ]):
        return "Development / anatomy"

    if any(kw in t for kw in [
        "volume", "grey matter", "voxel", "segment", "freesurfer",
        "mri", "fmri", "bold", "functional connectivity",
        "eeg", "meg", "electrophysiol",
        "diffusion", "tractograph", "dti",
        "pet", "spect",
        "atlas", "parcellat",
        "artery", "arteries", "vascular",
    ]):
        return "Methodology / imaging"

    return "Other"


print("  Assigning mega_theme to all sentences...")
df["mega_theme"] = df["sentence"].apply(assign_mega_theme_from_sentence)
print(f"  Mega-theme distribution:")
mt_counts = df["mega_theme"].value_counts()
for theme, count in mt_counts.items():
    pct = count / len(df) * 100
    print(f"    {theme:30s}: {count:7,} ({pct:5.1f}%)")

# ═══════════════════════════════════════════════════════════════════════════
# 2. CREATE BACKWARD-COMPATIBLE FILES
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("2. CREATING BACKWARD-COMPATIBLE FILES")
print("=" * 70)

# Backup old files
for fname in ["all_sentences_final.csv", "framing_combined_v3.csv"]:
    src = PROC / fname
    bak = PROC / fname.replace(".csv", "_old_backup.csv")
    if src.exists() and not bak.exists():
        src.rename(bak)
        print(f"  Backed up {fname} -> {bak.name}")

# Save enriched full corpus
df.to_csv(PROC / "all_sentences_full_corpus.csv", index=False)

# Create all_sentences_final.csv (what analysis scripts expect)
# Add placeholder BERTopic columns (empty) so scripts don't crash
df_compat = df.copy()
df_compat["bertopic_id"] = -1
df_compat["bertopic_label"] = ""
df_compat["bertopic_id_v2"] = -1
df_compat["bertopic_label_v2"] = ""
df_compat["topic_clean_v2"] = df_compat["mega_theme"]  # Use mega_theme as topic

df_compat.to_csv(PROC / "all_sentences_final.csv", index=False)
print(f"  Saved all_sentences_final.csv: {len(df_compat):,} rows")

# Create framing file (what confound_lme, task_extraction etc. expect)
framing = df[df["frame_type"].notna() & (df["frame_type"] != "")].copy()
framing.to_csv(PROC / "framing_combined_v3.csv", index=False)
framing.to_csv(PROC / "framing_combined_v2.csv", index=False)  # some scripts use v2
print(f"  Saved framing_combined_v2/v3.csv: {len(framing):,} rows")

# Copy embeddings
emb_src = PROC / "embeddings_full_corpus.npy"
emb_dst = PROC / "embeddings_final.npy"
if emb_src.exists():
    import shutil
    shutil.copy2(emb_src, emb_dst)
    print(f"  Copied embeddings -> embeddings_final.npy")

# ═══════════════════════════════════════════════════════════════════════════
# 3. RUN ALL ANALYSIS SCRIPTS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("3. RUNNING ALL ANALYSIS SCRIPTS")
print("=" * 70)


def run_script(path, timeout=600, desc=""):
    """Run a Python script and report success/failure."""
    print(f"\n  Running: {desc or path.name}")
    try:
        result = subprocess.run(
            [PYTHON, "-u", str(path)],
            capture_output=True, text=True, timeout=timeout,
            cwd=str(ROOT),
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        )
        if result.returncode == 0:
            # Print last 5 lines of output
            lines = result.stdout.strip().split("\n")
            for line in lines[-5:]:
                print(f"    | {line}")
            print(f"    OK ({desc})")
            return True
        else:
            # Print error
            err_lines = (result.stderr or result.stdout or "").strip().split("\n")
            for line in err_lines[-8:]:
                print(f"    ! {line}")
            print(f"    FAILED ({desc})")
            return False
    except subprocess.TimeoutExpired:
        print(f"    TIMEOUT ({desc})")
        return False
    except Exception as e:
        print(f"    ERROR: {e}")
        return False


scripts = [
    (CODE / "Analysis" / "generate_figures_v11.py", 300, "Main figures (Fig 1-8)"),
    (CODE / "Analysis" / "technique_analysis_v2.py", 300, "Technique analysis"),
    (CODE / "Analysis" / "task_extraction_v2.py", 300, "Task extraction"),
    (CODE / "Analysis" / "confound_lme_v2.py", 300, "Confound LME analysis"),
    (CODE / "Analysis" / "nucleus_similarity.py", 600, "Nucleus similarity clustering"),
    (CODE / "Analysis" / "advanced_nlp_analysis.py", 600, "Advanced NLP (dep. parsing, drift, topics, network)"),
]

results = {}
for script_path, timeout, desc in scripts:
    if script_path.exists():
        ok = run_script(script_path, timeout, desc)
        results[desc] = "OK" if ok else "FAILED"
    else:
        print(f"  SKIP: {script_path.name} not found")
        results[desc] = "SKIPPED"

# ═══════════════════════════════════════════════════════════════════════════
# 4. SUMMARY
# ═══════════════════════════════════════════════════════════════════════════
elapsed = time.time() - t0
print(f"\n{'='*70}")
print(f"RERUN COMPLETE — {elapsed/60:.1f} minutes")
print("=" * 70)

print(f"\n  Corpus: {len(df):,} sentences, {df['pmid'].nunique():,} papers")
print(f"  Framing: {len(framing):,} sentences")
print(f"  Mega-themes: {len(mt_counts)} groups")
print(f"\n  Script results:")
for desc, status in results.items():
    icon = "+" if status == "OK" else "X" if status == "FAILED" else "-"
    print(f"    [{icon}] {desc}: {status}")
print("=" * 70)
