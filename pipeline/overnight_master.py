"""
overnight_master.py — Complete overnight pipeline.

Chains all phases:
  A. Download OA papers (Unpaywall + Semantic Scholar + PMC)
  B. Process: tokenize, detect frames/nuclei/techniques, compute embeddings
  C. Embedding-based frame expansion + LLM validation
  D. Regenerate all figures + rebuild paper
  E. Quality report

Usage:
    python overnight_master.py [--max-papers 5000] [--skip-download] [--skip-figures]

Expected runtime: ~5-7 hours total on RTX 3060.
Logs to: Data/logs/overnight_master_YYYYMMDD.log
"""
import sys, io, os, re, time, subprocess, logging
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
CODE = ROOT / "Code"
PROC = ROOT / "Data" / "processed"
LOG_DIR = ROOT / "Data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

PYTHON = sys.executable

log_file = LOG_DIR / f"overnight_master_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(log_file), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


def run_script(script_path, args=None, timeout=None, description=""):
    """Run a Python script and log output."""
    cmd = [PYTHON, "-u", str(script_path)]
    if args:
        cmd.extend(args)

    log.info(f"Running: {description or script_path.name}")
    log.info(f"  Command: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
            encoding="utf-8", errors="replace",
        )
        # Log last 30 lines of output
        output_lines = result.stdout.strip().split("\n")
        for line in output_lines[-30:]:
            log.info(f"  | {line}")
        if result.returncode != 0:
            log.error(f"  FAILED (exit code {result.returncode})")
            err_lines = result.stderr.strip().split("\n")
            for line in err_lines[-10:]:
                log.error(f"  ! {line}")
            return False
        log.info(f"  OK")
        return True
    except subprocess.TimeoutExpired:
        log.error(f"  TIMEOUT after {timeout}s")
        return False
    except Exception as e:
        log.error(f"  ERROR: {e}")
        return False


def count_data():
    """Count current data state."""
    import pandas as pd
    counts = {}

    # Text files
    text_dir = ROOT / "Data" / "interim" / "scihub_text"
    counts["new_text_files"] = len(list(text_dir.glob("*.txt"))) if text_dir.exists() else 0

    # Existing sentences
    f = PROC / "all_sentences_final.csv"
    if f.exists():
        df = pd.read_csv(f, usecols=["pmid"])
        counts["existing_papers"] = df["pmid"].nunique()
        counts["existing_sentences"] = len(df)

    # Sci-Hub sentences
    f = PROC / "scihub_sentences.csv"
    if f.exists():
        df = pd.read_csv(f)
        counts["scihub_sentences"] = len(df)
        counts["scihub_papers"] = df["pmid"].nunique()
        counts["scihub_framing"] = (df["frame_type"].fillna("") != "").sum()

    # Latest framing combined
    fc_files = sorted(PROC.glob("framing_combined_v*.csv"), reverse=True)
    if fc_files:
        df = pd.read_csv(fc_files[0])
        counts["total_framing"] = len(df)
        counts["framing_file"] = fc_files[0].name

    # LLM validated
    f = PROC / "llm_frame_validation.csv"
    if f.exists():
        df = pd.read_csv(f)
        counts["llm_validated"] = df["llm_validated"].sum() if "llm_validated" in df.columns else 0

    return counts


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-papers", type=int, default=5000)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-figures", action="store_true")
    args = parser.parse_args()

    start_time = time.time()
    log.info("=" * 70)
    log.info("OVERNIGHT MASTER PIPELINE")
    log.info(f"Started: {datetime.now().isoformat()}")
    log.info(f"Config: max_papers={args.max_papers}, skip_download={args.skip_download}")
    log.info("=" * 70)

    # Pre-flight counts
    log.info("\n--- PRE-FLIGHT DATA COUNTS ---")
    pre_counts = count_data()
    for k, v in pre_counts.items():
        log.info(f"  {k}: {v}")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE A: DOWNLOAD OA PAPERS
    # ═══════════════════════════════════════════════════════════════════
    if not args.skip_download:
        log.info("\n" + "=" * 70)
        log.info("PHASE A: DOWNLOADING OA PAPERS")
        log.info("=" * 70)
        run_script(
            CODE / "pipeline" / "16b_fast_oa_download.py",
            timeout=6 * 3600,  # 6 hour timeout
            description="Fast 2-phase OA download (Unpaywall + S2, cached + parallel)",
        )

    # ═══════════════════════════════════════════════════════════════════
    # PHASE B: PROCESS NEW PAPERS
    # ═══════════════════════════════════════════════════════════════════
    log.info("\n" + "=" * 70)
    log.info("PHASE B: PROCESSING NEW PAPERS")
    log.info("=" * 70)
    run_script(
        CODE / "pipeline" / "14_integrate_scihub_text.py",
        timeout=1800,  # 30 min
        description="Tokenize + detect frames/nuclei/techniques + GPU embeddings",
    )

    # ═══════════════════════════════════════════════════════════════════
    # PHASE C: EMBEDDING-BASED FRAME EXPANSION + LLM VALIDATION
    # ═══════════════════════════════════════════════════════════════════
    log.info("\n" + "=" * 70)
    log.info("PHASE C: EMBEDDING FRAME EXPANSION + LLM VALIDATION")
    log.info("=" * 70)

    # Re-run embedding detection on expanded data
    run_script(
        CODE / "pipeline" / "11_embedding_frame_detection.py",
        timeout=600,
        description="Embedding-based frame detection",
    )

    # LLM validation
    run_script(
        CODE / "pipeline" / "15_llm_frame_validation.py",
        timeout=3600,  # 1 hour
        description="LLM frame validation (DeBERTa on GPU)",
    )

    # ═══════════════════════════════════════════════════════════════════
    # PHASE D: REGENERATE FIGURES + PAPER
    # ═══════════════════════════════════════════════════════════════════
    if not args.skip_figures:
        log.info("\n" + "=" * 70)
        log.info("PHASE D: REGENERATING FIGURES AND PAPER")
        log.info("=" * 70)

        scripts = [
            (CODE / "analysis" / "generate_figures_v11.py", "Figures 1-4, 6, S1-S3"),
            (CODE / "analysis" / "nucleus_similarity.py", "Figure 5 (clustering)"),
            (CODE / "analysis" / "technique_analysis_v2.py", "Figure 7 (techniques)"),
            (CODE / "analysis" / "task_extraction_v2.py", "Figure 8 (tasks)"),
            (CODE / "analysis" / "confound_lme_v2.py", "Figure 9 (confound)"),
        ]
        for script, desc in scripts:
            if script.exists():
                run_script(script, timeout=1800, description=desc)
            else:
                log.warning(f"  Script not found: {script}")

        # Rebuild paper
        paper_script = CODE / "paper" / "build_paper_v13.py"
        if paper_script.exists():
            run_script(paper_script, timeout=120, description="Build paper v13")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE D2: ADVANCED NLP ANALYSIS
    # ═══════════════════════════════════════════════════════════════════
    log.info("\n" + "=" * 70)
    log.info("PHASE D2: ADVANCED NLP ANALYSIS")
    log.info("=" * 70)

    advanced_nlp = CODE / "analysis" / "advanced_nlp_analysis.py"
    if advanced_nlp.exists():
        run_script(advanced_nlp, timeout=1800, description="Dep. parsing, semantic drift, dynamic topics, co-occurrence network")

    # ═══════════════════════════════════════════════════════════════════
    # PHASE E: QUALITY REPORT
    # ═══════════════════════════════════════════════════════════════════
    log.info("\n" + "=" * 70)
    log.info("PHASE E: QUALITY REPORT")
    log.info("=" * 70)

    post_counts = count_data()
    log.info("\n--- POST-PIPELINE DATA COUNTS ---")
    for k, v in post_counts.items():
        log.info(f"  {k}: {v}")

    log.info("\n--- CHANGES ---")
    for k in set(list(pre_counts.keys()) + list(post_counts.keys())):
        pre = pre_counts.get(k, 0)
        post = post_counts.get(k, 0)
        if isinstance(pre, (int, float)) and isinstance(post, (int, float)):
            diff = post - pre
            if diff != 0:
                log.info(f"  {k}: {pre:,} -> {post:,} ({diff:+,})")

    elapsed = time.time() - start_time
    log.info(f"\nTotal runtime: {elapsed/3600:.1f} hours")
    log.info(f"Log file: {log_file}")
    log.info("=" * 70)
    log.info("OVERNIGHT PIPELINE COMPLETE")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
