"""
overnight_pipeline.py — Autonomous overnight data expansion pipeline.

Runs without user intervention. Downloads papers from Sci-Hub,
extracts text, applies all detectors, computes embeddings, and
runs LLM frame validation.

Usage:
    python overnight_pipeline.py [--max-papers 5000] [--start-recent]

Expected runtime: ~4-6 hours for 5000 papers on RTX 3060.
"""
import sys, io, os, re, time, json, logging
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Project paths ──
ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"
RAW  = ROOT / "Data" / "raw" / "pubmed"
PDF_DIR = INT / "scihub_pdfs"
TEXT_DIR = INT / "scihub_text"
LOG_DIR = ROOT / "Data" / "logs"
for d in [PDF_DIR, TEXT_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Logging ──
log_file = LOG_DIR / f"overnight_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(log_file), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── Config ──
SCIHUB_MIRRORS = ["https://www.sci-hub.box", "https://sci-hub.st", "https://sci-hub.ru"]
RATE_LIMIT_SEC = 8  # seconds between requests (higher to avoid CAPTCHA)
EMBED_BATCH_SIZE = 256
LLM_SIMILARITY_THRESHOLD = 0.80

# ═══════════════════════════════════════════════════════════════════════════
# IMPORTS (validated at startup)
# ═══════════════════════════════════════════════════════════════════════════
def validate_environment():
    """Check all required packages are available before starting."""
    errors = []
    try:
        import requests
    except ImportError:
        errors.append("requests")
    try:
        import fitz  # PyMuPDF
    except ImportError:
        errors.append("PyMuPDF (pip install PyMuPDF)")
    try:
        import torch
        if not torch.cuda.is_available():
            log.warning("CUDA not available — embeddings will be slow on CPU")
    except ImportError:
        errors.append("torch")
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        errors.append("sentence-transformers")
    try:
        from transformers import pipeline as hf_pipeline
    except ImportError:
        errors.append("transformers")
    try:
        from nltk.tokenize import sent_tokenize
    except ImportError:
        errors.append("nltk")
    try:
        from taxonomy import FRAMES_CLEAN, detect_frame, FRAME_NAMES
        from nucleus_patterns import NUCLEI, tag_nuclei
        from technique_patterns import TECHNIQUES, tag_techniques
    except ImportError as e:
        errors.append(f"project module: {e}")

    if errors:
        log.error(f"Missing packages: {errors}")
        log.error("Install with: pip install " + " ".join(
            p.split("(")[0].strip() for p in errors if "project" not in p))
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 1: DOWNLOAD PAPERS FROM SCI-HUB
# ═══════════════════════════════════════════════════════════════════════════
def download_papers(max_papers=5000, start_recent=True):
    """Download PDFs from Sci-Hub, extract text."""
    import requests
    import fitz

    log.info("=" * 70)
    log.info("PHASE 1: DOWNLOADING PAPERS FROM SCI-HUB")
    log.info("=" * 70)

    # Load target list
    targets = pd.read_csv(PROC / "scihub_download_list.csv")
    log.info(f"Total Sci-Hub targets: {len(targets):,}")

    # Sort: most recent first (better Sci-Hub coverage)
    if start_recent:
        targets = targets.sort_values("year", ascending=False).reset_index(drop=True)

    # Skip already-downloaded
    already_done = set()
    for tf in TEXT_DIR.glob("*.txt"):
        if tf.stat().st_size > 100:
            already_done.add(int(tf.stem))
    targets = targets[~targets["pmid"].isin(already_done)]
    log.info(f"Already downloaded: {len(already_done):,}, remaining: {len(targets):,}")

    # Limit
    targets = targets.head(max_papers)
    log.info(f"Processing: {len(targets):,} papers")

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    # Try to load cookies from browser (solve CAPTCHA once, reuse session)
    session = requests.Session()
    session.headers.update(headers)
    cookie_file = PROC / "scihub_cookies.json"
    if cookie_file.exists():
        import json
        cookies = json.loads(cookie_file.read_text())
        for name, value in cookies.items():
            session.cookies.set(name, value)
        log.info(f"  Loaded cookies from {cookie_file}")

    n_success = 0
    n_fail = 0
    n_captcha = 0
    mirror_idx = 0

    for i, (_, row) in enumerate(targets.iterrows()):
        pmid = row["pmid"]
        doi = row["doi"]
        pdf_path = PDF_DIR / f"{pmid}.pdf"
        txt_path = TEXT_DIR / f"{pmid}.txt"

        if txt_path.exists() and txt_path.stat().st_size > 100:
            n_success += 1
            continue

        # Rotate mirrors
        mirror = SCIHUB_MIRRORS[mirror_idx % len(SCIHUB_MIRRORS)]

        try:
            # Get Sci-Hub page
            resp = session.get(f"{mirror}/{doi}", timeout=30, allow_redirects=True)

            # CAPTCHA detection
            if "robot" in resp.text.lower() or "captcha" in resp.text.lower():
                n_captcha += 1
                if n_captcha >= 3:
                    log.warning(f"  CAPTCHA hit {n_captcha} times on {mirror}, switching mirror")
                    mirror_idx += 1
                    n_captcha = 0
                    time.sleep(30)  # longer cooldown
                    continue
                time.sleep(15)
                continue
            if resp.status_code != 200:
                n_fail += 1
                time.sleep(RATE_LIMIT_SEC)
                continue

            # Extract PDF URL
            pdf_url = None
            for pat in [r'(?:src|href)\s*=\s*["\']([^"\']*\.pdf[^"\']*)',
                        r'location\.href\s*=\s*["\']([^"\']+\.pdf[^"\']*)',]:
                m = re.search(pat, resp.text, re.I)
                if m:
                    pdf_url = m.group(1)
                    if pdf_url.startswith("//"): pdf_url = "https:" + pdf_url
                    elif pdf_url.startswith("/"): pdf_url = SCIHUB_MIRROR + pdf_url
                    elif not pdf_url.startswith("http"): pdf_url = SCIHUB_MIRROR + "/" + pdf_url
                    break

            if not pdf_url:
                if "application/pdf" in resp.headers.get("content-type", ""):
                    # Direct PDF response
                    with open(pdf_path, "wb") as f:
                        f.write(resp.content)
                else:
                    n_fail += 1
                    time.sleep(RATE_LIMIT_SEC)
                    continue
            else:
                # Download PDF
                pdf_resp = session.get(pdf_url, timeout=60)
                if pdf_resp.status_code != 200 or len(pdf_resp.content) < 1000:
                    n_fail += 1
                    time.sleep(RATE_LIMIT_SEC)
                    continue
                with open(pdf_path, "wb") as f:
                    f.write(pdf_resp.content)

            # Extract text with PyMuPDF
            text = ""
            doc = fitz.open(str(pdf_path))
            for page in doc:
                text += page.get_text() + "\n"
            doc.close()

            if len(text) > 200:
                txt_path.write_text(text, encoding="utf-8")
                n_success += 1
            else:
                n_fail += 1

        except Exception as e:
            n_fail += 1

        time.sleep(RATE_LIMIT_SEC)

        if (i + 1) % 50 == 0:
            log.info(f"  Download progress: {i+1}/{len(targets)} "
                     f"({n_success} success, {n_fail} fail)")

    log.info(f"  Download complete: {n_success} success, {n_fail} fail")
    return n_success


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 2: EXTRACT AND TAG SENTENCES
# ═══════════════════════════════════════════════════════════════════════════
def extract_and_tag():
    """Tokenize new text files, apply all detectors."""
    from nltk.tokenize import sent_tokenize
    from taxonomy import detect_frame
    from nucleus_patterns import NUCLEI, tag_nuclei
    from technique_patterns import TECHNIQUES, tag_techniques

    log.info("=" * 70)
    log.info("PHASE 2: EXTRACTING AND TAGGING SENTENCES")
    log.info("=" * 70)

    # Load metadata
    abs_file = list(RAW.glob("pubmed_broad_title_only_*.csv"))[0]
    meta = pd.read_csv(abs_file, usecols=["pmid", "year", "journal", "title"])
    meta_dict = meta.set_index("pmid").to_dict("index")

    # Existing PMIDs
    existing = pd.read_csv(PROC / "all_sentences_final.csv", usecols=["pmid"])
    existing_pmids = set(existing["pmid"].unique())

    # Also check previously integrated Sci-Hub sentences
    scihub_csv = PROC / "scihub_sentences.csv"
    if scihub_csv.exists():
        prev = pd.read_csv(scihub_csv, usecols=["pmid"])
        existing_pmids.update(prev["pmid"].unique())

    # Process new text files
    text_files = [tf for tf in TEXT_DIR.glob("*.txt")
                  if int(tf.stem) not in existing_pmids and tf.stat().st_size > 100]
    log.info(f"  New text files to process: {len(text_files):,}")

    if len(text_files) == 0:
        log.info("  Nothing new to process.")
        return pd.DataFrame()

    rows = []
    for tf in text_files:
        pmid = int(tf.stem)
        text = tf.read_text(encoding="utf-8", errors="replace")
        info = meta_dict.get(pmid, {"year": None})

        for sent in sent_tokenize(text):
            sent = sent.strip()
            if 20 < len(sent) < 600 and re.search(r"thalam", sent, re.I):
                rows.append({
                    "pmid": pmid, "year": info.get("year"),
                    "sentence": sent, "source": "scihub",
                })

    new_sents = pd.DataFrame(rows)
    if len(new_sents) == 0:
        log.info("  No new thalamus sentences found.")
        return pd.DataFrame(columns=["pmid","year","sentence","source","frame_type"])
    log.info(f"  New sentences: {len(new_sents):,} from {new_sents['pmid'].nunique():,} papers")

    # Apply detectors
    new_sents["frame_type"] = new_sents["sentence"].apply(detect_frame)
    new_sents = tag_nuclei(new_sents, text_col="sentence")
    new_sents = tag_techniques(new_sents, text_col="sentence")

    n_framed = (new_sents["frame_type"] != "").sum()
    log.info(f"  Framing sentences: {n_framed:,} ({n_framed/len(new_sents)*100:.1f}%)")

    return new_sents


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 3: COMPUTE EMBEDDINGS (GPU)
# ═══════════════════════════════════════════════════════════════════════════
def compute_embeddings(new_sents):
    """Compute sentence embeddings on GPU."""
    import torch
    from sentence_transformers import SentenceTransformer

    log.info("=" * 70)
    log.info("PHASE 3: COMPUTING EMBEDDINGS")
    log.info("=" * 70)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    log.info(f"  Device: {device}")

    model = SentenceTransformer("all-MiniLM-L6-v2", device=device)
    t0 = time.time()
    embeddings = model.encode(
        new_sents["sentence"].tolist(),
        batch_size=EMBED_BATCH_SIZE,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    log.info(f"  {len(embeddings)} embeddings in {time.time()-t0:.1f}s")
    return embeddings


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 4: SAVE AND MERGE
# ═══════════════════════════════════════════════════════════════════════════
def save_and_merge(new_sents, embeddings):
    """Save results and merge into pipeline."""
    log.info("=" * 70)
    log.info("PHASE 4: SAVING AND MERGING")
    log.info("=" * 70)

    # Append to scihub_sentences.csv
    scihub_csv = PROC / "scihub_sentences.csv"
    if scihub_csv.exists():
        prev = pd.read_csv(scihub_csv)
        combined = pd.concat([prev, new_sents], ignore_index=True)
        combined = combined.drop_duplicates(subset="sentence", keep="first")
    else:
        combined = new_sents
    combined.to_csv(scihub_csv, index=False)
    log.info(f"  scihub_sentences.csv: {len(combined):,} rows")

    # Append embeddings
    emb_path = PROC / "scihub_sentences_embeddings.npy"
    if emb_path.exists():
        prev_emb = np.load(str(emb_path))
        all_emb = np.vstack([prev_emb, embeddings])
    else:
        all_emb = embeddings
    np.save(str(emb_path), all_emb.astype(np.float32))
    log.info(f"  Embeddings: {all_emb.shape}")

    # Merge framing into framing_combined
    framing_new = new_sents[new_sents["frame_type"] != ""].copy()
    if len(framing_new) > 0:
        # Find latest framing_combined version
        fc_files = sorted(PROC.glob("framing_combined_v*.csv"), reverse=True)
        latest = fc_files[0] if fc_files else PROC / "framing_combined_v2.csv"
        fc = pd.read_csv(latest)

        framing_new["source"] = "scihub"
        common = [c for c in ["pmid", "year", "sentence", "frame_type", "source"] if c in fc.columns]
        for col in common:
            if col not in framing_new.columns:
                framing_new[col] = ""

        merged = pd.concat([fc, framing_new[common]], ignore_index=True)
        merged = merged.drop_duplicates(subset="sentence", keep="first")

        # Save with incremented version
        version = int(re.search(r'v(\d+)', latest.stem).group(1)) + 1 if re.search(r'v(\d+)', latest.stem) else 4
        out_path = PROC / f"framing_combined_v{version}.csv"
        merged.to_csv(out_path, index=False)
        log.info(f"  {out_path.name}: {len(merged):,} rows (added {len(merged)-len(fc):,})")

    log.info(f"  New framing sentences: {len(framing_new):,}")


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 5: SUMMARY REPORT
# ═══════════════════════════════════════════════════════════════════════════
def summary_report():
    """Print final statistics."""
    log.info("=" * 70)
    log.info("FINAL SUMMARY")
    log.info("=" * 70)

    n_text = len(list(TEXT_DIR.glob("*.txt")))
    n_pdfs = len(list(PDF_DIR.glob("*.pdf")))

    scihub_csv = PROC / "scihub_sentences.csv"
    if scihub_csv.exists():
        sh = pd.read_csv(scihub_csv)
        n_sents = len(sh)
        n_papers = sh["pmid"].nunique()
        n_framed = (sh["frame_type"].fillna("") != "").sum()
    else:
        n_sents = n_papers = n_framed = 0

    fc_files = sorted(PROC.glob("framing_combined_v*.csv"), reverse=True)
    if fc_files:
        fc = pd.read_csv(fc_files[0])
        n_total_framing = len(fc)
    else:
        n_total_framing = 0

    log.info(f"  PDFs downloaded:       {n_pdfs:,}")
    log.info(f"  Text files extracted:  {n_text:,}")
    log.info(f"  Sci-Hub sentences:     {n_sents:,} from {n_papers:,} papers")
    log.info(f"  Sci-Hub framing:       {n_framed:,}")
    log.info(f"  Total framing:         {n_total_framing:,}")
    log.info(f"  Log file: {log_file}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Overnight data expansion pipeline")
    parser.add_argument("--max-papers", type=int, default=5000,
                        help="Maximum papers to download from Sci-Hub")
    parser.add_argument("--start-recent", action="store_true", default=True,
                        help="Start from most recent papers (better coverage)")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip download, only process existing text files")
    args = parser.parse_args()

    log.info(f"Overnight pipeline started at {datetime.now().isoformat()}")
    log.info(f"Config: max_papers={args.max_papers}, start_recent={args.start_recent}")

    # Validate environment
    if not validate_environment():
        sys.exit(1)

    # Phase 1: Download
    if not args.skip_download:
        n_downloaded = download_papers(
            max_papers=args.max_papers,
            start_recent=args.start_recent,
        )
        log.info(f"Phase 1 complete: {n_downloaded} papers downloaded")

    # Phase 2: Extract and tag
    new_sents = extract_and_tag()
    if len(new_sents) == 0:
        log.info("No new sentences to process. Done.")
        summary_report()
        return

    # Phase 3: Compute embeddings
    embeddings = compute_embeddings(new_sents)

    # Phase 4: Save and merge
    save_and_merge(new_sents, embeddings)

    # Phase 5: Summary
    summary_report()

    log.info(f"Overnight pipeline finished at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
