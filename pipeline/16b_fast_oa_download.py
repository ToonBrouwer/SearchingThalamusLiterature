"""
16b_fast_oa_download.py — Fast two-phase OA download.

Phase 1: Batch-check ALL DOIs against Unpaywall + Semantic Scholar (fast, 5-10/sec)
Phase 2: Download only the confirmed OA PDFs (parallel-ish)

Much faster than the sequential approach in 16_unpaywall_download.py
"""
import sys, io, os, re, time, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"
PDF_DIR = INT / "oa_pdfs"
TEXT_DIR = INT / "scihub_text"  # shared output dir
PDF_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR.mkdir(parents=True, exist_ok=True)

EMAIL = "e.a.brouwer2@uva.nl"

def extract_text_from_pdf(pdf_path):
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        text = "".join(page.get_text() + "\n" for page in doc)
        doc.close()
        return text
    except:
        return ""


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 1: BATCH CHECK OA AVAILABILITY
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("PHASE 1: BATCH CHECKING OA AVAILABILITY")
print("=" * 70)

# Load targets
targets = pd.read_csv(PROC / "scihub_download_list.csv")
print(f"Total papers with DOI: {len(targets):,}")

# Skip already downloaded
already = set()
existing_sents = pd.read_csv(PROC / "all_sentences_final.csv", usecols=["pmid"])
already.update(existing_sents["pmid"].unique())
for f in TEXT_DIR.glob("*.txt"):
    if f.stat().st_size > 100:
        already.add(int(f.stem))
targets = targets[~targets["pmid"].isin(already)].reset_index(drop=True)
print(f"Already have: {len(already):,}, to check: {len(targets):,}")

# Check existing results to skip already-checked DOIs
oa_cache_path = PROC / "oa_check_cache.json"
oa_cache = {}
if oa_cache_path.exists():
    oa_cache = json.loads(oa_cache_path.read_text())
    print(f"Loaded {len(oa_cache)} cached OA checks")

# Unpaywall batch check (fast: ~5-8 per second)
print(f"\nChecking Unpaywall API...")
oa_urls = {}  # pmid -> (pdf_url, source)
n_checked = 0
n_oa = 0

for i, (_, row) in enumerate(targets.iterrows()):
    doi = str(row["doi"])
    pmid = int(row["pmid"])

    # Check cache
    if doi in oa_cache:
        if oa_cache[doi]:
            oa_urls[pmid] = (oa_cache[doi], "cached")
            n_oa += 1
        n_checked += 1
        continue

    # Unpaywall
    pdf_url = None
    try:
        r = requests.get(f"https://api.unpaywall.org/v2/{doi}?email={EMAIL}", timeout=8)
        if r.status_code == 200:
            oa_loc = r.json().get("best_oa_location") or {}
            pdf_url = oa_loc.get("url_for_pdf")
    except:
        pass

    # Semantic Scholar (if Unpaywall didn't find it)
    if not pdf_url:
        try:
            r = requests.get(
                f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=openAccessPdf",
                timeout=8)
            if r.status_code == 200:
                pdf_info = r.json().get("openAccessPdf") or {}
                pdf_url = pdf_info.get("url")
        except:
            pass
        time.sleep(0.05)  # small extra delay for S2

    # Cache result
    oa_cache[doi] = pdf_url or ""
    if pdf_url:
        oa_urls[pmid] = (pdf_url, "unpaywall+s2")
        n_oa += 1
    n_checked += 1

    time.sleep(0.12)  # ~8 checks/sec

    if (i + 1) % 200 == 0:
        print(f"  Checked {i+1}/{len(targets)}: {n_oa} OA found ({n_oa/(i+1)*100:.0f}%)")
        # Save cache periodically
        oa_cache_path.write_text(json.dumps(oa_cache))

# Final cache save
oa_cache_path.write_text(json.dumps(oa_cache))
print(f"\nPhase 1 done: {n_checked:,} checked, {n_oa:,} OA found ({n_oa/max(n_checked,1)*100:.0f}%)")
print(f"Cache saved: {len(oa_cache):,} entries")

# ═══════════════════════════════════════════════════════════════════════════
# PHASE 2: DOWNLOAD OA PDFs
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"PHASE 2: DOWNLOADING {len(oa_urls):,} OA PDFs")
print("=" * 70)

n_success = 0
n_fail = 0

def download_one(pmid, pdf_url):
    """Download and extract text from one paper."""
    pdf_path = PDF_DIR / f"{pmid}.pdf"
    txt_path = TEXT_DIR / f"{pmid}.txt"

    if txt_path.exists() and txt_path.stat().st_size > 100:
        return pmid, "already_done", 0

    try:
        r = requests.get(pdf_url, headers={"User-Agent": "Mozilla/5.0"},
                         timeout=30, allow_redirects=True)
        if r.status_code != 200 or len(r.content) < 3000:
            return pmid, "download_fail", 0

        # Check if it's a PDF
        if not (r.content[:5] == b'%PDF-' or 'application/pdf' in r.headers.get('content-type', '')):
            return pmid, "not_pdf", 0

        pdf_path.write_bytes(r.content)
        text = extract_text_from_pdf(pdf_path)
        if len(text) > 200:
            txt_path.write_text(text, encoding="utf-8")
            return pmid, "success", len(text)
        else:
            return pmid, "no_text", 0
    except:
        return pmid, "error", 0

# Download with thread pool (4 parallel downloads)
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {}
    for pmid, (pdf_url, source) in oa_urls.items():
        f = executor.submit(download_one, pmid, pdf_url)
        futures[f] = pmid

    for i, future in enumerate(as_completed(futures)):
        pmid, status, text_len = future.result()
        if status == "success":
            n_success += 1
        elif status == "already_done":
            n_success += 1
        else:
            n_fail += 1

        if (i + 1) % 50 == 0 or (n_success <= 10 and status == "success"):
            print(f"  [{i+1}/{len(futures)}] success={n_success} fail={n_fail} "
                  f"(last: PMID {pmid} = {status})")

print(f"\n{'='*70}")
print(f"DONE: {n_success:,} downloaded, {n_fail:,} failed out of {len(oa_urls):,} OA papers")
print(f"Total text files: {len(list(TEXT_DIR.glob('*.txt'))):,}")
print(f"{'='*70}")
