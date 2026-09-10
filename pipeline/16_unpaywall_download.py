"""
16_unpaywall_download.py — Download open-access papers via multiple free APIs.

Sources (tried in order):
  1. Unpaywall API — legal OA, ~30-40% hit rate
  2. Semantic Scholar API — often finds different OA copies
  3. PubMed Central — direct PDF for PMC articles

No CAPTCHA needed. All legal OA access.
"""
import sys, io, os, re, time, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import requests
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"
PDF_DIR = INT / "unpaywall_pdfs"
TEXT_DIR = INT / "scihub_text"  # same output dir as scihub for integration
PDF_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR.mkdir(parents=True, exist_ok=True)

EMAIL = "e.a.brouwer2@uva.nl"
RATE_LIMIT = 0.15  # seconds between API requests

def extract_text_from_pdf(pdf_path):
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        text = ""
        for page in doc:
            text += page.get_text() + "\n"
        doc.close()
        return text
    except:
        return ""

def find_pdf_url(doi, pmid=None):
    """Try multiple APIs to find a free PDF URL."""
    headers = {"User-Agent": "Mozilla/5.0"}

    # 1. Unpaywall
    try:
        r = requests.get(f"https://api.unpaywall.org/v2/{doi}?email={EMAIL}", timeout=8)
        if r.status_code == 200:
            oa_loc = r.json().get("best_oa_location") or {}
            url = oa_loc.get("url_for_pdf")
            if url:
                return url, "unpaywall"
    except:
        pass

    # 2. Semantic Scholar
    try:
        r = requests.get(f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=openAccessPdf", timeout=8)
        if r.status_code == 200:
            pdf_info = r.json().get("openAccessPdf") or {}
            url = pdf_info.get("url")
            if url:
                return url, "semantic_scholar"
    except:
        pass

    # 3. PubMed Central (if we have PMID)
    if pmid:
        try:
            # Check if there's a PMC version
            r = requests.get(
                f"https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={pmid}&format=json",
                timeout=8)
            if r.status_code == 200:
                records = r.json().get("records", [])
                if records and records[0].get("pmcid"):
                    pmcid = records[0]["pmcid"]
                    return f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/pdf/", "pmc"
        except:
            pass

    return None, None

# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("UNPAYWALL OPEN ACCESS DOWNLOAD")
print("=" * 70)

# Load targets
targets = pd.read_csv(PROC / "scihub_download_list.csv")
print(f"Total papers with DOI: {len(targets):,}")

# Skip papers we already have full text for (PMC + Sci-Hub)
already = set()
# Existing full-text from PMC pipeline
existing_sents = pd.read_csv(PROC / "all_sentences_final.csv", usecols=["pmid"])
already.update(existing_sents["pmid"].unique())
# Already downloaded Sci-Hub text
for f in TEXT_DIR.glob("*.txt"):
    if f.stat().st_size > 100:
        already.add(int(f.stem))
targets = targets[~targets["pmid"].isin(already)]
print(f"Already have text (PMC + Sci-Hub): {len(already):,}, remaining: {len(targets):,}")

# Combined search + download: try Unpaywall, Semantic Scholar, PMC
print(f"\nSearching and downloading from multiple OA sources...")
n_success = 0
n_fail = 0
n_not_found = 0
source_counts = {"unpaywall": 0, "semantic_scholar": 0, "pmc": 0}
results = []

for i, (_, row) in enumerate(targets.iterrows()):
    doi = row["doi"]
    pmid = row["pmid"]
    txt_path = TEXT_DIR / f"{pmid}.txt"
    pdf_path = PDF_DIR / f"{pmid}.pdf"

    if txt_path.exists() and txt_path.stat().st_size > 100:
        n_success += 1
        continue

    # Find PDF URL from multiple sources
    pdf_url, source = find_pdf_url(doi, pmid)

    if not pdf_url:
        n_not_found += 1
        results.append({"pmid": pmid, "doi": doi, "status": "not_found"})
        time.sleep(RATE_LIMIT)
        continue

    # Download PDF
    try:
        r = requests.get(pdf_url, headers={"User-Agent": "Mozilla/5.0"},
                         timeout=30, allow_redirects=True)
        if r.status_code == 200 and len(r.content) > 5000:
            # Check if it's actually a PDF
            if r.content[:5] == b'%PDF-' or 'application/pdf' in r.headers.get('content-type', ''):
                pdf_path.write_bytes(r.content)
                text = extract_text_from_pdf(pdf_path)
                if len(text) > 200:
                    txt_path.write_text(text, encoding="utf-8")
                    n_success += 1
                    source_counts[source] = source_counts.get(source, 0) + 1
                    results.append({"pmid": pmid, "doi": doi, "status": "success", "source": source})
                    if n_success <= 5 or n_success % 50 == 0:
                        print(f"  [{n_success}] PMID {pmid} via {source}: {len(text):,} chars")
                else:
                    n_fail += 1
                    results.append({"pmid": pmid, "doi": doi, "status": "no_text", "source": source})
            else:
                n_fail += 1
                results.append({"pmid": pmid, "doi": doi, "status": "not_pdf", "source": source})
        else:
            n_fail += 1
            results.append({"pmid": pmid, "doi": doi, "status": "download_fail", "source": source})
    except Exception as e:
        n_fail += 1
        results.append({"pmid": pmid, "doi": doi, "status": f"error", "source": source})

    time.sleep(RATE_LIMIT * 3)  # slightly slower for PDF downloads

    if (i + 1) % 100 == 0:
        print(f"  Progress: {i+1}/{len(targets)} checked | "
              f"{n_success} downloaded | {n_not_found} not OA | {n_fail} failed")
        print(f"    Sources: {source_counts}")

print(f"\n{'='*70}")
print(f"DONE: {n_success} downloaded, {n_not_found} not OA, {n_fail} failed")
print(f"Sources: {source_counts}")
print(f"Text files in: {TEXT_DIR}")
print(f"{'='*70}")

# Save results
pd.DataFrame(results).to_csv(PROC / "oa_download_results.csv", index=False)
