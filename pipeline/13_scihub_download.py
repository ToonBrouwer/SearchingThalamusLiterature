"""
13_scihub_download.py — Download full-text PDFs from Sci-Hub.

Downloads papers in batches, extracts text from PDFs, and integrates
with the existing pipeline.

Usage:
    python 13_scihub_download.py [--batch-size 50] [--start 0] [--max 500]
"""
import sys, io, os, time, re, argparse
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import requests
from pathlib import Path
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PDF_DIR = ROOT / "Data" / "interim" / "scihub_pdfs"
PDF_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR = ROOT / "Data" / "interim" / "scihub_text"
TEXT_DIR.mkdir(parents=True, exist_ok=True)

SCIHUB_MIRRORS = [
    "https://www.sci-hub.box",
    "https://sci-hub.st",
    "https://sci-hub.ru",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ═══════════════════════════════════════════════════════════════════════════
def get_pdf_url_from_scihub(doi, mirror=SCIHUB_MIRRORS[0]):
    """Fetch the Sci-Hub page and extract the direct PDF link."""
    url = f"{mirror}/{doi}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if resp.status_code != 200:
            return None

        # Look for the PDF embed/iframe URL
        # Sci-Hub typically has: <embed src="..." or <iframe src="..."
        html = resp.text

        # Find any src or href containing .pdf
        pdf_patterns = [
            r'(?:embed|iframe)\s+[^>]*src\s*=\s*["\']([^"\']+\.pdf[^"\']*)',
            r'(?:src|href)\s*=\s*["\']([^"\']*\.pdf[^"\']*)',
            r'location\.href\s*=\s*["\']([^"\']+\.pdf[^"\']*)',
        ]
        for pat in pdf_patterns:
            m = re.search(pat, html, re.I)
            if m:
                pdf_url = m.group(1)
                if pdf_url.startswith("//"):
                    pdf_url = "https:" + pdf_url
                elif pdf_url.startswith("/"):
                    pdf_url = mirror + pdf_url
                elif not pdf_url.startswith("http"):
                    pdf_url = mirror + "/" + pdf_url
                return pdf_url

        # Direct PDF response (check content-type)
        if "application/pdf" in resp.headers.get("content-type", ""):
            return url

        return None
    except Exception as e:
        return None


def download_pdf(pdf_url, save_path):
    """Download a PDF file."""
    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=60, stream=True)
        if resp.status_code == 200 and len(resp.content) > 1000:
            with open(save_path, "wb") as f:
                f.write(resp.content)
            return True
    except:
        pass
    return False


def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF using PyMuPDF (fitz) or pdfplumber."""
    text = ""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(pdf_path))
        for page in doc:
            text += page.get_text() + "\n"
        doc.close()
    except ImportError:
        try:
            import pdfplumber
            with pdfplumber.open(str(pdf_path)) as pdf:
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        text += t + "\n"
        except ImportError:
            print("    WARNING: Neither PyMuPDF nor pdfplumber installed")
            print("    Install with: pip install PyMuPDF  or  pip install pdfplumber")
            return ""
    return text


# ═══════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20, help="Papers per batch")
    parser.add_argument("--start", type=int, default=0, help="Start index")
    parser.add_argument("--max", type=int, default=100, help="Max papers to download")
    parser.add_argument("--mirror", type=str, default=SCIHUB_MIRRORS[0])
    args = parser.parse_args()

    # Load target list
    targets = pd.read_csv(PROC / "scihub_download_list.csv")
    print(f"Total Sci-Hub targets: {len(targets):,}")

    # Filter to requested range
    targets = targets.iloc[args.start : args.start + args.max]
    print(f"Processing: rows {args.start} to {args.start + len(targets)}")

    results = []
    n_success = 0
    n_fail = 0
    n_text_extracted = 0

    for i, (_, row) in enumerate(targets.iterrows()):
        pmid = row["pmid"]
        doi = row["doi"]
        pdf_path = PDF_DIR / f"{pmid}.pdf"
        txt_path = TEXT_DIR / f"{pmid}.txt"

        # Skip if already downloaded
        if txt_path.exists() and txt_path.stat().st_size > 100:
            results.append({"pmid": pmid, "status": "already_done"})
            n_success += 1
            continue

        if pdf_path.exists() and pdf_path.stat().st_size > 1000:
            # PDF exists, just extract text
            text = extract_text_from_pdf(pdf_path)
            if text and len(text) > 200:
                txt_path.write_text(text, encoding="utf-8")
                n_text_extracted += 1
                n_success += 1
                results.append({"pmid": pmid, "status": "text_extracted"})
            else:
                results.append({"pmid": pmid, "status": "pdf_no_text"})
                n_fail += 1
            continue

        # Try to get PDF from Sci-Hub
        print(f"  [{i+1}/{len(targets)}] PMID {pmid} (DOI: {doi[:40]}...)", end=" ")

        pdf_url = get_pdf_url_from_scihub(doi, mirror=args.mirror)
        if pdf_url is None:
            print("-> no PDF URL found")
            results.append({"pmid": pmid, "status": "no_pdf_url"})
            n_fail += 1
            time.sleep(1)
            continue

        success = download_pdf(pdf_url, pdf_path)
        if not success:
            print("-> download failed")
            results.append({"pmid": pmid, "status": "download_failed"})
            n_fail += 1
            time.sleep(1)
            continue

        # Extract text
        text = extract_text_from_pdf(pdf_path)
        if text and len(text) > 200:
            txt_path.write_text(text, encoding="utf-8")
            print(f"-> OK ({len(text):,} chars)")
            n_text_extracted += 1
            n_success += 1
            results.append({"pmid": pmid, "status": "success", "text_len": len(text)})
        else:
            print("-> PDF but no text extracted")
            results.append({"pmid": pmid, "status": "pdf_no_text"})
            n_fail += 1

        # Rate limit: be respectful
        time.sleep(2)

        # Status update every 20 papers
        if (i + 1) % 20 == 0:
            print(f"\n  Progress: {n_success}/{i+1} success, {n_fail} failed\n")

    # Save results
    results_df = pd.DataFrame(results)
    results_path = PROC / "scihub_download_results.csv"
    if results_path.exists():
        old = pd.read_csv(results_path)
        results_df = pd.concat([old, results_df]).drop_duplicates(subset="pmid", keep="last")
    results_df.to_csv(results_path, index=False)

    print(f"\n{'='*70}")
    print(f"RESULTS: {n_success} success, {n_fail} failed, {n_text_extracted} text extracted")
    print(f"PDFs saved to: {PDF_DIR}")
    print(f"Text saved to: {TEXT_DIR}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
