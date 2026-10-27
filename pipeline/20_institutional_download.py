"""
20_institutional_download.py — Download full-text papers via UvA LibKey.

Uses Playwright with your existing Chrome profile (inherits all logins
including UvA SSO). Chrome must be CLOSED before running this script.

UvA LibKey library ID: 320
URL pattern: https://libkey.io/libraries/320/{doi}

Usage:
  python 20_institutional_download.py --test 10        # test first 10
  python 20_institutional_download.py --download        # full batch
  python 20_institutional_download.py --download --max 500 --start 0

Outputs:
  Data/interim/institutional_text/{pmid}.txt
  Data/interim/institutional_pdfs/{pmid}.pdf
  Data/processed/institutional_download_log.csv
"""
import sys, os, re, json, time, argparse
os.environ["PYTHONIOENCODING"] = "utf-8"

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"
TEXT_DIR = INT / "institutional_text"
PDF_DIR  = INT / "institutional_pdfs"
LOG_FILE = PROC / "institutional_download_log.csv"

TEXT_DIR.mkdir(parents=True, exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)

# Chrome profile
CHROME_USER_DATA = Path(os.environ.get(
    "CHROME_USER_DATA",
    os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome",
                 "User Data")
))

# LibKey config
LIBKEY_LIBRARY_ID = 320
LIBKEY_BASE = f"https://libkey.io/libraries/{LIBKEY_LIBRARY_ID}"

# Limits
REQUEST_DELAY = 2.0    # seconds between papers
MAX_PER_SESSION = 5000
PAGE_TIMEOUT = 30000   # ms


# ══════════════════════════════════════════════════════════════════════════
# TEXT EXTRACTION FROM PDF
# ══════════════════════════════════════════════════════════════════════════

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using PyMuPDF (fitz)."""
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        text = ""
        for page in doc:
            text += page.get_text() + "\n"
        doc.close()
        return text.strip()
    except Exception as e:
        return ""


def extract_text_from_pdf_bytes(pdf_bytes):
    """Extract text from in-memory PDF bytes."""
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text() + "\n"
        doc.close()
        return text.strip()
    except Exception:
        return ""


# ══════════════════════════════════════════════════════════════════════════
# SINGLE PAPER DOWNLOAD (via Playwright browser)
# ══════════════════════════════════════════════════════════════════════════

def download_one_paper(page, pmid, doi, download_dir):
    """Navigate to LibKey URL, find/download PDF, extract text.

    Uses Playwright page object with existing authenticated session.
    """
    result = {"pmid": pmid, "doi": doi, "status": "unknown",
              "n_chars": 0, "error": ""}
    txt_path = TEXT_DIR / f"{pmid}.txt"
    pdf_path = PDF_DIR / f"{pmid}.pdf"

    try:
        url = f"{LIBKEY_BASE}/{doi}"

        # Navigate — LibKey redirects to publisher or directly to PDF
        resp = page.goto(url, wait_until="domcontentloaded",
                         timeout=PAGE_TIMEOUT)

        if resp is None:
            result["status"] = "no_response"
            result["error"] = "No response from page"
            return result

        status = resp.status
        if status == 429:
            result["status"] = "rate_limited"
            result["error"] = "HTTP 429"
            return result
        if status == 403:
            result["status"] = "access_denied"
            result["error"] = "HTTP 403"
            return result
        if status >= 400:
            result["status"] = "http_error"
            result["error"] = f"HTTP {status}"
            return result

        # Wait a moment for any JS redirects
        time.sleep(1.5)
        final_url = page.url

        # Check if we landed on a PDF directly (content type or URL ending)
        content_type = resp.headers.get("content-type", "")

        if "application/pdf" in content_type or final_url.lower().endswith(".pdf"):
            # We got a PDF directly — save it
            pdf_bytes = page.evaluate("""async () => {
                const resp = await fetch(window.location.href);
                const buf = await resp.arrayBuffer();
                return Array.from(new Uint8Array(buf));
            }""")
            # Actually, for PDFs loaded in browser, let's use a download approach
            pass  # Fall through to PDF link detection

        # We're on an HTML page — try to find PDF link
        html = page.content()

        # Check if LibKey says not available
        if "not available" in html.lower() and "this article" in html.lower():
            result["status"] = "not_available"
            result["error"] = "LibKey: article not available"
            return result

        # Strategy 1: Look for citation_pdf_url meta tag
        pdf_url = None
        m = re.search(
            r'<meta\s+(?:name=["\']citation_pdf_url["\']\s+content=["\']([^"\']+)["\']|'
            r'content=["\']([^"\']+)["\']\s+name=["\']citation_pdf_url["\'])',
            html, re.IGNORECASE)
        if m:
            pdf_url = m.group(1) or m.group(2)

        # Strategy 2: Look for PDF links in the page
        if not pdf_url:
            for pat in [
                r'href=["\']([^"\']*?/pdf/[^"\']+)["\']',
                r'href=["\']([^"\']*?\.pdf(?:\?[^"\']*)?)["\']',
                r'href=["\']([^"\']*?/pdfdirect/[^"\']+)["\']',
            ]:
                m = re.search(pat, html, re.IGNORECASE)
                if m:
                    candidate = m.group(1)
                    if not any(x in candidate.lower()
                               for x in ["javascript:", ".png", ".jpg", "icon"]):
                        pdf_url = candidate
                        break

        # Strategy 3: Try /doi/pdf/ pattern
        if not pdf_url:
            from urllib.parse import urlparse
            parsed = urlparse(final_url)
            if "/doi/" in parsed.path and "/pdf/" not in parsed.path:
                pdf_url = final_url.replace("/doi/", "/doi/pdf/")

        if not pdf_url:
            # Try extracting text from HTML directly
            text = extract_text_from_page(page)
            if text and len(text) > 1000:
                txt_path.write_text(text, encoding="utf-8")
                result["status"] = "html_text"
                result["n_chars"] = len(text)
                return result

            result["status"] = "no_pdf_link"
            result["error"] = f"No PDF on {final_url[:80]}"
            return result

        # Make absolute URL
        if pdf_url.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(final_url)
            pdf_url = f"{parsed.scheme}://{parsed.netloc}{pdf_url}"

        # Download PDF via Playwright (uses authenticated session)
        # Use download event to catch PDF downloads
        try:
            with page.expect_download(timeout=60000) as download_info:
                page.goto(pdf_url, timeout=PAGE_TIMEOUT)
            download = download_info.value
            dl_path = str(download_dir / f"{pmid}.pdf")
            download.save_as(dl_path)
            pdf_path_tmp = Path(dl_path)
        except Exception:
            # Maybe it loaded inline instead of triggering download
            # Try fetching via page context
            try:
                resp2 = page.goto(pdf_url, wait_until="commit",
                                  timeout=PAGE_TIMEOUT)
                ct = resp2.headers.get("content-type", "") if resp2 else ""
                if "pdf" in ct:
                    # Get bytes from the response
                    body = resp2.body()
                    pdf_path.write_bytes(body)
                    pdf_path_tmp = pdf_path
                else:
                    result["status"] = "pdf_fetch_failed"
                    result["error"] = f"Not PDF: {ct[:50]}"
                    return result
            except Exception as e2:
                result["status"] = "pdf_fetch_failed"
                result["error"] = str(e2)[:150]
                return result

        # Extract text
        text = extract_text_from_pdf(pdf_path_tmp)

        # Move to final location if needed
        if pdf_path_tmp != pdf_path and pdf_path_tmp.exists():
            import shutil
            shutil.move(str(pdf_path_tmp), str(pdf_path))

        if not text or len(text) < 200:
            result["status"] = "extraction_failed"
            result["error"] = f"Only {len(text)} chars extracted"
            return result

        txt_path.write_text(text, encoding="utf-8")
        result["status"] = "success"
        result["n_chars"] = len(text)

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)[:200]

    return result


def extract_text_from_page(page):
    """Try to extract article body text from the current page via JS."""
    try:
        text = page.evaluate("""() => {
            // Try article element first
            let el = document.querySelector('article');
            if (el && el.innerText.length > 500) return el.innerText;

            // Try common class names
            for (const cls of ['article-body', 'fulltext', 'article__body',
                                'main-content', 'article-content']) {
                el = document.querySelector('.' + cls) ||
                     document.querySelector('[class*="' + cls + '"]');
                if (el && el.innerText.length > 500) return el.innerText;
            }

            // Try main
            el = document.querySelector('main');
            if (el && el.innerText.length > 1000) return el.innerText;

            return '';
        }""")
        return text.strip() if text else ""
    except Exception:
        return ""


# ══════════════════════════════════════════════════════════════════════════
# BATCH DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════

def batch_download(max_papers=MAX_PER_SESSION, start_idx=0):
    """Download papers using Playwright with existing Chrome profile."""
    from playwright.sync_api import sync_playwright

    print("=" * 70)
    print("INSTITUTIONAL DOWNLOAD VIA LIBKEY + CHROME PROFILE")
    print("=" * 70)
    print(f"  Chrome profile: {CHROME_USER_DATA}")
    print(f"  LibKey library: {LIBKEY_LIBRARY_ID}")
    print()
    print("  NOTE: Chrome must be CLOSED for this to work!")
    print()

    # Load DOI list
    targets = pd.read_csv(PROC / "scihub_download_list.csv")
    print(f"  Total papers with DOI: {len(targets):,}")

    # Find papers we already have
    already_have = set()

    pmc_path = INT / "fulltext" / "pmc_fulltext_2026-03-26.csv"
    if pmc_path.exists():
        pmc = pd.read_csv(pmc_path, usecols=["pmid", "full_text"], low_memory=False)
        pmc_ft = pmc[pmc["full_text"].notna() & (pmc["full_text"].str.len() > 100)]
        already_have.update(pmc_ft["pmid"].unique())
        print(f"  PMC fulltext: {len(pmc_ft):,} papers (skip)")

    oa_dir = INT / "scihub_text"
    if oa_dir.exists():
        n_oa = 0
        for f in oa_dir.glob("*.txt"):
            if f.stat().st_size > 100:
                try:
                    already_have.add(int(f.stem))
                    n_oa += 1
                except ValueError:
                    pass
        print(f"  OA text files: {n_oa:,} (skip)")

    n_inst = 0
    for f in TEXT_DIR.glob("*.txt"):
        if f.stat().st_size > 100:
            try:
                already_have.add(int(f.stem))
                n_inst += 1
            except ValueError:
                pass
    print(f"  Institutional (prior): {n_inst:,} (skip)")
    print(f"  Total already have: {len(already_have):,}")

    targets = targets[~targets["pmid"].isin(already_have)].reset_index(drop=True)
    print(f"  Remaining: {len(targets):,}")

    if len(targets) == 0:
        print("  Nothing to download!")
        return

    targets = targets.iloc[start_idx:start_idx + max_papers].reset_index(drop=True)
    print(f"  This batch: {len(targets):,} (from index {start_idx})")
    print()

    # Temp download directory
    dl_dir = INT / "institutional_downloads_tmp"
    dl_dir.mkdir(exist_ok=True)

    # Launch browser
    results = []
    t0 = time.time()

    with sync_playwright() as p:
        print("  Launching Chrome with your profile...")
        browser = p.chromium.launch_persistent_context(
            user_data_dir=str(CHROME_USER_DATA),
            channel="chrome",           # use installed Chrome
            headless=False,             # visible so you can handle SSO if needed
            accept_downloads=True,
            downloads_path=str(dl_dir),
            args=["--disable-blink-features=AutomationControlled"],
        )

        page = browser.pages[0] if browser.pages else browser.new_page()
        page.set_default_timeout(PAGE_TIMEOUT)

        # Quick test: can we reach LibKey?
        print("  Testing LibKey access...")
        try:
            test_resp = page.goto(f"{LIBKEY_BASE}/10.1016/j.neuron.2017.09.017",
                                   wait_until="domcontentloaded", timeout=15000)
            time.sleep(2)
            print(f"  Test URL → {page.url[:80]}")
            print(f"  Status: {test_resp.status if test_resp else 'no response'}")
            if test_resp and test_resp.status == 200:
                print("  ✓ LibKey access works!")
            else:
                print("  ⚠ Check if SSO login is needed (browser should be visible)")
                input("  Press Enter after you've logged in (or Ctrl+C to abort)...")
        except Exception as e:
            print(f"  Test failed: {e}")
            print("  Continuing anyway — you may need to log in via the browser window.")
            input("  Press Enter to continue...")

        # Download loop
        consecutive_failures = 0

        for idx, row in targets.iterrows():
            pmid = int(row["pmid"])
            doi = str(row["doi"])

            if (TEXT_DIR / f"{pmid}.txt").exists():
                if (TEXT_DIR / f"{pmid}.txt").stat().st_size > 100:
                    continue

            result = download_one_paper(page, pmid, doi, dl_dir)
            results.append(result)

            # Track failures
            if result["status"] in ("success", "html_text"):
                consecutive_failures = 0
            else:
                consecutive_failures += 1

            n_done = len(results)
            n_success = sum(1 for r in results
                            if r["status"] in ("success", "html_text"))
            elapsed = time.time() - t0

            if n_done % 5 == 0 or n_done <= 5:
                rate = n_done / elapsed * 60 if elapsed > 0 else 0
                print(f"  [{n_done}/{len(targets)}] "
                      f"ok={n_success} "
                      f"last={result['status'][:20]} "
                      f"{rate:.0f}/min "
                      f"pmid={pmid}")

            # Backoff
            if consecutive_failures >= 15:
                print("  ⚠ 15 consecutive failures — stopping.")
                break
            elif result["status"] == "rate_limited":
                print("  Rate limited — waiting 30s")
                time.sleep(30)
            else:
                time.sleep(REQUEST_DELAY)

        browser.close()

    # Save log
    if results:
        log_df = pd.DataFrame(results)
        if LOG_FILE.exists():
            old_log = pd.read_csv(LOG_FILE)
            log_df = pd.concat([old_log, log_df], ignore_index=True)
            log_df = log_df.drop_duplicates(subset="pmid", keep="last")
        log_df.to_csv(LOG_FILE, index=False)

        elapsed = time.time() - t0
        n_success = sum(1 for r in results
                        if r["status"] in ("success", "html_text"))
        total_chars = sum(r["n_chars"] for r in results)
        status_counts = pd.Series([r["status"] for r in results]).value_counts()

        print(f"\n{'=' * 70}")
        print(f"BATCH COMPLETE — {elapsed/60:.1f} minutes")
        print(f"{'=' * 70}")
        print(f"  Attempted:  {len(results):,}")
        print(f"  Successful: {n_success:,} ({n_success/len(results)*100:.1f}%)")
        print(f"  Total text: {total_chars:,} chars")
        print(f"\n  Status breakdown:")
        for status, count in status_counts.items():
            print(f"    {status:25s}: {count:,}")
        print(f"\n  Log: {LOG_FILE}")


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download papers via UvA LibKey using Chrome profile")
    parser.add_argument("--download", action="store_true",
                        help="Batch download papers")
    parser.add_argument("--max", type=int, default=MAX_PER_SESSION,
                        help=f"Max papers per batch (default {MAX_PER_SESSION})")
    parser.add_argument("--start", type=int, default=0,
                        help="Start index in DOI list")
    parser.add_argument("--test", type=int, default=0,
                        help="Test mode: download only N papers")
    args = parser.parse_args()

    if args.download or args.test > 0:
        max_papers = args.test if args.test > 0 else args.max
        batch_download(max_papers=max_papers, start_idx=args.start)
    else:
        parser.print_help()
        print("\n  IMPORTANT: Close Chrome before running!\n")
        print("Examples:")
        print("  python 20_institutional_download.py --test 10")
        print("  python 20_institutional_download.py --download --max 500")
        print("  python 20_institutional_download.py --download --start 500 --max 500")
