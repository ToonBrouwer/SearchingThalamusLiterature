"""
17_retry_and_download.py — Two-part data expansion:
  Part 1: Retry 1,700 failed PMC XML fulltext fetches (HTTPError)
  Part 2: Download ~1,700 remaining OA PDFs (URLs confirmed but not yet fetched)

No Claude interaction needed — fully autonomous.
"""
import sys, os, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"
XML_DIR = INT / "pmc_xml"
PDF_DIR = INT / "oa_pdfs"
TEXT_DIR = INT / "scihub_text"
XML_DIR.mkdir(parents=True, exist_ok=True)
PDF_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR.mkdir(parents=True, exist_ok=True)

from pmc import (
    load_cached_or_fetch_xml,
    extract_full_text_from_xml,
    fetch_pmc_jats_xml,
    save_xml_cache,
)


# ═══════════════════════════════════════════════════════════════════════════
# PART 1: RETRY FAILED PMC XML FETCHES
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("PART 1: RETRYING FAILED PMC XML FULLTEXT FETCHES")
print("=" * 70)

pmc_df = pd.read_csv(INT / "fulltext" / "pmc_fulltext_2026-03-26.csv", low_memory=False)

# Find rows that failed with HTTPError — these had a PMCID but the fetch failed
failed = pmc_df[pmc_df["xml_status"].str.contains("HTTPError", na=False)].copy()
print(f"Papers with HTTPError: {len(failed):,}")

# Also check for any already-cached XMLs we might have
already_cached = set()
if XML_DIR.exists():
    already_cached = {f.stem for f in XML_DIR.glob("*.xml")}
    print(f"Already cached XMLs: {len(already_cached)}")

n_retry = 0
n_success = 0
n_fail = 0
new_fulltexts = []

for i, (_, row) in enumerate(failed.iterrows()):
    pmcid = row.get("pmcid")
    pmid = row.get("pmid")

    if pd.isna(pmcid) or not str(pmcid).strip():
        continue

    pmcid = str(pmcid).strip()
    n_retry += 1

    try:
        xml_text = load_cached_or_fetch_xml(pmcid, xml_dir=XML_DIR)
        extracted = extract_full_text_from_xml(xml_text)
        ft = extracted.get("full_text")

        if ft and len(ft) > 100:
            new_fulltexts.append({
                "pmid": pmid,
                "pmcid": pmcid,
                "full_text": ft,
                "section_titles": extracted.get("section_titles"),
                "xml_status": "ok_retry",
            })
            n_success += 1
        else:
            n_fail += 1
    except Exception as e:
        n_fail += 1

    if (i + 1) % 100 == 0:
        print(f"  [{i+1}/{len(failed)}] success={n_success}, fail={n_fail}")

    time.sleep(0.35)  # ~3/sec, respect NCBI rate limit

print(f"\nPart 1 done: {n_retry} retried, {n_success} recovered, {n_fail} still failed")

# Update the PMC CSV with recovered fulltexts
if new_fulltexts:
    recovered_df = pd.DataFrame(new_fulltexts)
    print(f"Recovered {len(recovered_df)} new fulltexts!")

    # Merge back into the main PMC dataframe
    for _, rec in recovered_df.iterrows():
        mask = pmc_df["pmid"] == rec["pmid"]
        if mask.any():
            pmc_df.loc[mask, "full_text"] = rec["full_text"]
            pmc_df.loc[mask, "section_titles"] = rec["section_titles"]
            pmc_df.loc[mask, "xml_status"] = "ok_retry"

    out_path = INT / "fulltext" / "pmc_fulltext_2026-03-26.csv"
    pmc_df.to_csv(out_path, index=False)
    print(f"Updated {out_path}")
    print(f"Total papers with fulltext now: {pmc_df['full_text'].notna().sum():,}")


# ═══════════════════════════════════════════════════════════════════════════
# PART 2: DOWNLOAD REMAINING OA PDFs
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("PART 2: DOWNLOADING REMAINING OA PDFs")
print("=" * 70)

# Load OA cache
oa_cache_path = PROC / "oa_check_cache.json"
oa_cache = json.loads(oa_cache_path.read_text()) if oa_cache_path.exists() else {}
print(f"OA cache entries: {len(oa_cache):,}")

# Find DOIs with confirmed OA URLs
doi_to_url = {doi: url for doi, url in oa_cache.items() if url}
print(f"DOIs with OA PDF URL: {len(doi_to_url):,}")

# Map DOIs back to PMIDs
targets = pd.read_csv(PROC / "scihub_download_list.csv")
doi_to_pmid = dict(zip(targets["doi"].astype(str), targets["pmid"].astype(int)))

# Build download list, skipping already-done
already_txt = {int(f.stem) for f in TEXT_DIR.glob("*.txt") if f.stat().st_size > 100}
already_main = set(pd.read_csv(PROC / "all_sentences_final.csv", usecols=["pmid"])["pmid"].unique())
# Don't skip papers in main dataset — they might be abstract-only and we want fulltext too
# Only skip papers where we already have a text extract
skip = already_txt
print(f"Already have text for: {len(already_txt):,} papers")

to_download = {}
for doi, url in doi_to_url.items():
    pmid = doi_to_pmid.get(doi)
    if pmid and pmid not in skip:
        to_download[pmid] = url

print(f"Papers to download: {len(to_download):,}")


def extract_text_from_pdf(pdf_path):
    try:
        import fitz
        doc = fitz.open(str(pdf_path))
        text = "".join(page.get_text() + "\n" for page in doc)
        doc.close()
        return text
    except:
        return ""


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

        if not (r.content[:5] == b'%PDF-' or 'application/pdf' in r.headers.get('content-type', '')):
            return pmid, "not_pdf", 0

        pdf_path.write_bytes(r.content)
        text = extract_text_from_pdf(pdf_path)
        if len(text) > 200:
            txt_path.write_text(text, encoding="utf-8")
            return pmid, "success", len(text)
        else:
            return pmid, "no_text", 0
    except Exception as e:
        return pmid, f"error", 0


n_success = 0
n_fail = 0
n_skip = 0

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {}
    for pmid, url in to_download.items():
        f = executor.submit(download_one, pmid, url)
        futures[f] = pmid

    for i, future in enumerate(as_completed(futures)):
        pmid, status, text_len = future.result()
        if status == "success":
            n_success += 1
        elif status == "already_done":
            n_skip += 1
        else:
            n_fail += 1

        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(futures)}] success={n_success}, skip={n_skip}, fail={n_fail}")

total_txt = len(list(TEXT_DIR.glob("*.txt")))
print(f"\nPart 2 done: {n_success:,} new downloads, {n_skip:,} skipped, {n_fail:,} failed")
print(f"Total text files now: {total_txt:,}")

# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("SUMMARY")
print("=" * 70)
pmc_final = pd.read_csv(INT / "fulltext" / "pmc_fulltext_2026-03-26.csv", low_memory=False)
pmc_ft_count = pmc_final["full_text"].notna().sum()
print(f"PMC fulltext papers: {pmc_ft_count:,}")
print(f"OA PDF text extracts: {total_txt:,}")
print(f"Combined unique fulltext papers: ~{pmc_ft_count + total_txt:,}")
print("=" * 70)
