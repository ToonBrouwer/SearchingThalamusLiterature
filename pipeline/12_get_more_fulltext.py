"""
12_get_more_fulltext.py — Get DOIs and generate Sci-Hub URLs for missing full-text papers.

Steps:
  1. Identify papers with DOIs but no full text (immediate targets)
  2. Fetch DOIs from PubMed Entrez API for papers missing DOIs
  3. Generate Sci-Hub URLs for all papers with DOIs but no text
  4. Save URL list for batch download
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
INT  = ROOT / "Data" / "interim"
RAW  = ROOT / "Data" / "raw" / "pubmed"

SCIHUB_BASE = "https://sci-hub.se"  # Common mirror

# ═══════════════════════════════════════════════════════════════════════════
# 1. LOAD CURRENT STATE
# ═══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("1. CURRENT FULL-TEXT COVERAGE")
print("=" * 70)

ft_path = INT / "fulltext" / "pmc_fulltext_2026-03-26.csv"
ft = pd.read_csv(ft_path, usecols=["pmid", "doi", "year", "full_text"])
ft["has_text"] = ft["full_text"].fillna("").str.len() > 100
ft["has_doi"] = ft["doi"].notna() & (ft["doi"].str.len() > 3)

n_total = len(ft)
n_text = ft["has_text"].sum()
n_doi = ft["has_doi"].sum()
n_doi_notext = (ft["has_doi"] & ~ft["has_text"]).sum()
n_nodoi_notext = (~ft["has_doi"] & ~ft["has_text"]).sum()

print(f"  Total papers:           {n_total:,}")
print(f"  With full text:         {n_text:,}")
print(f"  With DOI:               {n_doi:,}")
print(f"  DOI but no text:        {n_doi_notext:,}  <-- immediate Sci-Hub targets")
print(f"  No DOI, no text:        {n_nodoi_notext:,}  <-- need DOI lookup first")

# ═══════════════════════════════════════════════════════════════════════════
# 2. FETCH DOIs FROM PUBMED FOR PAPERS MISSING THEM
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. FETCHING DOIs FROM PUBMED")
print("=" * 70)

need_doi = ft[~ft["has_doi"]]["pmid"].tolist()
print(f"  Papers needing DOI lookup: {len(need_doi):,}")

try:
    from Bio import Entrez
    Entrez.email = "e.a.brouwer2@uva.nl"

    # Batch fetch in chunks of 200
    BATCH_SIZE = 200
    fetched_dois = {}
    n_batches = (len(need_doi) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_i in range(min(n_batches, 65)):  # cap at 65 batches = 13,000 papers
        start = batch_i * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(need_doi))
        batch_pmids = need_doi[start:end]

        try:
            handle = Entrez.efetch(db="pubmed", id=batch_pmids,
                                   rettype="xml", retmode="xml")
            from Bio import Medline
            import xml.etree.ElementTree as ET
            xml_data = handle.read()
            handle.close()

            root = ET.fromstring(xml_data)
            for article in root.findall(".//PubmedArticle"):
                pmid_el = article.find(".//PMID")
                if pmid_el is None:
                    continue
                pmid = int(pmid_el.text)

                # Look for DOI in ArticleIdList
                for aid in article.findall(".//ArticleId"):
                    if aid.get("IdType") == "doi":
                        fetched_dois[pmid] = aid.text
                        break

            if (batch_i + 1) % 10 == 0:
                print(f"    Batch {batch_i + 1}/{min(n_batches, 65)}: "
                      f"fetched {len(fetched_dois):,} DOIs so far")

            time.sleep(0.35)  # NCBI rate limit

        except Exception as e:
            print(f"    Batch {batch_i + 1} error: {e}")
            time.sleep(1)
            continue

    print(f"  Fetched {len(fetched_dois):,} new DOIs from PubMed")

    # Update the dataframe
    for pmid, doi in fetched_dois.items():
        mask = ft["pmid"] == pmid
        if mask.any():
            ft.loc[mask, "doi"] = doi
            ft.loc[mask, "has_doi"] = True

except ImportError:
    print("  BioPython not installed, skipping DOI lookup")
    print("  Install with: pip install biopython")
    fetched_dois = {}

# ═══════════════════════════════════════════════════════════════════════════
# 3. GENERATE SCI-HUB URLs
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. GENERATING SCI-HUB URLs")
print("=" * 70)

targets = ft[ft["has_doi"] & ~ft["has_text"]].copy()
targets["scihub_url"] = targets["doi"].apply(lambda d: f"{SCIHUB_BASE}/{d}")
targets["pmid_url"] = targets["pmid"].apply(lambda p: f"{SCIHUB_BASE}/pmid/{p}")

print(f"  Papers with DOI but no full text: {len(targets):,}")

# Save URL list
url_file = PROC / "scihub_download_list.csv"
targets[["pmid", "doi", "year", "scihub_url", "pmid_url"]].to_csv(url_file, index=False)
print(f"  Saved: {url_file}")

# Also generate a simple text file with just URLs
url_txt = PROC / "scihub_urls.txt"
with open(url_txt, "w") as f:
    for _, row in targets.iterrows():
        f.write(f"{row['scihub_url']}\n")
print(f"  Saved: {url_txt} ({len(targets):,} URLs)")

# Year distribution of targets
print(f"\n  Sci-Hub targets by decade:")
targets["decade"] = (targets["year"] // 10 * 10).astype("Int64")
for dec, cnt in targets.groupby("decade").size().items():
    print(f"    {dec}s: {cnt:,}")

# ═══════════════════════════════════════════════════════════════════════════
# 4. ALSO TRY PMID-BASED SCI-HUB URLs FOR PAPERS WITHOUT DOI
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. PMID-BASED URLs FOR PAPERS WITHOUT DOI")
print("=" * 70)

no_doi_targets = ft[~ft["has_doi"] & ~ft["has_text"]].copy()
no_doi_targets["scihub_url"] = no_doi_targets["pmid"].apply(lambda p: f"{SCIHUB_BASE}/{p}")

print(f"  Papers without DOI or text: {len(no_doi_targets):,}")

# Save
no_doi_file = PROC / "scihub_pmid_urls.csv"
no_doi_targets[["pmid", "year", "scihub_url"]].to_csv(no_doi_file, index=False)
print(f"  Saved: {no_doi_file}")

# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════
final_doi_count = ft["has_doi"].sum()
final_notext = (~ft["has_text"]).sum()
print(f"\n{'='*70}")
print(f"SUMMARY")
print(f"  Papers with full text:     {n_text:,}")
print(f"  Papers with DOI, no text:  {(ft['has_doi'] & ~ft['has_text']).sum():,} (Sci-Hub ready)")
print(f"  Papers no DOI, no text:    {(~ft['has_doi'] & ~ft['has_text']).sum():,} (PMID-based)")
print(f"  Total potential expansion:  {final_notext:,} papers")
print(f"{'='*70}")
