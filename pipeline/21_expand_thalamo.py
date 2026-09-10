"""
21_expand_thalamo.py — Expand corpus with 'thalamo*' title papers.

Fetches metadata + abstracts for 1,625 papers with thalamocortical,
thalamostriatal, etc. in the title that weren't in our original corpus.

Adds them to:
  - Raw PubMed CSV (appended)
  - all_sentences_final.csv (new sentences added)
  - paper_techniques.csv (tagged)
"""
import sys, io, os, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np
from pathlib import Path
from Bio import Entrez

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW  = ROOT / "Data" / "raw" / "pubmed"

Entrez.email = "e.a.brouwer2@uva.nl"

# ═══════════════════════════════════════════════════════════════════════════
# 1. IDENTIFY NEW PMIDS
# ═══════════════════════════════════════════════════════════════════════════
print("Loading existing PMIDs...")
existing = pd.read_csv(RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                       low_memory=False)
existing_pmids = set(existing["pmid"].astype(int).tolist())
print(f"  Existing corpus: {len(existing_pmids):,} papers")

print("\nQuerying PubMed for thalamo* papers...")
handle = Entrez.esearch(
    db="pubmed",
    term='thalamo*[Title] AND ("1950"[Date - Publication] : "2026"[Date - Publication])',
    retmax=30000
)
result = Entrez.read(handle)
handle.close()
all_thalamo_pmids = set(int(p) for p in result["IdList"])
new_pmids = sorted(all_thalamo_pmids - existing_pmids)
print(f"  Total thalamo* papers: {len(all_thalamo_pmids):,}")
print(f"  New papers to fetch:   {len(new_pmids):,}")

# ═══════════════════════════════════════════════════════════════════════════
# 2. FETCH METADATA + ABSTRACTS
# ═══════════════════════════════════════════════════════════════════════════
print("\nFetching metadata from PubMed...")

BATCH_SIZE = 200
all_records = []

for batch_start in range(0, len(new_pmids), BATCH_SIZE):
    batch = new_pmids[batch_start:batch_start + BATCH_SIZE]
    pmid_str = ",".join(str(p) for p in batch)

    try:
        handle = Entrez.efetch(db="pubmed", id=pmid_str, rettype="xml", retmode="xml")
        records = Entrez.read(handle)
        handle.close()

        for article in records["PubmedArticle"]:
            try:
                medline = article["MedlineCitation"]
                pmid = int(str(medline["PMID"]))

                art = medline["Article"]
                title = str(art.get("ArticleTitle", ""))

                # Year
                year = None
                try:
                    pub_date = art["Journal"]["JournalIssue"]["PubDate"]
                    year = int(pub_date.get("Year", 0)) or None
                except (KeyError, ValueError):
                    pass
                if not year:
                    try:
                        year = int(medline["DateCompleted"]["Year"])
                    except (KeyError, ValueError):
                        pass

                # Journal
                try:
                    journal = str(art["Journal"]["Title"])
                except KeyError:
                    journal = ""

                # Abstract (concatenate all sections)
                abstract = ""
                try:
                    abs_parts = art["Abstract"]["AbstractText"]
                    if isinstance(abs_parts, list):
                        abstract = " ".join(str(p) for p in abs_parts)
                    else:
                        abstract = str(abs_parts)
                except KeyError:
                    pass

                # Publication types
                pub_types = []
                try:
                    for pt in art["PublicationTypeList"]:
                        pub_types.append(str(pt))
                except KeyError:
                    pass

                all_records.append({
                    "pmid": pmid, "year": year,
                    "journal": journal, "title": title,
                    "abstract": abstract,
                    "publication_types": ";".join(pub_types),
                    "query_start_year": 1950, "query_end_year": 2026,
                })
            except (KeyError, ValueError) as e:
                continue

        time.sleep(0.35)  # NCBI rate limit

        if (batch_start + BATCH_SIZE) % 1000 < BATCH_SIZE:
            print(f"  Fetched {batch_start + BATCH_SIZE:,} / {len(new_pmids):,}")

    except Exception as e:
        print(f"  Batch error: {e}")
        time.sleep(2)

print(f"\nTotal new records fetched: {len(all_records):,}")

# ═══════════════════════════════════════════════════════════════════════════
# 3. APPEND TO RAW CSV
# ═══════════════════════════════════════════════════════════════════════════
new_df = pd.DataFrame(all_records)
print(f"\nNew papers summary:")
print(f"  Year range: {new_df['year'].min()}\u2013{new_df['year'].max()}")
print(f"  With abstract: {new_df['abstract'].notna().sum():,}")

# Save as separate file for traceability
out_path = RAW / "pubmed_thalamo_expansion.csv"
new_df.to_csv(out_path, index=False)
print(f"  Saved: {out_path}")

# Append to existing CSV
combined = pd.concat([existing, new_df], ignore_index=True)
combined = combined.drop_duplicates(subset="pmid", keep="first")
combined_path = RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv"
# Backup first
backup_path = RAW / "pubmed_broad_title_only_1950_2026_2026-03-26_BACKUP.csv"
if not backup_path.exists():
    existing.to_csv(backup_path, index=False)
    print(f"  Backup saved: {backup_path}")
combined.to_csv(combined_path, index=False)
print(f"\nCombined corpus: {len(combined):,} papers (+{len(combined)-len(existing):,})")

print("\nDone. Next steps:")
print("  1. Rerun 18_full_corpus_build.py to add new sentences to corpus")
print("  2. Rerun 19_enrich_and_rerun.py to update frame detection")
print("  3. Rerun technique_enrichment.py to tag new papers' techniques")
print("  4. Rerun method_precedes_framing.py with enlarged corpus")
