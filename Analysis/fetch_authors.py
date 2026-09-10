"""Fetch first authors from OpenAlex — top 500 most-cited only."""
import sys, io, os, json, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import requests
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
AUTHOR_FILE = PROC / "paper_authors.csv"
EMAIL = "e.a.brouwer2@uva.nl"

cit = pd.read_csv(PROC / "citation_data.csv")
top = cit.nlargest(500, "cited_by_count")
print(f"Fetching authors for top {len(top)} most-cited papers...")

session = requests.Session()
session.headers.update({"User-Agent": f"ThalFraming/1.0 (mailto:{EMAIL})"})

rows = []
for i, (_, row) in enumerate(top.iterrows()):
    pmid = int(row["pmid"])
    url = f"https://api.openalex.org/works/pmid:{pmid}?select=id,authorships,title&mailto={EMAIL}"
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code == 200:
            d = resp.json()
            au = d.get("authorships", [])
            if au:
                first = au[0].get("author", {}).get("display_name", "Unknown")
                parts = first.split()
                surname = parts[-1] if parts else "Unknown"
                n = len(au)
                year = int(row["year"])
                if n == 1:
                    label = f"{surname} ({year})"
                elif n == 2:
                    s2 = au[1].get("author", {}).get("display_name", "").split()
                    surname2 = s2[-1] if s2 else ""
                    label = f"{surname} & {surname2} ({year})"
                else:
                    label = f"{surname} et al. ({year})"
                rows.append({"pmid": pmid, "first_author": first,
                             "surname": surname, "author_label": label})
    except:
        pass
    time.sleep(0.12)
    if (i+1) % 100 == 0:
        print(f"  {i+1}/500: {len(rows)} found")

df = pd.DataFrame(rows)
df.to_csv(AUTHOR_FILE, index=False)
print(f"\nSaved {len(df)} author records")
