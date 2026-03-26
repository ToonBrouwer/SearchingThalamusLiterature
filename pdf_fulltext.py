import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

import fitz  # PyMuPDF


# ----------------------------
# 1. Select candidates
# ----------------------------

def select_pdf_fallback_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select papers that:
    - have PMCID
    - do NOT have XML full text
    """
    candidates = df[
        df["pmcid"].notna() &
        (df["full_text"].isna() | (df["full_text"].str.strip() == ""))
    ].copy()

    return candidates


# ----------------------------
# 2. Download PDF
# ----------------------------

def download_pdf_for_pmcid(pmcid: str, pdf_dir: Path) -> Optional[Path]:
    """
    Download PDF from PMC.
    """
    pdf_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = pdf_dir / f"{pmcid}.pdf"

    if pdf_path.exists():
        return pdf_path

    url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/pdf/"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        with open(pdf_path, "wb") as f:
            f.write(response.content)

        return pdf_path

    except Exception:
        return None


# ----------------------------
# 3. Extract text
# ----------------------------

def extract_text_from_pdf(pdf_path: Path) -> Dict:
    """
    Extract text using PyMuPDF.
    """
    try:
        doc = fitz.open(pdf_path)

        texts = []
        n_pages = len(doc)

        for page in doc:
            text = page.get_text()
            if text:
                texts.append(text)

        full_text = "\n\n".join(texts).strip()

        char_count = len(full_text)

        return {
            "pdf_text": full_text if full_text else None,
            "pdf_n_pages": n_pages,
            "pdf_char_count": char_count,
            "pdf_status": "ok" if char_count > 1000 else "low_text",
        }

    except Exception as e:
        return {
            "pdf_text": None,
            "pdf_n_pages": None,
            "pdf_char_count": None,
            "pdf_status": f"error: {type(e).__name__}",
        }


# ----------------------------
# 4. Enrichment pipeline
# ----------------------------

def enrich_with_pdf_text(
    df: pd.DataFrame,
    pdf_dir: Path,
    max_n: Optional[int] = None,
    sleep_seconds: float = 0.5,
) -> pd.DataFrame:

    candidates = select_pdf_fallback_candidates(df)

    if max_n is not None:
        candidates = candidates.head(max_n)

    print(f"PDF candidates: {len(candidates)}")

    rows = []

    for i, (_, row) in enumerate(candidates.iterrows(), start=1):
        pmcid = row["pmcid"]

        print(f"[{i}] Processing {pmcid}")

        pdf_path = download_pdf_for_pmcid(pmcid, pdf_dir)

        if pdf_path is None:
            rows.append({
                "pmid": row["pmid"],
                "pmcid": pmcid,
                "pdf_path": None,
                "pdf_status": "download_failed",
                "pdf_text": None,
                "pdf_n_pages": None,
                "pdf_char_count": None,
            })
            continue

        result = extract_text_from_pdf(pdf_path)

        rows.append({
            "pmid": row["pmid"],
            "pmcid": pmcid,
            "pdf_path": str(pdf_path),
            **result
        })

        time.sleep(sleep_seconds)

    return pd.DataFrame(rows)