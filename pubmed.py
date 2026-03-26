import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests


BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


def _get_text(element: Optional[ET.Element]) -> Optional[str]:
    if element is None:
        return None
    text = "".join(element.itertext()).strip()
    return text if text else None


def _extract_year(article: ET.Element) -> Optional[str]:
    article_date = article.find(".//ArticleDate/Year")
    if article_date is not None and article_date.text:
        return article_date.text.strip()

    pub_date_year = article.find(".//PubDate/Year")
    if pub_date_year is not None and pub_date_year.text:
        return pub_date_year.text.strip()

    medline_date = article.find(".//PubDate/MedlineDate")
    if medline_date is not None and medline_date.text:
        match = re.search(r"\b(19|20)\d{2}\b", medline_date.text)
        if match:
            return match.group(0)

    return None


def search_pubmed(
    query: str,
    email: Optional[str] = None,
    tool: str = "searching_thalamus_literature",
    api_key: Optional[str] = None,
    retmax: int = 1000,
) -> Dict:
    url = f"{BASE_URL}/esearch.fcgi"
    params = {
        "db": "pubmed",
        "term": query,
        "retmode": "json",
        "retmax": retmax,
        "tool": tool,
    }

    if email:
        params["email"] = email
    if api_key:
        params["api_key"] = api_key

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()

    result = payload["esearchresult"]
    return {
        "count": int(result["count"]),
        "id_list": result["idlist"],
        "query_translation": result.get("querytranslation"),
    }


def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def fetch_pubmed_records(
    pmids: List[str],
    email: Optional[str] = None,
    tool: str = "searching_thalamus_literature",
    api_key: Optional[str] = None,
    chunk_size: int = 100,
    sleep_seconds: float = 0.34,
) -> pd.DataFrame:
    if not pmids:
        return pd.DataFrame(
            columns=[
                "pmid",
                "year",
                "journal",
                "title",
                "abstract",
                "publication_types",
            ]
        )

    url = f"{BASE_URL}/efetch.fcgi"
    records: List[Dict] = []

    for chunk in _chunk_list(pmids, chunk_size):
        params = {
            "db": "pubmed",
            "id": ",".join(chunk),
            "retmode": "xml",
            "tool": tool,
        }

        if email:
            params["email"] = email
        if api_key:
            params["api_key"] = api_key

        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()

        root = ET.fromstring(response.content)

        for article in root.findall(".//PubmedArticle"):
            pmid = _get_text(article.find(".//PMID"))
            title = _get_text(article.find(".//ArticleTitle"))

            abstract_parts = []
            for abstract_text in article.findall(".//Abstract/AbstractText"):
                label = abstract_text.attrib.get("Label")
                text = "".join(abstract_text.itertext()).strip()
                if text:
                    abstract_parts.append(f"{label}: {text}" if label else text)

            abstract = " ".join(abstract_parts) if abstract_parts else None
            journal = _get_text(article.find(".//Journal/Title"))
            year = _extract_year(article)

            publication_types = [
                pt.text.strip()
                for pt in article.findall(".//PublicationType")
                if pt.text
            ]

            records.append(
                {
                    "pmid": pmid,
                    "year": year,
                    "journal": journal,
                    "title": title,
                    "abstract": abstract,
                    "publication_types": "; ".join(publication_types),
                }
            )

        time.sleep(sleep_seconds)

    return pd.DataFrame(records)


def save_pubmed_outputs(
    df: pd.DataFrame,
    metadata: Dict,
    output_csv: str | Path,
    output_json: str | Path,
) -> None:
    output_csv = Path(output_csv)
    output_json = Path(output_json)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_csv, index=False)

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)