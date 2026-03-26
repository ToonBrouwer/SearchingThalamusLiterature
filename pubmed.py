import json
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests


BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


# ----------------------------
# Low-level helpers
# ----------------------------

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


def _chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


# ----------------------------
# Core PubMed functions
# ----------------------------

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
        "id_list": result.get("idlist", []),
        "query_translation": result.get("querytranslation"),
    }


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
    output_csv: str,
    output_json: str,
) -> None:
    output_csv = Path(output_csv)
    output_json = Path(output_json)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_csv, index=False)

    with output_json.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)


# ----------------------------
# Date chunking layer
# ----------------------------

@dataclass
class DateChunk:
    start_year: int
    end_year: int
    count: int


def add_date_range_to_query(base_query: str, start_year: int, end_year: int) -> str:
    return f"({base_query}) AND ({start_year}/01/01:{end_year}/12/31[dp])"


def count_pubmed_results(
    query: str,
    email: Optional[str] = None,
    tool: str = "searching_thalamus_literature",
    api_key: Optional[str] = None,
) -> int:
    result = search_pubmed(
        query=query,
        email=email,
        tool=tool,
        api_key=api_key,
        retmax=0,
    )
    return result["count"]


def build_date_chunks(
    base_query: str,
    start_year: int,
    end_year: int,
    max_chunk_size: int,
    email: Optional[str] = None,
    tool: str = "searching_thalamus_literature",
    api_key: Optional[str] = None,
) -> List[DateChunk]:
    dated_query = add_date_range_to_query(base_query, start_year, end_year)
    count = count_pubmed_results(
        query=dated_query,
        email=email,
        tool=tool,
        api_key=api_key,
    )

    print(f"Checking chunk {start_year}-{end_year}: {count} results")

    if count <= max_chunk_size:
        return [DateChunk(start_year=start_year, end_year=end_year, count=count)]

    if start_year == end_year:
        raise ValueError(
            f"Single year {start_year} still has {count} results, "
            f"which exceeds max_chunk_size={max_chunk_size}. "
            f"You may need month-level splitting."
        )

    mid_year = (start_year + end_year) // 2

    left_chunks = build_date_chunks(
        base_query=base_query,
        start_year=start_year,
        end_year=mid_year,
        max_chunk_size=max_chunk_size,
        email=email,
        tool=tool,
        api_key=api_key,
    )

    right_chunks = build_date_chunks(
        base_query=base_query,
        start_year=mid_year + 1,
        end_year=end_year,
        max_chunk_size=max_chunk_size,
        email=email,
        tool=tool,
        api_key=api_key,
    )

    return left_chunks + right_chunks


def fetch_pubmed_records_chunked(
    base_query: str,
    start_year: int,
    end_year: int,
    max_chunk_size: int,
    email: Optional[str] = None,
    tool: str = "searching_thalamus_literature",
    api_key: Optional[str] = None,
) -> Tuple[pd.DataFrame, List[DateChunk]]:
    chunks = build_date_chunks(
        base_query=base_query,
        start_year=start_year,
        end_year=end_year,
        max_chunk_size=max_chunk_size,
        email=email,
        tool=tool,
        api_key=api_key,
    )

    all_dfs = []

    for chunk in chunks:
        print(
            f"Fetching chunk {chunk.start_year}-{chunk.end_year} "
            f"with {chunk.count} results"
        )

        dated_query = add_date_range_to_query(
            base_query,
            chunk.start_year,
            chunk.end_year,
        )

        search_result = search_pubmed(
            query=dated_query,
            email=email,
            tool=tool,
            api_key=api_key,
            retmax=chunk.count,
        )

        pmids = search_result["id_list"]

        df = fetch_pubmed_records(
            pmids=pmids,
            email=email,
            tool=tool,
            api_key=api_key,
        )

        df["query_start_year"] = chunk.start_year
        df["query_end_year"] = chunk.end_year
        all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame(), chunks

    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset="pmid").reset_index(drop=True)

    return merged_df, chunks