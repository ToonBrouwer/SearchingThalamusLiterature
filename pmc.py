import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests


PMC_ID_CONVERTER_URL = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
PMC_OAI_BASE_URL = "https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/"


def chunk_list(items: List[str], chunk_size: int) -> List[List[str]]:
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def normalize_pmid(value) -> Optional[str]:
    """
    Normalize PMID values so they are safe to send to the converter API.
    """
    if pd.isna(value):
        return None

    text = str(value).strip()

    if not text or text.lower() == "nan":
        return None

    # remove trailing .0 if pandas read integer IDs as floats
    if text.endswith(".0"):
        text = text[:-2]

    return text


def prepare_pmids(pubmed_df: pd.DataFrame, max_n: Optional[int] = None) -> List[str]:
    """
    Extract, normalize, deduplicate PMIDs from a PubMed dataframe.
    """
    pmids = pubmed_df["pmid"].apply(normalize_pmid).dropna().tolist()

    # preserve order while deduplicating
    seen = set()
    unique_pmids = []
    for pmid in pmids:
        if pmid not in seen:
            seen.add(pmid)
            unique_pmids.append(pmid)

    if max_n is not None:
        unique_pmids = unique_pmids[:max_n]

    return unique_pmids


def map_pmids_to_pmcids(
    pmids: List[str],
    tool: str = "searching_thalamus_literature",
    email: Optional[str] = None,
    chunk_size: int = 200,
    sleep_seconds: float = 0.34,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Map PMIDs to PMCIDs using the PMC ID Converter API.
    """
    rows = []

    pmids = [normalize_pmid(p) for p in pmids]
    pmids = [p for p in pmids if p is not None]

    total_input = len(pmids)

    if verbose:
        print(f"Mapping {total_input} PMIDs to PMCIDs...")

    for i, chunk in enumerate(chunk_list(pmids, chunk_size), start=1):
        params = {
            "ids": ",".join(chunk),
            "format": "json",
            "tool": tool,
        }
        if email:
            params["email"] = email

        response = requests.get(PMC_ID_CONVERTER_URL, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()

        records = payload.get("records", [])

        if verbose:
            print(f"  Chunk {i}: sent {len(chunk)} PMIDs, got {len(records)} records back")

        returned_pmids = set()

        for record in records:
            pmid = normalize_pmid(record.get("pmid"))
            returned_pmids.add(pmid)

            pmcid = record.get("pmcid")
            doi = record.get("doi")
            status = record.get("status")
            errmsg = record.get("errmsg")

            rows.append(
                {
                    "pmid": pmid,
                    "pmcid": pmcid,
                    "doi": doi,
                    "has_pmcid": isinstance(pmcid, str) and pmcid.strip() != "",
                    "idconv_status": status,
                    "idconv_errmsg": errmsg,
                }
            )

        # make sure PMIDs that came back with no usable record are still represented
        missing_from_response = [pmid for pmid in chunk if pmid not in returned_pmids]
        for pmid in missing_from_response:
            rows.append(
                {
                    "pmid": pmid,
                    "pmcid": None,
                    "doi": None,
                    "has_pmcid": False,
                    "idconv_status": "missing_from_response",
                    "idconv_errmsg": None,
                }
            )

        time.sleep(sleep_seconds)

    df = pd.DataFrame(rows)

    if df.empty:
        return pd.DataFrame(
            columns=[
                "pmid",
                "pmcid",
                "doi",
                "has_pmcid",
                "idconv_status",
                "idconv_errmsg",
            ]
        )

    df = df.drop_duplicates(subset=["pmid"], keep="first").reset_index(drop=True)

    if verbose:
        print(f"Finished PMID→PMCID mapping.")
        print(f"  Unique PMIDs mapped: {len(df)}")
        print(f"  PMIDs with PMCID: {df['has_pmcid'].sum()}")
        print(f"  PMIDs without PMCID: {(~df['has_pmcid']).sum()}")

    return df


def fetch_pmc_jats_xml(pmcid: str, timeout: int = 60) -> str:
    """
    Fetch article XML through the PMC OAI-PMH endpoint.
    Expects pmcid like 'PMC1234567'.
    """
    params = {
        "verb": "GetRecord",
        "metadataPrefix": "pmc",
        "identifier": f"oai:pubmedcentral.nih.gov:{pmcid.replace('PMC', '')}",
    }
    response = requests.get(PMC_OAI_BASE_URL, params=params, timeout=timeout)
    response.raise_for_status()
    return response.text


def save_xml_cache(xml_text: str, xml_dir: Path, pmcid: str) -> Path:
    xml_dir.mkdir(parents=True, exist_ok=True)
    out_path = xml_dir / f"{pmcid}.xml"
    out_path.write_text(xml_text, encoding="utf-8")
    return out_path


def load_cached_or_fetch_xml(pmcid: str, xml_dir: Path) -> str:
    xml_path = xml_dir / f"{pmcid}.xml"
    if xml_path.exists():
        return xml_path.read_text(encoding="utf-8")

    xml_text = fetch_pmc_jats_xml(pmcid)
    save_xml_cache(xml_text, xml_dir, pmcid)
    return xml_text


def strip_xml_namespaces(root: ET.Element) -> ET.Element:
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
    return root


def extract_full_text_from_xml(xml_text: str) -> Dict[str, Optional[str]]:
    """
    Extract article body text from JATS-like XML using top-level body sections.
    """
    root = ET.fromstring(xml_text)
    root = strip_xml_namespaces(root)

    body = root.find(".//body")
    if body is None:
        return {
            "full_text": None,
            "section_titles": None,
        }

    section_titles = []
    section_chunks = []

    for sec in body.findall("./sec"):
        title_elem = sec.find("title")
        title_text = None

        if title_elem is not None:
            title_text = " ".join("".join(title_elem.itertext()).split())
            if title_text:
                section_titles.append(title_text)

        paragraphs = sec.findall(".//p")
        paragraph_texts = [
            " ".join("".join(p.itertext()).split())
            for p in paragraphs
            if "".join(p.itertext()).strip()
        ]

        if paragraph_texts:
            chunk = "\n\n".join(paragraph_texts)
            if title_text:
                chunk = f"{title_text}\n{chunk}"
            section_chunks.append(chunk)

    if not section_chunks:
        paragraphs = body.findall(".//p")
        paragraph_texts = [
            " ".join("".join(p.itertext()).split())
            for p in paragraphs
            if "".join(p.itertext()).strip()
        ]
        if paragraph_texts:
            section_chunks.append("\n\n".join(paragraph_texts))

    full_text = "\n\n".join(section_chunks).strip()

    return {
        "full_text": full_text if full_text else None,
        "section_titles": " | ".join(section_titles) if section_titles else None,
    }


def enrich_pubmed_with_fulltext(
    pubmed_df: pd.DataFrame,
    xml_dir: Path,
    tool: str = "searching_thalamus_literature",
    email: Optional[str] = None,
    max_n: Optional[int] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Given a PubMed dataframe with a 'pmid' column, map to PMCIDs,
    fetch XML where available, extract full body text, and merge back.
    """
    pmids = prepare_pmids(pubmed_df, max_n=max_n)

    if verbose:
        print(f"Prepared {len(pmids)} unique PMIDs from input dataframe")

    id_map_df = map_pmids_to_pmcids(
        pmids=pmids,
        tool=tool,
        email=email,
        verbose=verbose,
    )

    working_df = pubmed_df.copy()
    working_df["pmid"] = working_df["pmid"].apply(normalize_pmid)

    if max_n is not None:
        keep_pmids = set(pmids)
        working_df = working_df[working_df["pmid"].isin(keep_pmids)].copy()

    merged = working_df.merge(id_map_df, on="pmid", how="left")

    if verbose:
        print(f"Merged rows: {len(merged)}")
        print(f"Rows with PMCID after merge: {merged['pmcid'].notna().sum()}")

    rows = []

    for _, row in merged.iterrows():
        pmcid = row.get("pmcid")

        full_text = None
        section_titles = None
        xml_status = "no_pmcid"

        if isinstance(pmcid, str) and pmcid.strip():
            try:
                xml_text = load_cached_or_fetch_xml(pmcid, xml_dir=xml_dir)
                extracted = extract_full_text_from_xml(xml_text)

                full_text = extracted["full_text"]
                section_titles = extracted["section_titles"]
                xml_status = "ok" if full_text else "no_body_text"
            except Exception as e:
                xml_status = f"error: {type(e).__name__}"

        rows.append(
            {
                "pmid": row.get("pmid"),
                "pmcid": pmcid,
                "doi": row.get("doi"),
                "year": row.get("year"),
                "journal": row.get("journal"),
                "title": row.get("title"),
                "publication_types": row.get("publication_types"),
                "has_pmcid": row.get("has_pmcid"),
                "idconv_status": row.get("idconv_status"),
                "idconv_errmsg": row.get("idconv_errmsg"),
                "section_titles": section_titles,
                "full_text": full_text,
                "xml_status": xml_status,
            }
        )

    return pd.DataFrame(rows)