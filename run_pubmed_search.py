import argparse
from datetime import datetime
from pathlib import Path

from config import get_query, load_settings
from paths import build_data_dirs
from pubmed import (
    fetch_pubmed_records,
    save_pubmed_outputs,
    search_pubmed,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a PubMed search and save records.")
    parser.add_argument(
        "--query-name",
        required=True,
        help="Name of the query in config/queries.yml",
    )
    parser.add_argument(
        "--retmax",
        type=int,
        default=500,
        help="Maximum number of PubMed IDs to retrieve",
    )
    parser.add_argument(
        "--settings-path",
        default="settings.yml",
        help="Path to settings YAML",
    )
    parser.add_argument(
        "--queries-path",
        default="queries.yml",
        help="Path to queries YAML",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    settings = load_settings(args.settings_path)
    query = get_query(args.query_name, args.queries_path)

    dirs = build_data_dirs(settings["data_root"])

    email = settings.get("email")
    tool = settings.get("tool", "searching_thalamus_literature")
    api_key = settings.get("api_key")

    print(f"Running query: {args.query_name}")
    print(query)

    search_result = search_pubmed(
        query=query,
        email=email,
        tool=tool,
        api_key=api_key,
        retmax=args.retmax,
    )

    pmids = search_result["id_list"]
    total_available = search_result["count"]

    print(f"Total available in PubMed: {total_available}")
    print(f"Retrieved PMIDs: {len(pmids)}")

    df = fetch_pubmed_records(
        pmids=pmids,
        email=email,
        tool=tool,
        api_key=api_key,
    )

    date_tag = datetime.now().strftime("%Y-%m-%d")
    base_name = f"pubmed_{args.query_name}_{date_tag}"

    output_csv = dirs["pubmed_raw"] / f"{base_name}.csv"
    output_json = dirs["pubmed_raw"] / f"{base_name}_metadata.json"

    metadata = {
        "query_name": args.query_name,
        "query": query,
        "date": date_tag,
        "retmax": args.retmax,
        "total_available": total_available,
        "n_pmids_retrieved": len(pmids),
        "n_records_fetched": len(df),
        "output_csv": str(output_csv),
        "query_translation": search_result.get("query_translation"),
    }

    save_pubmed_outputs(df, metadata, output_csv, output_json)

    print(f"Saved records to: {output_csv}")
    print(f"Saved metadata to: {output_json}")


if __name__ == "__main__":
    main()