import argparse
import json
from datetime import datetime

from config import get_query, load_settings
from paths import build_data_dirs
from pubmed import fetch_pubmed_records_chunked, save_pubmed_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a PubMed search and save records.")
    parser.add_argument("--query-name", required=True, help="Name of the query in queries.yml")
    parser.add_argument("--start-year", type=int, default=1950)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--max-chunk-size", type=int, default=8000)
    parser.add_argument("--settings-path", default="settings.yml")
    parser.add_argument("--queries-path", default="queries.yml")
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

    df, chunks = fetch_pubmed_records_chunked(
        base_query=query,
        start_year=args.start_year,
        end_year=args.end_year,
        max_chunk_size=args.max_chunk_size,
        email=email,
        tool=tool,
        api_key=api_key,
    )

    date_tag = datetime.now().strftime("%Y-%m-%d")
    base_name = f"pubmed_{args.query_name}_{args.start_year}_{args.end_year}_{date_tag}"

    output_csv = dirs["pubmed_raw"] / f"{base_name}.csv"
    output_json = dirs["pubmed_raw"] / f"{base_name}_metadata.json"

    metadata = {
        "query_name": args.query_name,
        "query": query,
        "date": date_tag,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "max_chunk_size": args.max_chunk_size,
        "n_records_fetched": len(df),
        "n_chunks": len(chunks),
        "chunks": [
            {
                "start_year": chunk.start_year,
                "end_year": chunk.end_year,
                "count": chunk.count,
            }
            for chunk in chunks
        ],
        "output_csv": str(output_csv),
    }

    save_pubmed_outputs(df, metadata, output_csv, output_json)

    print(f"Saved merged records to: {output_csv}")
    print(f"Saved metadata to: {output_json}")


if __name__ == "__main__":
    main()