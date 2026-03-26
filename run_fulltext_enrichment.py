import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import load_settings
from paths import build_data_dirs
from pmc import enrich_pubmed_with_fulltext

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich PubMed records with PMC introduction text.")
    parser.add_argument("--input-csv", required=True, help="Path to merged PubMed CSV")
    parser.add_argument("--settings-path", default="settings.yml")
    parser.add_argument("--max-n", type=int, default=None, help="Optional pilot limit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    settings = load_settings(args.settings_path)
    dirs = build_data_dirs(settings["data_root"])

    email = settings.get("email")
    tool = settings.get("tool", "searching_thalamus_literature")

    input_csv = Path(args.input_csv)
    if not input_csv.exists():
        raise FileNotFoundError(f"Input file not found: {input_csv}")

    pubmed_df = pd.read_csv(input_csv, dtype={"pmid": str})

    xml_dir = dirs["raw"] / "pmc_xml"
    out_dir = dirs["interim"] / "fulltext"
    out_dir.mkdir(parents=True, exist_ok=True)

    enriched_df = enrich_pubmed_with_fulltext(
        pubmed_df=pubmed_df,
        xml_dir=xml_dir,
        tool=tool,
        email=email,
        max_n=args.max_n,
    )

    date_tag = datetime.now().strftime("%Y-%m-%d")
    base_name = f"pmc_fulltext_{date_tag}"

    if args.max_n is not None:
        base_name += f"_pilot_{args.max_n}"

    output_csv = out_dir / f"{base_name}.csv"
    enriched_df.to_csv(output_csv, index=False)

    print(f"Saved enriched full-text file to: {output_csv}")
    print(f"Rows: {len(enriched_df)}")
    print(f"With PMCID: {enriched_df['pmcid'].notna().sum()}")
    print(f"With full text: {enriched_df['full_text'].notna().sum()}")

if __name__ == "__main__":
    main()