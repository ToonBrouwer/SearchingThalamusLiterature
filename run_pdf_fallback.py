import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import load_settings
from paths import build_data_dirs
from pdf_fulltext import enrich_with_pdf_text


def parse_args():
    parser = argparse.ArgumentParser(description="PDF fallback extraction")
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--max-n", type=int, default=None)
    parser.add_argument("--settings-path", default="settings.yml")
    return parser.parse_args()


def main():
    args = parse_args()

    settings = load_settings(args.settings_path)
    dirs = build_data_dirs(settings["data_root"])

    input_csv = Path(args.input_csv)

    df = pd.read_csv(input_csv, dtype={"pmid": str})

    pdf_dir = dirs["raw"] / "pdfs"
    out_dir = dirs["interim"] / "pdf_text"
    out_dir.mkdir(parents=True, exist_ok=True)

    result_df = enrich_with_pdf_text(
        df=df,
        pdf_dir=pdf_dir,
        max_n=args.max_n,
    )

    date_tag = datetime.now().strftime("%Y-%m-%d")
    base_name = f"pdf_fallback_{date_tag}"

    if args.max_n:
        base_name += f"_pilot_{args.max_n}"

    output_path = out_dir / f"{base_name}.csv"

    result_df.to_csv(output_path, index=False)

    print("\nDONE")
    print(f"Saved to: {output_path}")
    print(result_df["pdf_status"].value_counts())


if __name__ == "__main__":
    main()