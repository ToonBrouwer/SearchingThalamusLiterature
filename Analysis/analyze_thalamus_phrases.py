"""
Thalamus n-gram phrase analysis.

Counts the N words that follow every occurrence of 'thalamus' in the full-text
corpus, for N = 1 … max_n (default 6).  This gives a quick, vocabulary-level
view of the language used around the word 'thalamus' without any prior framing
assumptions.

Inputs:
    Data/interim/fulltext/pmc_fulltext_<date>.csv
        Must contain a 'full_text' column.  Update the filename in main() if
        the date stamp differs.

Outputs (Data/interim/phrase_analysis/):
    thalamus_next_1_words.csv … thalamus_next_6_words.csv
        Each file lists every N-word phrase that immediately follows 'thalamus',
        together with its raw count, sorted descending.

Note on paths:
    This script defines ROOT relative to the Analysis/ directory so it works
    regardless of where the repository is cloned.  The project-level utilities
    Code/config.py and Code/paths.py are not used here because this is an
    exploratory / preprocessing helper that predates that infrastructure; a
    future refactor could wire it in via load_settings() + build_data_dirs().
"""

import re
from collections import Counter
from pathlib import Path

import pandas as pd

# ── Paths (resolved relative to this script's location) ──────────────────────
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT          = _ANALYSIS_DIR.parents[1]   # project root
INTERIM       = ROOT / "Data" / "interim"

# Input: update date stamp if a newer fulltext file has been produced
INPUT_FILE = INTERIM / "fulltext" / "pmc_fulltext_2026-03-26.csv"

# Output directory
OUTPUT_DIR = INTERIM / "phrase_analysis"


# ── Tokenisation helpers ──────────────────────────────────────────────────────
def tokenize(text):
    """Return a list of lowercase word tokens from *text*."""
    return re.findall(r"\b\w+\b", text.lower())


# ── Core phrase extraction ────────────────────────────────────────────────────
def extract_phrases(df, max_n=6):
    """
    Build n-gram counters for every occurrence of 'thalamus' in df['full_text'].

    Returns a dict  {n: Counter}  where each Counter maps the n-word phrase
    immediately following 'thalamus' to its total count across the corpus.
    """
    counters = {n: Counter() for n in range(1, max_n + 1)}

    for _, row in df.iterrows():
        text = row.get("full_text")

        if not isinstance(text, str):
            continue

        tokens = tokenize(text)

        for i, token in enumerate(tokens):
            if token == "thalamus":
                for n in range(1, max_n + 1):
                    if i + n < len(tokens):
                        phrase = " ".join(tokens[i+1:i+1+n])
                        counters[n][phrase] += 1

    return counters


# ── I/O helpers ───────────────────────────────────────────────────────────────
def save_results(counters, output_dir):
    """Save one CSV per n-gram length to *output_dir*, sorted by count desc."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for n, counter in counters.items():
        df = pd.DataFrame(counter.items(), columns=["phrase", "count"])
        df = df.sort_values("count", ascending=False)

        output_path = output_dir / f"thalamus_next_{n}_words.csv"
        df.to_csv(output_path, index=False)

        print(f"Saved: {output_path}")


def print_top_results(counters, top_k=15):
    """Print the top *top_k* phrases for each n-gram length."""
    for n, counter in counters.items():
        print(f"\nTop {top_k} phrases ({n} words after 'thalamus'):")

        for phrase, count in counter.most_common(top_k):
            print(f"{phrase:<40} {count}")


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    print("Extracting phrases...")
    counters = extract_phrases(df, max_n=6)

    save_results(counters, OUTPUT_DIR)
    print_top_results(counters)


if __name__ == "__main__":
    main()