"""
corpus_description.py — Step 0: Comprehensive corpus summary.

Produces:
  - Decade breakdown (papers, sentences, framing sentences)
  - Text source distribution per decade
  - Journal distribution (top 20 + long tail)
  - Data quality checks (abstract length, missing data)
  - Figure: Graphs/pub/fig_corpus_overview.png
  - Print summary to stdout
"""
import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import Counter

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW  = ROOT / "Data" / "raw" / "pubmed"
PUB  = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
raw = pd.read_csv(RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv", low_memory=False)
df = pd.read_csv(PROC / "all_sentences_final.csv", low_memory=False)

print(f"  Raw PubMed papers: {len(raw):,}")
print(f"  Corpus sentences:  {len(df):,}")
print(f"  Corpus papers:     {df['pmid'].nunique():,}")

# ═══════════════════════════════════════════════════════════════════════════
# DECADE BINNING
# ═══════════════════════════════════════════════════════════════════════════
raw["decade"] = (raw["year"] // 10) * 10
df["decade"] = (df["year"] // 10) * 10

# Also 5-year bins for robustness
raw["half_decade"] = (raw["year"] // 5) * 5
df["half_decade"] = (df["year"] // 5) * 5

# ═══════════════════════════════════════════════════════════════════════════
# 1. DECADE BREAKDOWN
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("1. DECADE BREAKDOWN")
print("=" * 70)

# Papers per decade (raw PubMed)
papers_decade = raw.groupby("decade").size().rename("n_papers")

# Sentences per decade
sents_decade = df.groupby("decade").size().rename("n_sentences")

# Framing sentences per decade
has_frame = df["frame_type"].notna() & (df["frame_type"] != "")
framing_decade = df[has_frame].groupby("decade").size().rename("n_framing")

# Papers in corpus per decade
corpus_papers_decade = df.groupby("decade")["pmid"].nunique().rename("n_corpus_papers")

decade_summary = pd.concat([papers_decade, corpus_papers_decade, sents_decade, framing_decade], axis=1).fillna(0).astype(int)
decade_summary["sents_per_paper"] = (decade_summary["n_sentences"] / decade_summary["n_corpus_papers"].replace(0, np.nan)).round(1)
decade_summary["framing_pct"] = (decade_summary["n_framing"] / decade_summary["n_sentences"].replace(0, np.nan) * 100).round(2)

print(decade_summary.to_string())

# ═══════════════════════════════════════════════════════════════════════════
# 2. TEXT SOURCE DISTRIBUTION PER DECADE
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("2. TEXT SOURCE DISTRIBUTION PER DECADE")
print("=" * 70)

if "source" in df.columns:
    src_decade = df.groupby(["decade", "source"]).size().unstack(fill_value=0)
    print(src_decade.to_string())

    # Percent fulltext per decade
    ft_cols = [c for c in src_decade.columns if c != "abstract"]
    src_decade["pct_fulltext"] = (src_decade[ft_cols].sum(axis=1) / src_decade.sum(axis=1) * 100).round(1)
    print("\nFulltext % per decade:")
    for dec, row in src_decade.iterrows():
        print(f"  {int(dec)}s: {row['pct_fulltext']:.1f}%")

# ═══════════════════════════════════════════════════════════════════════════
# 3. JOURNAL DISTRIBUTION
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("3. JOURNAL DISTRIBUTION")
print("=" * 70)

journal_counts = raw["journal"].value_counts()
n_journals = len(journal_counts)
top20 = journal_counts.head(20)
print(f"Total unique journals: {n_journals}")
print(f"Top 20 journals (of {n_journals}):")
for j, c in top20.items():
    print(f"  {j:50s}: {c:>4}")
print(f"\nTop 20 cover {top20.sum():,} / {len(raw):,} papers ({top20.sum()/len(raw)*100:.1f}%)")
print(f"Journals with 1 paper: {(journal_counts == 1).sum()}")
print(f"Journals with ≤5 papers: {(journal_counts <= 5).sum()}")

# ═══════════════════════════════════════════════════════════════════════════
# 4. DATA QUALITY
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("4. DATA QUALITY")
print("=" * 70)

# Missing abstracts
n_no_abstract = raw["abstract"].isna().sum()
print(f"Papers with no abstract: {n_no_abstract:,} ({n_no_abstract/len(raw)*100:.1f}%)")

# Short abstracts
if raw["abstract"].notna().any():
    abs_len = raw.loc[raw["abstract"].notna(), "abstract"].str.len()
    print(f"Abstract length: median={abs_len.median():.0f}, mean={abs_len.mean():.0f}, "
          f"min={abs_len.min()}, max={abs_len.max()}")
    n_short = (abs_len < 50).sum()
    print(f"Abstracts < 50 chars: {n_short}")

# Missing years
n_no_year = raw["year"].isna().sum()
print(f"Papers with no year: {n_no_year}")

# Pre-1980 papers
n_pre1980 = (raw["year"] < 1980).sum()
print(f"Papers before 1980: {n_pre1980}")

# Sentence length distribution
sent_len = df["sentence"].str.len()
print(f"\nSentence length: median={sent_len.median():.0f}, mean={sent_len.mean():.0f}, "
      f"min={sent_len.min()}, max={sent_len.max()}")

# Token count (rough)
token_counts = df["sentence"].str.split().str.len()
print(f"Tokens per sentence: median={token_counts.median():.0f}, mean={token_counts.mean():.1f}")

# Papers not in corpus (no thalamus-mentioning sentences)
in_corpus = set(df["pmid"].unique())
not_in_corpus = set(raw["pmid"]) - in_corpus
print(f"\nPapers with no thalamus-mentioning sentences: {len(not_in_corpus):,}")

# ═══════════════════════════════════════════════════════════════════════════
# 5. PUBLICATION TYPES
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("5. PUBLICATION TYPES")
print("=" * 70)

if "publication_types" in raw.columns:
    # Parse publication types (often semicolon or pipe separated)
    all_types = Counter()
    for pt in raw["publication_types"].dropna():
        for t in str(pt).split(";"):
            t = t.strip()
            if t:
                all_types[t] += 1
    print("Publication types:")
    for t, c in all_types.most_common(15):
        print(f"  {t:40s}: {c:>5}")

# ═══════════════════════════════════════════════════════════════════════════
# 6. FIGURE: CORPUS OVERVIEW (4-panel)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("6. GENERATING FIGURE")
print("=" * 70)

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("Corpus Overview", fontsize=16, fontweight="bold", y=0.98)

# Panel A: Papers per year
ax = axes[0, 0]
year_counts = raw.groupby("year").size()
year_counts = year_counts[(year_counts.index >= 1975) & (year_counts.index <= 2026)]
ax.bar(year_counts.index, year_counts.values, color=WONG["blue"], alpha=0.8, width=0.8)
ax.set_xlabel("Year")
ax.set_ylabel("Papers")
ax.set_title("A. Papers per year (PubMed)")
despine(ax)
panel_label(ax, "A")

# Panel B: Source composition per decade (merged: all non-abstract = fulltext)
ax = axes[0, 1]
if "source" in df.columns:
    df["source_merged"] = df["source"].replace({
        "fulltext": "fulltext", "institutional": "fulltext",
        "oa_pdf": "fulltext", "abstract": "abstract",
    })
    src_dec = df.groupby(["decade", "source_merged"]).size().unstack(fill_value=0)
    src_dec = src_dec[(src_dec.index >= 1980) & (src_dec.index <= 2020)]
    source_order = ["fulltext", "abstract"]
    source_colors = {"fulltext": WONG["blue"], "abstract": WONG["grey"]}
    bottom = np.zeros(len(src_dec))
    for src in source_order:
        if src in src_dec.columns:
            vals = src_dec[src].values
            ax.bar(src_dec.index, vals, bottom=bottom, width=8,
                   label=src, color=source_colors.get(src, WONG["grey"]), alpha=0.85)
            bottom += vals
    ax.set_xlabel("Decade")
    ax.set_ylabel("Sentences")
    ax.set_title("B. Sentences by source per decade")
    ax.legend(fontsize=8, loc="upper left")
    despine(ax)
panel_label(ax, "B")

# Panel C: Top 15 journals
ax = axes[1, 0]
top15 = journal_counts.head(15)
y_pos = np.arange(len(top15))
ax.barh(y_pos, top15.values, color=WONG["green"], alpha=0.8)
ax.set_yticks(y_pos)
ax.set_yticklabels([j[:40] for j in top15.index], fontsize=7)
ax.invert_yaxis()
ax.set_xlabel("Papers")
ax.set_title("C. Top 15 journals")
despine(ax)
panel_label(ax, "C")

# Panel D: Framing sentence rate per decade
ax = axes[1, 1]
dec_valid = decade_summary[(decade_summary.index >= 1980) & (decade_summary.index <= 2020)]
ax.bar(dec_valid.index, dec_valid["framing_pct"], width=8, color=WONG["red"], alpha=0.85)
ax.set_xlabel("Decade")
ax.set_ylabel("Framing sentences (%)")
ax.set_title("D. Framing detection rate per decade")
for i, (dec, row) in enumerate(dec_valid.iterrows()):
    ax.annotate(f"n={int(row['n_framing']):,}", (dec, row["framing_pct"]),
                ha="center", va="bottom", fontsize=7)
despine(ax)
panel_label(ax, "D")

plt.tight_layout(rect=[0, 0, 1, 0.96])
out_path = PUB / "fig_corpus_overview.png"
save_fig(fig, out_path)
print(f"  Saved: {out_path}")

print("\nDone.")
