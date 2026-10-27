# Data outside git

Everything under `Data/`, `Graphs/` and `Paper/` lives outside the repository
(4.1 GB, about 10,800 files). `MANIFEST.csv` in the project root records each
file's path, tier, size and SHA-256 checksum, so any copy can be checked:

```bash
python Code/tools/data_manifest.py summary
python Code/tools/data_manifest.py verify [--root PATH] [--tiers core] [--quick]
python Code/tools/data_manifest.py build          # after data changes
```

## Tiers

| Tier | Size | Contents | Needed for |
|------|------|----------|------------|
| `core` | 412 MB | All of `Data/processed/v18/` and `Data/processed/clustering_stability/`, the decade word2vec models, and the canonical corpus files listed below | Rebuilding every v18 figure and statistic, and continuing the v18 analyses |
| `pipeline` | 3.6 GB | The rest of `Data/`: raw downloads, extracted full text, embeddings, BERTopic models, pre-v18 outputs | Re-running earlier pipeline stages only |
| `outputs` | 126 MB | `Graphs/pub/` figures and `Paper/` drafts and notes | Writing; figures can be regenerated |
| `root` | <1 MB | Root README, CLAUDE.md, logs | - |

The `core` tier was traced, not guessed: an `open()` audit hook recorded every
file read by `v18_figures.py --figs all`, `relay_sentence_merge.py` and
`v18_w2v_categories.py`. A copy holding only `Code/` plus `core` rebuilt
everything at a different location.

## Canonical inputs (read through `Code/paper/v18_data.py`)

| File | Content |
|------|---------|
| `Data/raw/pubmed/pubmed_broad_title_only_1950_2026_2026-03-26.csv` | PubMed records: pmid, year, journal, title, abstract, publication types |
| `Data/processed/all_sentences_full_corpus.csv` | Sentence corpus: one row per thalamus sentence, with source, section label, nucleus flags and regex frame. Includes reference-list rows, which the loaders drop by default |
| `Data/processed/framing_full_corpus.csv` | The framing sentences (21-frame regex taxonomy) |
| `Data/processed/paper_abstracts.csv`, `specter2_pmids.npy` | Paper index used by the domain clustering |
| `Data/processed/clustering_stability/` | Leiden memberships, UMAP and model selection. The winner is `mpnet__raw__r005`; `r008` is a supplementary figure |
| `Data/processed/syntactic_roles.csv`, `cooccurrence_temporal.csv` | Inputs to Fig 6 (convergence) |
| `Data/models/w2v_{1980..2020}.model` | Decade word2vec models; cached as `v18/w2v_category_similarities.csv` |

## `Data/processed/v18/`: the v18 work

| Path | Content |
|------|---------|
| `sentence_sections_*.csv`, `pmc_*` | Section labels for full-text sentences (PMC XML and PDF heuristics) |
| `institutional_title_check.csv`, `institutional_repair_map.csv` | PDF-to-pmid attribution check and repair |
| `paper_techniques*.csv` | Technique flags per paper, from full text and from title+abstract (the latter is used for trends) |
| `abstract_framing.csv`, `abstract_robustness_stats.csv` | Abstract-only robustness check |
| `paper_authors_openalex.csv`, `_authors_partial.json` | OpenAlex author IDs and positions. **Contains real names: keep private** |
| `planck_*` | Planck's-principle test (generational vs within-author change) |
| `relay_stance_classified.csv`, `relay_correction_results.txt` | Regex relay stance layer (superseded by the LLM coding, kept for comparison) |
| `llm_pilot/`, `llm_full/` | Paper-level LLM tier: one text file per paper (`<pmid>.txt`, 220 in the full run), `manifest.csv` (sample), `batches/`, coder output `results/batch_*.jsonl`, merged `paper_framing_results.jsonl` (pilot: `pilot_results.jsonl`), plus `frames_flat.csv` and `frame_family_map.csv` |
| `llm_relay/` | Sentence-level relay coding: `batches/` (what the coders saw), `results/` (their codes), `key.csv` (id to paper), `reliability/` (re-code sample), `relay_sentence_codes.csv` and `relay_sentence_report.txt` (merged), `gold_standard_to_code.csv` and `gold_standard_codebook.md` (human check, not yet done) |

The LLM outputs cannot be regenerated identically, so treat `llm_*` as
primary data.

## Superseded or backup files (kept for provenance; do not use)

- `Data/processed/all_sentences_final.csv`, `framing_combined_v3.csv`:
  v17-era corpus.
- `*.bak`, `*.bak3`, `*_old_backup.csv`, `*_BACKUP.csv`: copies made before
  corrections.
- `Data/processed/llm_classifications/`: the abandoned pay-per-token API run
  (May 2026).
- Many other `Data/processed/*.csv` files belong to pre-v18 analyses in
  `Code/Analysis/`.

## Sensitive material

- Full texts in `Data/interim/` and `Data/raw/` (institutional PDFs/text,
  Sci-Hub, OA PDFs, PMC XML) are for private analysis only. They must never
  go into the public repository or be shared.
- `Data/interim/chrome_profile_copy/` is a copy of a Chrome browser profile.
  It is excluded from the manifest and the transfer command, and can be
  deleted.
- `.env` (API key) is excluded. Recreate it from `.env.example` if a legacy
  API script is ever needed.
- `Paper/v18_figure_plan.md` and `Paper/claude_memory/` contain unpublished
  results and, in places, researcher names from a private validation check.
  They stay on the drive.
