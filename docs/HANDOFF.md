# Hand-off: where the v18 work stands (2026-10-27)

Results, numbers and every decision made with the author are in the private
decision log **`Paper/v18_figure_plan.md`** (on the drive, never in git).
Read it before changing an analysis, and append new decisions to it. This file
covers status, workflow and next steps.

The manuscript draft is written by the author (OneDrive,
`ThalamicFraming_V0.docx`). The code produces figures and statistics for it
and never edits it.

## Status

**Corpus (canonical since 2026-09-02).** Reference-list sentences are
excluded by default, institutional PDF-to-paper attribution is repaired, and
section labels (abstract/introduction/methods/results/discussion) exist for
most full-text sentences.

**Figures.** All are built by `Code/paper/v18_figures.py`, which writes
`Graphs/pub/v18_*.png|svg`.

| Key | Output | Topic |
|-----|--------|-------|
| `1` | `v18_fig1_corpus` | Corpus and pipeline |
| `2` | `v18_fig2_landscape` | Leiden domain landscape and technique trends (title+abstract) |
| `3` | `v18_fig3_framing_shift` | Framing shift (regex frames) |
| `4` | `v18_fig4_context` | Frame by technique and by domain |
| `5` | `v18_fig5_anatomy` | Frame by nucleus, framing-only nucleus clustering |
| `6` | `v18_fig6_convergence` | Syntax, word2vec, co-occurrence |
| `7` | `v18_fig7_method_framing` | fMRI adoption precedes hub framing |
| `8`, `8b` | `v18_fig8_llm_framing`, `v18_fig8b_conceptual_change` | Paper-level LLM tier |
| `planck` | `v18_fig_planck` | Generational vs within-author change (anonymised) |
| `relay-genre` | `v18_fig_relay_genre` | Where relay declined and where it persists (sentence-level LLM codes) |
| `nucdom` | `v18_fig_nuclei_by_domain` | Nuclei per domain |
| `supp-reg`, `supp-nuc`, `supp-r008`, `supp-abs` | `v18_suppA..D_*` | Regression controls, nucleus trends, r008 subdomains, abstract-only robustness |

**Analyses done.** Abstract-only robustness (`pipeline/28`); Planck test on
OpenAlex author IDs (`pipeline/29-31`); regex relay stance layer
(`pipeline/32`); paper-level LLM tier on 220 papers (`llm/`); blind
sentence-level LLM coding of all 1,728 regex relay sentences, with an
independent re-code of 145 for reliability (`llm/relay_sentence_*`).

## How LLM coding is run (no API)

The pay-per-token Anthropic API is too expensive for this project and must
not be used. Coding runs inside a Claude Code session instead (subscription),
with parallel subagents:

1. A setup script writes self-contained batch files: `llm/full_select_extract.py`
   (paper-level) or `llm/relay_sentence_setup.py` (sentence-level, blind: id +
   sentence only).
2. One general-purpose subagent per batch reads the codebook
   (`llm/paper_frame_prompt.py` or `llm/relay_sentence_prompt.py`) and one
   batch file. It writes JSON lines to `results/batch_XX.jsonl` and checks its
   own output. Coders must not open key files or other batches.
3. A merge script validates and joins everything: `llm/merge_validate_results.py`
   + `llm/normalize_frames.py`, or `llm/relay_sentence_merge.py` (agreement
   with regex, genre analysis, Planck recheck, gold standard, reliability).
4. For reliability, `llm/relay_sentence_reliability.py` draws a cross-batch
   sample for an independent re-code.

## Next steps

1. **Gold standard (author).** Code the 100 sentences in
   `Data/processed/v18/llm_relay/gold_standard_to_code.csv` using
   `gold_standard_codebook.md`, blind (do not open `key.csv` or `results/`
   first). Save as `gold_standard_coded.csv` and rerun
   `python Code/llm/relay_sentence_merge.py`; section 7 reports human vs LLM
   and human vs regex agreement. Until then, LLM-LLM agreement is the only
   check.
2. **Fig 3 relay line.** It still plots the raw regex relay trend, which
   mixes anatomical labels ("relay nucleus") with claims. Offered, not yet
   decided: split it into LLM-coded relay claims vs labels, as in
   `relay-genre`.
3. **Analysis-methods taxonomy.** Extract experimental methods from Methods
   sections (not only abstracts), get an overview first, then measure trends
   on uniformly available text.
4. **Core/matrix and named theories.** Code them in a later LLM verification
   run.
5. **Nucleus nomenclature.** Check whether subdivision schemes for nuclei
   changed over time. Only the nucleus-frame association stays in the
   manuscript; nucleus lead-lag was dropped.
6. **Regex taxonomy fixes.** Move `anti_relay` above `relay` in
   `taxonomy.PRIORITY` (about 4% of relay hits are challenges currently coded
   as relay), and add foils such as "way-station" and "merely a relay".
   Report relay results from the LLM-coded analysis, not the raw regex trend.
7. **Paper-level LLM validation.** Manually check a sample of the Tier-1
   codes.
8. **Smaller items** (check status in the decision log first): misdated
   1959-65 records with structured abstracts; whether the lesion technique
   pattern conflates method and pathology; re-running `syntactic_roles.csv`
   on the canonical corpus; softening the semantic-displacement claim;
   optional Tier-2 conceptual-metaphor run (`llm/frame_prompt.py`).

## Conventions

- `paper/v18_data.py` is the single source for paths, loaders, domain names,
  the circuit/clinical genre split (`genre_of_domain`) and colours. Scripts
  derive numbers live from the canonical files and never hand-copy them.
- Caches in `Data/processed/v18/` can be deleted to force a recompute, except
  the `llm_*` folders, which are primary data.
- Author-level results are shown anonymised. Researcher names may be listed
  privately for validation, never in figures, text or the repository.
- Commit and push only when asked. The GitHub repository is public.
