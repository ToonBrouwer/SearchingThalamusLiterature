# Thalamic framing project: instructions for Claude Code

PhD project: a computational literature analysis of how the thalamus is
framed (relay, hub, gate, ...) in about 19k PubMed papers, 1950-2026. The
author writes the manuscript; Claude builds analyses, figures and checks.

Start with `Code/docs/HANDOFF.md` (status, workflow, next steps) and the
private decision log `Paper/v18_figure_plan.md` (results and every decision
taken with the author). Data layout: `Code/docs/DATA.md`. Setup:
`Code/docs/SETUP.md`.

## Hard rules

- **No pay-per-token API for LLM work.** Run LLM coding inside the Claude
  Code session with parallel subagents (see HANDOFF, "How LLM coding is run").
- **Never edit the manuscript** (OneDrive `ThalamicFraming_V0.docx`). The
  author writes it. Discuss analyses, suggest, and produce figures and numbers.
- **Never name individual researchers** in figures, text, code or commits.
  Author-level results stay anonymised. Names may be shown to the author in
  chat for validation only.
- **The GitHub repo `Code/` is public.** Never commit data, full texts,
  `Paper/` notes, author names or the decision log. Commit or push only when
  the author asks for it.
- Don't add the author's email address to requests sent to external services
  unless they ask.

## Working conventions

- Environment: conda env `thalamus-embed` (`Code/environment.yml`). On
  Windows, run Python with `PYTHONIOENCODING=utf-8`. Run scripts from the
  project root, e.g. `python Code/paper/v18_figures.py --figs 3`.
- Load data through `Code/paper/v18_data.py` (`load_sentences`,
  `load_framing`, `load_domains`, ...). Reference-list sentences are excluded
  by default. Derive every number live; never hand-copy statistics.
- Relay results come from the LLM-coded sentence analysis
  (`Code/llm/relay_sentence_merge.py`), not from the raw regex relay trend,
  which mixes anatomical labels with claims.
- New figures go into `v18_figures.py` (`FIGS` dict). Keep panels to 4 or
  fewer, one question per figure, n annotated, colours from `v18_data`.
- Record new decisions in `Paper/v18_figure_plan.md`. After data changes,
  rebuild `MANIFEST.csv` with `python Code/tools/data_manifest.py build`.
