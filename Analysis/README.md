# Analysis Pipeline — LiteratureQueeryThalamusTime

This folder contains all Python analysis scripts for the thalamus-framing
literature project.  The project investigates how the thalamus has been
framed in neuroscience publications from 1950 to 2026, tracing a shift from
relay metaphors toward hub/integration metaphors.

---

## Environment

All scripts should be run inside the **`thalamus-embed`** conda environment,
which provides the following key packages:

- `pandas`, `numpy`, `matplotlib`, `scipy`
- `sentence-transformers` (all-MiniLM-L6-v2)
- `umap-learn`, `hdbscan`
- `pyyaml` (for `Code/config.py`)

Activate with:

```
conda activate thalamus-embed
```

Scripts that only do text processing and plotting (steps 1–3 and 5–10 in the
table below) run on CPU without GPU.  The embedding step (`embed_and_cluster.py`)
benefits from a GPU but is not required.

---

## Data pipeline overview

```
Data/raw/pubmed/
    pubmed_broad_title_only_1950_2026_<date>.csv   ← PubMed search results (13,824 records)
Data/raw/pmc_xml/                                   ← PMC XML full-text downloads
Data/raw/pdfs/                                      ← PDF fallback full texts

        ↓  Code/run_fulltext_enrichment.py / run_pdf_fallback.py

Data/interim/fulltext/
    pmc_fulltext_<date>.csv                        ← merged full-text corpus

        ↓  extract_framing_sentences.py             (step 1a — full-text)
           phase_b_extract_abstract_framing.py      (step 1b — abstracts)

Data/interim/sentence_extraction/
    thalamus_all_sentences.csv                     ← every thalamus sentence (full-text)
    thalamus_framing_sentences.csv                 ← framing subset (full-text)
    thalamus_abstract_all_sentences.csv            ← every thalamus sentence (abstracts)
    thalamus_abstract_framing_sentences.csv        ← framing subset (abstracts)
    abstract_framing_sentences.csv                 ← also written by classify_framing.py

        ↓  classify_framing.py                      (step 2)

Data/processed/
    framing_sentences_classified.csv
    all_sentences_classified.csv
    abstract_framing_sentences_classified.csv
    combined_framing_classified.csv

        ↓  embed_and_cluster.py                     (step 3)

Data/processed/
    framing_embeddings.npy
    framing_embedded_clustered.csv
    cluster_examples.csv

        ↓  print_clusters_for_labelling.py          (step 4 — human step)
           apply_cluster_labels.py                  (step 4 — apply labels)

        ↓  explore_all_sentences.py                 (step 5 — optional QC)

Data/processed/
    all_sentences_with_sid.csv
    non_framing_sentences_flagged.csv

        ↓  temporal_analysis.py                     (step 6)
           analysis_nucleus.py                      (step 7)
           analysis_methods.py                      (step 8)
           analysis_not_merely_relay.py             (step 9)
           analysis_framing_conclusions.py          (step 10)
           phase_a_robustness.py                    (step 11)
           phase_b_abstract_analysis.py             (step 12)
           phase_c_showcase.py                      (step 13)

        ↓  phase_e_conclusion_clustering.py         (step E)

Data/processed/
    conclusion_sentences_clustered.csv
    conclusion_cluster_examples.csv
    conclusion_framing_matrix.csv

        ↓  phase_f_publication_figures.py           (step F)

Graphs/pub/
    pub_fig1_temporal_trend.{png,svg}
    pub_fig2_umap.{png,svg}
    pub_fig3_narrative.{png,svg}
    pub_fig4_conclusion_heatmap.{png,svg}
    pub_fig5_knowledge_map.{png,svg}

Graphs/
    fig1_*.png … fig11_*.png
    fig_A1_*.png … fig_A4_*.png
    fig_B1_*.png … fig_B3_*.png
    fig_C1_*.png … fig_C3_*.png
    01_*.png … 08_*.png
```

---

## Script reference table

| # | Script | What it does | Inputs | Outputs |
|---|--------|-------------|--------|---------|
| 1a | `extract_framing_sentences.py` | Split PMC full-text articles into sentences; flag sentences mentioning 'thalamus' and classify as framing (verb-trigger or appositive) | `Data/interim/fulltext/pmc_fulltext_<date>.csv` | `Data/interim/sentence_extraction/thalamus_all_sentences.csv`, `thalamus_framing_sentences.csv` |
| 1b | `phase_b_extract_abstract_framing.py` | Same extraction applied to PubMed abstracts (1950–2026); extends temporal coverage | `Data/raw/pubmed/pubmed_broad_title_only_1950_2026_<date>.csv` | `Data/interim/sentence_extraction/thalamus_abstract_all_sentences.csv`, `thalamus_abstract_framing_sentences.csv` |
| 0 | `analyze_thalamus_phrases.py` | Count N-gram phrases following 'thalamus' (1–6 words); exploratory vocabulary analysis | `Data/interim/fulltext/pmc_fulltext_<date>.csv` | `Data/interim/phrase_analysis/thalamus_next_N_words.csv` |
| 2 | `classify_framing.py` | Apply a keyword/regex taxonomy (10 categories: relay, hub_node, gateway, modulator, integrator, network_component, role_player, clinical_target, structure_description, involved_in) to framing sentences; also extracts abstract framing sentences and plots temporal trends | `thalamus_framing_sentences.csv`, `thalamus_all_sentences.csv`, PubMed CSV | `framing_sentences_classified.csv`, `all_sentences_classified.csv`, `abstract_framing_sentences_classified.csv`, `combined_framing_classified.csv`; Graphs `01–04_*.png` |
| 3 | `embed_and_cluster.py` | Embed all framing sentences with sentence-transformers (all-MiniLM-L6-v2); reduce to 2D with UMAP; cluster with HDBSCAN | `framing_sentences_classified.csv`, `abstract_framing_sentences_classified.csv` | `framing_embedded_clustered.csv`, `framing_embeddings.npy`, `cluster_examples.csv`; Graphs `05–08_*.png` |
| 4a | `print_clusters_for_labelling.py` | Print cluster summaries to stdout for manual label assignment | `framing_embedded_clustered.csv`, `cluster_examples.csv` | stdout only |
| 4b | `apply_cluster_labels.py` | Apply the manually assigned CLUSTER_LABELS dict to add a `cluster_label` column | `framing_embedded_clustered.csv`, `cluster_examples.csv` | Both files updated in-place |
| 5 | `explore_all_sentences.py` | Exploratory QC: add `sid` to all sentences; analyse what framing language was missed by the verb-trigger detector | `thalamus_all_sentences.csv` | `all_sentences_with_sid.csv`, `non_framing_sentences_flagged.csv` |
| 6 | `temporal_analysis.py` | Plot framing-sentence volume and cluster-share over time; relay vs hub head-to-head normalised % per year; keyword-category trends | `framing_embedded_clustered.csv` | `fig1–fig5_*.png` |
| 7 | `analysis_nucleus.py` | Tag framing sentences by thalamic nucleus mentioned (12 nucleus categories); compare relay vs hub framing balance per nucleus | `framing_embedded_clustered.csv` | `nucleus_framing_pct.csv`; `fig6_framing_by_nucleus.png` |
| 8 | `analysis_methods.py` | Tag papers by experimental method (12 method categories detected from abstract + framing sentence); compare framing balance per method; method prevalence over time | `framing_embedded_clustered.csv`, PubMed CSV | `method_framing_pct.csv`, `method_keyword_pct.csv`; `fig7_framing_by_method.png`, `fig8_method_prevalence_over_time.png` |
| 9 | `analysis_not_merely_relay.py` | Find sentences explicitly challenging the relay metaphor ("not merely a relay", "more than a relay", etc.); temporal trend of challenge language | `thalamus_all_sentences.csv` | `not_merely_relay_sentences.csv`; `fig9_not_merely_relay_trend.png` |
| 10 | `analysis_framing_conclusions.py` | Extract conclusion sentences; compare topic keyword frequencies between relay-framed and hub-framed papers; analyse where in the paper framing sentences appear | `framing_embedded_clustered.csv`, `thalamus_all_sentences.csv` | `framing_conclusion_topics.csv`; `fig10_framing_conclusion_topics.png`, `fig11_framing_position_in_paper.png` |
| 11 | `phase_a_robustness.py` | Robustness checks: (1) normalised temporal trend + Wilson 95% CI; (2) Mann-Kendall monotonic trend test; (3) decade-stratified nucleus analysis; (4) decade-stratified method analysis; (5) review vs original-research comparison | `framing_embedded_clustered.csv`, PubMed CSV | `table_A1_mannkendall.csv`; `fig_A1–fig_A4_*.png` |
| 12 | `phase_b_abstract_analysis.py` | Classify abstract framing sentences with keyword taxonomy; plot extended temporal trend (1950–2026); replication check (abstracts vs full-text); keyword distribution comparison | `thalamus_abstract_framing_sentences.csv`, `framing_embedded_clustered.csv` | `abstract_framing_classified.csv`; `fig_B1–fig_B3_*.png` |
| 13 | `phase_c_showcase.py` | Publication-quality showcase figures: appositive phrase analysis, UMAP coloured by framing keyword and year, 3-panel narrative (relay + hub + challenge sentences) | `framing_embedded_clustered.csv`, `abstract_framing_classified.csv`, `not_merely_relay_sentences.csv` | `fig_C1_appositives.png`, `fig_C2_umap_showcase.png`, `fig_C3_narrative.png` |
| E | `phase_e_conclusion_clustering.py` | Extract Discussion/Conclusion section sentences from PMC full texts; embed with all-MiniLM-L6-v2; UMAP + HDBSCAN; link each paper's dominant framing type to its conclusion cluster(s). Targets only relay/hub papers + 200 random others (cap: 8 sentences per paper) for speed | `pmc_fulltext_<date>.csv`, `framing_embedded_clustered.csv` | `conclusion_sentences_clustered.csv`, `conclusion_cluster_examples.csv`, `conclusion_framing_matrix.csv` |
| F | `phase_f_publication_figures.py` | Final publication-quality figures (PNG 300 dpi + SVG): Fig 1 temporal trend with Mann-Kendall on raw proportions + Wilson 95% CI; Fig 2 UMAP semantic landscape; Fig 3 narrative relay→hub transition; Fig 4 framing × conclusion-cluster heatmap; Fig 5 conclusion sentence knowledge map | All `Data/processed/` CSVs above | `Graphs/pub/pub_fig1–5_{temporal_trend,umap,narrative,conclusion_heatmap,knowledge_map}.{png,svg}` |
| — | `plot_style.py` | Shared style module imported by phase_c, phase_f. Defines the Wong (2011) colorblind-safe palette, matplotlib RC params, `FIG_2COL` (180 mm double-column width), and `save_fig()` helper that writes PNG + SVG | — | — |

---

## How to run from scratch

Run the scripts in the order shown below.  All commands assume the
`thalamus-embed` conda environment is active and that the working directory
is `Code/Analysis/` (or adjust `sys.path` to include `Code/` so that
`config.py` and `paths.py` are importable where needed).

```bash
conda activate thalamus-embed
cd Code/Analysis

# Step 0 — optional: explore vocabulary around 'thalamus'
python analyze_thalamus_phrases.py

# Step 1a — extract framing sentences from PMC full-text
python extract_framing_sentences.py

# Step 1b — extract framing sentences from PubMed abstracts
python phase_b_extract_abstract_framing.py

# Step 2 — apply keyword/regex framing taxonomy
python classify_framing.py

# Step 3 — embed, UMAP, HDBSCAN cluster  (~2–5 min on CPU)
python embed_and_cluster.py

# Step 4a — review clusters (output to stdout; pipe to file for notes)
python print_clusters_for_labelling.py > cluster_review.txt

# Step 4b — apply your labels (edit CLUSTER_LABELS dict first if needed)
python apply_cluster_labels.py

# Step 5 — optional QC: explore non-framing sentences
python explore_all_sentences.py

# Step 6 — temporal trend figures
python temporal_analysis.py

# Step 7 — nucleus analysis
python analysis_nucleus.py

# Step 8 — method analysis
python analysis_methods.py

# Step 9 — 'not merely a relay' challenge analysis
python analysis_not_merely_relay.py

# Step 10 — framing vs conclusions analysis
python analysis_framing_conclusions.py

# Step 11 — robustness checks (Phase A)
python phase_a_robustness.py

# Step 12 — abstract analysis (Phase B)
python phase_b_abstract_analysis.py

# Step 13 — showcase figures (Phase C)
python phase_c_showcase.py

# Step E — conclusion clustering (requires PMC full texts; ~3 min on CPU)
python phase_e_conclusion_clustering.py

# Step F — final publication figures (requires step E outputs)
python phase_f_publication_figures.py
```

Steps 6–13 are independent of each other once step 4b is complete; they can
be run in any order or re-run in isolation.  Step F depends on step E.

---

## Notes on path conventions

All scripts define path constants near the top of the file using:

```python
_ANALYSIS_DIR = Path(__file__).resolve().parent
ROOT          = _ANALYSIS_DIR.parents[1]   # project root
```

This means the scripts work correctly regardless of where the repository is
cloned, as long as the `Data/` and `Graphs/` directories are siblings of
`Code/` inside the project root.

The two scripts that predate this convention (`extract_framing_sentences.py`)
instead use the project-level utilities `Code/config.py` (loads `settings.yml`)
and `Code/paths.py` (builds directory paths from `data_root`).  All other
scripts bypass those utilities and resolve paths directly from `__file__`.
A future refactor could unify these two approaches.

---

## Key files in Data/processed/

| File | Produced by | Description |
|------|-------------|-------------|
| `framing_embedded_clustered.csv` | `embed_and_cluster.py` + `apply_cluster_labels.py` | Central analysis file: one row per framing sentence with cluster_id, cluster_label, umap_x/y, framing_primary, year, pmid, source |
| `abstract_framing_classified.csv` | `phase_b_abstract_analysis.py` | Abstract framing sentences with keyword classification and broad relay/hub grouping |
| `not_merely_relay_sentences.csv` | `analysis_not_merely_relay.py` | Sentences explicitly challenging the relay framing |
| `nucleus_framing_pct.csv` | `analysis_nucleus.py` | Relay/hub % per thalamic nucleus |
| `method_framing_pct.csv` | `analysis_methods.py` | Relay/hub % per experimental method |
| `framing_conclusion_topics.csv` | `analysis_framing_conclusions.py` | Conclusion topic % for relay vs hub papers |
| `table_A1_mannkendall.csv` | `phase_a_robustness.py` | Mann-Kendall trend test results |
| `conclusion_sentences_clustered.csv` | `phase_e_conclusion_clustering.py` | One row per conclusion sentence with HDBSCAN cluster_id, umap_x/y, framing_broad |
| `conclusion_cluster_examples.csv` | `phase_e_conclusion_clustering.py` | 5 representative sentences per conclusion cluster (closest to centroid) |
| `conclusion_framing_matrix.csv` | `phase_e_conclusion_clustering.py` | Cross-tabulation: framing type × conclusion cluster (papers) |

---

## Key cluster IDs

### Framing sentence clusters (embed_and_cluster.py)

The following HDBSCAN cluster IDs are used as constants in several scripts.
If the clustering is re-run with different parameters, these IDs will change
and the constants in all analysis scripts must be updated accordingly.

| Cluster ID | Label |
|------------|-------|
| 53 | hub / integration framing |
| 54 | information flow / cortico-thalamic mediator |
| 55 | relay / gateway framing |
| 56 | sensory filtering / higher-order relay / gating |
| 46 | somatosensory thalamus / first- vs higher-order relay |

`relay_ids = [55, 56, 46]` and `hub_ids = [53, 54]` appear in
`temporal_analysis.py`, `analysis_nucleus.py`, `analysis_methods.py`,
`analysis_framing_conclusions.py`, `phase_a_robustness.py`,
`phase_b_abstract_analysis.py`, `phase_c_showcase.py`, `phase_e_conclusion_clustering.py`,
and `phase_f_publication_figures.py`.

### Conclusion sentence clusters (phase_e_conclusion_clustering.py)

The conclusion clustering produces 12 HDBSCAN clusters.  Note that several
clusters correspond to **boilerplate text** (ethics approvals, informed consent,
data availability statements, author contributions) rather than scientific
conclusions.  The primary scientific-content clusters are:

| Cluster ID | Content | Dominant framing |
|------------|---------|-----------------|
| 10 | Main scientific conclusions (n ≈ 4,567) | relay (56%) / hub (17%) / other (27%) |
| 7  | Pain processing / sensory relay (n ≈ 109) | relay-dominated |
| 9  | Epilepsy / thalamocortical circuit dysfunction (n ≈ 75) | mixed |
| 0  | Migraine / headache pain pathophysiology (n ≈ 30) | relay-dominated |
| 1,2,4,5,6,11 | Boilerplate (ethics, consent, COI, data availability, author contributions) | — |
