# Domain Clustering Pipeline

Paper-level domain clustering of the thalamus corpus (14,947 papers with abstracts ≥50 chars). Follows the methodology of "The Evolving Landscape of Neuroscience" (Valerio et al.): embed → contrastive refinement → FAISS k-NN graph → Leiden CPM → stability/characterization.

---

## Pipeline (canonical order)

Run each script from the project root. All scripts cache their outputs, so reruns are cheap.

**Steps 1–3 (one-time setup):**

```
1. Prepare abstracts               → paper_abstracts.csv
   domain_clustering_pipeline.py

2. Embed 3 models (SPECTER2, PubMedBERT, MPNet) → {model}_embeddings.npy (768d)
   domain_clustering_pipeline.py     (SPECTER2 only)
   compare_embedding_models.py       (PubMedBERT + MPNet)

3. Contrastive refinement → {model}_refined_embeddings.npy (64d)
   refine_all_embeddings.py          (all three models)
```

**Steps 4–9 (analysis):** runnable independently; see file descriptions below.

```
4. Modularity sweep + resolution selection
   clustering_modularity_sweep.py
      → modularity_sweep.csv, modularity_picked_resolution.csv

5. 24-config stability analysis
   clustering_stability_full.py
      → memberships_{config}.npy, stability_full.csv, stability_matrix.csv

6. UMAP projections for visualization
   clustering_space_viz.py
      → clustering_stability/umap/umap_{model}__{cond}.npy

7. Hierarchical (cluster-of-clusters) umbrella detection
   clustering_hierarchy.py
      → hierarchy/linkage_{config}.npy,
         hierarchy/cluster_to_umbrella_{config}.csv,
         hierarchy/umbrella_labels_{config}.csv

8. Model-selection scoring + winner
   clustering_model_selection.py
      → model_selection_scores.csv

9. Publication figures
   clustering_figures.py --figures all
      → fig_cluster_stability.png
      → fig_modularity_sweep.png
      → fig_cluster_space_umap.png
      → fig_cluster_hierarchy.png
      → fig_model_selection_scorecard.png

10. Methods writeup
    write_clustering_methods.py
      → Paper/clustering_methods_writeup.docx

11. Walkthrough notebook
    Analysis/clustering_walkthrough.ipynb  (interactive reproduction)
```

---

## File index

**Core pipeline:**
- `domain_clustering_pipeline.py` — canonical full pipeline, SPECTER2-based (use `--phase N` to stop early)
- `compare_embedding_models.py` — embeds papers with PubMedBERT & MPNet (only needs one run; cached)
- `refine_all_embeddings.py` — applies the contrastive NN to all three embedding sets

**Unified analysis suite** (new, share `_clustering_utils.py`):
- `_clustering_utils.py` — shared helpers: `load_embeddings`, `build_knn_graph`, `run_leiden`, `best_jaccard`, `compute_domain_flags`, `top_tfidf_terms`, `cluster_centroids`, `config_key`
- `clustering_stability_full.py` — runs Leiden on all 24 (3 × 2 × 4) configs, computes per-cluster best-Jaccard stability vs every other config, saves memberships + `stability_full.csv` + `stability_matrix.csv`
- `clustering_modularity_sweep.py` — modularity vs resolution across 6 (model, condition) pairs; picks argmax-modularity resolution per pair
- `clustering_space_viz.py` — 2-D UMAP projections for each of the 6 embedding sets
- `clustering_hierarchy.py` — agglomerative clustering of Leiden-cluster centroids (umbrella detection at k = 4, 6, 8, 10, 12 cuts)
- `clustering_model_selection.py` — scores 6 configs on {stability, silhouette, domain-coverage, modularity}, produces composite ranking
- `clustering_figures.py` — publication figures from the above outputs (stability, modularity, UMAP panels, dendrogram, scorecard)
- `write_clustering_methods.py` — generates `Paper/clustering_methods_writeup.docx` narrative

**Legacy analysis** (superseded but still referenced):
- `multi_resolution_comparison.py` — older sweep using different resolution grid
- `cluster_stability.py` — older stability analysis (replaced by `clustering_stability_full.py`)
- `cluster_tables.py` — per-config tables (superseded by `clustering_stability_full.py`, kept for DOCX output)

**Characterization:**
- `characterize_domain_clusters.py` — TF-IDF, centrality, temporal profiles
- `apply_cluster_labels.py` — map integer cluster IDs → human labels (for framing clusters)
- `print_clusters_for_labelling.py` — print summaries for manual labeling

**Not part of this pipeline** (sentence-level framing, different flow):
- `embed_and_cluster.py` — framing-sentence HDBSCAN clustering
- `phase_e_conclusion_clustering.py` — conclusion-sentence clustering

---

## Outputs (canonical)

**`Data/processed/`:**
| File | Shape | Produced by |
|------|-------|-------------|
| `paper_abstracts.csv` | 14,947 × 6 | Phase 1 |
| `specter2_embeddings.npy` | 14,947 × 768 | Phase 2 |
| `pubmedbert_embeddings.npy` | 14,947 × 768 | Phase 2 |
| `mpnet_embeddings.npy` | 14,947 × 768 | Phase 2 |
| `specter2_pmids.npy` | 14,947 | Phase 2 (shared row order) |
| `specter2_refined_embeddings.npy` | 14,947 × 64 | Phase 3 |
| `pubmedbert_refined_embeddings.npy` | 14,947 × 64 | Phase 3 |
| `mpnet_refined_embeddings.npy` | 14,947 × 64 | Phase 3 |
| `domain_clusters_raw.csv` | pmid → cluster_raw | Phase 4 (SPECTER2 768-dim) |
| `domain_clusters_refined.csv` | pmid → cluster_refined | Phase 4 (SPECTER2 64-dim) |
| `leiden_sweep_{raw,refined}.csv` | (resolution, modularity, n_clusters) | Phase 4 |
| `cluster_tables/{MODEL}_res{NNN}.csv` | per-cluster summaries | Phase 7 |
| `cluster_tables/combined.csv` | all configs concatenated | Phase 7 |
| `cluster_stability_summary.csv` | cross-config Jaccard | Phase 6 |

**`Graphs/pub/`:**
- `fig_cluster_table_comparison.png` — 3-model bar plot colored by domain, sorted by stability
- `fig_resolution_sweep_overview.png` — modularity vs resolution across models (from Phase 5)
- `fig_domain_clusters_umap.png` — UMAP projection with cluster fills (from Phase 8)

**`Paper/`:**
- `cluster_tables.docx` — formatted per-config tables with stability scores

---

## Hyperparameters (match the Valerio reference)

**Contrastive NN** (`train_contrastive()` in `domain_clustering_pipeline.py`):
- Architecture: `input_dim → 512 → 256 → 128 → 64`, BatchNorm after FC1/FC2, ELU, Dropout 0.1 after FC1/FC2, L2-normalized output
- Loss: InfoNCE with positive threshold = 0.85, negative threshold = 0.75, stability term +1.0, temperature = 0.1
- Correlation penalty on output dims (weight = 0.1)
- Adam (lr=1e-4), ExponentialLR (γ=0.995), weight_decay=0.01
- Grokfast: α=0.98, λ=2.0
- 2000 epochs, batch size 256, 95/5 train/val split
- Input dimension adapted to each model's native dim (768 for all three here; reference uses 1024)

**FAISS k-NN graph:**
- `IndexFlatIP` (cosine on L2-normalized embeddings)
- k = 50 nearest neighbours
- Symmetrized with `combine_edges="max"`

**Leiden:**
- `leidenalg.CPMVertexPartition`, seed=42
- Resolution sweep: 1000 values linspace(0.001, 1.0), pick argmax modularity
- Early stop: 30 non-improving steps (reference uses 5; we use 30 because the modularity curve can be non-monotone at low resolutions)
- Minimum cluster size for reporting: 50 papers

---

## Reproducibility

All randomness is seeded:
- `leidenalg` → `seed=42`
- contrastive train/val split → `np.random.RandomState(42)`

Caches are keyed by output path. Delete the target file to force recompute.

---

## Archived / superseded

See `Code/_archive/`:
- `superseded_figures/generate_figures_v{2,3,10,11}.py` — earlier iterations of the main figure script (`generate_figures_final.py` is current)
- `logs/` — old stdout/stderr from past runs
- Legacy `domain_clusters_{model}_r{N}.csv` in `Data/processed/` — superseded by `cluster_tables/` outputs, but kept because `compare_embedding_models.py` still references them
