"""
write_clustering_methods.py — Generate Paper/clustering_methods_writeup.docx.

Produces a narrative methods section for the paper-level domain clustering,
combining:
  - Pipeline overview (embed → refine → graph → Leiden → select → characterise)
  - Hyperparameters (matching the Valerio et al. reference methodology)
  - Model-selection rationale (pulls numbers from model_selection_scores.csv)
  - Stability diagnostic (pulls numbers from stability_full.csv)
  - Modularity-based resolution selection (from modularity_sweep.csv)
  - Hierarchical structure summary (from umbrella_labels_*.csv)

Outputs:
    Paper/clustering_methods_writeup.docx
"""
from __future__ import annotations
import sys, io, os
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pathlib import Path
import numpy as np
import pandas as pd

from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH

from _clustering_utils import PROC  # noqa: E402

ROOT = PROC.parents[1]
STAB_DIR = PROC / "clustering_stability"
OUT = ROOT / "Paper" / "clustering_methods_writeup.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)


def safe_read(p: Path):
    return pd.read_csv(p) if p.exists() else None


def add_heading(doc, text, level=1):
    doc.add_heading(text, level=level)


def add_para(doc, text, size=11, bold=False):
    p = doc.add_paragraph()
    run = p.add_run(text)
    run.font.size = Pt(size)
    if bold:
        run.bold = True
    return p


def add_bullets(doc, items):
    for item in items:
        p = doc.add_paragraph(style="List Bullet")
        p.add_run(item).font.size = Pt(11)


def main():
    doc = Document()
    for section in doc.sections:
        section.top_margin = Inches(0.8)
        section.bottom_margin = Inches(0.8)
        section.left_margin = Inches(0.9)
        section.right_margin = Inches(0.9)

    add_heading(doc, "Methods: paper-level domain clustering", level=1)
    add_para(doc,
        "To map the topic structure of the thalamus literature and to check "
        "whether the framing shifts we report are confined to particular "
        "subdomains, we grouped abstracts into research-domain clusters "
        "following the methodology of Valerio et al. (\"The Evolving "
        "Landscape of Neuroscience\"). Our pipeline has four stages — "
        "embedding, optional contrastive refinement, graph-based clustering, "
        "and characterisation — evaluated across three language-model "
        "backbones and two dimensionality conditions so the final choice "
        "rests on quantitative criteria rather than a single configuration.")

    add_heading(doc, "Corpus", level=2)
    papers_path = PROC / "paper_abstracts.csv"
    n_papers = len(pd.read_csv(papers_path, low_memory=False)) if papers_path.exists() else "≈14,900"
    add_para(doc,
        f"We restricted the analysis to the {n_papers} papers in our thalamus "
        f"corpus with a title and an abstract ≥ 50 characters (of 18,891 "
        f"papers total). Titles were concatenated with abstracts using the "
        f"SPECTER2 convention "
        f"(\"title [SEP] abstract\") and truncated at 2,000 characters.")

    add_heading(doc, "Stage 1 — embedding", level=2)
    add_para(doc,
        "Each abstract was embedded with three pre-trained language models "
        "chosen to cover general, biomedical, and scientific text:")
    add_bullets(doc, [
        "SPECTER2 (AllenAI) — 768-dim contrastively pretrained on citation pairs from scientific papers; used as the primary backbone.",
        "PubMedBERT (neuml/pubmedbert-base-embeddings) — 768-dim, biomedical-domain.",
        "MPNet (sentence-transformers/all-mpnet-base-v2) — 768-dim, general sentence embedding.",
    ])
    add_para(doc,
        "All three produce unit-normalised 768-dim vectors. Embeddings were "
        "L2-normalised so that inner-product queries in FAISS correspond to "
        "cosine similarity.")

    add_heading(doc, "Stage 2 — contrastive refinement", level=2)
    add_para(doc,
        "Following the reference methodology, we trained a small feed-forward "
        "neural network to map each 768-dim embedding into a 64-dim "
        "domain-specific representation. Architecture: three hidden layers "
        "(512 → 256 → 128) with batch normalisation after the first two, ELU "
        "activations throughout, and dropout of 0.1 after the first two "
        "activations. A final linear layer projects to 64 dimensions and the "
        "output is L2-normalised.")
    add_para(doc,
        "Training used a self-supervised multi-positive multi-negative "
        "InfoNCE variant: for each anchor, positives are the other examples "
        "whose raw-embedding cosine similarity is ≥ 0.85 and negatives are "
        "those with similarity < 0.75. Intermediate pairs (0.75–0.85) "
        "contribute only through an additive +1 in the denominator, which "
        "keeps the loss bounded when an anchor has few explicit positives or "
        "negatives in a batch. Temperature was 0.1. An additional off-diagonal "
        "correlation penalty on the 64 output dimensions (weight 0.1) "
        "discouraged redundant features.")
    add_para(doc,
        "Optimisation used Adam (learning rate 1e-4 with exponential decay "
        "γ = 0.995, L2 weight decay 0.01) for 2,000 epochs with batch size "
        "256; 95/5 train/val split. Grokfast (α = 0.98, λ = 2.0) was applied "
        "to the gradients to accelerate convergence. The same architecture "
        "and hyperparameters were applied to each of the three embeddings "
        "(see code/analysis/refine_all_embeddings.py).")

    add_heading(doc, "Stage 3 — graph construction and Leiden clustering", level=2)
    add_para(doc,
        "For each embedding set (raw 768-dim or refined 64-dim), we built a "
        "semantic similarity graph. A FAISS IndexFlatIP was used to retrieve "
        "the top 50 nearest neighbours of every paper in cosine distance; "
        "this defines a directed k-NN graph which we symmetrised with "
        "combine_edges=\"max\" (so reciprocated edges take the larger of the "
        "two similarities).")
    add_para(doc,
        "We clustered the resulting undirected graph with Leiden community "
        "detection (leidenalg 0.10.2) using the Constant Potts Model. To "
        "select the resolution we swept 100 log-spaced values spanning the "
        "meaningful range for this corpus (roughly 5e-4 to 0.1) and recorded "
        "the modularity at each. The working resolution was chosen as the "
        "argmax of modularity subject to producing ≥ 15 clusters of "
        "≥ 50 papers — this balances resolution against our need for "
        "subdomain granularity.")

    mod_picked = safe_read(STAB_DIR / "modularity_picked_resolution.csv")
    if mod_picked is not None:
        add_para(doc, "Argmax-modularity resolutions per configuration:",
                 bold=True)
        lines = []
        for _, r in mod_picked.iterrows():
            lines.append(f"{r['model']} / {r['condition']}: "
                         f"resolution={r['resolution']:.4f}, "
                         f"modularity={r['modularity']:.3f}, "
                         f"n_clusters={int(r['n_clusters'])}")
        add_bullets(doc, lines)

    add_heading(doc, "Stage 4 — stability and model selection", level=2)
    add_para(doc,
        "To avoid anchoring on a single configuration we ran the full "
        "pipeline under every combination of 3 embeddings × 2 dimensionality "
        "conditions × 4 resolutions (0.005, 0.008, 0.012, 0.020) — 24 "
        "configurations in total. For every cluster in every configuration "
        "we computed the best Jaccard overlap against clusters in each of "
        "the other 23 configurations, and took the mean as that cluster's "
        "stability score.")

    scores = safe_read(STAB_DIR / "model_selection_scores.csv")
    if scores is not None and len(scores) > 0:
        winner = scores.iloc[0]
        top3 = scores.head(3)
        add_para(doc,
            "Four complementary metrics were tracked per (model, condition) "
            "at the working resolution (each normalised to [0, 1] across the "
            "6 configurations):", bold=True)
        add_bullets(doc, [
            "Stability — mean per-cluster Jaccard across the other 23 configurations.",
            "Silhouette — mean silhouette (on the 2-D UMAP projection) over a 5,000-paper sample.",
            "Domain coverage — fraction of 14 canonical thalamic subfields (visual, auditory, somatosensory, pain, DBS, clinical, development, epilepsy, sleep, memory, stroke, oscillation, tracing, circuit) for which some cluster was ≥ 50% dominated by that subfield.",
            "Modularity — Leiden modularity at the working resolution.",
        ])
        add_para(doc,
            f"The composite score (equal-weighted average of the four "
            f"normalised metrics) selected {winner['config']} as the "
            f"working configuration (composite = {winner['composite']:.3f}). "
            f"Top three:",
            bold=True)
        add_bullets(doc, [
            f"{r['config']}: stability={r['stability']:.3f}, "
            f"silhouette={r['silhouette']:.3f}, "
            f"domain_coverage={r['domain_cov']:.3f}, "
            f"modularity={r['modularity']:.3f} "
            f"(composite = {r['composite']:.3f})"
            for _, r in top3.iterrows()
        ])

    add_heading(doc, "Stage 5 — hierarchical organisation", level=2)
    add_para(doc,
        "To recover broader research themes that encompass several Leiden "
        "clusters, we treated each cluster as a single unit (mean "
        "L2-normalised embedding of its members) and applied "
        "average-linkage agglomerative clustering on cosine distances "
        "between centroids. Flat cuts at k ∈ {4, 6, 8, 10, 12} umbrella "
        "groups were recorded; the k = 6 cut is reported in the main text "
        "(see Paper/cluster_tables.docx and Figure X for the dendrogram and "
        "umbrella-level TF-IDF terms).")

    add_heading(doc, "Reproducibility", level=2)
    add_para(doc,
        "All random seeds are fixed (Leiden seed = 42, train/validation "
        "split seed = 42, UMAP random_state = 42). The pipeline is driven by "
        "six scripts under Code/analysis/ (see CLUSTERING_README.md for the "
        "canonical execution order); each caches its outputs so reruns are "
        "cheap. A step-by-step walkthrough is available as "
        "clustering_walkthrough.ipynb.")

    doc.save(str(OUT))
    print(f"Saved: {OUT}")


if __name__ == "__main__":
    main()
