"""
build_paper_methods_results.py — Version B: Methods + Results + Figures only.

Extensive figure captions, detailed stats, all tables.
No intro/discussion — user writes those separately.
"""
import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from docx import Document
from docx.shared import Inches, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PUB  = ROOT / "Graphs" / "pub"
OUT  = ROOT / "Paper" / "thalamus_methods_results_figures_v2.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)

doc = Document()

# ── Styles ──
for section in doc.sections:
    section.top_margin = Inches(1)
    section.bottom_margin = Inches(1)
    section.left_margin = Inches(1)
    section.right_margin = Inches(1)


def h1(text):
    return doc.add_heading(text, level=1)

def h2(text):
    return doc.add_heading(text, level=2)

def h3(text):
    return doc.add_heading(text, level=3)

def body(text):
    return doc.add_paragraph(text, style="Normal")

def add_fig(path, caption, width=Inches(6.5)):
    """Insert figure with caption."""
    full = PUB / path
    if full.exists():
        doc.add_picture(str(full), width=width)
        p = doc.add_paragraph(caption, style="Normal")
        p.runs[0].italic = True
        p.runs[0].font.size = Pt(9)
        print(f"  Inserted: {path}")
    else:
        body(f"[MISSING FIGURE: {path}]")
        print(f"  MISSING: {path}")


# ═══════════════════════════════════════════════════════════════════════════
print("Building Methods + Results + Figures document...")

h1("Methods, Results, and Figures")
body("Thalamus Framing Analysis — Computational Text-Mining of 17,280 Papers (1961–2026)")
body("Version B: Methods + Results + Figures with extensive descriptions. "
     "351,220 thalamus-mentioning sentences from 14,004 papers. "
     "43% full-text coverage via PMC, institutional access (UvA LibKey), and OA PDFs.")

# ═══════════════════════════════════════════════════════════════════════════
# METHODS
# ═══════════════════════════════════════════════════════════════════════════
doc.add_page_break()
h1("1. Methods")

h2("1.1 Corpus Construction")
body(
    "Papers were retrieved from PubMed using the query 'thalamus[Title] OR thalamic[Title]' "
    "spanning 1950–2026, yielding 17,280 papers. Full-text was obtained from four sources: "
    "(1) PubMed Central XML via OAI-PMH API (3,296 papers), "
    "(2) institutional access via University of Amsterdam LibKey (3,843 papers), "
    "(3) open-access PDFs via Unpaywall/Semantic Scholar (256 papers), and "
    "(4) PubMed abstracts as fallback (6,609 papers). "
    "Priority: PMC > institutional > OA > abstract. "
    "Sentences were tokenized with NLTK sent_tokenize, filtered for 'thalam' mentions "
    "and 20–600 character length, yielding 351,220 sentences from 14,004 papers. "
    "94% of sentences come from full-text sources; 6% from abstracts only."
)

h2("1.2 Frame Detection (Regex-Based)")
body(
    "21 frame types organized in 3 groups: Metaphorical (9: relay, hub, gateway, gate, "
    "switchboard, blackboard, bridge, bottleneck, interface), Functional (9: modulator, "
    "integrator, filter, regulator, coordinator, pacemaker, amplifier, distributor, processor), "
    "and Descriptive (3: network_node, clinical_target, anti_relay). Each frame is detected "
    "by a regex pattern applied to all 351,220 sentences. A sentence matches at most one frame "
    "(first-match priority). This detected 6,062 framing sentences across 3,002 papers."
)

h2("1.3 Mega-Theme Assignment")
body(
    "Each sentence was assigned to one of 8 mega-themes via keyword matching on sentence "
    "content (first match wins): Clinical/disease (26.0%), Circuit/cellular (15.0%), "
    "Sensory/perception (11.4%), Methodology/imaging (5.5%), Development/anatomy (5.2%), "
    "Motor (5.2%), Cognitive/behavioral (4.8%), Sleep/oscillations (3.4%). "
    "Residual 'Other' category: 23.5%."
)

h2("1.4 Sentence Embeddings")
body(
    "All 351,220 sentences were embedded with all-MiniLM-L6-v2 (sentence-transformers) "
    "on an NVIDIA RTX 3060 GPU, producing 384-dimensional vectors. "
    "Embeddings used for BERTopic clustering, UMAP visualization, and similarity analyses."
)

h2("1.5 BERTopic Topic Modeling")
body(
    "BERTopic was applied to sentence embeddings using HDBSCAN clustering. "
    "186 topics were identified. Topics were tracked over time using BERTopic's "
    "Topics Over Time module with 5-year bins."
)

h2("1.6 Nucleus Tagging")
body(
    "16 thalamic nuclei were identified via regex patterns: MD, VP/VPM, VL, Pulvinar/LP, "
    "LGN, MGN, ANT, CM, CL, PF, PO/POm, PVT, Reuniens, TRN, AD, VM. "
    "Each sentence was tagged with all matching nuclei (multi-label)."
)

h2("1.7 Technique and Task Detection")
body(
    "11 recording techniques (fMRI, electrophysiology, optogenetics, etc.) and "
    "10 experimental paradigms (working memory, fear conditioning, etc.) were detected "
    "via regex patterns applied to all papers."
)

h2("1.8 Hierarchical Clustering of Nuclei")
body(
    "Feature vectors: 40-dimensional per nucleus (21 framing + 8 mega-theme + 11 technique proportions). "
    "Six clustering methods: 2 distances (cosine, correlation) × 3 linkages (Ward, complete, average). "
    "Cophenetic correlation for method validation. Adjusted Rand Index (ARI) for cross-method agreement "
    "(mean ARI = 0.658). Consensus co-clustering matrix from all 6 methods."
)

h2("1.9 Syntactic Role Classification")
body(
    "spaCy (en_core_web_md) dependency parsing on 31,062 sentences (all 6,062 framing + "
    "25,000 random non-framing). Each thalamus mention classified as: SUBJECT (38.6%), "
    "OBJECT (21.3%), PREPOSITIONAL (33.5%), or NONE (6.6%). Active vs passive voice "
    "detected via nsubjpass dependency relation."
)

h2("1.10 Diachronic Word Embeddings")
body(
    "Following Hamilton et al. (2016), decade-specific word2vec models (skip-gram, dim=200, "
    "window=5, min_count=10, 15 epochs) were trained per decade (1980s–2020s) and per "
    "5-year bin (1980–2025). The 2010s/2020 model served as anchor space. All other models "
    "aligned via Orthogonal Procrustes (scipy.linalg.orthogonal_procrustes). "
    "Semantic displacement: cosine distance between consecutive time bins. "
    "Pairwise similarity: cosine between 'thalamus' and 16 conceptual anchors per decade."
)

h2("1.11 Co-occurrence Network Analysis")
body(
    "Sentence-level word co-occurrence networks per decade. Content words only "
    "(stopwords, brain regions, method terms, OCR artifacts filtered). "
    "Top 500 edges per decade. Degree centrality tracked over time. "
    "Louvain community detection on full-corpus network (441 nodes, 1,000 edges)."
)

h2("1.12 Citation Network Analysis")
body(
    "Citation data for all 17,280 papers retrieved from OpenAlex API (17,064 found, 98.8%). "
    "Directed within-corpus citation graph: 17,280 nodes, 159,809 edges. "
    "Per-decade subgraphs with Leiden clustering (leidenalg, ModularityVertexPartition). "
    "Key nodes identified by PageRank. Betweenness centrality for bridging papers. "
    "Modularity ranges from 0.58 (1980s) to 0.64 (2020s)."
)

h2("1.13 Confound Analysis")
body(
    "Logistic regression: hub (vs relay) ~ year + fMRI + electrophysiology + lesion. "
    "Year OR = 1.17 [95% CI: 1.13–1.21], p < 1e-30 after controlling for technique. "
    "fMRI independently associated with hub framing (OR = 3.71, p = 0.009)."
)

h2("1.14 LLM Paper-Level Frame Classification")
body(
    "Zero-shot classification using facebook/bart-large-mnli on 7,182 introduction sections. "
    "8 codebook categories derived from inductive coding of 200 stratified introductions: "
    "sensory_relay, motor_integration, inhibitory_gate, network_hub, oscillatory, "
    "cognitive, clinical, developmental. Each intro classified to single best-matching "
    "category with confidence score (high >0.5, medium 0.3–0.5, low <0.3)."
)

# ═══════════════════════════════════════════════════════════════════════════
# RESULTS + FIGURES
# ═══════════════════════════════════════════════════════════════════════════
doc.add_page_break()
h1("2. Results and Figures")

# ── Corpus Overview ──
h2("2.1 Corpus Overview")
body(
    "The corpus comprises 17,280 PubMed papers (1961–2026) yielding 351,220 "
    "thalamus-mentioning sentences from 14,004 papers. Publication volume increased "
    "from ~100/year in the 1970s to >600/year in the 2020s. Full-text coverage is 43%, "
    "with 94% of sentences derived from full-text sources. The top 20 journals account for "
    "32.6% of papers, led by Brain Research (782), J Comparative Neurology (682), "
    "and J Neuroscience (629). 20% of papers lack abstracts (pre-digital era). "
    "Framing detection rate increases from 1.1% (1980s) to 2.3% (2020s), "
    "suggesting increasingly explicit metaphorical language."
)
add_fig("fig_corpus_overview.png",
        "Figure 1. Corpus overview. (A) Papers per year (PubMed, 1975–2026). "
        "(B) Sentence source distribution per decade: PMC fulltext (blue), institutional/UvA "
        "LibKey fulltext (orange), OA PDF (green), abstract only (grey). "
        "(C) Top 15 journals by paper count. (D) Framing detection rate per decade "
        "(% of sentences matching any of 21 frame patterns).")

# ── Framing Over Time ──
h2("2.2 Framing Shift Over Time")
body(
    "Relay is the most prevalent frame (2,078 sentences) but its relative proportion declines "
    "from the 1980s onward. Gate (929), hub (692), gateway (629), clinical_target (439), "
    "and pacemaker (410) show rising or stable trajectories. The relay-to-hub shift is "
    "confirmed by Mann-Kendall trend test: hub τ = +0.82 (p = 0.0004). "
    "Year remains a significant predictor of hub vs relay framing after controlling for "
    "recording technique (logistic regression OR = 1.17/year, p < 1e-30)."
)
add_fig("fig3_framing_over_time.png",
        "Figure 2. Conceptual framing of the thalamus over time (n = 6,062 framing sentences). "
        "(A) Proportion of 21 frame types across five decades, showing the diversification "
        "of thalamic framing from relay-dominated (1980s) to a multi-frame vocabulary. "
        "(B) Relay vs hub prevalence with Wilson confidence intervals. "
        "(C) Stacked area plot of the top 8 frame types. "
        "(D) Decade-specific frame distributions.")

# ── Nuclei × Techniques ──
h2("2.3 Framing Across Nuclei and Techniques")
body(
    "Nucleus-specific analysis across 16 thalamic nuclei reveals precise functional "
    "vocabulary: VP/VPM is predominantly relay-framed, TRN is gate-framed, "
    "MD and PVT are hub/gateway-framed. Chi-squared test confirms significant "
    "technique-framing association (χ² = 569.2, df = 49, p = 1.05e-89). "
    "fMRI papers favor hub framing; electrophysiology papers maintain relay framing."
)
add_fig("fig4_framing_nuclei_techniques.png",
        "Figure 3. Framing across thalamic nuclei and recording techniques "
        "(n = 6,062 framing sentences for panels A–B; n = 14,004 papers for panels C–F). "
        "(A) Column-normalized heatmap: each column sums to 100%, showing frame distribution "
        "per nucleus. (B) Row-normalized heatmap: each row sums to 100%, showing which nuclei "
        "are most associated with each frame. (C–F) Technique prevalence and framing balance "
        "per technique, showing fMRI favors hub while electrophysiology favors relay.")

# ── Nucleus Clustering ──
h2("2.4 Nucleus Similarity and Functional Groupings")
body(
    "Hierarchical clustering of 16 nuclei by 40-dimensional feature vectors (framing + "
    "mega-theme + technique proportions) reveals three consensus groups across 6 clustering "
    "methods (mean ARI = 0.658): (i) sensory relay nuclei (VP, LGN, MGN, VL), "
    "(ii) higher-order association nuclei (MD, PVT, Reuniens, ANT), and "
    "(iii) modulatory/intralaminar nuclei (TRN, CM, CL, PF)."
)
add_fig("fig5_nucleus_similarity.png",
        "Figure 4. Nucleus similarity and functional groupings (16 thalamic nuclei). "
        "(A) Best dendrogram (cosine + Ward, cophenetic r = 0.82). "
        "(B) Consensus co-clustering heatmap (fraction of 6 methods agreeing). "
        "(C) MDS 2D projection colored by dominant frame. "
        "(D) Framing profile heatmap (nucleus × frame_type). "
        "(E) Technique profile heatmap (nucleus × technique). "
        "(F) Method comparison: cophenetic correlations for all 6 methods.")

# ── Syntactic Roles ──
h2("2.5 Syntactic Agency of the Thalamus")
body(
    "Dependency parsing reveals a temporal shift in grammatical role: "
    "SUBJECT increases from 35.9% (1980s) to 42.3% (2020s), "
    "PREPOSITIONAL decreases from 39.7% to 27.7%. "
    "When thalamus is subject, active voice dominates (70%), with verbs: "
    "serve, play, receive, contribute. When object: examine, investigate, demonstrate. "
    "The subject:object ratio increases across decades, indicating growing active agency."
)
add_fig("fig_syntactic_roles.png",
        "Figure 5. Syntactic role of the thalamus in scientific text. "
        "(A) Grammatical role distribution per decade: SUBJECT (blue, rising 35.9%→42.3%), "
        "OBJECT (red, rising 15.6%→25.5%), PREPOSITIONAL (orange, declining 39.7%→27.7%), "
        "NONE (grey, declining). Based on spaCy dependency parsing of 31,062 sentences. "
        "(B) Subject:Object agency ratio per decade (>1 indicates more active framing). "
        "(C) Top 12 verbs when thalamus is grammatical subject (active agency). "
        "(D) Top 12 verbs when thalamus is grammatical object (passive framing).")

# ── Diachronic Embeddings ──
h2("2.6 Diachronic Word Embeddings")
body(
    "Decade-specific word2vec models aligned via Procrustes show: "
    "thalamus↔cortex similarity increases strongly (+0.264 from 1980s to 2020s), "
    "followed by thalamus↔hub (+0.113), thalamus↔network (+0.093), "
    "thalamus↔sensory (+0.102). thalamus↔gate decreases (−0.125). "
    "Semantic displacement analysis with 5-year bins confirms the largest shift "
    "in the 1980s–1990s period (cosine distance 0.57), stabilizing after 2005."
)
add_fig("fig_displacement_5yr.png",
        "Figure 6. Semantic displacement over time (5-year bins, Procrustes-aligned word2vec). "
        "(A) Displacement of frame terms: 'thalamus' (orange), 'relay' (blue), 'hub' (red), "
        "'gate' (green), 'network' (brown), 'connectivity' (purple). Higher values indicate "
        "larger semantic shift between consecutive 5-year periods. "
        "(B) Displacement of nucleus terms: mediodorsal, pulvinar, geniculate, reticular, "
        "ventral, anterior, intralaminar, paraventricular. "
        "(C) Thalamus displacement vs control words (cortex, neuron, brain, receptor, synaptic) "
        "shown in grey dashed lines.")

add_fig("fig_w2v_similarity.png",
        "Figure 7. Pairwise similarity trajectories (decade-level word2vec). "
        "Cosine similarity between the 'thalamus' vector and key conceptual anchors "
        "across decades in Procrustes-aligned word2vec space. "
        "Strongest increases: thalamus↔cortex (+0.264), thalamus↔hub (+0.113), "
        "thalamus↔network (+0.093). Strongest decrease: thalamus↔gate (−0.125). "
        "Lines connect decade centroids; each point represents one decade's word2vec model.")

add_fig("fig_neighbors_filtered.png",
        "Figure 8. Semantic neighbors of key framing terms across decades "
        "(brain regions, OCR artifacts, and author names filtered; morphological variants collapsed). "
        "(A) Neighbors of 'relay': dominated by 'station' and 'gating' in 1980s, expanding to include "
        "'integrator', 'gateway', 'gatekeeper' by 2020s — indicating semantic convergence with hub/gate. "
        "(B) Neighbors of 'hub': only available from 2010s (too rare before). Shows proper network "
        "vocabulary: connector, node, integrative, nexus, gatekeeper. "
        "(C) Neighbors of 'gate': evolves from 'transfer'/'station' (1990s) to "
        "'gatekeeper'/'searchlight'/'orchestrate'/'modulate' (2020s) — gate becomes active, not just inhibitory. "
        "Cell values show cosine similarity. Only decades with >50 occurrences of the target word shown.")

# ── Word Frequency Trends ──
h2("2.7 Vocabulary Evolution")
body(
    "After aggressive filtering (stopwords, brain regions, method terms, author names, "
    "PDF boilerplate), the clearest vocabulary trends are: "
    "'connectivity' rises from 0% (1980s) to 8.96% (2020s) — the single largest vocabulary shift. "
    "'functional' rises from 0% to 5.81%. 'network' appears in 2010s (2.73%) and grows to 3.86%. "
    "Declining: 'projections' (7.86%→2.94%), 'reticular' (5.47%→declining), "
    "'complex' and 'ventrobasal' disappear from top-20."
)
add_fig("fig_word_trends.png",
        "Figure 9. Rising and falling vocabulary in thalamus literature. "
        "Filtered: no stopwords, brain region names, method terms, or PDF boilerplate. "
        "Colored lines (↑): rising terms. Grey dashed lines (↓): declining terms. "
        "The dominant shift is from anatomical vocabulary (projections, reticular, complex) "
        "to systems/connectivity vocabulary (connectivity, functional, network, volume).")

# ── Co-occurrence ──
h2("2.8 Co-occurrence Network Evolution")
body(
    "Sentence-level co-occurrence networks per decade show structural vocabulary shift. "
    "1980s: dominated by anatomical pairs (horseradish–peroxidase, ventrobasal–complex, "
    "spinothalamic–tract). "
    "1990s: oscillation vocabulary emerges (oscillations–spindle, spike–wave). "
    "2000s: clinical vocabulary rises (deep–brain, essential–tremor). "
    "2010s–2020s: connectivity vocabulary dominates (functional–connectivity at 2,568 co-occurrences "
    "in 2020s, resting–state, brain–connectivity). "
    "Degree centrality: 'neurons' declines (−0.18), 'connectivity'/'brain' rise (+0.04)."
)
add_fig("fig_cooccurrence_temporal.png",
        "Figure 10. Co-occurrence network analysis. "
        "(A) Sentence-level word co-occurrence network for 1990s (filtered, top 500 edges). "
        "(B) Same for 2020s, showing structural shift from anatomy to connectivity vocabulary. "
        "(C) Top co-occurring words per nucleus (MD, VP, LGN, TRN, Pulvinar). "
        "(D) Rising and falling concepts by degree centrality change (1990s→2020s).")

# ── Confound ──
h2("2.9 Confound Analysis")
body(
    "Logistic regression confirms the temporal framing shift persists after controlling "
    "for recording technique. Year OR = 1.17 per year [95% CI: 1.13–1.21], z = 11.5, "
    "p < 1e-30. fMRI independently associated with hub framing (OR = 3.71, p = 0.009). "
    "This dual finding — genuine temporal trend + genuine methodological influence — "
    "suggests both intellectual evolution and technical affordances drive the shift."
)
add_fig("fig_confound_lme_v2.png",
        "Figure 11. Confound analysis. "
        "(A) Logistic regression: hub vs relay framing predicted by year, fMRI, "
        "electrophysiology, and lesion study. Year remains highly significant after "
        "controlling for all technique indicators. "
        "(B) Temporal trend persists within fMRI and non-fMRI subsets separately. "
        "(C) Forest plot of predictor effects with 95% confidence intervals.")

# ── Citation Network ──
h2("2.10 Citation Network Analysis")
body(
    "The within-corpus citation network comprises 17,280 nodes and 159,809 edges. "
    "OpenAlex provided data for 17,064 papers (98.8%). Total citations: 806,203 "
    "(mean 46.7/paper, median 18). Citation inequality is high (Gini 0.60–0.71). "
    "Leiden clustering identifies 15–22 communities per decade with increasing modularity "
    "(0.58→0.64), indicating growing field specialization. "
    "The most-cited within-corpus paper is Steriade et al. (1993) 'Thalamocortical oscillations' "
    "with 507 local citations. Hub framing appears in citation communities from the 2010s onward."
)
add_fig("fig_citation_detailed.png",
        "Figure 12. Citation network overview. "
        "(A) Publication volume and citation impact over time. "
        "(B) Citation distribution per decade (violin plots). "
        "(C) Mean citations by dominant frame type. "
        "(D) Top 50 most-cited papers colored by frame (blue=relay, red=hub, orange=gate). "
        "(E) Cross-decade citation flow heatmap (% of each decade's references). "
        "(F) Within-corpus citation density per decade.")

# Per-decade citation networks
for dec in [1980, 1990, 2000, 2010, 2020]:
    add_fig(f"fig_citation_v3_{dec}.png",
            f"Figure 12{chr(97+[1980,1990,2000,2010,2020].index(dec))}. "
            f"Citation network — {dec}s. "
            f"Top 120 papers by PageRank, colored by Leiden cluster. "
            f"Edge width scales with connection strength. "
            f"Labels show first author et al. (year) for top 3 papers per cluster. "
            f"Frame annotations in brackets where detected.")

# ── BERTopic ──
h2("2.11 Research Topics (BERTopic)")
add_fig("fig2_research_topics.png",
        "Figure 13. BERTopic topic model overview. "
        "186 topics identified from 351,220 sentence embeddings via HDBSCAN clustering. "
        "Topic prevalence tracked over 5-year bins.")

# ── Task Extraction ──
h2("2.12 Experimental Paradigms")
add_fig("fig_task_extraction_v2.png",
        "Figure 14. Experimental paradigm classification. "
        "10 paradigm categories detected via regex across all 14,004 papers. "
        "Task-nucleus cross-tabulation confirms expected associations: "
        "fear conditioning → ANT/PVT, spatial memory → ANT, pain → VP/VPM, "
        "auditory → MGN.")

# ── Techniques ──
h2("2.13 Recording Techniques")
add_fig("fig6_techniques_v3.png",
        "Figure 15. Recording technique prevalence and framing balance. "
        "11 technique categories tracked across decades. "
        "fMRI rises sharply from 2000s; electrophysiology remains stable. "
        "Small-multiples show technique-specific framing trajectories.")

# ── Supplementary ──
doc.add_page_break()
h1("3. Supplementary Figures")

sup_figs = [
    ("sup_S1_umap_topics.png",
     "Figure S1. UMAP visualization of sentence embeddings colored by BERTopic topic. "
     "Grey = unassigned by HDBSCAN (67%, expected for heterogeneous sentence data)."),
    ("sup_S2_abstract_vs_fulltext.png",
     "Figure S2. Abstract vs full-text replication. Key framing trends replicate across "
     "both data sources (r > 0.9)."),
    ("sup_S3_robustness.png",
     "Figure S3. Frame detection robustness. Primary vs all-pattern comparison; "
     "multi-frame overlap analysis."),
    ("sup_S4_frame_derivation.png",
     "Figure S4. Frame derivation pipeline. Stage 1: verb triggers (11,089 candidates). "
     "Stage 2: proto-categories. Stage 3: 21 final patterns. "
     "Stage 4: mega-theme composition (Other = 23.5%)."),
    ("sup_S5_nucleus_trends.png",
     "Figure S5. Nucleus prevalence trends (Mann-Kendall). VP/VPM declining (τ=−1.00), "
     "ANT/PVT/PO rising (τ=+0.60)."),
    ("fig_w2v_displacement.png",
     "Figure S6. Semantic displacement curves (decade-level). Target words vs control words. "
     "Largest displacement for 'thalamus' in 1980s→1990s (0.43)."),
    ("fig_w2v_umap_decades.png",
     "Figure S7. Per-decade UMAP of aligned word2vec space. Red = thalamus/nucleus terms, "
     "blue = conceptual anchors. Shows spatial rearrangement across decades."),
    ("fig_w2v_neighbors.png",
     "Figure S8. Semantic neighborhood heatmap for 'thalamus' (unfiltered). "
     "Top 30 nearest neighbors across decades."),
    ("fig_corpus_overview.png",
     "Figure S9. Detailed corpus overview. Papers per year, source distribution, "
     "journal distribution, framing detection rate."),
]

for fname, caption in sup_figs:
    add_fig(fname, caption, width=Inches(6.0))

# ── LLM Classification (if available) ──
llm_fig = PUB / "fig_llm_frames_over_time.png"
if llm_fig.exists():
    doc.add_page_break()
    h2("2.14 LLM Paper-Level Frame Classification")
    body(
        "Zero-shot BART-NLI classification of 7,182 introduction sections into 8 codebook "
        "categories. Results pending — figure will show frame prevalence per decade "
        "and classification confidence distribution."
    )
    add_fig("fig_llm_frames_over_time.png",
            "Figure 16. LLM paper-level frame classification (BART-NLI, 8 categories). "
            "(A) Stacked bar: frame proportions per decade. "
            "(B) Classification confidence distribution per decade.")

# ═══════════════════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════════════════
doc.save(str(OUT))
print(f"\nSaved: {OUT}")
print("Done.")
