import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
"""
build_paper_v10.py
Generates thalamus_framing_paper_v10.docx using python-docx.

Key changes from v9:
  - Updated abstract (2,536 combined framing sentences, mega-themes, Other <27%)
  - Methods 2.2: topic label refinement / mega-theme info
  - Methods 2.3: more precise framing detection description
  - Methods 2.7: conclusion theme extraction
  - Results 3.4: new nucleus similarity section with expanded text
  - Discussion: technique-frame co-evolution paragraph with stats
  - Figure reorder: 1-Pipeline, 2-Topics, 3-Framing, 4-Framing*Nuclei,
    5-NucleusSimilarity, 6-Framing*Topics, 7-Techniques, 8-Tasks, 9-Confound
  - Updated supplementary notes (S1 grey points, S4 anti_relay / c-TF-IDF)
  - All figures from Graphs/pub/
"""

import os
from pathlib import Path
from docx import Document
from docx.shared import Inches, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

# -- paths -----------------------------------------------------------------
ROOT = Path(r"C:\Users\brouw\Documents\PhD\Projects\LiteratureQueeryThalamusTime")
PUB  = ROOT / "Graphs" / "pub"
OUT  = ROOT / "Paper" / "thalamus_framing_paper_v10.docx"
OUT.parent.mkdir(parents=True, exist_ok=True)

# -- figure mapping --------------------------------------------------------
MAIN_FIGS = {
    1: (PUB, "fig1_pipeline_corpus.png"),
    2: (PUB, "fig2_research_topics.png"),
    3: (PUB, "fig3_framing_over_time.png"),
    4: (PUB, "fig4_framing_nuclei_techniques.png"),
    5: (PUB, "fig5_nucleus_similarity.png"),
    6: (PUB, "fig6_framing_topics.png"),
    7: (PUB, "fig6_techniques_v2.png"),
    8: (PUB, "fig_task_extraction.png"),
    9: (PUB, "fig_confound_lme.png"),
}

MAIN_CAPTIONS = {
    1: ("Figure 1. Analysis pipeline and corpus overview. "
        "(A) Data collection pipeline showing PubMed, PMC, and open-access DOI lookup sources. "
        "(B) Corpus statistics: 17,280 abstracts and 55,848 full-text sentences. "
        "(C) BERTopic topic model overview with 186 research topics. "
        "(D) Temporal distribution of the corpus."),
    2: ("Figure 2. Research topics over time. "
        "(A) Temporal evolution of the top 15 research topics identified by BERTopic. "
        "(B) Mann-Kendall trend statistics for emerging and declining topics. "
        "(C) Topic prevalence heatmap across five-year bins."),
    3: ("Figure 3. Conceptual framing of the thalamus over time. "
        "(A) Proportion of 21 metaphorical frame types across five decades. "
        "(B) Relay framing dominance in the 1980s\u20131990s and subsequent decline. "
        "(C) Hub framing overtaking relay in the 2020s (Mann-Kendall \u03c4 = +0.52, p < 0.001). "
        "(D) Simultaneous emergence of gateway, clinical target, and other specific frames."),
    4: ("Figure 4. Framing across thalamic nuclei and recording techniques. "
        "(A) Frame distribution heatmap per thalamic nucleus. "
        "(B) Frame distribution heatmap per recording technique. "
        "(C) Temporal prevalence of framing for selected nuclei. "
        "(D) Nucleus-level discourse employs precise functional vocabulary rather than relay/hub labels."),
    5: ("Figure 5. Nucleus similarity and functional groupings. "
        "(A) Hierarchical clustering of 19 thalamic nuclei by framing profile. "
        "(B) Hierarchical clustering by research topic profile. "
        "(C) Hierarchical clustering by technique profile. "
        "(D) Combined clustering synthesizing framing, topic, and technique dimensions. "
        "(E) Cosine similarity matrix across nucleus framing profiles."),
    6: ("Figure 6. Framing and research topics. "
        "(A) Frame distribution across the top BERTopic research topics. "
        "(B) Topic-frame associations: epilepsy \u2192 clinical_target, pain \u2192 relay, "
        "schizophrenia \u2192 gate, auditory \u2192 relay. "
        "(C) Chi-squared residuals highlighting significant associations."),
    7: ("Figure 7. Recording techniques over time. "
        "(A) Temporal prevalence of 11 recording techniques. "
        "(B) Fastest-growing: optogenetics (\u03c4 = +0.858) and chemogenetics (\u03c4 = +0.909). "
        "(C) Declining: tract tracing and lesion studies. "
        "(D) Technique co-occurrence with thalamic nuclei."),
    8: ("Figure 8. Experimental paradigms and task extraction. "
        "(A) Prevalence of 10 experimental paradigm categories across the corpus. "
        "(B) Task-nucleus cross-tabulation: fear conditioning \u2192 ANT/PVT, "
        "spatial memory \u2192 ANT, pain \u2192 VP/VPM, auditory \u2192 MGN. "
        "(C) Temporal trends in experimental paradigm usage."),
    9: ("Figure 9. Confound analysis: logistic regression. "
        "(A) Odds ratios for hub vs. relay framing: year (OR = 1.17, p < 1e-30), "
        "fMRI (OR = 3.71, p = 0.009). "
        "(B) Temporal trend persists after controlling for recording technique. "
        "(C) Forest plot of predictor effects with 95% confidence intervals."),
}

SUP_FIGS = {
    "S1": (PUB, "sup_S1_umap_topics.png",
           "Figure S1. UMAP visualization. "
           "UMAP projection of sentence embeddings colored by BERTopic topic assignment, "
           "showing cluster separation and topic coherence across the 186-topic model. "
           "Grey points = sentences not assigned to any topic by HDBSCAN (67% of corpus; "
           "expected with conservative clustering on heterogeneous sentence data)."),
    "S2": (PUB, "sup_S2_abstract_vs_fulltext.png",
           "Figure S2. Abstract vs. full-text replication. "
           "(A) Framing proportions estimated from abstracts only vs. full-text sentences. "
           "(B) Correlation between abstract and full-text Mann-Kendall trend statistics. "
           "Key trends replicate across both data sources."),
    "S3": (PUB, "sup_S3_robustness.png",
           "Figure S3. Frame detection robustness. "
           "(A) Primary vs. all-pattern match comparison for each frame type. "
           "(B) Multi-frame overlap analysis showing co-occurrence patterns."),
    "S4": (PUB, "sup_S4_topic_keywords.png",
           "Figure S4. Topic keyword profiles. "
           "Top keywords for the 12 largest BERTopic research topics, showing the most "
           "representative terms identified by c-TF-IDF weighting. "
           "anti_relay = sentences explicitly challenging the relay metaphor "
           "('not merely a relay', 'more than just a relay'). "
           "c-TF-IDF = class-based TF-IDF, weighting terms by their representativeness "
           "for each topic."),
}


# -- helpers ---------------------------------------------------------------
def set_style_font(doc, style_name, font_name="Calibri", size_pt=11,
                   bold=False, italic=False):
    style = doc.styles[style_name]
    font = style.font
    font.name = font_name
    font.size = Pt(size_pt)
    font.bold = bold
    font.italic = italic
    pf = style.paragraph_format
    pf.space_after = Pt(6)
    return style


def add_body(doc, text):
    return doc.add_paragraph(text, style="Normal")


def add_heading(doc, text, level=1):
    return doc.add_heading(text, level=level)


def add_figure(doc, fig_num=None, sup_key=None, width=None):
    if width is None:
        width = Inches(6.0) if sup_key else Inches(6.5)

    if fig_num is not None:
        fig_dir, fname = MAIN_FIGS.get(fig_num, (None, None))
        caption = MAIN_CAPTIONS.get(fig_num, "")
    elif sup_key is not None:
        entry = SUP_FIGS.get(sup_key)
        if entry is None:
            return
        fig_dir, fname, caption = entry
    else:
        return

    if fname is None:
        return

    fpath = fig_dir / fname
    if not fpath.exists():
        print(f"  WARNING: figure not found, skipping: {fpath}")
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run(f"[IMAGE NOT FOUND: {fname}]")
        run.italic = True
        run.font.size = Pt(10)
    else:
        p = doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run()
        run.add_picture(str(fpath), width=width)
        print(f"  Inserted: {fname}")

    cap_p = doc.add_paragraph()
    cap_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    cap_run = cap_p.add_run(caption)
    cap_run.italic = True
    cap_run.font.size = Pt(10)
    cap_run.font.name = "Calibri"
    cap_p.paragraph_format.space_after = Pt(12)


# -- build document --------------------------------------------------------
def build():
    doc = Document()

    # Page margins: 1 inch all around
    for section in doc.sections:
        section.top_margin = Inches(1)
        section.bottom_margin = Inches(1)
        section.left_margin = Inches(1)
        section.right_margin = Inches(1)

    # Styles
    set_style_font(doc, "Normal", size_pt=11)
    set_style_font(doc, "Heading 1", size_pt=14, bold=True)
    set_style_font(doc, "Heading 2", size_pt=12, bold=True)
    set_style_font(doc, "Heading 3", size_pt=11, bold=True, italic=True)

    # ================================================================
    # TITLE
    # ================================================================
    title_p = doc.add_paragraph()
    title_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title_run = title_p.add_run(
        "From relay station to integrative hub: Tracing the conceptual "
        "evolution of the thalamus through five decades of neuroscience literature"
    )
    title_run.bold = True
    title_run.font.size = Pt(14)
    title_run.font.name = "Calibri"
    title_p.paragraph_format.space_after = Pt(18)

    # ================================================================
    # ABSTRACT  (updated: 2,536 combined, mega-themes, Other <27%)
    # ================================================================
    add_heading(doc, "Abstract", level=1)
    add_body(doc,
        "The thalamus has undergone a fundamental reconceptualization in neuroscience. "
        "Using computational text-mining of 17,280 PubMed abstracts and 55,848 full-text "
        "sentences spanning 1975\u20132026, we trace this evolution through two complementary "
        "analyses. Topic modeling (BERTopic) identified 186 research topics organized into "
        "seven mega-themes (Clinical/disease, Sensory/perception, Motor, Cognitive/behavioral, "
        "Sleep/oscillations, Circuit/cellular, Development/anatomy) with the residual Other "
        "category comprising less than 27% of topics. Temporal trends reveal shifts from "
        "anatomical tract-tracing and lesion studies towards functional connectivity, "
        "optogenetics (Mann-Kendall \u03c4 = +0.858), and clinical neurostimulation. "
        "Conceptual frame analysis, using 21 regex-based pattern detectors for metaphorical "
        "descriptions of thalamic function, identified 2,536 combined framing sentences "
        "(1,621 full-text and 935 abstract, 2 overlapping) with no residual unclassified "
        "category. The relay metaphor dominated the 1980s (76% of frames) but has been "
        "progressively supplemented by hub (\u03c4 = +0.52, p < 0.001), gateway, clinical "
        "target, and other specific functional descriptions, with hub framing overtaking "
        "relay in the 2020s. Logistic regression controlling for recording technique "
        "confirms this temporal shift is not a methodological artifact (year OR = 1.17, "
        "p < 1e-30), though fMRI is independently associated with hub framing (OR = 3.71, "
        "p = 0.009). Recording technique and framing are strongly associated "
        "(\u03c7\u00b2 p = 2.5e-23): DBS papers predominantly use clinical_target framing, "
        "electrophysiology favours relay, and fMRI favours hub. Nucleus-specific analysis "
        "across 19 thalamic nuclei reveals that individual nuclei are described with precise "
        "functional vocabulary rather than relay/hub labels \u2014 the relay-vs-hub debate "
        "operates at the whole-thalamus level. Hierarchical clustering of nuclei by their "
        "framing, topic, and technique profiles reveals functional groupings: relay-dominant "
        "sensory nuclei (VP, VL, LGN, MGN), hub-dominant association nuclei (MD, PVT, "
        "Reuniens), and gate-dominant modulatory nuclei (TRN). Task/paradigm analysis shows "
        "working memory, spatial navigation, and fear conditioning as the most common "
        "experimental contexts. These findings demonstrate that the field has developed an "
        "increasingly rich and precise conceptual vocabulary for thalamic function."
    )

    # Keywords
    kw_p = doc.add_paragraph()
    kw_run = kw_p.add_run("Keywords: ")
    kw_run.bold = True
    kw_run.font.name = "Calibri"
    kw_run.font.size = Pt(11)
    kw_p.add_run(
        "thalamus, text mining, BERTopic, conceptual framing, metaphor, "
        "neuroscience history, topic modeling"
    ).font.size = Pt(11)

    # ================================================================
    # 1. INTRODUCTION
    # ================================================================
    add_heading(doc, "1. Introduction", level=1)
    add_body(doc,
        "Scientific concepts evolve, and the language used to describe brain structures "
        "both reflects and shapes theoretical understanding. The thalamus offers a "
        "compelling case study in conceptual evolution because it has been described "
        "using a rich vocabulary of metaphorical language \u2014 relay station, hub, gateway, "
        "switchboard \u2014 that carries implicit theoretical commitments about its "
        "computational role. Unlike purely terminological changes, these metaphors encode "
        "assumptions about whether the thalamus passively transmits, actively integrates, "
        "or selectively gates information flow in the brain."
    )
    add_body(doc,
        "For much of the 20th century, the thalamus was conceived primarily as a sensory "
        "relay station: a set of nuclei that faithfully forwarded peripheral sensory "
        "signals to their cortical targets. This view was grounded in the anatomical "
        "observation that most sensory pathways synapse in the thalamus before reaching "
        "cortex, and was reinforced by electrophysiological studies demonstrating high-"
        "fidelity signal transmission through thalamic relay cells. However, accumulating "
        "evidence from multiple methodological traditions has challenged this simple "
        "characterization."
    )
    add_body(doc,
        "The advent of functional neuroimaging, particularly fMRI, revealed that thalamic "
        "nuclei participate in large-scale brain networks extending far beyond classical "
        "sensory pathways, supporting a reconceptualization of the thalamus as a network "
        "hub or integrative center. Simultaneously, optogenetic and chemogenetic "
        "approaches have enabled precise interrogation of thalamic circuit function, "
        "revealing complex modulatory roles that transcend passive relay. The growing "
        "clinical interest in deep brain stimulation (DBS) of thalamic targets has "
        "introduced yet another conceptual frame: the thalamus as a therapeutic target "
        "whose modulation can ameliorate neurological and psychiatric conditions."
    )
    add_body(doc,
        "Computational text mining provides a systematic approach to tracing such "
        "conceptual shifts at a scale impossible through traditional literature review. "
        "By applying natural language processing to thousands of scientific articles, we "
        "can quantify the prevalence, temporal dynamics, and contextual associations of "
        "different conceptual frames. This approach complements qualitative analyses of "
        "scientific metaphor by providing empirical evidence of when, where, and how "
        "metaphorical descriptions change."
    )
    add_body(doc,
        "In this study, we address five research questions: "
        "(1) What research topics characterize thalamus studies, and how have they evolved? "
        "(2) What metaphorical frames are used to describe the thalamus? "
        "(3) How have these frames changed over five decades? "
        "(4) Do framing patterns differ by recording technique and thalamic nucleus? "
        "(5) What is the relationship between research topics, experimental paradigms, "
        "and conceptual framing?"
    )

    # ================================================================
    # 2. METHODS
    # ================================================================
    add_heading(doc, "2. Methods", level=1)

    # 2.1 Corpus construction
    add_heading(doc, "2.1 Corpus construction", level=2)
    add_body(doc,
        "We constructed a comprehensive corpus of thalamus research literature from "
        "PubMed. The search query targeted articles containing \"thalamus\" or \"thalamic\" "
        "in the title field, yielding 17,280 unique articles published between 1950 and "
        "2026. For each article, we retrieved the abstract directly from PubMed. Full-text "
        "articles were obtained through two complementary sources: PubMed Central (PMC) "
        "open-access archive and DOI-based open-access lookup via open-access DOI lookup, yielding "
        "3,468 full-text papers."
    )
    add_body(doc,
        "Sentence tokenization was performed using NLTK's Punkt tokenizer with a regex "
        "fallback for edge cases. We retained sentences that mentioned the thalamus or "
        "thalamic structures, yielding 55,848 thalamus-mentioning sentences from full-text "
        "articles and 45,665 sentences from abstracts. This dual corpus (abstracts for "
        "broad coverage, full text for detailed analysis) allows cross-validation of "
        "findings across data sources."
    )

    # 2.2 Topic modeling (updated: mega-theme info)
    add_heading(doc, "2.2 Topic modeling", level=2)
    add_body(doc,
        "Topic modeling was performed using BERTopic v0.17.4, a transformer-based topic "
        "modeling framework that leverages contextual sentence embeddings. The pipeline "
        "consisted of: (1) sentence embedding with all-MiniLM-L6-v2 (sentence-transformers "
        "5.3.0, producing 384-dimensional vectors); (2) dimensionality reduction with "
        "UMAP-learn 0.5.11 (5 components, cosine metric, 30 neighbours, min_dist = 0.05); "
        "(3) clustering with HDBSCAN 0.8.41 (min_cluster_size = 25, Euclidean metric, EOM "
        "selection method, prediction_data enabled); (4) topic representation via c-TF-IDF "
        "with CountVectorizer (English stop words removed)."
    )
    add_body(doc,
        "The initial model produced 303 topics, which were automatically reduced to 186 "
        "through BERTopic's built-in topic merging of semantically similar clusters. One "
        "catch-all topic containing generic thalamus descriptions (n = 11,643 sentences) "
        "and one PDF-artifact topic were removed from analysis. Approximately 33% of "
        "sentences were confidently assigned to topics; the remainder constitutes expected "
        "HDBSCAN noise, which is typical for heterogeneous sentence-level data where many "
        "sentences do not belong to any coherent cluster."
    )
    add_body(doc,
        "Topic labels were refined through systematic keyword-based categorization into "
        "seven mega-themes (Clinical/disease, Sensory/perception, Motor, "
        "Cognitive/behavioral, Sleep/oscillations, Circuit/cellular, "
        "Development/anatomy, Methodology/imaging). Two artifact topics (MR spectroscopy "
        "ratios and histological methodology) were removed."
    )

    # Figure 1
    add_figure(doc, fig_num=1)

    # 2.3 Framing detection (updated: more precise)
    add_heading(doc, "2.3 Framing detection", level=2)
    add_body(doc,
        "We developed 21 regex patterns targeting metaphorical and conceptual descriptions "
        "of thalamic function. These emerged from an iterative discovery process: "
        "(1) initial extraction using verb-trigger patterns ('the thalamus is/serves "
        "as/acts as...') across all 55,848 sentences, (2) manual inspection of 200+ "
        "random sentences to catalogue all metaphors used in the literature, "
        "(3) systematic refinement into 21 distinct frame types. The types are organized "
        "into three semantic groups: metaphorical frames (relay, hub, gateway, gate, "
        "switchboard, blackboard, bridge, bottleneck, interface), functional frames "
        "(modulator, integrator, filter, regulator, coordinator, pacemaker, amplifier, "
        "distributor, processor), and descriptive frames (network_node, clinical_target, "
        "anti_relay). The 'anti_relay' category captures sentences explicitly challenging "
        "the relay metaphor ('not merely a relay', 'more than just a relay'). Emphasis "
        "descriptors ('key structure', 'critical region') were excluded as they convey "
        "importance without specifying a functional metaphor. Sentences not matching any "
        "pattern were excluded \u2014 there is no residual 'other' category. This yielded "
        "1,621 full-text and 935 abstract framing sentences (2,536 combined, 2 overlapping)."
    )

    # 2.4 Recording technique classification
    add_heading(doc, "2.4 Recording technique classification", level=2)
    add_body(doc,
        "Recording techniques were classified using 11 regex-based pattern detectors "
        "applied to both abstracts and full-text sentences. The technique categories are: "
        "fMRI, electrophysiology, calcium imaging, optogenetics, chemogenetics, EEG/MEG, "
        "PET, tract tracing, DBS, lesion studies, and structural MRI. Each paper was "
        "assigned all matching technique labels, allowing multi-technique studies to be "
        "correctly represented."
    )

    # 2.5 Nucleus identification
    add_heading(doc, "2.5 Nucleus identification", level=2)
    add_body(doc,
        "Nineteen thalamic nuclei were identified via regex pattern matching of standard "
        "anatomical nomenclature and common abbreviations: mediodorsal (MD), "
        "ventral posterolateral/posteromedial (VP/VPM), ventral lateral (VL), pulvinar, "
        "dorsal lateral geniculate nucleus (dLGN), anterior nuclear group (ANT), "
        "centromedian (CM), paraventricular (PVT), thalamic reticular nucleus (TRN), "
        "centrolateral (CL), posterior medial (POm), nucleus reuniens, habenula, medial "
        "geniculate (MGN/MGB), anterodorsal (AD), lateral posterior (LP), posterior (PO), "
        "parafascicular (PF), and ventromedial (VM)."
    )

    # 2.6 Task/paradigm classification
    add_heading(doc, "2.6 Task/paradigm classification", level=2)
    add_body(doc,
        "Experimental paradigms were classified into 10 categories using regex-based "
        "pattern matching: working memory, spatial memory, fear conditioning, sensory "
        "discrimination, attention, motor tasks, pain assessment, sleep recording, "
        "reward/motivation, and decision-making. Patterns were designed to capture both "
        "explicit paradigm names and characteristic methodological descriptions associated "
        "with each category."
    )

    # 2.7 Statistical methods (updated: conclusion theme extraction)
    add_heading(doc, "2.7 Statistical methods", level=2)
    add_body(doc,
        "Temporal trends were assessed using the Mann-Kendall monotonic trend test on "
        "annual proportions. Wilson score 95% confidence intervals were computed for all "
        "proportions. Categorical associations between framing, techniques, and nuclei "
        "were tested using chi-squared tests. Confound analysis employed logistic "
        "regression (statsmodels GLM with binomial family) predicting hub vs. relay "
        "framing from year, recording technique indicators (fMRI, electrophysiology, "
        "lesion study), and publication type. All analyses were conducted in Python 3.11."
    )
    add_body(doc,
        "Conclusion sections were identified in full-text papers using regex matching "
        "of section headers. Thalamus-mentioning conclusion sentences (n = 44,301) were "
        "classified into 10 thematic categories using keyword patterns: "
        "Network/connectivity, Clinical/therapeutic, Cognitive/attention, Sensory/circuit, "
        "Memory/learning, Sleep/oscillations, Pain/nociception, Motor/movement, "
        "Development/plasticity, and Emotion/stress."
    )

    # ================================================================
    # 3. RESULTS
    # ================================================================
    add_heading(doc, "3. Results", level=1)

    # 3.1
    add_heading(doc, "3.1 The thalamic research landscape", level=2)
    add_body(doc,
        "BERTopic identified 186 research topics spanning the full breadth of thalamus "
        "research (Figure 2). The most prevalent topics include seizures and epilepsy, "
        "pain and nociception, schizophrenia, sleep, motor control and basal ganglia "
        "interactions, auditory processing, tremor and DBS, brain volume, and viral "
        "tracing. Temporal evolution analysis reveals a clear methodological transition: "
        "anatomical tract-tracing and lesion study topics have declined over recent "
        "decades, while functional connectivity, optogenetics, and chemogenetics topics "
        "have emerged as dominant research themes."
    )
    add_body(doc,
        "Vision-related research is distributed across several topics (pulvinar/visual "
        "attention, LGN/retinal processing, superior colliculus interactions) rather than "
        "forming a single dominant cluster, reflecting the distributed nature of visual "
        "thalamic research across multiple nuclei and processing stages. This "
        "fragmentation is itself informative, suggesting that visual thalamic research "
        "encompasses distinct sub-communities with different focal nuclei and questions."
    )

    add_figure(doc, fig_num=2)

    # 3.2
    add_heading(doc, "3.2 Conceptual framing of the thalamus", level=2)
    add_body(doc,
        "The relay metaphor dominated the conceptual landscape of thalamus research in "
        "the 1980s, accounting for approximately 76% of all framing sentences in abstracts "
        "(Figure 3). Over the subsequent four decades, this dominance has been progressively "
        "eroded as alternative frames emerged. In the 2020s, hub framing (approximately "
        "30% of framing sentences) overtook relay framing (approximately 20%) for the "
        "first time, marking a symbolic inflection point in how the neuroscience community "
        "conceptualizes the thalamus."
    )
    add_body(doc,
        "Gate and gateway framing has maintained a consistent presence since the 1980s, "
        "reflecting the longstanding literature on the thalamic reticular nucleus and its "
        "role in selective gating of thalamocortical transmission. Clinical target framing "
        "emerged primarily in the 2010s, coinciding with the growth of DBS research for "
        "epilepsy, tremor, and pain. Mann-Kendall trend analysis confirms relay is "
        "declining (\u03c4 = \u22120.31, p < 0.05) while hub (\u03c4 = +0.52, p < 0.001), "
        "gateway, and clinical_target frames are all significantly increasing."
    )
    add_body(doc,
        "The relay-to-hub crossover occurred approximately in 2019\u20132020, with hub "
        "framing showing a steeper growth trajectory. Importantly, this is not simply a "
        "one-for-one replacement: all 21 specific frame types are present in recent "
        "literature, indicating that the field has developed an increasingly diverse "
        "conceptual vocabulary rather than merely substituting one dominant metaphor for "
        "another."
    )

    add_figure(doc, fig_num=3)

    # 3.3
    add_heading(doc, "3.3 Framing across nuclei and techniques", level=2)
    add_body(doc,
        "Framing profiles differ substantially across thalamic nuclei (Figure 4). Sensory "
        "relay nuclei \u2014 the ventral posterior (VP), ventral lateral (VL), lateral "
        "geniculate (LGN), and medial geniculate (MGN) nuclei \u2014 are predominantly "
        "described using relay framing, consistent with their classical role in faithfully "
        "transmitting sensory and motor signals to cortex. In contrast, association nuclei "
        "such as the mediodorsal (MD), paraventricular (PVT), and reuniens nuclei "
        "preferentially attract hub framing, reflecting their participation in distributed "
        "cognitive and limbic networks."
    )
    add_body(doc,
        "The thalamic reticular nucleus (TRN) is uniquely associated with gate and gating "
        "metaphors, consistent with its established role as an inhibitory modulator of "
        "thalamocortical transmission. Clinical target framing concentrates in the anterior "
        "nuclear group (ANT) and centromedian nucleus (CM), which are the primary DBS "
        "targets for epilepsy and pain, respectively."
    )
    add_body(doc,
        "Recording techniques also show distinct framing patterns: fMRI papers "
        "preferentially use hub and network_node framing; electrophysiology papers favour "
        "relay framing; DBS papers predominantly use clinical_target framing; and "
        "optogenetics papers employ modulator and gateway language. These associations "
        "suggest that the choice of recording technique shapes not only what aspects of "
        "thalamic function are studied but also how researchers conceptualize and describe "
        "those functions."
    )

    add_figure(doc, fig_num=4)

    # 3.4 Nucleus similarity (updated: expanded text)
    add_heading(doc, "3.4 Nucleus similarity and functional groupings", level=2)
    add_body(doc,
        "Hierarchical clustering of 19 nuclei by their framing, topic, and technique "
        "profiles reveals distinct functional groupings (Figure 5). When clustered by "
        "framing profile, sensory nuclei (VP, LGN, MGN) group together with relay-dominant "
        "framing, while association nuclei (MD, PVT) cluster separately with hub-dominant "
        "framing. TRN forms its own cluster with gate-dominant framing."
    )
    add_body(doc,
        "When clustered by research topic profile, the groupings shift: nuclei studied "
        "in similar disease contexts cluster together regardless of their framing. "
        "Technique profiles reveal a third grouping: nuclei targeted by DBS (ANT, CM, CL) "
        "form a clinical cluster distinct from nuclei studied primarily with "
        "electrophysiology (LGN, MGN, VP)."
    )
    add_body(doc,
        "The combined clustering synthesizes all three dimensions, revealing that thalamic "
        "nuclei occupy distinct positions in a multidimensional conceptual space defined "
        "by how they are described, what they are studied in the context of, and what "
        "methods are used. These data-driven groupings are not predetermined by the framing "
        "categories but emerge from the joint distribution of framing, topic, and technique "
        "associations, providing independent validation that the conceptual vocabulary used "
        "in the literature reflects genuine functional distinctions among thalamic nuclei."
    )

    add_figure(doc, fig_num=5)

    # 3.5
    add_heading(doc, "3.5 Framing and research topics", level=2)
    add_body(doc,
        "Topic-framing associations reveal meaningful patterns linking research content "
        "to conceptual vocabulary (Figure 6). Epilepsy research topics are strongly "
        "associated with clinical_target framing, reflecting the prominence of DBS and "
        "neurostimulation approaches. Pain research topics favour relay framing, "
        "consistent with the classical view of the ventral posterior thalamus as a relay "
        "in the spinothalamic pain pathway."
    )
    add_body(doc,
        "Schizophrenia research topics are associated with gate and filter framing, "
        "echoing influential theories of thalamic sensory gating deficits in "
        "schizophrenia. Auditory processing topics favour relay framing (via the medial "
        "geniculate nucleus). Sleep research topics show associations with pacemaker and "
        "gate framing, reflecting the thalamus's dual role in generating sleep oscillations "
        "and gating sensory input during sleep."
    )

    add_figure(doc, fig_num=6)

    # 3.6
    add_heading(doc, "3.6 Recording techniques and framing evolution", level=2)
    add_body(doc,
        "The association between recording technique and conceptual framing is highly "
        "significant (\u03c7\u00b2 test, p = 2.5e-23), confirming that methodological "
        "choice and conceptual vocabulary are tightly coupled (Figure 7). Small-multiples "
        "temporal analysis reveals that the rise of hub framing is most pronounced in "
        "fMRI papers, while electrophysiology papers maintain relatively stable relay "
        "framing across decades."
    )
    add_body(doc,
        "This observation raises the question of whether the relay-to-hub shift is merely "
        "a methodological artifact of the growing dominance of fMRI in thalamus research. "
        "We address this question directly in Section 3.8 through logistic regression, "
        "demonstrating that the temporal trend persists after controlling for recording "
        "technique. Nevertheless, the strong technique-framing association indicates that "
        "methodological and conceptual evolution are intertwined: new techniques enable "
        "new observations, which in turn motivate new conceptual frameworks."
    )

    add_figure(doc, fig_num=7)

    # 3.7
    add_heading(doc, "3.7 Experimental paradigms", level=2)
    add_body(doc,
        "Task and paradigm classification across the corpus reveals sleep recording, "
        "working memory, and motor tasks as the most frequently mentioned experimental "
        "contexts (Figure 8). Task-nucleus cross-tabulation confirms expected "
        "associations: fear conditioning is primarily linked to ANT and PVT, spatial "
        "memory to ANT and other hippocampus-connected nuclei, pain assessment to VP/VPM, "
        "and auditory discrimination to MGN."
    )
    add_body(doc,
        "Temporal trends in paradigm usage show the emergence of optogenetic manipulation "
        "paradigms and reward/motivation tasks in recent years, while classical sensory "
        "discrimination paradigms have remained stable. Working memory paradigms show a "
        "notable increase coinciding with growing interest in the mediodorsal nucleus and "
        "prefrontal-thalamic circuits."
    )

    add_figure(doc, fig_num=8)

    # 3.8
    add_heading(doc, "3.8 Confound analysis", level=2)
    add_body(doc,
        "To determine whether the temporal shift from relay to hub framing is "
        "independent of methodological changes, we fitted a logistic regression model "
        "predicting hub (vs. relay) framing from publication year, fMRI usage, "
        "electrophysiology usage, and lesion study presence (Figure 9). Year remains a "
        "highly significant predictor (OR = 1.17 per year, z = 11.5, p < 1e-30) after "
        "controlling for all technique indicators, confirming that the conceptual shift "
        "is not merely a consequence of changing methodology."
    )
    add_body(doc,
        "fMRI is independently associated with hub framing (OR = 3.71, p = 0.009), "
        "indicating that papers using fMRI are approximately 3.7 times more likely to "
        "describe the thalamus as a hub than as a relay, even after accounting for "
        "temporal trends. This dual finding \u2014 a genuine temporal trend that exists "
        "independently of methodology, coupled with a genuine methodological influence "
        "on framing \u2014 suggests that both intellectual evolution and technical "
        "affordances contribute to the changing conceptual landscape."
    )

    add_figure(doc, fig_num=9)

    # ================================================================
    # 4. DISCUSSION
    # ================================================================
    add_heading(doc, "4. Discussion", level=1)

    # 4.1
    add_heading(doc, "4.1 Beyond relay-to-hub: a diversifying conceptual vocabulary", level=2)
    add_body(doc,
        "The central finding of this study is that the conceptual evolution of the "
        "thalamus cannot be reduced to a simple relay-to-hub transition. While hub "
        "framing has indeed overtaken relay framing in the 2020s, the more important "
        "trend is the diversification of the conceptual vocabulary. All 21 specific "
        "frame types are represented in recent literature, and the proportional dominance "
        "of any single metaphor has decreased over time. The field has moved from a "
        "monoculture (relay) to an ecosystem of conceptual frames, each capturing "
        "distinct aspects of thalamic function."
    )
    add_body(doc,
        "This diversity is theoretically significant. The blackboard metaphor (cf. "
        "Mumford, 1991) emphasizes the thalamus as a shared workspace for cortical "
        "computation. Pacemaker framing, rooted in the sleep and oscillation literature, "
        "highlights the thalamus's role in generating and sustaining rhythmic neural "
        "activity. Bottleneck framing, from the attention literature, conceptualizes the "
        "thalamus as a capacity-limited processing stage. Interface framing, drawn from "
        "computational models, positions the thalamus as a translational layer between "
        "different processing regimes. Each metaphor carries distinct experimental "
        "predictions and theoretical commitments."
    )

    # 4.2 (updated: technique-frame co-evolution with stats)
    add_heading(doc, "4.2 Technique-frame co-evolution", level=2)
    add_body(doc,
        "Recording techniques and conceptual frames co-evolve: the introduction of fMRI "
        "enabled visualization of thalamic participation in large-scale networks, naturally "
        "favouring hub/network framing (OR = 3.71, p = 0.009). Optogenetics, by enabling "
        "precise circuit-level manipulation, is associated with modulator and gateway "
        "framing. DBS studies almost exclusively use clinical_target framing. However, "
        "logistic regression controlling for technique confirms the temporal shift from "
        "relay to hub persists independently (year OR = 1.17, p < 1e-30), indicating "
        "that the conceptual transformation is not merely an artifact of methodological "
        "change."
    )
    add_body(doc,
        "This is not a confound to be eliminated but a feature of scientific practice: "
        "new tools generate new types of evidence, which require new conceptual frameworks "
        "for interpretation. The reciprocal relationship between technique and framing "
        "illustrates how methodological innovation drives conceptual change in "
        "neuroscience."
    )

    # 4.3
    add_heading(doc, "4.3 The scale distinction", level=2)
    add_body(doc,
        "A key insight from the nucleus-level analysis is that the relay-vs-hub debate "
        "operates primarily at the level of the whole thalamus as a unified concept. "
        "When researchers discuss individual nuclei, they employ precise functional "
        "descriptions rather than broad metaphorical labels. The LGN is described in "
        "terms of visual processing and receptive field properties, not as a \"relay\" "
        "or \"hub.\" The TRN is described in terms of inhibition and gating, not with "
        "generic architectural metaphors."
    )
    add_body(doc,
        "This scale distinction has important implications. The relay-vs-hub debate "
        "may be partly a consequence of discussing the thalamus at a level of abstraction "
        "that obscures the functional heterogeneity of its constituent nuclei. Our "
        "hierarchical clustering analysis shows that different nuclei naturally group "
        "into functionally meaningful clusters based on their conceptual profiles, "
        "suggesting that the diversity of thalamic function is better captured by a "
        "taxonomy of nucleus-specific roles than by a single overarching metaphor."
    )

    # 4.4
    add_heading(doc, "4.4 Limitations", level=2)
    add_body(doc,
        "Several limitations should be acknowledged. First, the full-text corpus is "
        "biased towards post-2008 publications due to PMC open-access availability, "
        "though abstract-level analyses confirm that key trends replicate across both "
        "data sources (Figure S2). Second, the corpus is limited to English-language "
        "publications, potentially missing conceptual developments in non-English "
        "neuroscience traditions."
    )
    add_body(doc,
        "Third, regex-based frame detection has inherent sensitivity limitations: it "
        "captures explicit metaphorical language but may miss implicit conceptual "
        "framing. Sensitivity analyses (Figure S3) show that our main findings are "
        "robust to threshold variations. Fourth, HDBSCAN's conservative clustering "
        "approach leaves approximately 67% of sentences unassigned to topics, which "
        "is expected for heterogeneous sentence-level data but limits the coverage of "
        "topic-based analyses. Fifth, Wilson confidence intervals are wide for rare "
        "frame categories in early decades when sample sizes are small. Finally, there "
        "may exist conceptual frames not captured by our 21 regex patterns, though "
        "manual inspection of unmatched sentences suggests this residual is small."
    )

    # ================================================================
    # 5. CONCLUSION
    # ================================================================
    add_heading(doc, "5. Conclusion", level=1)
    add_body(doc,
        "Through computational text mining of five decades of thalamus research, we have "
        "traced a fundamental reconceptualization of this brain structure. The thalamus "
        "has evolved from being described primarily as a passive relay station to being "
        "characterized through a rich and diverse conceptual vocabulary encompassing hub, "
        "gateway, modulator, integrator, clinical target, and numerous other functional "
        "descriptions. This shift is not a methodological artifact but reflects a genuine "
        "intellectual evolution in how neuroscientists understand thalamic function."
    )
    add_body(doc,
        "Crucially, the field has not simply replaced one metaphor with another. The "
        "increasing diversity of conceptual frames \u2014 from 21 distinct categories, "
        "each associated with specific nuclei, techniques, and research topics \u2014 "
        "demonstrates that the neuroscience community has developed an increasingly "
        "precise vocabulary for describing thalamic function. The thalamus is not merely "
        "a relay or a hub; it is a collection of functionally distinct nuclei, each "
        "described with the specific language appropriate to its role in brain circuitry."
    )

    # ================================================================
    # SUPPLEMENTARY MATERIALS
    # ================================================================
    doc.add_page_break()
    add_heading(doc, "Supplementary Materials", level=1)

    for key in ("S1", "S2", "S3", "S4"):
        add_figure(doc, sup_key=key)

    # ================================================================
    # SAVE
    # ================================================================
    doc.save(str(OUT))
    print(f"\nSaved: {OUT}")


if __name__ == "__main__":
    build()
