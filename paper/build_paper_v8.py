import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
"""
build_paper_v8.py
Generates thalamus_framing_paper_v8.docx using python-docx.

Key changes from v7:
  - TWO CLEAR ANALYSES: topics (BERTopic) vs framing (pattern-matching)
  - NO "other" category — 21 genuine metaphorical frame types
  - 5 main figures + 1 technique figure + 4 supplementary figures
  - Honest frame detection description
  - Updated title, abstract, and structure
"""

import os
from pathlib import Path
from docx import Document
from docx.shared import Inches, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

# ── paths ──────────────────────────────────────────────────────────────
ROOT = Path(r"C:\Users\brouw\Documents\PhD\Projects\LiteratureQueeryThalamusTime")
FIG_DIR = ROOT / "Graphs" / "pub"
OUT_DIR = ROOT / "Paper"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "thalamus_framing_paper_v8.docx"

# ── figure mapping ─────────────────────────────────────────────────────
MAIN_FIGS = {
    1: "fig1_pipeline_corpus_overview.png",
    2: "fig2_research_topics.png",
    3: "fig3_framing_over_time.png",
    4: "fig4_framing_nuclei_techniques.png",
    5: "fig5_framing_topics.png",
    6: "fig7_techniques.png",   # reused from earlier; renumbered as Figure 6
}

MAIN_CAPTIONS = {
    1: ("Figure 1. Analysis pipeline and corpus overview. "
        "(A) Data collection pipeline showing PubMed, PMC, and Unpaywall sources. "
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
    5: ("Figure 5. Framing and research topics. "
        "(A) Frame distribution across the top BERTopic research topics. "
        "(B) Topic-frame associations: epilepsy\u2192clinical_target, pain\u2192relay, "
        "schizophrenia\u2192gate, auditory\u2192relay. "
        "(C) Chi-squared residuals highlighting significant associations."),
    6: ("Figure 6. Recording techniques over time. "
        "(A) Temporal prevalence of 11 recording techniques. "
        "(B) Fastest-growing: optogenetics (\u03c4 = +0.858) and chemogenetics (\u03c4 = +0.909). "
        "(C) Declining: tract tracing and lesion studies. "
        "(D) Technique co-occurrence with thalamic nuclei."),
}

SUP_FIGS = {
    "S1": ("sup_S1_umap_topics.png",
           "Figure S1. UMAP visualization. "
           "UMAP projection of sentence embeddings colored by BERTopic topic assignment, "
           "showing cluster separation and topic coherence across the 186-topic model."),
    "S2": ("sup_S2_abstract_vs_fulltext_replication.png",
           "Figure S2. Abstract vs. full-text replication. "
           "(A) Framing proportions estimated from abstracts only vs. full-text sentences. "
           "(B) Correlation between abstract and full-text Mann-Kendall trend statistics. "
           "Key trends replicate across both data sources."),
    "S3": ("sup_S3_robustness_frame_detection.png",
           "Figure S3. Frame detection robustness. "
           "(A) Precision and recall estimates for each of the 21 frame types. "
           "(B) Sensitivity analysis: framing trends with relaxed and strict pattern thresholds. "
           "(C) Hub framing with and without fMRI papers."),
    "S4": ("sup_S4_topic_wordclouds.png",
           "Figure S4. Topic keyword profiles. "
           "Word clouds for the top 20 BERTopic research topics, showing the most "
           "representative terms identified by c-TF-IDF weighting."),
}


# ── helpers ────────────────────────────────────────────────────────────
def set_style_font(doc, style_name, font_name="Calibri", size_pt=11,
                   bold=False, italic=False):
    """Configure a built-in style."""
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
    """Add a Normal paragraph."""
    return doc.add_paragraph(text, style="Normal")


def add_figure(doc, fig_num=None, sup_key=None, width=Inches(6.5)):
    """Insert a figure image + italic caption. Skips gracefully if missing."""
    if fig_num is not None:
        fname = MAIN_FIGS.get(fig_num)
        caption = MAIN_CAPTIONS.get(fig_num, "")
    elif sup_key is not None:
        fname, caption = SUP_FIGS.get(sup_key, (None, ""))
    else:
        return

    if fname is None:
        return

    fpath = FIG_DIR / fname
    if not fpath.exists():
        print(f"  WARNING: figure not found, skipping: {fpath.name}")
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

    # Caption
    cap_p = doc.add_paragraph()
    cap_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    cap_run = cap_p.add_run(caption)
    cap_run.italic = True
    cap_run.font.size = Pt(10)
    cap_run.font.name = "Calibri"
    cap_p.paragraph_format.space_after = Pt(12)


# ── build document ─────────────────────────────────────────────────────
def build():
    doc = Document()

    # Page margins: 1 inch all around
    for section in doc.sections:
        section.top_margin = Inches(1)
        section.bottom_margin = Inches(1)
        section.left_margin = Inches(1)
        section.right_margin = Inches(1)

    # Configure styles
    set_style_font(doc, "Title", size_pt=14, bold=True)
    set_style_font(doc, "Heading 1", size_pt=13, bold=True)
    set_style_font(doc, "Heading 2", size_pt=12, bold=True)
    set_style_font(doc, "Normal", size_pt=11)

    # ── Title ──────────────────────────────────────────────────────────
    title_p = doc.add_paragraph(
        "From relay station to integrative hub: Tracing the conceptual evolution "
        "of the thalamus through five decades of neuroscience literature",
        style="Title"
    )
    title_p.alignment = WD_ALIGN_PARAGRAPH.CENTER

    # Authors
    auth_p = doc.add_paragraph()
    auth_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    auth_run = auth_p.add_run("[Authors to be filled]")
    auth_run.font.size = Pt(11)
    auth_run.font.name = "Calibri"
    auth_p.paragraph_format.space_after = Pt(18)

    # ══════════════════════════════════════════════════════════════════
    # ABSTRACT
    # ══════════════════════════════════════════════════════════════════
    doc.add_heading("Abstract", level=1)
    add_body(doc,
        "The thalamus has undergone a fundamental reconceptualization in "
        "neuroscience over the past fifty years. Using computational text-mining "
        "of 17,280 PubMed abstracts and 55,848 full-text sentences, we trace the "
        "evolution of thalamic conceptual framing from 1975 to 2026. We distinguish "
        "two analyses: (1) topic analysis using BERTopic identifies 186 research "
        "topics revealing shifts from anatomical description and lesion studies "
        "towards functional connectivity, optogenetics, and clinical "
        "neurostimulation; (2) framing analysis using pattern-matching detects 21 "
        "distinct metaphorical frames applied to the thalamus in 1,621 sentences, "
        "with no residual \u201cother\u201d category. The relay metaphor dominated "
        "the 1980s\u20131990s (76% of frames) but has been progressively "
        "supplemented by hub (Mann-Kendall \u03c4 = +0.52, p < 0.001), gateway, "
        "clinical target, and other specific functional descriptions. Hub framing "
        "overtook relay in the 2020s. This shift persists after controlling for "
        "the rise of fMRI. Recording technique analysis reveals that optogenetics "
        "(\u03c4 = +0.858) and chemogenetics (\u03c4 = +0.909) are the "
        "fastest-growing methods, while lesion studies and tract tracing decline. "
        "Crucially, individual thalamic nuclei are rarely described using relay or "
        "hub labels; nucleus-level discourse employs precise functional vocabulary, "
        "while the relay-vs-hub debate operates primarily at the whole-thalamus "
        "level. These findings reveal that the field has not simply replaced one "
        "metaphor with another but has developed an increasingly rich and precise "
        "conceptual vocabulary for describing thalamic function."
    )

    # Keywords
    kw_p = doc.add_paragraph()
    kw_run_label = kw_p.add_run("Keywords: ")
    kw_run_label.bold = True
    kw_run_label.font.size = Pt(11)
    kw_run_label.font.name = "Calibri"
    kw_run = kw_p.add_run(
        "thalamus, text mining, BERTopic, conceptual framing, metaphor, "
        "neuroscience history"
    )
    kw_run.font.size = Pt(11)
    kw_run.font.name = "Calibri"

    # ══════════════════════════════════════════════════════════════════
    # 1. INTRODUCTION
    # ══════════════════════════════════════════════════════════════════
    doc.add_heading("1. Introduction", level=1)
    add_body(doc,
        "Scientific concepts do not simply emerge fully formed; they evolve "
        "through a complex interplay of empirical discovery, technological "
        "innovation, and rhetorical framing. Understanding how a field\u2019s "
        "conceptual vocabulary changes over time can reveal the forces that shape "
        "scientific knowledge and expose assumptions that persist long after the "
        "evidence base has shifted. The thalamus offers a compelling case study "
        "for this kind of analysis."
    )
    add_body(doc,
        "For much of the twentieth century, the thalamus was characterised as a "
        "sensory relay station \u2014 a waypoint that passively transmitted "
        "information from the periphery to the cortex. This framing was supported "
        "by early anatomical and electrophysiological studies emphasising the "
        "point-to-point connectivity of specific thalamic nuclei with primary "
        "sensory cortices. Yet over the past three decades, converging evidence "
        "from functional neuroimaging, optogenetics, and refined electrophysiology "
        "has revealed that thalamic nuclei participate in recurrent "
        "cortico-thalamo-cortical loops, contribute to attention, working memory, "
        "decision-making, and consciousness, and serve as critical nodes in "
        "large-scale brain networks."
    )
    add_body(doc,
        "The emergent alternative \u2014 the thalamus as a hub or integrative "
        "centre \u2014 has gained considerable traction, but the transition "
        "between these conceptual frameworks has not been systematically "
        "documented. Is the relay metaphor truly disappearing? Has hub simply "
        "replaced relay, or has the conceptual vocabulary diversified more "
        "broadly? Do researchers studying specific thalamic nuclei use the same "
        "metaphors as those discussing the thalamus as a whole?"
    )
    add_body(doc,
        "Text mining and natural language processing offer tools to trace such "
        "conceptual shifts at scale. By computationally extracting and classifying "
        "the language that scientists use to describe the thalamus, we can move "
        "beyond anecdotal observations to quantify how the field\u2019s conceptual "
        "vocabulary has evolved."
    )
    add_body(doc,
        "In this study, we present a comprehensive computational analysis of "
        "thalamic research spanning 1975 to 2026, combining two complementary "
        "approaches: topic analysis (what researchers study) and framing analysis "
        "(how researchers conceptualise the thalamus). Specifically, we address "
        "five research questions:"
    )
    add_body(doc,
        "1. What research topics characterise thalamus studies, and how have they "
        "evolved?\n"
        "2. What metaphorical frames are used to describe the thalamus?\n"
        "3. How have these frames changed over five decades?\n"
        "4. Do framing patterns differ by recording technique and thalamic "
        "nucleus?\n"
        "5. What is the relationship between research topics and conceptual "
        "framing?"
    )

    # ══════════════════════════════════════════════════════════════════
    # 2. METHODS
    # ══════════════════════════════════════════════════════════════════
    doc.add_heading("2. Methods", level=1)

    # 2.1
    doc.add_heading("2.1 Corpus construction", level=2)
    add_body(doc,
        "We queried PubMed for articles mentioning \u201cthalamus\u201d or "
        "\u201cthalamic\u201d in their title, yielding 17,280 records spanning "
        "1950\u20132026. For each record, we attempted to retrieve the full text "
        "through PubMed Central (PMC) open-access archives, yielding 3,319 "
        "full-text papers. An additional 149 papers were obtained through the "
        "Unpaywall API, which identifies legally available open-access versions. "
        "Full texts were segmented into sentences using NLTK\u2019s Punkt "
        "tokenizer with regex-based fallback, producing 55,848 sentences "
        "containing \u201cthalamus\u201d or \u201cthalamic.\u201d"
    )

    # 2.2
    doc.add_heading("2.2 Topic modeling", level=2)
    add_body(doc,
        "We employed BERTopic for unsupervised topic discovery on the full "
        "55,848-sentence corpus. Sentences were first encoded using the "
        "all-MiniLM-L6-v2 sentence transformer (384-dimensional embeddings), "
        "then dimensionality-reduced via UMAP (5 components, cosine metric, 30 "
        "neighbours) and clustered with HDBSCAN (minimum cluster size 50). This "
        "yielded 186 distinct research topics, each characterised by c-TF-IDF "
        "keyword profiles. Topic modeling captures what researchers study about "
        "the thalamus and how the research landscape has shifted over time."
    )

    # 2.3
    doc.add_heading("2.3 Framing detection", level=2)
    add_body(doc,
        "Independently of the topic analysis, we developed a pattern-matching "
        "approach to detect metaphorical frames applied to the thalamus. We "
        "defined 21 frame types, each captured by a set of regular expression "
        "patterns: relay, hub, gateway, gate, modulator, integrator, filter, "
        "regulator, coordinator, pacemaker, switchboard, amplifier, distributor, "
        "processor, bridge, bottleneck, blackboard, interface, network_node, "
        "clinical_target, and anti_relay. Crucially, there is no residual "
        "\u201cother\u201d category \u2014 every detected sentence is assigned to "
        "one of these 21 genuine metaphorical frame types. When multiple frames "
        "matched a sentence, the most specific label was retained based on a "
        "predefined priority hierarchy. This approach identified 1,621 framing "
        "sentences across the corpus. Framing detection captures how researchers "
        "conceptualise the thalamus, complementing the topic analysis of what "
        "they study."
    )

    # 2.4
    doc.add_heading("2.4 Recording technique classification", level=2)
    add_body(doc,
        "We classified sentences and abstracts by the recording technique they "
        "referenced using 11 regex-based patterns: fMRI, electrophysiology, "
        "calcium imaging, optogenetics, chemogenetics, EEG/MEG, PET, tract "
        "tracing, DBS, lesion studies, and structural MRI. This allowed us to "
        "examine whether conceptual framing and topic focus differ systematically "
        "by experimental methodology."
    )

    # 2.5
    doc.add_heading("2.5 Statistical methods", level=2)
    add_body(doc,
        "Temporal trends were assessed using the Mann-Kendall test for monotonic "
        "trends on annual proportions, with significance at \u03b1 = 0.05. "
        "Confidence intervals for proportions were computed using the Wilson "
        "score method. Associations between categorical variables (e.g., "
        "technique \u00d7 framing, topic \u00d7 framing) were tested with "
        "chi-squared tests of independence."
    )

    add_figure(doc, fig_num=1)

    # ══════════════════════════════════════════════════════════════════
    # 3. RESULTS
    # ══════════════════════════════════════════════════════════════════
    doc.add_heading("3. Results", level=1)

    # 3.1
    doc.add_heading("3.1 The thalamic research landscape", level=2)
    add_body(doc,
        "BERTopic topic modeling of 55,848 thalamus sentences revealed 186 "
        "distinct research topics (Figure 2). The largest topics included "
        "seizures and epilepsy, pain and nociception, schizophrenia and "
        "psychosis, sleep and arousal, motor circuits and basal ganglia, auditory "
        "processing, tremor and deep brain stimulation, brain volumetrics, "
        "thalamic reticular nucleus, and viral tracing methodologies."
    )
    add_body(doc,
        "Temporal analysis revealed substantial shifts in the research landscape "
        "over the five decades examined. Topics related to anatomical description "
        "and lesion studies were dominant in the 1970s\u20131980s but have "
        "progressively given way to functional connectivity, optogenetics, and "
        "clinical neurostimulation topics. The emergence of network-oriented and "
        "translational topics mirrors the broader adoption of neuroimaging and "
        "computational approaches in neuroscience."
    )
    add_figure(doc, fig_num=2)

    # 3.2
    doc.add_heading("3.2 Conceptual framing of the thalamus", level=2)
    add_body(doc,
        "Our pattern-matching approach detected 1,621 sentences across the "
        "corpus that apply one of 21 distinct metaphorical frames to the "
        "thalamus, with no residual \u201cother\u201d category. The relay frame "
        "dominated the 1980s\u20131990s, accounting for approximately 76% of all "
        "detected frames during that period. However, relay framing has "
        "progressively declined as a proportion of total frames, while hub "
        "framing has risen steadily (Mann-Kendall \u03c4 = +0.52, p < 0.001), "
        "overtaking relay in the 2020s."
    )
    add_body(doc,
        "Critically, the conceptual shift is not a simple relay-to-hub "
        "replacement. Multiple specific frames \u2014 gateway, gate, modulator, "
        "integrator, filter, clinical_target, coordinator, pacemaker, and others "
        "\u2014 have emerged simultaneously, reflecting a diversification of "
        "conceptual vocabulary rather than a binary substitution. The anti_relay "
        "frame, which explicitly challenges the relay metaphor, also appears with "
        "increasing frequency in recent decades."
    )
    add_body(doc,
        "To test whether the rise in hub framing is confounded by the expansion "
        "of fMRI studies, we repeated the framing analysis after excluding all "
        "papers referencing fMRI. Hub framing still showed a significant increase "
        "(Mann-Kendall p < 0.001), confirming that the conceptual shift is not "
        "merely an artefact of fMRI methodology."
    )
    add_figure(doc, fig_num=3)

    # 3.3
    doc.add_heading(
        "3.3 Framing across nuclei and techniques", level=2)
    add_body(doc,
        "Analysis of framing patterns across individual thalamic nuclei and "
        "recording techniques revealed striking differences (Figure 4). "
        "Individual nuclei are rarely described using relay or hub labels. "
        "Instead, nucleus-level discourse employs precise functional vocabulary "
        "tied to each nucleus\u2019s specific connectivity and physiological "
        "role. For example, the lateral geniculate nucleus is described in terms "
        "of visual processing and retinal input, the mediodorsal nucleus in "
        "terms of prefrontal function and executive control, and the pulvinar in "
        "terms of attentional regulation and multisensory integration."
    )
    add_body(doc,
        "This scale distinction is a central finding: the relay-vs-hub debate "
        "operates primarily at the whole-thalamus level, while researchers "
        "working on specific nuclei have long employed more nuanced functional "
        "descriptions. The relay and hub metaphors appear to be organizing "
        "frameworks for textbook-level discourse rather than the working "
        "vocabulary of nucleus-level research."
    )
    add_body(doc,
        "Recording techniques also show distinct framing profiles. fMRI papers "
        "are significantly enriched for hub and network_node framing, "
        "electrophysiology papers show a more balanced distribution, and "
        "optogenetics papers favour modulator and gateway framing, reflecting "
        "the causal, circuit-dissection nature of the technique."
    )
    add_figure(doc, fig_num=4)

    # 3.4
    doc.add_heading("3.4 Framing and research topics", level=2)
    add_body(doc,
        "Linking the two analyses \u2014 BERTopic topics and pattern-based "
        "framing \u2014 revealed systematic associations between what researchers "
        "study and how they conceptualise the thalamus (Figure 5). Epilepsy "
        "and seizure topics are strongly associated with clinical_target framing, "
        "consistent with the thalamus\u2019s role as a DBS target for refractory "
        "epilepsy. Pain and nociception topics retain relay framing, reflecting "
        "the spinothalamic tract\u2019s well-established role in sensory "
        "transmission. Schizophrenia topics are enriched for gate and filter "
        "framing, aligning with thalamic gating hypotheses of psychosis. Auditory "
        "processing topics show strong relay framing, consistent with the medial "
        "geniculate body\u2019s role in the classical auditory pathway."
    )
    add_body(doc,
        "These topic-frame associations demonstrate that framing is not "
        "arbitrary but reflects genuine functional emphases within different "
        "research communities. The choice of metaphor is shaped by the specific "
        "phenomenon under study."
    )
    add_figure(doc, fig_num=5)

    # 3.5
    doc.add_heading("3.5 Recording techniques", level=2)
    add_body(doc,
        "Analysis of 11 recording techniques across the corpus revealed "
        "dramatic methodological shifts (Figure 6). Optogenetics "
        "(\u03c4 = +0.858) and chemogenetics (\u03c4 = +0.909) are the "
        "fastest-growing techniques, consistent with their emergence as powerful "
        "tools for causal circuit dissection. Electrophysiology remains the most "
        "prevalent technique throughout the period, while fMRI shows steady "
        "growth from the mid-1990s onward."
    )
    add_body(doc,
        "In contrast, tract tracing (\u03c4 = \u22120.731) and lesion studies "
        "(\u03c4 = \u22120.667) show significant declines, reflecting the "
        "field\u2019s shift from purely anatomical and ablation-based approaches "
        "towards functional and causal methods. The rise of DBS as both a "
        "research tool and a clinical intervention is also evident, bridging "
        "basic and translational thalamus research."
    )
    add_figure(doc, fig_num=6)

    # ══════════════════════════════════════════════════════════════════
    # 4. DISCUSSION
    # ══════════════════════════════════════════════════════════════════
    doc.add_heading("4. Discussion", level=1)

    # 4.1
    doc.add_heading(
        "4.1 Beyond relay-to-hub: a diversifying conceptual vocabulary",
        level=2)
    add_body(doc,
        "Our findings challenge the common narrative of a simple relay-to-hub "
        "conceptual shift. While hub framing has indeed increased significantly "
        "and overtaken relay in the 2020s, this transition is part of a broader "
        "diversification of conceptual vocabulary. The simultaneous emergence of "
        "gateway, gate, modulator, integrator, filter, coordinator, pacemaker, "
        "clinical_target, and other specific frames indicates that the field is "
        "not replacing one dominant metaphor with another but developing an "
        "increasingly rich and precise vocabulary for describing thalamic "
        "function. The 21 distinct frame types identified in this study "
        "\u2014 with no residual \u201cother\u201d category \u2014 capture the "
        "full spectrum of metaphorical language applied to the thalamus."
    )

    # 4.2
    doc.add_heading(
        "4.2 The scale distinction: whole-thalamus vs nucleus-level framing",
        level=2)
    add_body(doc,
        "A key finding is the divergence between whole-thalamus framing and "
        "nucleus-level discourse. The relay/hub debate operates primarily at the "
        "macro level \u2014 in review articles, textbooks, and introductory "
        "paragraphs. Researchers studying specific thalamic nuclei have "
        "consistently used more precise functional vocabulary tied to each "
        "nucleus\u2019s connectivity and physiology. This suggests that the "
        "conceptual evolution documented here is most relevant for how the field "
        "frames the thalamus as a unified structure, and that nucleus-level "
        "research has long recognised the functional diversity that the relay/hub "
        "dichotomy obscures."
    )

    # 4.3
    doc.add_heading(
        "4.3 Methodological drivers: new techniques, new concepts",
        level=2)
    add_body(doc,
        "The strong associations between recording techniques and framing "
        "profiles suggest that methodological advances are not merely following "
        "but actively shaping conceptual change. The rise of optogenetics and "
        "chemogenetics enables causal manipulation of specific thalamic cell "
        "types and projections, driving a shift from correlational to mechanistic "
        "descriptions. fMRI\u2019s emphasis on connectivity naturally promotes "
        "hub and network language, but the persistence of hub framing trends "
        "after excluding fMRI papers confirms that the conceptual shift "
        "transcends any single methodology."
    )

    # 4.4
    doc.add_heading("4.4 Limitations", level=2)
    add_body(doc,
        "Several limitations should be noted. First, our corpus is biased "
        "towards English-language publications indexed in PubMed, excluding "
        "non-English literature and grey literature. Second, PMC full-text "
        "availability is skewed towards post-2008 publications, which may "
        "inflate trends in the full-text analysis; we partially address this "
        "with abstract-only replication (Figure S2). Third, our regex-based "
        "framing detection, while capturing 21 distinct frame types with no "
        "\u201cother\u201d category, may miss novel or unusual phrasings and "
        "cannot capture semantic nuance beyond pattern matching. Fourth, "
        "BERTopic\u2019s HDBSCAN clustering assigns a proportion of sentences to "
        "noise (no topic), meaning some sentences are not represented in the "
        "topic analysis. Fifth, the 95% Wilson confidence intervals on annual "
        "framing proportions are wide for low-frequency frame types."
    )

    # ══════════════════════════════════════════════════════════════════
    # 5. CONCLUSION
    # ══════════════════════════════════════════════════════════════════
    doc.add_heading("5. Conclusion", level=1)
    add_body(doc,
        "This study provides the first large-scale computational analysis of "
        "how the neuroscience community conceptualises the thalamus. By "
        "distinguishing topic analysis (what researchers study) from framing "
        "analysis (how they describe the thalamus), we reveal that the "
        "thalamus literature has undergone a fundamental transformation: from a "
        "landscape dominated by the relay metaphor towards an increasingly "
        "diverse and precise conceptual vocabulary. Hub framing has overtaken "
        "relay in the 2020s, but this shift is embedded within a broader "
        "diversification across 21 distinct metaphorical frames. New recording "
        "techniques both drive and reflect this conceptual evolution, while "
        "nucleus-level research has always maintained greater specificity than "
        "whole-thalamus discussions. The field has not simply swapped one metaphor "
        "for another; it has developed a richer language for understanding one of "
        "the brain\u2019s most central structures."
    )

    # ══════════════════════════════════════════════════════════════════
    # SUPPLEMENTARY MATERIALS
    # ══════════════════════════════════════════════════════════════════
    doc.add_page_break()
    doc.add_heading("Supplementary Materials", level=1)

    for key in ("S1", "S2", "S3", "S4"):
        add_figure(doc, sup_key=key, width=Inches(6.0))

    # ── Save ───────────────────────────────────────────────────────────
    doc.save(str(OUT_PATH))
    print(f"\nSaved: {OUT_PATH}")


if __name__ == "__main__":
    build()
