import sys, io; sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
"""
build_paper.py
Generates thalamus_framing_paper_v7.docx using python-docx.
"""

import os
from pathlib import Path
from docx import Document
from docx.shared import Inches, Pt, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.section import WD_ORIENT

# ── paths ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
FIG_DIR = ROOT / "Graphs" / "pub"
OUT_DIR = ROOT / "Paper"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "thalamus_framing_paper_v7.docx"

# ── figure mapping ─────────────────────────────────────────────────────
MAIN_FIGS = {
    1: "fig1_pipeline_landscape.png",
    2: "fig2_topics_over_time.png",
    3: "fig3_framing_taxonomy.png",
    4: "fig4_subtopics_fmri.png",
    5: "fig5_nucleus_prevalence.png",
    6: "fig6_vocabulary_evolution.png",
    7: "fig7_techniques.png",
    8: "fig8_summary.png",
}

SUP_FIGS = {
    "S1": ("sup_fig_S1_embedding_quality.png",
           "Figure S1. Embedding space quality. (A) UMAP projection of sentence embeddings "
           "colored by BERTopic cluster. (B) Silhouette score distribution. (C) Topic coherence metrics."),
    "S2": ("sup_fig_S2_abstract_replication.png",
           "Figure S2. Abstract vs. full-text replication. (A) Framing proportions estimated from "
           "abstracts only vs. full-text sentences. (B) Correlation between abstract and full-text "
           "Mann-Kendall statistics."),
    "S3": ("sup_fig_S3_confound_controls.png",
           "Figure S3. Confound controls. (A) Hub framing with and without fMRI papers. "
           "(B) Publication volume normalisation. (C) PMC availability bias analysis."),
    "S4": ("sup_fig_S4_landscape_clusters.png",
           "Figure S4. Research landscape clusters. (A) Hierarchical clustering of 102 BERTopic "
           "topics. (B) Topic similarity matrix. (C) Cluster-level temporal trends."),
    "S5": ("sup_fig_S5_nucleus_framing_decade.png",
           "Figure S5. Nucleus x framing x decade. (A) Framing distribution per nucleus across "
           "decades. (B) Shift indices for each nucleus. (C) Nucleus-level Mann-Kendall statistics."),
}

# ── figure captions ────────────────────────────────────────────────────
MAIN_CAPTIONS = {
    1: ("Figure 1. Analysis pipeline and corpus overview. "
        "(A) Data collection and processing pipeline. "
        "(B) BERTopic topic visualization of 55,848 thalamus sentences. "
        "(C) Top 15 research topics by frequency. "
        "(D) Corpus statistics."),
    2: ("Figure 2. Research topics over time. "
        "(A) Temporal evolution of the top 15 research topics. "
        "(B) Mann-Kendall trend statistics for emerging and declining topics. "
        "(C) Comparison with hippocampus and cerebellum hub framing."),
    3: ("Figure 3. Framing taxonomy and examples. "
        "(A) UMAP visualization of 10,634 framing sentences colored by category. "
        "(B) Category distribution with BERTopic sub-topics within 'other'. "
        "(C) Representative example sentences per category. "
        "(D) Temporal trends in framing proportions."),
    4: ("Figure 4. Framing sub-topics and fMRI confound. "
        "(A) BERTopic sub-topics within previously unclassified framing. "
        "(B) Temporal trends of sub-topics. "
        "(C) Hub framing decomposition. "
        "(D) fMRI confound control."),
    5: ("Figure 5. Nucleus prevalence and framing. "
        "(A) Temporal prevalence of 11 thalamic nuclei. "
        "(B) Nucleus x framing heatmap. "
        "(C) Mann-Kendall prevalence trends. "
        "(D) Framing profiles per nucleus."),
    6: ("Figure 6. Nucleus vocabulary evolution. "
        "(A) TF-IDF vocabulary shift in abstracts (early vs. recent). "
        "(B) Same in full-text sentences. "
        "(C) Key term emergence highlights."),
    7: ("Figure 7. Recording techniques and framing. "
        "(A) Technique prevalence over time. "
        "(B) Framing distribution per technique. "
        "(C) Topic distribution per technique. "
        "(D) Technique x nucleus co-occurrence."),
    8: ("Figure 8. Summary. "
        "(A) Overall framing trajectory 1990-2025. "
        "(B) Mann-Kendall statistics for all framing categories. "
        "(C) Key findings."),
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
    p = doc.add_paragraph(text, style="Normal")
    return p


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
        # Still add the caption so the document structure is clear
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
        "From relay to hub and beyond: A computational analysis of evolving "
        "conceptual framing in thalamus research (1975\u20132026)",
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

    # ── Abstract ───────────────────────────────────────────────────────
    doc.add_heading("Abstract", level=1)
    add_body(doc,
        "The thalamus, historically characterised as a passive sensory relay station, "
        "has undergone a profound conceptual transformation in neuroscience. Using "
        "computational text-mining of 17,280 PubMed abstracts and 55,848 full-text "
        "sentences spanning five decades, we trace the evolution of thalamic conceptual "
        "framing from a binary relay/hub dichotomy towards a rich, multi-dimensional "
        "vocabulary of functional descriptions. BERTopic topic modeling identified 102 "
        "distinct research topics across the corpus. While hub/network framing has "
        "increased significantly (Mann-Kendall \u03c4 = +0.739, p < 0.001), this rise "
        "is part of a broader shift towards conceptual precision: all specific framing "
        "categories \u2014 including gateway, modulator, integrator, and clinical "
        "target \u2014 show significant increases, while generic functional descriptions "
        "decline (\u03c4 = \u22120.665, p < 0.001). Semi-supervised topic modeling "
        "resolved 52% of previously unclassified framing sentences into meaningful "
        "sub-categories spanning clinical applications, connectivity, sleep/arousal, "
        "cognitive function, and structural description. Recording technique analysis "
        "reveals that newer methods (optogenetics \u03c4 = +0.858, chemogenetics "
        "\u03c4 = +0.909) are growing fastest, alongside shifts from anatomical "
        "tract-tracing towards functional connectivity approaches. Nucleus-specific "
        "analysis shows that individual thalamic nuclei are described with precise "
        "functional vocabulary rather than relay/hub labels \u2014 the relay-vs-hub "
        "debate operates at the whole-thalamus level while nucleus-level discourse has "
        "always been more nuanced. These findings suggest the field has moved beyond a "
        "simple relay-to-hub narrative towards an increasingly precise and multifaceted "
        "understanding of thalamic function."
    )

    # Keywords
    kw_p = doc.add_paragraph()
    kw_run_label = kw_p.add_run("Keywords: ")
    kw_run_label.bold = True
    kw_run_label.font.size = Pt(11)
    kw_run_label.font.name = "Calibri"
    kw_run = kw_p.add_run(
        "thalamus, text mining, BERTopic, conceptual framing, neuroscience history, "
        "topic modeling"
    )
    kw_run.font.size = Pt(11)
    kw_run.font.name = "Calibri"

    # ── 1. Introduction ────────────────────────────────────────────────
    doc.add_heading("1. Introduction", level=1)
    add_body(doc,
        "The thalamus occupies a unique position in the history of neuroscience "
        "concepts. For much of the twentieth century, it was characterised as a "
        "sensory relay station \u2014 a waypoint that passively transmitted sensory "
        "information from the periphery to the cortex. This framing was supported by "
        "early anatomical and electrophysiological studies that emphasised the "
        "point-to-point connectivity of specific thalamic nuclei with primary sensory "
        "cortices."
    )
    add_body(doc,
        "Over the past three decades, this relay metaphor has been increasingly "
        "challenged. Functional neuroimaging, optogenetics, and refined "
        "electrophysiology have revealed that thalamic nuclei participate in recurrent "
        "cortico-thalamo-cortical loops, contribute to attention, working memory, "
        "decision-making, and consciousness, and serve as critical nodes in large-scale "
        "brain networks. The emergent alternative \u2014 the thalamus as a hub or "
        "integrative centre \u2014 has gained traction, but the transition between "
        "these conceptual frameworks has not been systematically documented."
    )
    add_body(doc,
        "Text mining and natural language processing offer tools to trace such "
        "conceptual shifts at scale. By extracting and classifying sentences that "
        "explicitly describe the thalamus in specific functional terms, we can move "
        "beyond anecdotal observations to quantify how the scientific community has "
        "reconceptualised this structure."
    )
    add_body(doc,
        "In this study, we present a comprehensive computational analysis of thalamic "
        "framing in the neuroscience literature spanning 1975 to 2026. We combine "
        "keyword-based corpus construction, sentence-level framing extraction, "
        "embedding-based topic modelling (BERTopic), and statistical trend analysis to "
        "address five questions:"
    )
    add_body(doc,
        "1. How has the research landscape of thalamus studies evolved over five decades?\n"
        "2. What conceptual framings are used to describe the thalamus, and how have they changed?\n"
        "3. Is the rise in hub framing confounded by the expansion of fMRI studies?\n"
        "4. Do individual thalamic nuclei follow the same framing trajectory as the thalamus as a whole?\n"
        "5. How do different recording techniques relate to the conceptual framing of the thalamus?"
    )

    # ── 2. Methods ─────────────────────────────────────────────────────
    doc.add_heading("2. Methods", level=1)

    # 2.1
    doc.add_heading("2.1 Corpus construction", level=2)
    add_body(doc,
        "We queried PubMed for articles mentioning \u201cthalamus\u201d or "
        "\u201cthalamic\u201d in their title, yielding 17,280 records spanning "
        "1950\u20132026. For each record, we attempted to retrieve the full text "
        "through PubMed Central (PMC) open-access archives, yielding 3,319 full-text "
        "papers. An additional 149 papers were obtained through the Unpaywall API, "
        "which identifies legally available open-access versions of articles. Full "
        "texts were segmented into sentences using a combination of NLTK\u2019s Punkt "
        "tokenizer and regex-based fallback, producing 55,848 sentences containing "
        "\u201cthalamus\u201d or \u201cthalamic.\u201d"
    )
    add_figure(doc, fig_num=1)

    # 2.2
    doc.add_heading("2.2 Sentence extraction and framing detection", level=2)
    add_body(doc,
        "Framing sentences were identified using two complementary patterns. First, "
        "verb-trigger patterns captured sentences where the thalamus was the "
        "grammatical subject of a descriptive predicate (e.g., \u201cthe thalamus is a "
        "relay station,\u201d \u201cthe thalamus serves as a hub\u201d). We defined 18 "
        "verb triggers including \u201cis,\u201d \u201cacts as,\u201d \u201cserves "
        "as,\u201d \u201cplays,\u201d \u201ccontributes,\u201d and \u201cmediates.\u201d "
        "Second, appositive patterns identified noun phrases set off by commas "
        "immediately following \u201cthe thalamus\u201d (e.g., \u201cthe thalamus, a "
        "key subcortical structure, ...\u201d). Bad appositives (discourse markers such "
        "as \u201chowever\u201d and \u201ctherefore\u201d) were filtered. This dual "
        "approach yielded 10,634 framing sentences."
    )

    # 2.3
    doc.add_heading("2.3 Topic modeling", level=2)
    add_body(doc,
        "We employed BERTopic for topic discovery. Sentences were first encoded using "
        "the all-MiniLM-L6-v2 sentence transformer (384-dimensional embeddings), then "
        "reduced via UMAP (5 components, cosine metric, 30 neighbours) and clustered "
        "with HDBSCAN (minimum cluster size 50 for all sentences, 20 for framing "
        "sentences). We ran two models: Model 1 on all 55,848 sentences for general "
        "topic structure (yielding 102 topics), and Model 2 on the 10,634 framing "
        "sentences with semi-supervised labels from our keyword taxonomy, where known "
        "framing categories (relay, hub, gateway, modulator, etc.) were provided as "
        "seed labels while \u201cother\u201d sentences were left unlabelled for "
        "BERTopic to discover sub-topics (yielding 61 topics, resolving 52% of "
        "previously unclassified sentences)."
    )

    # 2.4
    doc.add_heading("2.4 Framing taxonomy", level=2)
    add_body(doc,
        "We developed a keyword-based taxonomy to classify framing sentences into ten "
        "categories: relay, hub/network node, gateway, modulator, integrator, network "
        "component, clinical target, role player, structure description, and general "
        "involvement. Each category was defined by a set of regular expression patterns "
        "(e.g., \u201crelay\u201d matched \u201crelay station,\u201d \u201csensory "
        "relay,\u201d \u201crelays information\u201d). When multiple categories matched "
        "a sentence, the highest-priority label was assigned based on specificity "
        "(relay > hub > gateway > modulator > integrator > network > clinical > role "
        "player > structure > involved_in)."
    )

    # 2.5
    doc.add_heading("2.5 Recording technique classification", level=2)
    add_body(doc,
        "We classified sentences and abstracts by the recording technique they "
        "referenced using 11 regex-based patterns: fMRI, electrophysiology, calcium "
        "imaging, optogenetics, chemogenetics, EEG/MEG, PET, tract tracing, DBS, "
        "lesion studies, and structural MRI. This allowed us to examine whether "
        "conceptual framing differs systematically by experimental methodology."
    )

    # 2.6
    doc.add_heading("2.6 Statistical methods", level=2)
    add_body(doc,
        "Temporal trends were assessed using the Mann-Kendall test for monotonic "
        "trends on annual proportions, with significance at \u03b1 = 0.05. Confidence "
        "intervals for proportions were computed using the Wilson score method. To test "
        "associations between categorical variables (e.g., technique \u00d7 framing), "
        "we used chi-squared tests of independence."
    )

    # ── 3. Results ─────────────────────────────────────────────────────
    doc.add_heading("3. Results", level=1)

    # 3.1
    doc.add_heading("3.1 The thalamic research landscape", level=2)
    add_body(doc,
        "Topic modeling of 55,848 thalamus sentences revealed 102 distinct research "
        "topics (Figure 1B\u2013C). The largest topics included stroke and lesion "
        "studies, epilepsy and seizure propagation, pain and nociception, schizophrenia "
        "and psychosis, sleep and arousal, basal ganglia and motor circuits, auditory "
        "processing, tremor and deep brain stimulation, brain volumetrics, habenula "
        "function, thalamic reticular nucleus and GABAergic inhibition, multiple "
        "sclerosis, and viral tracing methodologies."
    )
    add_body(doc,
        "Temporal analysis revealed substantial shifts in research focus over the five "
        "decades examined. Clinical and translational topics have expanded markedly, "
        "while purely anatomical classification topics have declined. The emergence of "
        "functional connectivity and network-oriented topics mirrors the broader "
        "adoption of neuroimaging and computational approaches in neuroscience."
    )
    add_figure(doc, fig_num=2)
    add_body(doc,
        "Comparison with other brain regions showed that the hub framing increase is "
        "not unique to the thalamus but is most pronounced there. Both hippocampus and "
        "cerebellum show similar but smaller increases in hub/network language, "
        "suggesting a field-wide trend towards network-oriented descriptions that is "
        "particularly strong for the thalamus given its anatomical position as a "
        "connectivity nexus."
    )

    # 3.2
    doc.add_heading("3.2 Framing taxonomy and temporal trends", level=2)
    add_body(doc,
        "Of the 10,634 framing sentences, our taxonomy classified them into 10 "
        "categories. The largest category was general involvement, followed by "
        "structure description, role player, hub/network node, modulator, relay, "
        "gateway, integrator, clinical target, and network component. Mann-Kendall "
        "trend tests revealed that all specific categories show significant increases "
        "over time, while generic descriptions decline."
    )
    add_body(doc,
        "Semi-supervised BERTopic modeling discovered 61 distinct framing topics within "
        "these sentences, resolving 52% of previously unclassified (\u201cother\u201d) "
        "sentences into meaningful sub-categories. This demonstrates that the large "
        "\u201cother\u201d category is not noise but contains structured and "
        "interpretable content that simply did not match our predefined taxonomy."
    )
    add_figure(doc, fig_num=3)

    # 3.3
    doc.add_heading("3.3 Framing sub-topics and the fMRI confound", level=2)
    add_body(doc,
        "The previously unclassified \u201cother\u201d category contained 8,432 "
        "sentences. BERTopic identified coherent sub-topics within this group, spanning "
        "clinical applications (DBS outcomes, anaesthetic mechanisms, pain "
        "modulation), connectivity descriptions (corticothalamic projections, "
        "thalamocortical loops), sleep and arousal (thalamic gating during sleep, "
        "arousal regulation), cognitive function (attention, working memory, "
        "consciousness), and structural descriptions (cytoarchitecture, laminar "
        "organisation)."
    )
    add_body(doc,
        "A critical question is whether the rise of hub framing is driven by the "
        "expansion of fMRI studies, which naturally emphasise network connectivity. "
        "To address this, we repeated the framing analysis after excluding all papers "
        "referencing fMRI. Hub framing still showed a significant increase "
        "(Mann-Kendall p < 0.001), although the magnitude was slightly attenuated. "
        "This confirms that the conceptual shift towards hub framing is not merely an "
        "artefact of fMRI methodology but reflects a genuine change in how the "
        "thalamus is conceptualised across experimental approaches."
    )
    add_figure(doc, fig_num=4)

    # 3.4
    doc.add_heading("3.4 Nucleus-specific framing and vocabulary", level=2)
    add_body(doc,
        "Analysis of 14 individual thalamic nuclei revealed distinct framing profiles. "
        "Critically, individual nuclei are rarely described using relay or hub "
        "labels \u2014 instead, they receive precise functional descriptions tied to "
        "their specific connectivity and physiological roles. For example, the lateral "
        "geniculate nucleus (LGN) is described in terms of visual processing and "
        "retinal input, while the mediodorsal nucleus (MD) is linked to prefrontal "
        "function and executive control."
    )
    add_figure(doc, fig_num=5)
    add_body(doc,
        "Vocabulary analysis using TF-IDF comparison between early (pre-2000) and "
        "recent (post-2015) publications reveals substantial shifts in the terms "
        "associated with specific nuclei. Early publications emphasised anatomical "
        "connectivity and morphological descriptions, while recent publications "
        "feature functional, circuit-level, and clinical terminology."
    )
    add_figure(doc, fig_num=6)

    # 3.5
    doc.add_heading("3.5 Recording techniques and framing", level=2)
    add_body(doc,
        "Eleven recording techniques were classified across the corpus. Mann-Kendall "
        "tests showed optogenetics (\u03c4 = +0.858) and chemogenetics "
        "(\u03c4 = +0.909) as the fastest-growing techniques, while tract tracing "
        "(\u03c4 = \u22120.731) and lesion studies (\u03c4 = \u22120.667) declined. "
        "Electrophysiology remained the most prevalent technique throughout the "
        "period, while fMRI showed steady growth from the mid-1990s onward."
    )
    add_body(doc,
        "Different techniques were associated with different framing profiles. "
        "fMRI papers were significantly more likely to use hub and network language, "
        "while electrophysiology papers showed a more even distribution across framing "
        "categories. Optogenetics papers were notably enriched for modulator and "
        "gateway framing, reflecting the causal, circuit-dissection nature of the "
        "technique."
    )
    add_figure(doc, fig_num=7)

    # ── 4. Discussion ──────────────────────────────────────────────────
    doc.add_heading("4. Discussion", level=1)

    # 4.1
    doc.add_heading("4.1 Beyond the relay-to-hub narrative", level=2)
    add_body(doc,
        "Our findings challenge the common narrative of a simple relay-to-hub "
        "conceptual shift. While hub framing has indeed increased significantly, this "
        "rise is part of a broader transformation towards conceptual precision. The "
        "decline of generic, unspecified functional descriptions is the dominant trend, "
        "with all specific categories \u2014 relay, hub, gateway, modulator, "
        "integrator, clinical target \u2014 emerging simultaneously. This suggests "
        "that the field is not simply replacing one label with another but is "
        "developing a richer vocabulary for describing thalamic function."
    )

    # 4.2
    doc.add_heading("4.2 Methodological drivers of conceptual change", level=2)
    add_body(doc,
        "The strong emergence of optogenetics, chemogenetics, and calcium imaging "
        "coincides with increasingly precise functional characterisation of thalamic "
        "circuits. These techniques enable causal manipulation and real-time "
        "monitoring of specific cell types and pathways, driving a shift from "
        "correlational to mechanistic descriptions. The technique-framing associations "
        "we observe suggest that methodological advances are not merely following but "
        "actively shaping conceptual change. The fMRI confound analysis demonstrates "
        "that hub framing increases even when fMRI papers are excluded, confirming "
        "that this conceptual shift transcends any single methodology."
    )

    # 4.3
    doc.add_heading("4.3 The scale distinction", level=2)
    add_body(doc,
        "A key finding is the divergence between whole-thalamus framing and "
        "nucleus-level discourse. The relay/hub debate operates primarily at the macro "
        "level; individual nucleus papers have always used more precise functional "
        "vocabulary. This suggests that the conceptual shift is most relevant for how "
        "the field discusses the thalamus as a unified structure, and that "
        "researchers working on specific nuclei have long recognised the functional "
        "diversity that the hub/relay dichotomy obscures."
    )

    # 4.4
    doc.add_heading("4.4 Limitations", level=2)
    add_body(doc,
        "Several limitations should be noted. First, our corpus is biased towards "
        "English-language publications indexed in PubMed. Second, PMC full-text "
        "availability is skewed towards post-2008 publications, which may inflate "
        "trends in the full-text analysis. Third, our keyword-based framing taxonomy, "
        "while comprehensive, may miss novel or unusual phrasings. Fourth, "
        "BERTopic\u2019s HDBSCAN clustering assigned 46% of sentences to noise, "
        "meaning nearly half of sentences were not assigned to any topic. Fifth, the "
        "95% Wilson confidence intervals on annual proportions are wide for categories "
        "with few instances. The shaded regions in temporal plots represent these 95% "
        "CIs."
    )
    add_figure(doc, fig_num=8)

    # ── 5. Conclusion ──────────────────────────────────────────────────
    doc.add_heading("5. Conclusion", level=1)
    add_body(doc,
        "This study provides the first comprehensive computational analysis of "
        "conceptual framing evolution in thalamus research. The thalamus literature "
        "has undergone a transformation from generic, unspecified descriptions towards "
        "increasingly precise functional characterisations. Rather than a simple "
        "relay-to-hub transition, we observe the simultaneous emergence of multiple "
        "specific framing categories, reflecting a field that is developing a more "
        "nuanced and multifaceted understanding of thalamic function. New recording "
        "techniques are both driving and reflecting this conceptual change, while "
        "nucleus-level research has always maintained greater specificity than "
        "whole-thalamus discussions."
    )

    # ── Supplementary Materials ────────────────────────────────────────
    doc.add_page_break()
    doc.add_heading("Supplementary Materials", level=1)

    for key in ("S1", "S2", "S3", "S4", "S5"):
        add_figure(doc, sup_key=key, width=Inches(6.0))

    # ── Save ───────────────────────────────────────────────────────────
    doc.save(str(OUT_PATH))
    print(f"\nSaved: {OUT_PATH}")


if __name__ == "__main__":
    build()
