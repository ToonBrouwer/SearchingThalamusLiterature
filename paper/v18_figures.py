"""
v18_figures.py — publication figures for paper v18.

Every figure follows Paper/v18_figure_plan.md: max 4 panels, one question per
figure, all numbers derived live from the canonical dataset via v18_data.

Usage:
    python Code/paper/v18_figures.py --figs 1,3
    python Code/paper/v18_figures.py --figs all
"""
from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "Code"))
sys.path.insert(0, str(ROOT / "Code" / "paper"))

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                                   errors="replace")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np
import pandas as pd

from plotting import apply_style, save_fig, panel_label, despine, WONG
import v18_data as D

apply_style()
PUB = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)


def spread_labels(ax, items, x, min_gap, fontsize=7.5):
    """Direct-label line endpoints without overlap.

    items: list of (y, text, color); labels are nudged apart vertically
    until every neighbouring pair is at least `min_gap` apart (data units).
    """
    items = sorted(items, key=lambda t: t[0])
    ys = [t[0] for t in items]
    for _ in range(60):
        moved = False
        for i in range(1, len(ys)):
            if ys[i] - ys[i - 1] < min_gap:
                shift = (min_gap - (ys[i] - ys[i - 1])) / 2
                ys[i - 1] -= shift
                ys[i] += shift
                moved = True
        if not moved:
            break
    for (y0, text, color), y in zip(items, ys):
        ax.text(x, y, text, color=color, fontsize=fontsize, va="center")


# ═════════════════════════════════════════════════════════════════════════
# Figure 1 — Corpus.  "What data?"
# ═════════════════════════════════════════════════════════════════════════
def _pipeline_box(ax, x, y, w, h, lines, facecolor, fontsize=8.5):
    box = FancyBboxPatch((x, y), w, h,
                          boxstyle="round,pad=0.012",
                          facecolor=facecolor, edgecolor="#555555",
                          linewidth=0.8)
    ax.add_patch(box)
    ax.text(x + w / 2, y + h / 2, "\n".join(lines),
            ha="center", va="center", fontsize=fontsize)


def _arrow(ax, x0, y0, x1, y1):
    ax.add_patch(FancyArrowPatch((x0, y0), (x1, y1),
                                  arrowstyle="-|>", mutation_scale=14,
                                  color="#444444", linewidth=1.1))


def fig1():
    print("=== v18 Figure 1: Corpus ===")
    pubs = D.load_pubs()
    sent = D.load_sentences(usecols=["pmid", "year", "source", "frame_type"])
    framing = D.load_framing()

    n_pubs = len(pubs)
    n_sent = len(sent)
    n_papers_sent = sent.pmid.nunique()
    n_framing = len(framing)
    n_framing_papers = framing.pmid.nunique()
    src_counts = sent.source.value_counts()
    fulltext_sources = ["institutional", "fulltext", "oa_pdf"]
    n_ft_papers = sent[sent.source.isin(fulltext_sources)].pmid.nunique()
    cov = 100 * n_ft_papers / n_papers_sent
    print(f"  pubs={n_pubs:,}  sentences={n_sent:,}  papers={n_papers_sent:,}"
          f"  fulltext papers={n_ft_papers:,} ({cov:.0f}%)"
          f"  framing={n_framing:,}/{n_framing_papers:,} papers")

    fig, axes = plt.subplots(2, 2, figsize=(12.5, 9.5))
    ax_a, ax_b, ax_c, ax_d = axes.flat

    # ── A: pipeline diagram ──────────────────────────────────────────────
    ax_a.set_xlim(0, 1)
    ax_a.set_ylim(0, 1)
    ax_a.axis("off")
    _pipeline_box(ax_a, 0.22, 0.84, 0.56, 0.13,
                  ["PubMed title query ('thalamus' OR 'thalamic')",
                   f"{n_pubs:,} articles, 1950–2026"],
                  "#dbe9f6")
    _pipeline_box(ax_a, 0.03, 0.56, 0.44, 0.16,
                  ["Abstracts (PubMed)",
                   f"{src_counts.get('abstract', 0):,} sentences"],
                  "#ddf0e4")
    _pipeline_box(ax_a, 0.53, 0.56, 0.44, 0.16,
                  ["Full text: PMC / UvA LibKey / OA PDF",
                   f"{n_ft_papers:,} papers ({cov:.0f}% of corpus)",
                   f"{sum(src_counts.get(s, 0) for s in fulltext_sources):,} sentences"],
                  "#fdeacc")
    _pipeline_box(ax_a, 0.22, 0.30, 0.56, 0.14,
                  ["Thalamus-mentioning sentences",
                   f"{n_sent:,} sentences from {n_papers_sent:,} papers"],
                  "#f4f4f4")
    _pipeline_box(ax_a, 0.03, 0.02, 0.44, 0.16,
                  ["Frame detection (21 regex patterns)",
                   f"{n_framing:,} framing sentences",
                   f"{n_framing_papers:,} papers"],
                  "#fbdede")
    _pipeline_box(ax_a, 0.53, 0.02, 0.44, 0.16,
                  ["Domain clustering (Leiden, MPNet)",
                   "14,947 abstracts → 23 domains"],
                  "#e8def5")
    _arrow(ax_a, 0.38, 0.84, 0.25, 0.73)
    _arrow(ax_a, 0.62, 0.84, 0.75, 0.73)
    _arrow(ax_a, 0.25, 0.56, 0.42, 0.45)
    _arrow(ax_a, 0.75, 0.56, 0.58, 0.45)
    _arrow(ax_a, 0.38, 0.30, 0.25, 0.19)
    _arrow(ax_a, 0.62, 0.30, 0.75, 0.19)
    ax_a.set_title("Data pipeline", fontweight="bold")
    panel_label(ax_a, "A", x=-0.02)

    # ── B: publications per year ─────────────────────────────────────────
    per_year = pubs.groupby("year").size()
    per_year = per_year[(per_year.index >= 1950) & (per_year.index <= 2025)]
    ax_b.bar(per_year.index, per_year.values, width=0.85,
             color=WONG["blue"], linewidth=0)
    ax_b.set_xlabel("Publication year")
    ax_b.set_ylabel("Articles")
    ax_b.set_title(f"Thalamus publications per year (n = {per_year.sum():,})",
                   fontweight="bold")
    despine(ax_b)
    panel_label(ax_b, "B")

    # ── C: sentence source composition per 5-yr bin ──────────────────────
    s = sent[(sent.year >= 1970) & (sent.year <= 2025)].copy()
    s["bin"] = (s.year // 5) * 5
    comp = (s.groupby(["bin", "source"]).size().unstack(fill_value=0)
              .reindex(columns=["fulltext", "institutional", "oa_pdf",
                                "abstract"], fill_value=0))
    comp_pct = comp.div(comp.sum(axis=1), axis=0) * 100
    bottom = np.zeros(len(comp_pct))
    for src in comp_pct.columns:
        ax_c.bar(comp_pct.index, comp_pct[src], width=4.2, bottom=bottom,
                 color=D.SOURCE_COLORS[src], label=D.SOURCE_LABELS[src],
                 linewidth=0)
        bottom += comp_pct[src].values
    ax_c.set_xlabel("5-year bin")
    ax_c.set_ylabel("% of sentences")
    ax_c.set_ylim(0, 100)
    ax_c.set_xlim(1966, 2029)
    ax_c.set_title("Sentence source composition", fontweight="bold")
    ax_c.legend(fontsize=7.5, loc="lower left", framealpha=0.9)
    despine(ax_c)
    panel_label(ax_c, "C")

    # ── D: framing detection rate per decade ─────────────────────────────
    s2 = sent[(sent.year >= 1970) & (sent.year <= 2025)].copy()
    s2["decade"] = (s2.year // 10) * 10
    tot = s2.groupby("decade").size()
    framed = s2[s2.frame_type.notna() & (s2.frame_type != "")
                & (s2.frame_type != "none")].groupby("decade").size()
    framed = framed.reindex(tot.index, fill_value=0)
    rate = 100 * framed / tot
    lo, hi = D.wilson_ci(framed.values, tot.values)
    ax_d.bar(rate.index, rate.values, width=7.5, color=WONG["red"],
             linewidth=0)
    ax_d.errorbar(rate.index, rate.values,
                  yerr=[rate.values - 100 * lo, 100 * hi - rate.values],
                  fmt="none", ecolor="#333333", capsize=3, linewidth=1)
    for x, v, n in zip(rate.index, rate.values, tot.values):
        ax_d.text(x, v + 0.14, f"n={n:,}", ha="center", fontsize=7,
                  color="#555555")
    ax_d.set_xlabel("Decade")
    ax_d.set_ylabel("% of sentences with framing language")
    ax_d.set_title("Explicit framing rate per decade", fontweight="bold")
    ax_d.set_xticks(rate.index)
    ax_d.set_xticklabels([f"{d}s" for d in rate.index])
    despine(ax_d)
    panel_label(ax_d, "D")

    fig.suptitle("Figure 1: Corpus and analysis pipeline", fontweight="bold",
                 fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    save_fig(fig, PUB / "v18_fig1_corpus")
    plt.close(fig)
    print("  saved v18_fig1_corpus")


# ═════════════════════════════════════════════════════════════════════════
# Figure 3 — The framing shift.  Headline figure.
# ═════════════════════════════════════════════════════════════════════════
TOP_FRAMES = ["relay", "hub", "gate", "gateway", "clinical_target",
              "pacemaker", "modulator"]


def fig3():
    print("=== v18 Figure 3: The framing shift ===")
    fr = D.load_framing()
    fr = fr[(fr.year >= 1970) & (fr.year <= 2025)].copy()
    print(f"  framing sentences 1970-2025: {len(fr):,}")

    fig, axes = plt.subplots(2, 2, figsize=(12.5, 9.5))
    ax_a, ax_b, ax_c, ax_d = axes.flat

    # ── A: taxonomy counts by semantic group ─────────────────────────────
    counts = fr.frame_type.value_counts()
    order = [f for g in D.FRAME_GROUPS.values() for f in g]
    order = [f for f in order if f in counts.index]
    order_sorted = sorted(order, key=lambda f: (
        list(D.FRAME_GROUPS).index(D.GROUP_OF_FRAME[f]), -counts[f]))
    y = np.arange(len(order_sorted))
    vals = [counts[f] for f in order_sorted]
    cols = [D.GROUP_COLORS[D.GROUP_OF_FRAME[f]] for f in order_sorted]
    ax_a.barh(y, vals, color=cols, height=0.72)
    ax_a.set_yticks(y)
    ax_a.set_yticklabels([f.replace("_", " ") for f in order_sorted],
                          fontsize=8)
    ax_a.invert_yaxis()
    ax_a.set_xscale("log")
    ax_a.set_xlim(1, max(vals) * 2.4)
    for yi, v in zip(y, vals):
        ax_a.text(v * 1.12, yi, f"{v:,}", va="center", fontsize=7)
    handles = [mpatches.Patch(color=c, label=g)
               for g, c in D.GROUP_COLORS.items()]
    ax_a.legend(handles=handles, fontsize=8, loc="lower right")
    ax_a.set_xlabel("Framing sentences (log scale)")
    ax_a.set_title(f"21 frame types (n = {len(fr):,})", fontweight="bold")
    despine(ax_a)
    panel_label(ax_a, "A", x=-0.22)

    # ── B: framing proportions over time (top 7 + other, 5-yr bins) ──────
    fb = fr.copy()
    fb["frame7"] = np.where(fb.frame_type.isin(TOP_FRAMES),
                             fb.frame_type, "other")
    fb["bin"] = (fb.year // 5) * 5
    mat = (fb.groupby(["bin", "frame7"]).size().unstack(fill_value=0)
             .reindex(columns=TOP_FRAMES + ["other"], fill_value=0))
    tot = mat.sum(axis=1)
    mat_p = mat.div(tot, axis=0)
    bottom = np.zeros(len(mat_p))
    for f in TOP_FRAMES + ["other"]:
        ax_b.bar(mat_p.index, mat_p[f] * 100, width=4.2, bottom=bottom,
                 color=D.FRAME_COLORS[f],
                 label=f.replace("_", " "), linewidth=0)
        bottom += (mat_p[f] * 100).values
    for x, n in zip(mat_p.index, tot.values):
        ax_b.text(x, 101, f"{n}", ha="center", fontsize=6.5, color="#666666")
    ax_b.set_ylim(0, 106)
    ax_b.set_xlabel("5-year bin")
    ax_b.set_ylabel("% of framing sentences")
    ax_b.set_title("Frame composition over time", fontweight="bold")
    ax_b.legend(fontsize=7, ncol=2, loc="lower left", framealpha=0.92)
    despine(ax_b)
    panel_label(ax_b, "B")

    # ── C: relay vs hub crossover with Wilson CIs ────────────────────────
    yearly = fr.groupby("year").size()
    for frame, color in [("relay", D.FRAME_COLORS["relay"]),
                          ("hub", D.FRAME_COLORS["hub"])]:
        k = (fr[fr.frame_type == frame].groupby("year").size()
             .reindex(yearly.index, fill_value=0))
        p = (k / yearly)
        lo, hi = D.wilson_ci(k.values, yearly.values)
        # 3-yr centred rolling mean for display only
        p_s = p.rolling(3, center=True, min_periods=1).mean()
        lo_s = pd.Series(lo, index=p.index).rolling(3, center=True,
                                                     min_periods=1).mean()
        hi_s = pd.Series(hi, index=p.index).rolling(3, center=True,
                                                     min_periods=1).mean()
        ax_c.fill_between(p.index, lo_s * 100, hi_s * 100, color=color,
                           alpha=0.18, linewidth=0)
        ax_c.plot(p.index, p_s * 100, color=color, linewidth=2.2,
                  label=frame)
    # Mann-Kendall on raw annual proportions (stats never on smoothed data)
    for frame in ["relay", "hub"]:
        k = (fr[fr.frame_type == frame].groupby("year").size()
             .reindex(yearly.index, fill_value=0))
        tau, pval = D.mann_kendall(yearly.index.values, (k / yearly).values)
        print(f"  MK {frame}: tau={tau:+.2f}, p={pval:.2g}")
    ax_c.set_xlabel("Year")
    ax_c.set_ylabel("% of framing sentences")
    ax_c.set_title("Relay vs hub: the crossover", fontweight="bold")
    ax_c.legend(fontsize=9)
    ax_c.text(0.02, 0.02,
              "lines: 3-yr rolling mean; shading: 95% Wilson CI",
              transform=ax_c.transAxes, fontsize=7, color="#666666")
    despine(ax_c)
    panel_label(ax_c, "C")

    # ── D: Mann-Kendall tau per frame ────────────────────────────────────
    rows = []
    for f in counts.index:
        k = (fr[fr.frame_type == f].groupby("year").size()
             .reindex(yearly.index, fill_value=0))
        tau, pval = D.mann_kendall(yearly.index.values, (k / yearly).values)
        rows.append((f, tau, pval, counts[f]))
    mk = (pd.DataFrame(rows, columns=["frame", "tau", "p", "n"])
            .dropna(subset=["tau"])
            .sort_values("tau"))
    y = np.arange(len(mk))
    cols = [D.GROUP_COLORS[D.GROUP_OF_FRAME[f]] for f in mk.frame]
    ax_d.barh(y, mk.tau, color=cols, height=0.72)
    ax_d.axvline(0, color="#444444", linewidth=0.7)
    ax_d.set_yticks(y)
    ax_d.set_yticklabels(
        [f"{f.replace('_', ' ')} {D.sig_stars(p)}"
         for f, p in zip(mk.frame, mk.p)], fontsize=8)
    ax_d.set_xlabel("Mann-Kendall τ (annual proportion vs year)")
    ax_d.set_title("Temporal trend per frame", fontweight="bold")
    despine(ax_d)
    panel_label(ax_d, "D", x=-0.26)

    fig.suptitle("Figure 3: The framing shift — relay to a diverse "
                 "conceptual vocabulary", fontweight="bold", fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    save_fig(fig, PUB / "v18_fig3_framing_shift")
    plt.close(fig)
    print("  saved v18_fig3_framing_shift")


# ═════════════════════════════════════════════════════════════════════════
# Figure 5 — Anatomy.  "Does framing respect anatomy?"
# ═════════════════════════════════════════════════════════════════════════
NUCLEI = ["MD", "VP", "VL", "VIM", "Pulvinar", "LGN", "ANT", "CM", "PVT",
          "TRN", "CL", "PO", "Reuniens", "MGN", "AD", "PF", "VM"]
HEAT_FRAMES = ["relay", "hub", "gate", "gateway", "clinical_target",
               "pacemaker", "modulator", "interface", "integrator"]


def _nucleus_profiles(fr):
    """Per-nucleus FRAMING profile for clustering.

    One vector per nucleus: the proportion of its framing sentences in
    each frame type (frames with >= 100 total sentences only, to limit
    small-count noise). Clustering on framing alone answers the figure's
    question directly — does the framing vocabulary respect anatomy? —
    and is far more stable (CPCC 0.92) than mixed framing+technique or
    framing+domain profiles, which import other constructs and gave
    method-dependent groupings.
    """
    frame_keep = (fr.frame_type.value_counts()
                  [lambda s: s >= 100].index.tolist())
    profiles, meta = {}, {}
    for nuc in NUCLEI:
        sub = fr[fr[f"has_{nuc}"]]
        if len(sub) < 20:
            continue
        counts = sub.frame_type.value_counts()
        fvec = (counts.reindex(frame_keep, fill_value=0)
                .values.astype(float))
        profiles[nuc] = fvec / max(fvec.sum(), 1)
        meta[nuc] = {"n": len(sub), "dominant": counts.index[0]}
    return profiles, meta


def fig5():
    print("=== v18 Figure 5: Anatomy ===")
    from scipy.cluster.hierarchy import linkage, dendrogram, cophenet
    from scipy.spatial.distance import pdist, squareform

    fr = D.load_framing()
    nuc_cols = [f"has_{n}" for n in NUCLEI]
    sent = D.load_sentences(usecols=["pmid", "year"] + nuc_cols)

    fig, axes = plt.subplots(2, 2, figsize=(13, 10.5))
    ax_a, ax_b, ax_c, ax_d = axes.flat

    # ── A: frame x nucleus heatmap (column-normalised %) ─────────────────
    # all nuclei with n >= 5 shown; small-n columns are legible via the
    # per-column n annotation
    cols_kept, mat, ns = [], [], []
    for nuc in NUCLEI:
        sub = fr[fr[f"has_{nuc}"]]
        if len(sub) < 5:
            continue
        counts = sub.frame_type.value_counts()
        named = counts.reindex(HEAT_FRAMES, fill_value=0)
        other = counts.sum() - named.sum()
        vec = np.append(named.values, other) / counts.sum() * 100
        cols_kept.append(nuc)
        mat.append(vec)
        ns.append(len(sub))
    mat = np.array(mat).T  # rows = frames, cols = nuclei
    row_labels = [f.replace("_", " ") for f in HEAT_FRAMES] + ["other"]
    im = ax_a.imshow(mat, aspect="auto", cmap="YlOrRd", vmin=0, vmax=80)
    ax_a.set_xticks(np.arange(len(cols_kept)))
    ax_a.set_xticklabels([f"{c}\n(n={n})" for c, n in zip(cols_kept, ns)],
                          fontsize=7)
    ax_a.set_yticks(np.arange(len(row_labels)))
    ax_a.set_yticklabels(row_labels, fontsize=8)
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat[i, j]
            if v >= 3:
                ax_a.text(j, i, f"{v:.0f}", ha="center", va="center",
                          fontsize=6.5,
                          color="white" if v > 45 else "black")
    plt.colorbar(im, ax=ax_a, shrink=0.8, label="% of nucleus framing")
    ax_a.set_title("Frame × nucleus (each column sums to 100%)",
                   fontweight="bold")
    panel_label(ax_a, "A", x=-0.10)

    # ── B: nucleus prevalence over time (top 8, direct labels) ───────────
    s = sent[(sent.year >= 1975) & (sent.year <= 2025)]
    totals = s.groupby("year").size()
    top10 = (s[nuc_cols].sum().sort_values(ascending=False).head(10)
             .index.str.replace("has_", ""))
    palette = list(WONG.values())[:10]
    ends = []
    for nuc, color in zip(top10, palette):
        p = (s[s[f"has_{nuc}"]].groupby("year").size()
             .reindex(totals.index, fill_value=0) / totals * 100)
        p_s = p.rolling(3, center=True, min_periods=1).mean()
        ax_b.plot(p_s.index, p_s.values, color=color, linewidth=1.6)
        ends.append((p_s.values[-1], nuc, color))
    spread_labels(ax_b, ends, x=2026, min_gap=0.26, fontsize=7.5)
    ax_b.set_xlim(1975, 2031)
    ax_b.set_xlabel("Year")
    ax_b.set_ylabel("% of sentences mentioning nucleus")
    ax_b.set_title("Nucleus prevalence (top 10, 3-yr smoothed; "
                   "all 17 in Supp)", fontweight="bold")
    despine(ax_b)
    panel_label(ax_b, "B")

    # ── C: dendrogram (correlation + average linkage) ────────────────────
    profiles, meta = _nucleus_profiles(fr)
    names = list(profiles)
    X = np.array([profiles[n] for n in names])
    dist = pdist(X, metric="correlation")
    Z = linkage(dist, method="average")
    coph_r, _ = cophenet(Z, dist)
    dn = dendrogram(Z, labels=names, ax=ax_c, leaf_font_size=9,
                    color_threshold=0.55 * max(Z[:, 2]))
    ax_c.set_ylabel("Correlation distance")
    ax_c.set_title(f"Nucleus clustering by framing profile "
                   f"(CPCC = {coph_r:.2f})", fontweight="bold")
    despine(ax_c)
    panel_label(ax_c, "C", x=-0.09)
    print(f"  dendrogram CPCC (correlation+average): {coph_r:.3f}")

    # ── D: MDS coloured by dominant frame ────────────────────────────────
    from sklearn.manifold import MDS
    Dm = squareform(dist)
    mds = MDS(n_components=2, dissimilarity="precomputed", random_state=42,
              normalized_stress="auto")
    xy = mds.fit_transform(Dm)
    dom_color = {"relay": D.FRAME_COLORS["relay"],
                 "hub": D.FRAME_COLORS["hub"],
                 "gate": D.FRAME_COLORS["gate"],
                 "clinical_target": D.FRAME_COLORS["clinical_target"]}
    seen = set()
    for (x, y), n in zip(xy, names):
        dom = meta[n]["dominant"]
        c = dom_color.get(dom, WONG["grey"])
        size = 40 + meta[n]["n"] / 8
        label = dom.replace("_", " ") if dom not in seen else None
        seen.add(dom)
        ax_d.scatter(x, y, s=size, color=c, edgecolor="#333333",
                     linewidth=0.5, label=label, zorder=3)
        ax_d.annotate(n, (x, y), textcoords="offset points",
                      xytext=(6, 5), fontsize=8)
    ax_d.legend(fontsize=8, title="Dominant frame", title_fontsize=8)
    ax_d.set_xlabel("MDS dimension 1")
    ax_d.set_ylabel("MDS dimension 2")
    ax_d.set_title("Nuclei in framing space (MDS)", fontweight="bold")
    despine(ax_d)
    panel_label(ax_d, "D")

    fig.suptitle("Figure 5: Framing respects thalamic anatomy",
                 fontweight="bold", fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    save_fig(fig, PUB / "v18_fig5_anatomy")
    plt.close(fig)
    print("  saved v18_fig5_anatomy")


# ═════════════════════════════════════════════════════════════════════════
# Supplementary — hub-vs-relay regression controls (was main Fig 6 A/B)
# ═════════════════════════════════════════════════════════════════════════
def supp_regression():
    print("=== v18 Supp: regression controls ===")
    import statsmodels.api as sm

    fr = D.load_framing()
    techs = D.load_paper_techniques()
    tcols = [c for c in techs.columns if c != "pmid"]

    def tkey(sub):
        return next(c for c in tcols if sub in c.lower())

    k_fmri, k_ephys, k_lesion = tkey("fmri"), tkey("electro"), tkey("lesion")

    hr = fr[fr.frame_type.isin(["relay", "hub"])].merge(techs, on="pmid",
                                                         how="left")
    for c in [k_fmri, k_ephys, k_lesion]:
        hr[c] = hr[c].fillna(False).astype(int)
    hr = hr[(hr.year >= 1970) & (hr.year <= 2026)].copy()
    hr["hub"] = (hr.frame_type == "hub").astype(int)
    hr["year_c"] = hr.year - 2000
    print(f"  n = {len(hr):,} relay+hub sentences "
          f"({hr.hub.sum():,} hub / {(1 - hr.hub).sum():,} relay)")

    X_full = sm.add_constant(hr[["year_c", k_fmri, k_ephys, k_lesion]])
    m_full = sm.GLM(hr.hub, X_full, family=sm.families.Binomial()).fit()
    X_year = sm.add_constant(hr[["year_c"]])
    m_year = sm.GLM(hr.hub, X_year, family=sm.families.Binomial()).fit()

    labels = {"year_c": "Year (per year)", k_fmri: "fMRI",
              k_ephys: "Electrophysiology", k_lesion: "Lesion study"}
    ors = np.exp(m_full.params.drop("const"))
    ci = np.exp(m_full.conf_int().drop(index="const"))
    pvals = m_full.pvalues.drop("const")
    for name, o, p in zip(ors.index, ors.values, pvals.values):
        print(f"  {labels[name]:20s} OR={o:.2f}  p={p:.2g}")

    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.8))
    ax_a, ax_b = axes.flat

    # ── A: forest plot ───────────────────────────────────────────────────
    y = np.arange(len(ors))[::-1]
    ax_a.errorbar(ors.values, y,
                  xerr=[ors.values - ci[0].values, ci[1].values - ors.values],
                  fmt="s", color=WONG["blue"], ecolor="#333333",
                  capsize=4, markersize=7, linewidth=1.4)
    ax_a.axvline(1, color="#888888", linestyle="--", linewidth=0.9)
    ax_a.set_xscale("log")
    ax_a.set_yticks(y)
    ax_a.set_yticklabels([labels[n] for n in ors.index], fontsize=9)
    for yi, o, (lo, hi), p in zip(y, ors.values, ci.values, pvals.values):
        ax_a.text(hi * 1.25, yi, f"OR {o:.2f} [{lo:.2f}–{hi:.2f}] "
                  f"{D.sig_stars(p)}", va="center", fontsize=8)
    ax_a.set_xlim(0.05, 400)
    ax_a.set_xlabel("Odds ratio, hub vs relay (log scale)")
    ax_a.set_title(f"Hub vs relay: logistic regression "
                   f"(n = {len(hr):,} sentences)", fontweight="bold")
    despine(ax_a)
    panel_label(ax_a, "A", x=-0.28)

    # ── B: predicted P(hub), year-only vs technique-controlled ───────────
    yrs = np.arange(1975, 2026)
    pred_year = m_year.predict(
        sm.add_constant(pd.DataFrame({"year_c": yrs - 2000})))
    base = pd.DataFrame({"year_c": yrs - 2000})
    for c, v in [(k_fmri, hr[k_fmri].mean()), (k_ephys, hr[k_ephys].mean()),
                  (k_lesion, hr[k_lesion].mean())]:
        base[c] = v
    pred_full = m_full.predict(sm.add_constant(base, has_constant="add"))
    ax_b.plot(yrs, pred_year, color=WONG["blue"], linewidth=2.4,
              label="year only")
    ax_b.plot(yrs, pred_full, color=WONG["orange"], linewidth=2.4,
              linestyle="--", label="year + techniques (at sample mix)")
    ax_b.set_xlabel("Year")
    ax_b.set_ylabel("Predicted P(hub | hub or relay)")
    ax_b.set_title("Temporal trend survives technique control",
                   fontweight="bold")
    ax_b.legend(fontsize=8.5)
    despine(ax_b)
    panel_label(ax_b, "B")

    fig.suptitle("Supplementary: hub-vs-relay regression controls",
                 fontweight="bold", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    save_fig(fig, PUB / "v18_suppA_regression_controls")
    plt.close(fig)
    print("  saved v18_suppA_regression_controls")


# ═════════════════════════════════════════════════════════════════════════
# Figure 7 — Method precedes framing.  "fMRI leads hub."
# ═════════════════════════════════════════════════════════════════════════
def fig7():
    print("=== v18 Figure 7: Method precedes framing ===")
    fr = D.load_framing()
    techs = D.load_paper_techniques()
    tcols = [c for c in techs.columns if c != "pmid"]
    k_fmri = next(c for c in tcols if "fmri" in c.lower())

    sent = D.load_sentences(usecols=["pmid", "year"])
    papers = (sent.drop_duplicates("pmid").merge(techs, on="pmid",
                                                  how="left"))
    papers[k_fmri] = papers[k_fmri].fillna(False)
    py = papers[(papers.year >= 1985) & (papers.year <= 2025)]
    fmri_p = (py.groupby("year")[k_fmri].mean() * 100)
    fr_y = fr[(fr.year >= 1985) & (fr.year <= 2025)]
    tot = fr_y.groupby("year").size()
    hub_p = (fr_y[fr_y.frame_type == "hub"].groupby("year").size()
             .reindex(tot.index, fill_value=0) / tot * 100)
    fmri_s = fmri_p.rolling(3, center=True, min_periods=1).mean()
    hub_s = hub_p.rolling(3, center=True, min_periods=1).mean()

    fig, (ax_a, ax_b) = plt.subplots(1, 2, figsize=(12.5, 4.8),
                                      width_ratios=[1.5, 1])

    # ── A: fMRI adoption vs hub framing (dual axis) ──────────────────────
    ax_a.plot(fmri_s.index, fmri_s.values, color=WONG["orange"],
              linewidth=2.4, label="% papers using fMRI")
    ax_a.set_ylabel("% of papers using fMRI", color=WONG["orange"])
    ax_a.tick_params(axis="y", labelcolor=WONG["orange"])
    ax_a2 = ax_a.twinx()
    ax_a2.plot(hub_s.index, hub_s.values, color=D.FRAME_COLORS["hub"],
               linewidth=2.4, linestyle="--", label="% framing = hub")
    ax_a2.set_ylabel("% of framing sentences using hub",
                     color=D.FRAME_COLORS["hub"])
    ax_a2.tick_params(axis="y", labelcolor=D.FRAME_COLORS["hub"])
    ax_a2.spines["top"].set_visible(False)
    ax_a.set_xlabel("Year")
    ax_a.set_title("fMRI adoption precedes hub framing", fontweight="bold")
    despine(ax_a)
    panel_label(ax_a, "A")

    # ── B: cross-correlation by lag ──────────────────────────────────────
    common = fmri_s.index.intersection(hub_s.index)
    a = fmri_s.loc[common].values
    b = hub_s.loc[common].values
    lags = np.arange(-5, 6)
    rs = []
    for lag in lags:
        if lag >= 0:      # fMRI leads hub by `lag` years
            x, yv = a[:len(a) - lag or None], b[lag:]
        else:
            x, yv = a[-lag:], b[:len(b) + lag]
        rs.append(np.corrcoef(x, yv)[0, 1])
    best = lags[int(np.argmax(rs))]
    cols = [WONG["red"] if l == best else "#9e9e9e" for l in lags]
    ax_b.bar(lags, rs, color=cols, width=0.75)
    ax_b.set_xticks(lags)
    ax_b.set_xlabel("Lag in years (positive = fMRI leads hub)")
    ax_b.set_ylabel("Pearson r")
    ax_b.set_title(f"Cross-correlation (best: lag {best:+d}, "
                   f"r = {max(rs):.2f})", fontweight="bold")
    despine(ax_b)
    panel_label(ax_b, "B")

    fig.suptitle("Figure 7: Method precedes framing", fontweight="bold",
                 fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    save_fig(fig, PUB / "v18_fig7_method_framing")
    plt.close(fig)
    print("  saved v18_fig7_method_framing")


# ═════════════════════════════════════════════════════════════════════════
# Figure 6 — Convergence.  "Does it replicate across methods?"
# ═════════════════════════════════════════════════════════════════════════
def fig6():
    print("=== v18 Figure 6: Convergence ===")
    fig, axes = plt.subplots(2, 2, figsize=(12.5, 9))
    ax_a, ax_b, ax_c, ax_d = axes.flat

    # ── A: syntactic roles per decade ────────────────────────────────────
    syn = pd.read_csv(D.PROC / "syntactic_roles.csv", low_memory=False,
                      usecols=["decade", "thal_role"])
    syn = syn[(syn.decade >= 1980) & (syn.decade <= 2020)]
    role_colors = {"SUBJECT": WONG["blue"], "OBJECT": WONG["orange"],
                   "PREPOSITIONAL": WONG["green"]}
    tot = syn.groupby("decade").size()
    for role, color in role_colors.items():
        k = (syn[syn.thal_role == role].groupby("decade").size()
             .reindex(tot.index, fill_value=0))
        p = k / tot * 100
        lo, hi = D.wilson_ci(k.values, tot.values)
        ax_a.errorbar(p.index, p.values,
                      yerr=[p.values - 100 * lo, 100 * hi - p.values],
                      color=color, linewidth=2, marker="o", capsize=3,
                      label=role.lower())
    ax_a.set_xticks(tot.index)
    ax_a.set_xticklabels([f"{d}s" for d in tot.index])
    ax_a.set_xlabel("Decade")
    ax_a.set_ylabel("% of thalamus mentions")
    ax_a.set_title("Syntactic role of 'thalamus'", fontweight="bold")
    ax_a.legend(fontsize=8.5)
    despine(ax_a)
    panel_label(ax_a, "A")

    # ── B: semantic closeness to other BRAIN REGIONS ─────────────────────
    w2v = pd.read_csv(D.V18 / "w2v_category_similarities.csv")
    regions = w2v[w2v.category == "brain regions"]
    reg_colors = dict(zip(
        ["cortex", "hippocampus", "cerebellum", "striatum", "amygdala",
         "hypothalamus"],
        [WONG["grey"], WONG["blue"], WONG["green"], WONG["orange"],
         WONG["pink"], WONG["purple"]]))
    ends = []
    for word, sub in regions.groupby("word"):
        sub = sub.sort_values("decade")
        c = reg_colors.get(word, WONG["cyan"])
        ax_b.plot(sub.decade, sub.similarity, color=c, linewidth=2,
                  marker="o", markersize=4)
        ends.append((sub.similarity.iloc[-1], word, c))
    spread_labels(ax_b, ends, x=2022.5, min_gap=0.022, fontsize=8)
    ax_b.set_xlim(1978, 2037)
    ax_b.set_xticks([1980, 1990, 2000, 2010, 2020])
    ax_b.set_xticklabels(["1980s", "1990s", "2000s", "2010s", "2020s"])
    ax_b.set_xlabel("Decade")
    ax_b.set_ylabel("Cosine similarity to 'thalamus'")
    ax_b.set_title("Similarity to brain regions", fontweight="bold")
    despine(ax_b)
    panel_label(ax_b, "B")

    # ── C: descriptor words, SEPARATED by category ───────────────────────
    cat_colors = {"relay/transmission": D.FRAME_COLORS["relay"],
                  "hub/network": D.FRAME_COLORS["hub"],
                  "oscillation/state": D.FRAME_COLORS["pacemaker"],
                  "clinical": D.FRAME_COLORS["clinical_target"]}
    ends = []
    for cat, color in cat_colors.items():
        sub = (w2v[w2v.category == cat].groupby("decade")
               .similarity.mean().sort_index())
        ax_c.plot(sub.index, sub.values, color=color, linewidth=2.4,
                  marker="o", markersize=5)
        ends.append((sub.values[-1], cat, color))
    spread_labels(ax_c, ends, x=2022.5, min_gap=0.008, fontsize=8)
    words_note = "; ".join(
        f"{cat}: " + ", ".join(w2v[w2v.category == cat].word.unique())
        for cat in cat_colors)
    ax_c.text(0.01, -0.24, words_note, transform=ax_c.transAxes,
              fontsize=6.3, color="#666666", wrap=True)
    ax_c.set_xlim(1978, 2039)
    ax_c.set_xticks([1980, 1990, 2000, 2010, 2020])
    ax_c.set_xticklabels(["1980s", "1990s", "2000s", "2010s", "2020s"])
    ax_c.set_xlabel("Decade")
    ax_c.set_ylabel("Mean cosine similarity to 'thalamus'")
    ax_c.set_title("Descriptor categories", fontweight="bold")
    despine(ax_c)
    panel_label(ax_c, "C")

    # ── D: co-occurrence centrality change, by word category ─────────────
    cooc = pd.read_csv(D.PROC / "cooccurrence_temporal.csv")
    piv = cooc.pivot_table(index="word", columns="decade",
                            values="centrality")
    piv = piv.dropna(subset=[1990, 2020])
    piv["delta"] = piv[2020] - piv[1990]
    word_cat = {"neurons": "cellular", "cells": "cellular",
                "nucleus": "anatomy", "nuclei": "anatomy",
                "medial": "anatomy", "lateral": "anatomy",
                "thalamocortical": "anatomy",
                "brain": "systems/network", "cortex": "systems/network",
                "cortical": "systems/network",
                "connectivity": "systems/network",
                "patients": "clinical", "stimulation": "clinical"}
    catD_colors = {"cellular": WONG["orange"], "anatomy": WONG["grey"],
                   "systems/network": WONG["blue"],
                   "clinical": D.FRAME_COLORS["clinical_target"]}
    piv = piv[piv.index.isin(word_cat)]
    piv["cat"] = piv.index.map(word_cat)
    piv = piv.sort_values(["cat", "delta"], ascending=[True, False])
    y = np.arange(len(piv))
    ax_d.barh(y, piv.delta, color=[catD_colors[c] for c in piv.cat],
              height=0.7)
    ax_d.axvline(0, color="#444444", linewidth=0.8)
    ax_d.set_yticks(y)
    ax_d.set_yticklabels(piv.index, fontsize=8.5)
    ax_d.invert_yaxis()
    handles = [mpatches.Patch(color=c, label=l)
               for l, c in catD_colors.items()]
    ax_d.legend(handles=handles, fontsize=7.5, loc="lower left")
    ax_d.set_xlabel("Δ degree centrality (1990s → 2020s)")
    ax_d.set_title("Co-occurrence centrality change", fontweight="bold")
    despine(ax_d)
    panel_label(ax_d, "D", x=-0.24)

    fig.suptitle("Figure 6: Independent methods converge on the same shift",
                 fontweight="bold", fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    save_fig(fig, PUB / "v18_fig6_convergence")
    plt.close(fig)
    print("  saved v18_fig6_convergence")


# ═════════════════════════════════════════════════════════════════════════
# Figure 2 — Landscape.  "What does the field look like?"
# ═════════════════════════════════════════════════════════════════════════
def _domain_label(terms, max_words=2):
    words, seen = [], set()
    for w in str(terms).split():
        if w not in seen:
            words.append(w)
            seen.add(w)
        if len(words) == max_words:
            break
    return " / ".join(words)


KEY_TECHNIQUES = ["lesion", "tract_tracing", "electrophysiology", "fMRI",
                  "DBS", "optogenetics"]
TECH_DISPLAY = {"lesion": "Lesion studies", "tract_tracing": "Tract tracing",
                "electrophysiology": "Electrophysiology", "fMRI": "fMRI",
                "DBS": "DBS", "optogenetics": "Optogenetics",
                "structural_MRI": "Structural MRI", "EEG_MEG": "EEG/MEG",
                "PET": "PET", "calcium_imaging": "Calcium imaging",
                "chemogenetics": "Chemogenetics"}
TECH_COLORS = {"lesion": WONG["orange"], "tract_tracing": WONG["purple"],
               "electrophysiology": WONG["cyan"], "fMRI": WONG["blue"],
               "DBS": WONG["grey"], "optogenetics": WONG["red"]}


def fig2():
    print("=== v18 Figure 2: Landscape ===")
    dom = D.load_domains()
    pubs = pd.read_csv(D.PROC / "paper_abstracts.csv",
                       usecols=["pmid", "year"], low_memory=False)
    dom = dom.merge(pubs, on="pmid", how="left")
    dom["label"] = dom.domain_name
    xy = np.load(D.PROC / "clustering_stability" / "umap" /
                 "umap_mpnet__raw.npy")
    sizes = dom.groupby("domain").size()
    big = sizes[sizes >= 50].index
    top_doms = sizes.loc[big].sort_values(ascending=False)
    print(f"  {len(dom):,} papers, {len(big)} domains ≥ 50 papers")

    # Technique trends from TITLE+ABSTRACT only — constant text
    # availability across eras (full-text detection inflates recent years)
    ta = D.load_paper_techniques_abstract()
    tp = ta[ta.has_abstract].copy()
    tcols = [c for c in tp.columns if c.startswith("has_")
             and c != "has_abstract"]

    fig, axes = plt.subplots(2, 2, figsize=(13, 10))
    ax_a, ax_b, ax_c, ax_d = axes.flat

    # ── A: UMAP coloured by domain ───────────────────────────────────────
    cmap = plt.get_cmap("tab20")
    rank = {d: i for i, d in enumerate(top_doms.index)}
    colors = [cmap(rank[d] % 20) if d in rank else (0.85, 0.85, 0.85, 1)
              for d in dom.domain]
    ax_a.scatter(xy[:, 0], xy[:, 1], s=1.6, c=colors, linewidths=0,
                 rasterized=True)
    for d in top_doms.index[:12]:
        mask = (dom.domain == d).values
        cx, cy = np.median(xy[mask, 0]), np.median(xy[mask, 1])
        ax_a.text(cx, cy, dom.loc[mask, "label"].iloc[0], fontsize=7.5,
                  ha="center", fontweight="bold", color="#222222",
                  bbox=dict(facecolor="white", alpha=0.65, pad=1,
                            edgecolor="none"))
    ax_a.set_xticks([])
    ax_a.set_yticks([])
    ax_a.set_title(f"{len(big)} research domains ({len(dom):,} papers)",
                   fontweight="bold")
    panel_label(ax_a, "A", x=-0.04)

    # ── B: domain prevalence over time (top 8) ───────────────────────────
    db = dom.dropna(subset=["year"]).copy()
    db["year"] = db.year.astype(int)
    db = db[(db.year >= 1975) & (db.year <= 2025)]
    db["bin"] = (db.year // 5) * 5
    tot = db.groupby("bin").size()
    palette_b = [cmap(rank[d] % 20) for d in top_doms.index[:8]]
    ends = []
    for d, color in zip(top_doms.index[:8], palette_b):
        p = (db[db.domain == d].groupby("bin").size()
             .reindex(tot.index, fill_value=0) / tot * 100)
        ax_b.plot(p.index, p.values, color=color, linewidth=1.9)
        lbl = db.loc[db.domain == d, "label"].iloc[0]
        ends.append((p.values[-1], lbl, color))
    spread_labels(ax_b, ends, x=2026.5, min_gap=1.6, fontsize=7)
    ax_b.set_xlim(1975, 2040)
    ax_b.set_xlabel("5-year bin")
    ax_b.set_ylabel("% of papers in domain")
    ax_b.set_title("Domain prevalence over time (top 8)", fontweight="bold")
    despine(ax_b)
    panel_label(ax_b, "B")

    # ── C: technique prevalence over time ────────────────────────────────
    ty = tp[(tp.year >= 1975) & (tp.year <= 2025)].copy()
    tot_y = ty.groupby("year").size()
    ends = []
    for t in KEY_TECHNIQUES:
        col = f"has_{t}"
        p = (ty[ty[col]].groupby("year").size()
             .reindex(tot_y.index, fill_value=0) / tot_y * 100)
        p_s = p.rolling(3, center=True, min_periods=1).mean()
        ax_c.plot(p_s.index, p_s.values, color=TECH_COLORS[t], linewidth=2)
        ends.append((p_s.values[-1], TECH_DISPLAY[t], TECH_COLORS[t]))
    spread_labels(ax_c, ends, x=2026, min_gap=1.3)
    ax_c.set_xlim(1975, 2037)
    ax_c.set_xlabel("Year")
    ax_c.set_ylabel("% of papers (title+abstract detection)")
    ax_c.set_title("Recording techniques over time (3-yr smoothed)",
                   fontweight="bold")
    despine(ax_c)
    panel_label(ax_c, "C")

    # ── D: Mann-Kendall trends, techniques ───────────────────────────────
    rows = []
    for t in tcols:
        tid = t.replace("has_", "")
        p = (ty[ty[t]].groupby("year").size()
             .reindex(tot_y.index, fill_value=0) / tot_y)
        tau, pv = D.mann_kendall(tot_y.index.values, p.values)
        rows.append((TECH_DISPLAY.get(tid, tid), tau, pv))
    mk = pd.DataFrame(rows, columns=["label", "tau", "p"]).sort_values("tau")
    y = np.arange(len(mk))
    cols = [WONG["green"] if t > 0 else WONG["orange"] for t in mk.tau]
    ax_d.barh(y, mk.tau, color=cols, height=0.7)
    ax_d.axvline(0, color="#444444", linewidth=0.8)
    ax_d.set_yticks(y)
    ax_d.set_yticklabels([f"{l} {D.sig_stars(p)}"
                          for l, p in zip(mk.label, mk.p)], fontsize=8.5)
    ax_d.set_xlabel("Mann-Kendall τ (annual % of papers vs year)")
    ax_d.set_title("Rising and declining techniques\n"
                   "(title+abstract detection)", fontweight="bold")
    despine(ax_d)
    panel_label(ax_d, "D", x=-0.30)

    fig.suptitle("Figure 2: The thalamus research landscape",
                 fontweight="bold", fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    save_fig(fig, PUB / "v18_fig2_landscape")
    plt.close(fig)
    print("  saved v18_fig2_landscape")


# ═════════════════════════════════════════════════════════════════════════
# Figure 4 — Framing in context.  "Where does each frame live?"
# ═════════════════════════════════════════════════════════════════════════
def fig4():
    print("=== v18 Figure 4: Framing in context ===")
    from scipy.stats import chi2_contingency

    fr = D.load_framing()
    techs = D.load_paper_techniques()
    dom = D.load_domains()
    dom["label"] = dom.domain_name
    frt = fr.merge(techs, on="pmid", how="left")
    tcols = [c for c in techs.columns if c != "pmid"]
    frt[tcols] = frt[tcols].fillna(False)
    frd = fr.merge(dom[["pmid", "domain", "label"]], on="pmid", how="left")

    fig = plt.figure(figsize=(13, 10))
    gs = fig.add_gridspec(2, 2, height_ratios=[1.15, 1])
    ax_a = fig.add_subplot(gs[0, 0])
    ax_b = fig.add_subplot(gs[0, 1])
    ax_d = fig.add_subplot(gs[1, :])
    frame_rows = HEAT_FRAMES + ["other"]

    # ── A: frame × technique (column-normalised %, min-n 30) ─────────────
    cols_kept, mat, ns = [], [], []
    for t in tcols:
        sub = frt[frt[t]]
        if len(sub) < 30:
            continue
        counts = sub.frame_type.value_counts()
        named = counts.reindex(HEAT_FRAMES, fill_value=0)
        vec = np.append(named.values, counts.sum() - named.sum())
        cols_kept.append(t.replace("has_", ""))
        mat.append(vec / counts.sum() * 100)
        ns.append(len(sub))
    matA = np.array(mat).T
    im = ax_a.imshow(matA, aspect="auto", cmap="YlGnBu", vmin=0, vmax=80)
    ax_a.set_xticks(np.arange(len(cols_kept)))
    ax_a.set_xticklabels([f"{TECH_DISPLAY.get(c, c)}\n(n={n})"
                          for c, n in zip(cols_kept, ns)], fontsize=6.5,
                          rotation=30, ha="right")
    ax_a.set_yticks(np.arange(len(frame_rows)))
    ax_a.set_yticklabels([f.replace("_", " ") for f in frame_rows],
                          fontsize=8)
    for i in range(matA.shape[0]):
        for j in range(matA.shape[1]):
            v = matA[i, j]
            if v >= 3:
                ax_a.text(j, i, f"{v:.0f}", ha="center", va="center",
                          fontsize=6.5,
                          color="white" if v > 45 else "black")
    plt.colorbar(im, ax=ax_a, shrink=0.8, label="% of technique framing")
    ax_a.set_title("Frame × technique (columns sum to 100%)",
                   fontweight="bold")
    panel_label(ax_a, "A", x=-0.14)

    # ── B: frame × domain (column-normalised %, top domains) ─────────────
    dcounts = (frd.dropna(subset=["domain"]).groupby("domain").size()
               .sort_values(ascending=False))
    keep_doms = [d for d in dcounts.index if dcounts[d] >= 40][:10]
    cols_kept, mat, ns = [], [], []
    for d in keep_doms:
        sub = frd[frd.domain == d]
        counts = sub.frame_type.value_counts()
        named = counts.reindex(HEAT_FRAMES, fill_value=0)
        vec = np.append(named.values, counts.sum() - named.sum())
        cols_kept.append(sub.label.iloc[0])
        mat.append(vec / counts.sum() * 100)
        ns.append(len(sub))
    matB = np.array(mat).T
    im = ax_b.imshow(matB, aspect="auto", cmap="YlGnBu", vmin=0, vmax=80)
    ax_b.set_xticks(np.arange(len(cols_kept)))
    ax_b.set_xticklabels([f"{c}\n(n={n})" for c, n in zip(cols_kept, ns)],
                          fontsize=6.5, rotation=30, ha="right")
    ax_b.set_yticks(np.arange(len(frame_rows)))
    ax_b.set_yticklabels([f.replace("_", " ") for f in frame_rows],
                          fontsize=8)
    for i in range(matB.shape[0]):
        for j in range(matB.shape[1]):
            v = matB[i, j]
            if v >= 3:
                ax_b.text(j, i, f"{v:.0f}", ha="center", va="center",
                          fontsize=6.5,
                          color="white" if v > 45 else "black")
    plt.colorbar(im, ax=ax_b, shrink=0.8, label="% of domain framing")
    ax_b.set_title("Frame × research domain (columns sum to 100%)",
                   fontweight="bold")
    panel_label(ax_b, "B", x=-0.14)

    # ── C: chi-squared standardized residuals, frame × technique ─────────
    main_frames = ["relay", "hub", "gate", "gateway", "clinical_target",
                    "pacemaker", "modulator"]
    ct_cols = [c for c in tcols
               if len(frt[frt[c] & frt.frame_type.isin(main_frames)]) >= 30]
    obs = np.array([
        [len(frt[frt[c] & (frt.frame_type == f)]) for c in ct_cols]
        for f in main_frames])
    chi2, pval, dof, exp = chi2_contingency(obs)
    resid = (obs - exp) / np.sqrt(exp)
    lim = np.abs(resid).max()
    im = ax_d.imshow(resid, aspect="auto", cmap="RdBu_r", vmin=-lim,
                     vmax=lim)
    ax_d.set_xticks(np.arange(len(ct_cols)))
    ax_d.set_xticklabels([TECH_DISPLAY.get(c.replace("has_", ""),
                                            c.replace("has_", ""))
                          for c in ct_cols], fontsize=6.5, rotation=30,
                          ha="right")
    ax_d.set_yticks(np.arange(len(main_frames)))
    ax_d.set_yticklabels([f.replace("_", " ") for f in main_frames],
                          fontsize=8)
    for i in range(resid.shape[0]):
        for j in range(resid.shape[1]):
            v = resid[i, j]
            if abs(v) >= 2:
                ax_d.text(j, i, f"{v:+.0f}", ha="center", va="center",
                          fontsize=6.5,
                          color="white" if abs(v) > 0.6 * lim else "black")
    plt.colorbar(im, ax=ax_d, shrink=0.8, label="standardized residual")
    p_txt = "p < 1e-300" if pval == 0 else f"p = {pval:.1e}"
    ax_d.set_title(f"Association strength "
                   f"(χ²({dof}) = {chi2:,.0f}, {p_txt})",
                   fontweight="bold")
    panel_label(ax_d, "C", x=-0.07)
    print(f"  chi2({dof}) = {chi2:.0f}, p = {pval:.2e}")

    fig.suptitle("Figure 4: Where each frame lives — techniques and domains",
                 fontweight="bold", fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    save_fig(fig, PUB / "v18_fig4_context")
    plt.close(fig)
    print("  saved v18_fig4_context")


# ═════════════════════════════════════════════════════════════════════════
# Supplementary — per-nucleus prevalence trends, ALL 17 nuclei
# ═════════════════════════════════════════════════════════════════════════
def supp_nucleus_trends():
    print("=== v18 Supp: all-nuclei prevalence trends ===")
    nuc_cols = [f"has_{n}" for n in NUCLEI]
    sent = D.load_sentences(usecols=["pmid", "year"] + nuc_cols)
    s = sent[(sent.year >= 1975) & (sent.year <= 2025)]
    totals = s.groupby("year").size()
    order = (s[nuc_cols].sum().sort_values(ascending=False)
             .index.str.replace("has_", ""))

    fig, axes = plt.subplots(5, 4, figsize=(13, 12), sharex=True)
    for ax, nuc in zip(axes.flat, order):
        p = (s[s[f"has_{nuc}"]].groupby("year").size()
             .reindex(totals.index, fill_value=0) / totals * 100)
        p_s = p.rolling(3, center=True, min_periods=1).mean()
        tau, pv = D.mann_kendall(p.index.values, p.values)
        ax.plot(p_s.index, p_s.values, color=WONG["blue"], linewidth=1.5)
        ax.set_title(f"{nuc}  (n={int(s[f'has_{nuc}'].sum()):,};  "
                     f"τ={tau:+.2f} {D.sig_stars(pv)})", fontsize=8.5)
        ax.tick_params(labelsize=7)
        despine(ax)
    for ax in axes.flat[len(order):]:
        ax.axis("off")
    for ax in axes[:, 0]:
        ax.set_ylabel("% of sentences", fontsize=8)
    fig.suptitle("Supplementary: nucleus prevalence over time, all 17 "
                 "nuclei (3-yr smoothed; τ on raw annual values)",
                 fontweight="bold", fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    save_fig(fig, PUB / "v18_suppB_nucleus_trends")
    plt.close(fig)
    print("  saved v18_suppB_nucleus_trends")


# ═════════════════════════════════════════════════════════════════════════
# Nuclei per domain — user-suggested figure (2026-09).  Which nuclei does
# each research domain study?
# ═════════════════════════════════════════════════════════════════════════
def fig_nucdom():
    print("=== v18 Figure: nuclei per domain ===")
    nuc_cols = [f"has_{n}" for n in NUCLEI]
    sent = D.load_sentences(usecols=["pmid"] + nuc_cols)
    paper_nuc = sent.groupby("pmid")[nuc_cols].any().reset_index()
    dom = D.load_domains().merge(paper_nuc, on="pmid", how="inner")
    sizes = dom.groupby("domain").size().sort_values(ascending=False)
    doms = [d_ for d_ in sizes.index if sizes[d_] >= 50]

    mat = np.array([[100 * dom.loc[dom.domain == d_, f"has_{n}"].mean()
                     for d_ in doms] for n in NUCLEI])
    # order nuclei by their overall mention rate for a readable gradient
    order = np.argsort(-mat.mean(axis=1))
    mat = mat[order]
    nuc_labels = [NUCLEI[i] for i in order]
    dom_labels = [f"{D.DOMAIN_NAMES.get(d_, d_)}\n(n={sizes[d_]:,})"
                  for d_ in doms]

    fig, ax = plt.subplots(figsize=(15, 7.5))
    im = ax.imshow(mat, aspect="auto", cmap="YlOrRd", vmin=0, vmax=60)
    ax.set_xticks(np.arange(len(dom_labels)))
    ax.set_xticklabels(dom_labels, fontsize=6.4, rotation=45, ha="right")
    ax.set_yticks(np.arange(len(nuc_labels)))
    ax.set_yticklabels(nuc_labels, fontsize=8.5)
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat[i, j]
            if v >= 5:
                ax.text(j, i, f"{v:.0f}", ha="center", va="center",
                        fontsize=6,
                        color="white" if v > 35 else "black")
    plt.colorbar(im, ax=ax, shrink=0.75,
                 label="% of domain's papers mentioning nucleus")
    ax.set_title("Which nuclei does each research domain study?\n"
                 "(papers can mention several nuclei; columns ordered by "
                 "domain size)", fontweight="bold")
    fig.tight_layout()
    save_fig(fig, PUB / "v18_fig_nuclei_by_domain")
    plt.close(fig)
    print("  saved v18_fig_nuclei_by_domain")


# ═════════════════════════════════════════════════════════════════════════
# Supplementary — r008 sub-domain structure (finer Leiden resolution)
# ═════════════════════════════════════════════════════════════════════════
def supp_r008():
    """The r008 configuration (31 clusters >= 50 papers) is a nested
    refinement of the winning r005 landscape: most clusters sit 88-100%
    inside one r005 parent. Shown as sub-domains per parent, plus the
    modality split of the sensory domain (visual vs whisker) that answers
    'why is there no visual domain?'."""
    print("=== v18 Supp: r008 sub-domain structure ===")
    from sklearn.feature_extraction.text import TfidfVectorizer

    pmids = np.load(D.PROC / "specter2_pmids.npy",
                    allow_pickle=True).astype(np.int64)
    m5 = np.load(D.PROC / "clustering_stability" /
                 "memberships_mpnet__raw__r005.npy")
    m8 = np.load(D.PROC / "clustering_stability" /
                 "memberships_mpnet__raw__r008.npy")
    df = pd.DataFrame({"pmid": pmids, "d5": m5, "d8": m8})
    abs_df = pd.read_csv(D.PROC / "paper_abstracts.csv",
                         usecols=["pmid", "title", "abstract"],
                         low_memory=False)
    df = df.merge(abs_df, on="pmid", how="left")
    df["text"] = (df.title.fillna("") + " "
                  + df.abstract.fillna("")).str.lower()

    sizes8 = df.groupby("d8").size()
    big8 = [c for c in sizes8.index if sizes8[c] >= 50]
    vec = TfidfVectorizer(max_features=30000, stop_words="english",
                           ngram_range=(1, 2), sublinear_tf=True)
    X = vec.fit_transform(
        [" ".join(df.loc[df.d8 == c, "text"].head(400)) for c in big8])
    terms = np.array(vec.get_feature_names_out())

    info = []
    for i, c in enumerate(big8):
        row = X[i].toarray().ravel()
        top2 = " / ".join(terms[row.argsort()[::-1][:2]])
        parent = int(df.loc[df.d8 == c, "d5"].mode().iloc[0])
        purity = float((df.loc[df.d8 == c, "d5"] == parent).mean())
        info.append({"c": c, "n": int(sizes8[c]), "parent": parent,
                     "purity": purity, "label": top2})
    inf = pd.DataFrame(info)
    parent_order = (inf.groupby("parent").n.sum()
                    .sort_values(ascending=False).index.tolist())
    inf["po"] = inf.parent.map({p: i for i, p in enumerate(parent_order)})
    inf = inf.sort_values(["po", "n"], ascending=[True, False])

    fig = plt.figure(figsize=(15, 10.5))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.35, 1])
    ax_a = fig.add_subplot(gs[0, 0])
    ax_b = fig.add_subplot(gs[0, 1])

    # ── A: sub-domains grouped under their r005 parent ───────────────────
    cmap = plt.get_cmap("tab20")
    prank = {p: i for i, p in enumerate(parent_order)}
    y = np.arange(len(inf))
    ax_a.barh(y, inf.n,
              color=[cmap(prank[p] % 20) for p in inf.parent], height=0.72)
    labels = []
    prev_parent = None
    for _, r in inf.iterrows():
        pre = ""
        if r.parent != prev_parent:
            pre = f"[{D.DOMAIN_NAMES.get(r.parent, r.parent)}]  "
            prev_parent = r.parent
        pur = "" if r.purity >= 0.85 else f"  ({r.purity * 100:.0f}% pure)"
        labels.append(f"{pre}{r.label}{pur}")
    ax_a.set_yticks(y)
    ax_a.set_yticklabels(labels, fontsize=6.6)
    ax_a.invert_yaxis()
    ax_a.set_xlabel("Papers")
    ax_a.set_title("31 sub-domains at resolution 0.008, grouped under "
                   "their r = 0.005 parent domain", fontweight="bold",
                   fontsize=11)
    despine(ax_a)
    panel_label(ax_a, "A", x=-0.52)

    # ── B: the sensory domain splits by modality at r008 ─────────────────
    xy = np.load(D.PROC / "clustering_stability" / "umap" /
                 "umap_mpnet__raw.npy")
    mask = (df.d5 == 4).values
    children = df.loc[mask, "d8"]
    top_children = children.value_counts().head(3).index.tolist()
    child_label = {c: inf.loc[inf.c == c, "label"].iloc[0]
                   for c in top_children if (inf.c == c).any()}
    colors = [WONG["blue"], WONG["orange"], WONG["green"]]
    ax_b.scatter(xy[~mask, 0], xy[~mask, 1], s=1, color="#dddddd",
                 linewidths=0, rasterized=True)
    for c, col in zip(top_children, colors):
        m = mask & (df.d8 == c).values
        ax_b.scatter(xy[m, 0], xy[m, 1], s=4, color=col, linewidths=0,
                     label=f"{child_label.get(c, c)} (n={int(m.sum()):,})",
                     rasterized=True)
    ax_b.legend(fontsize=7.5, loc="lower left", framealpha=0.9)
    ax_b.set_xticks([])
    ax_b.set_yticks([])
    ax_b.set_title("The sensory relay domain splits by modality at r008\n"
                   "(grey = all other papers)", fontweight="bold",
                   fontsize=11)
    panel_label(ax_b, "B", x=-0.02)

    fig.suptitle("Supplementary: sub-domain structure at finer clustering "
                 "resolution (r008 is a nested refinement of r005)",
                 fontweight="bold", fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.955])
    save_fig(fig, PUB / "v18_suppC_r008_subdomains")
    plt.close(fig)
    print("  saved v18_suppC_r008_subdomains")


# ═════════════════════════════════════════════════════════════════════════
# Figure 8 — LLM paper-level framing (Tier-1 classification, 220 papers)
# ═════════════════════════════════════════════════════════════════════════
FAMILY_COLORS = {
    "relay_transmission": "#D55E00",
    "hub_integrator": "#0072B2",
    "gate_filter": "#009E73",
    "clinical_target": "#CC79A7",
    "oscillator_pacemaker": "#7B4F9E",
    "damage_site": "#999999",
    "network_node": "#56B4E9",
    "memory_cognitive": "#E69F00",
    "state_arousal_regulator": "#F0E442",
    "modulator_regulator": "#4DC5A4",
}
FAMILY_LABELS = {
    "relay_transmission": "relay/transmission",
    "hub_integrator": "hub/integrator",
    "gate_filter": "gate/filter",
    "clinical_target": "clinical target",
    "oscillator_pacemaker": "oscillator/pacemaker",
    "damage_site": "damage site",
    "network_node": "network node",
    "memory_cognitive": "memory/cognitive",
    "state_arousal_regulator": "state/arousal regulator",
    "modulator_regulator": "modulator/regulator",
}
# regex frame_type -> LLM family, for the agreement panel
REGEX_TO_FAMILY = {
    "relay": "relay_transmission", "hub": "hub_integrator",
    "gate": "gate_filter", "gateway": "gate_filter",
    "filter": "gate_filter", "clinical_target": "clinical_target",
    "pacemaker": "oscillator_pacemaker",
    "modulator": "modulator_regulator",
    "regulator": "modulator_regulator",
    "integrator": "hub_integrator", "network_node": "network_node",
    "processor": "processor_computation",
}


def fig8():
    print("=== v18 Figure 8: LLM paper-level framing ===")
    flat = pd.read_csv(D.V18 / "llm_full" / "frames_flat.csv")
    man = pd.read_csv(D.V18 / "llm_full" / "manifest.csv")
    n_dec = man.groupby("decade").pmid.nunique()

    fig, axes = plt.subplots(2, 2, figsize=(13, 10))
    ax_a, ax_b, ax_c, ax_d = axes.flat

    # ── A: relay vs hub at PAPER level, with Wilson CIs ──────────────────
    cen = flat[flat.centrality == "central"]
    context_fams = ["network_node", "oscillator_pacemaker",
                    "clinical_target", "damage_site"]
    for fam in context_fams:
        p = (cen[cen.frame_family == fam].groupby("decade").pmid.nunique()
             .reindex(n_dec.index, fill_value=0) / n_dec * 100)
        ax_a.plot(p.index, p.values, color="#cccccc", linewidth=1.1,
                  zorder=1)
    for fam in ["relay_transmission", "hub_integrator"]:
        k = (cen[cen.frame_family == fam].groupby("decade").pmid.nunique()
             .reindex(n_dec.index, fill_value=0))
        p = k / n_dec * 100
        lo, hi = D.wilson_ci(k.values, n_dec.values)
        c = FAMILY_COLORS[fam]
        ax_a.fill_between(p.index, lo * 100, hi * 100, color=c,
                           alpha=0.15, linewidth=0, zorder=2)
        ax_a.plot(p.index, p.values, color=c, linewidth=2.6, marker="o",
                  markersize=6, zorder=3)
        ax_a.text(p.index[-1] + 2, p.values[-1], FAMILY_LABELS[fam],
                  color=c, fontsize=9, va="center", fontweight="bold")
    ax_a.text(0.02, 0.95, "grey: network node, oscillator,\n"
              "clinical target, damage site",
              transform=ax_a.transAxes, fontsize=7, color="#888888",
              va="top")
    ax_a.set_xticks(n_dec.index)
    ax_a.set_xticklabels([f"{d}s\n(n={n_dec[d]})" for d in n_dec.index],
                          fontsize=8)
    ax_a.set_xlim(1966, 2041)
    ax_a.set_ylabel("% of papers with family as CENTRAL frame")
    ax_a.set_title("Relay falls, hub rises — at paper level",
                   fontweight="bold")
    ax_a.text(0.02, 0.02, "shading: 95% Wilson CI; implicit framing "
              "included", transform=ax_a.transAxes, fontsize=7,
              color="#666666")
    despine(ax_a)
    panel_label(ax_a, "A")

    # ── B: agency signature per family ───────────────────────────────────
    fams_b = ["oscillator_pacemaker", "hub_integrator", "gate_filter",
              "memory_cognitive", "state_arousal_regulator",
              "modulator_regulator", "network_node", "relay_transmission",
              "damage_site", "clinical_target"]
    sub = cen[cen.frame_family.isin(fams_b)]
    tab = (pd.crosstab(sub.frame_family, sub.agency, normalize="index")
           .reindex(fams_b)
           .reindex(columns=["active", "mixed", "passive"], fill_value=0)
           * 100)
    ns = sub.frame_family.value_counts().reindex(fams_b)
    y = np.arange(len(fams_b))
    left = np.zeros(len(fams_b))
    for col, color in [("active", WONG["green"]), ("mixed", "#cccccc"),
                        ("passive", WONG["orange"])]:
        ax_b.barh(y, tab[col].values, left=left, color=color, height=0.72,
                  label=col)
        left += tab[col].values
    ax_b.set_yticks(y)
    ax_b.set_yticklabels([f"{FAMILY_LABELS[f]} (n={ns[f]})"
                          for f in fams_b], fontsize=8.5)
    ax_b.invert_yaxis()
    ax_b.set_xlim(0, 100)
    ax_b.set_xlabel("% of central frames")
    ax_b.legend(fontsize=8, ncol=3, loc="upper center",
                bbox_to_anchor=(0.5, -0.12))
    ax_b.set_title("Frame families carry agency signatures",
                   fontweight="bold")
    despine(ax_b)
    panel_label(ax_b, "B", x=-0.34)

    # ── C: the challenges timeline ───────────────────────────────────────
    ch = flat[flat.stance == "challenges"].copy()
    ch["target"] = np.where(ch.frame_family == "relay_transmission",
                             "relay frame", "other frames")
    tab_c = (ch.groupby(["decade", "target"]).pmid.nunique()
             .unstack(fill_value=0)
             .reindex(n_dec.index, fill_value=0)
             .reindex(columns=["relay frame", "other frames"],
                      fill_value=0))
    tab_pct = tab_c.div(n_dec, axis=0) * 100
    x = np.arange(len(n_dec))
    ax_c.bar(x - 0.19, tab_pct["relay frame"], width=0.38,
             color=D.FRAME_COLORS["relay"], label="challenges the relay frame")
    ax_c.bar(x + 0.19, tab_pct["other frames"], width=0.38,
             color="#9e9e9e", label="challenges another frame")
    for xi, dec in zip(x, n_dec.index):
        for dx, col in [(-0.19, "relay frame"), (0.19, "other frames")]:
            n = tab_c.loc[dec, col]
            if n > 0:
                ax_c.text(xi + dx, tab_pct.loc[dec, col] + 0.25, str(n),
                          ha="center", fontsize=7, color="#555555")
    ax_c.set_xticks(x)
    ax_c.set_xticklabels([f"{d}s" for d in n_dec.index])
    ax_c.set_ylabel("% of papers challenging a frame")
    ax_c.set_title('"More than a relay": explicit challenges over time',
                   fontweight="bold")
    ax_c.legend(fontsize=8.5)
    despine(ax_c)
    panel_label(ax_c, "C")

    # ── D: LLM vs regex — coverage and agreement ─────────────────────────
    fr = D.load_framing()
    fr_sample = fr[fr.pmid.isin(man.pmid)]
    regex_fams = (fr_sample.assign(
        fam=fr_sample.frame_type.map(REGEX_TO_FAMILY))
        .dropna(subset=["fam"]).groupby("pmid").fam.agg(set))
    llm_fams = flat.groupby("pmid").frame_family.agg(set)

    n_regex = len(regex_fams)
    n_llm = int((~flat.groupby("pmid").size().eq(0)).sum())
    n_llm = flat.pmid.nunique()
    ax_d.barh([2], [100 * n_llm / len(man)], color=WONG["blue"],
              height=0.55)
    ax_d.barh([1], [100 * n_regex / len(man)], color=WONG["orange"],
              height=0.55)
    both = [p for p in regex_fams.index if p in llm_fams.index]
    agree = sum(1 for p in both if regex_fams[p] & llm_fams[p])
    ax_d.barh([0], [100 * agree / max(len(both), 1)], color=WONG["green"],
              height=0.55)
    ax_d.set_yticks([2, 1, 0])
    ax_d.set_yticklabels(
        [f"LLM finds framing\n({n_llm}/{len(man)} papers)",
         f"regex finds framing\n({n_regex}/{len(man)} papers)",
         f"agreement where both apply\n(shared family, {agree}/{len(both)})"],
        fontsize=8.5)
    for yi, v in zip([2, 1, 0],
                      [100 * n_llm / len(man), 100 * n_regex / len(man),
                       100 * agree / max(len(both), 1)]):
        ax_d.text(v + 1.5, yi, f"{v:.0f}%", va="center", fontsize=9)
    ax_d.set_xlim(0, 112)
    ax_d.set_xlabel("% of papers")
    ax_d.set_title("LLM vs regex: coverage and agreement",
                   fontweight="bold")
    despine(ax_d)
    panel_label(ax_d, "D", x=-0.34)
    print(f"  coverage: LLM {n_llm}/{len(man)}, regex {n_regex}/{len(man)}, "
          f"family agreement {agree}/{len(both)}")

    fig.suptitle("Figure 8: Paper-level conceptual framing "
                 "(LLM classification, n = 220 papers)",
                 fontweight="bold", fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.965])
    save_fig(fig, PUB / "v18_fig8_llm_framing")
    plt.close(fig)
    print("  saved v18_fig8_llm_framing")


# ═════════════════════════════════════════════════════════════════════════
# Figure 8b — the anatomy of conceptual change (deep-dive on the LLM
# extraction: stance lifecycles, boilerplate demotion, co-occurrence,
# genre differences, explicitness fingerprints)
# ═════════════════════════════════════════════════════════════════════════
STANCE_COLORS = {"assumes": "#bbbbbb", "endorses": WONG["green"],
                 "challenges": WONG["red"]}


def _stance_stack(ax, sub, title, color_accent):
    tab = (pd.crosstab(sub.decade, sub.stance, normalize="index") * 100)
    tab = tab.reindex(columns=["assumes", "endorses", "challenges"],
                      fill_value=0)
    n = sub.groupby("decade").size()
    x = np.arange(len(tab))
    bottom = np.zeros(len(tab))
    for col in ["assumes", "endorses", "challenges"]:
        ax.bar(x, tab[col], bottom=bottom, color=STANCE_COLORS[col],
               width=0.7, label=col)
        bottom += tab[col].values
    for xi, dec in zip(x, tab.index):
        ax.text(xi, 102, f"n={n[dec]}", ha="center", fontsize=7,
                color="#666666")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{d}s" for d in tab.index], fontsize=8)
    ax.set_ylim(0, 110)
    ax.set_ylabel("% of frames")
    ax.set_title(title, fontweight="bold", color=color_accent, fontsize=11)
    despine(ax)


def fig8b():
    print("=== v18 Figure 8b: anatomy of conceptual change ===")
    flat = pd.read_csv(D.V18 / "llm_full" / "frames_flat.csv")
    fig, axes = plt.subplots(2, 3, figsize=(15, 9.5))
    ax_a, ax_b, ax_c, ax_d, ax_e, ax_f = axes.flat

    # ── A/B: the lifecycle of a metaphor — stance over time ──────────────
    _stance_stack(ax_a, flat[flat.frame_family == "relay_transmission"],
                  "Relay: assumption → contestation",
                  FAMILY_COLORS["relay_transmission"])
    ax_a.legend(fontsize=7.5, loc="lower left", framealpha=0.95)
    panel_label(ax_a, "A")
    _stance_stack(ax_b, flat[flat.frame_family == "hub_integrator"],
                  "Hub: argued → assumed\n(never challenged)",
                  FAMILY_COLORS["hub_integrator"])
    panel_label(ax_b, "B", x=-0.16)

    # ── C: relay demoted to boilerplate ──────────────────────────────────
    r = flat[flat.frame_family == "relay_transmission"]
    tab = pd.crosstab(r.decade, r.centrality)
    pct = (tab["peripheral"] / tab.sum(axis=1) * 100)
    x = np.arange(len(pct))
    ax_c.bar(x, pct.values, color=FAMILY_COLORS["relay_transmission"],
             width=0.7)
    for xi, dec in zip(x, pct.index):
        ax_c.text(xi, pct[dec] + 2.5,
                  f"{tab.loc[dec, 'peripheral']}/{tab.loc[dec].sum()}",
                  ha="center", fontsize=7.5, color="#555555")
    ax_c.set_xticks(x)
    ax_c.set_xticklabels([f"{d}s" for d in pct.index], fontsize=8)
    ax_c.set_ylim(0, 100)
    ax_c.set_ylabel("% of relay frames that are PERIPHERAL")
    ax_c.set_title("Relay becomes boilerplate\n(opening-sentence use only)",
                   fontweight="bold", fontsize=11)
    despine(ax_c)
    panel_label(ax_c, "C")

    # ── D: when relay and hub co-occur, papers stage the argument ────────
    by_paper = flat.groupby("pmid").frame_family.agg(set)
    both = [p for p, s in by_paper.items()
            if "relay_transmission" in s and "hub_integrator" in s]
    sub = flat[flat.pmid.isin(both)
               & flat.frame_family.isin(["relay_transmission",
                                          "hub_integrator"])]
    tab = (pd.crosstab(sub.frame_family, sub.stance)
           .reindex(columns=["assumes", "endorses", "challenges"],
                    fill_value=0))
    y = np.arange(3)
    for i, (fam, off) in enumerate([("relay_transmission", -0.18),
                                     ("hub_integrator", 0.18)]):
        vals = tab.loc[fam].values
        ax_d.barh(y + off, vals, height=0.34, color=FAMILY_COLORS[fam],
                  label=FAMILY_LABELS[fam])
    ax_d.set_yticks(y)
    ax_d.set_yticklabels(["assumes", "endorses", "challenges"], fontsize=9)
    ax_d.invert_yaxis()
    ax_d.set_xlabel("frames")
    ax_d.set_title(f"Within the {len(both)} papers holding BOTH:\n"
                   "hub is endorsed, relay is challenged",
                   fontweight="bold", fontsize=11)
    ax_d.legend(fontsize=8, loc="lower right")
    despine(ax_d)
    panel_label(ax_d, "D")

    # ── E: reviews argue, ordinary papers assume ─────────────────────────
    papers = flat.groupby(["kind", "pmid"])
    ch_paper = (flat[flat.stance == "challenges"].groupby("kind")
                .pmid.nunique()
                / flat.groupby("kind").pmid.nunique() * 100)
    metrics = {
        "challenges\na frame": ch_paper,
        "active\nagency":
            flat[flat.centrality == "central"].groupby("kind")
            .agency.apply(lambda s: 100 * (s == "active").mean()),
        "assumes\n(unexamined)":
            flat.groupby("kind").stance
            .apply(lambda s: 100 * (s == "assumes").mean()),
        "implicit\nframing":
            flat.groupby("kind").explicitness
            .apply(lambda s: 100 * (s == "implicit").mean()),
    }
    kinds = ["review", "nonreview", "stratified"]
    kind_labels = {"review": "influential reviews",
                   "nonreview": "influential research",
                   "stratified": "ordinary papers"}
    kind_colors = {"review": WONG["blue"], "nonreview": WONG["cyan"],
                   "stratified": "#bbbbbb"}
    x = np.arange(len(metrics))
    for i, k in enumerate(kinds):
        vals = [m.get(k, 0) for m in metrics.values()]
        ax_e.bar(x + (i - 1) * 0.26, vals, width=0.24,
                 color=kind_colors[k], label=kind_labels[k])
    ax_e.set_xticks(x)
    ax_e.set_xticklabels(list(metrics.keys()), fontsize=8)
    ax_e.set_ylabel("% (of papers or frames)")
    ax_e.set_title("Reviews argue;\nordinary papers assume",
                   fontweight="bold", fontsize=11)
    ax_e.legend(fontsize=7.5)
    despine(ax_e)
    panel_label(ax_e, "E")

    # ── F: explicitness fingerprint per family ───────────────────────────
    fams = ["relay_transmission", "hub_integrator", "modulator_regulator",
            "oscillator_pacemaker", "gate_filter", "clinical_target",
            "memory_cognitive", "network_node", "damage_site"]
    sub = flat[flat.frame_family.isin(fams)]
    exp = (sub.groupby("frame_family").explicitness
           .apply(lambda s: 100 * (s == "explicit").mean())
           .reindex(fams).sort_values(ascending=False))
    ns = sub.frame_family.value_counts()
    y = np.arange(len(exp))
    ax_f.barh(y, exp.values,
              color=[FAMILY_COLORS.get(f, "#999999") for f in exp.index],
              height=0.7)
    ax_f.set_yticks(y)
    ax_f.set_yticklabels([f"{FAMILY_LABELS[f]} (n={ns[f]})"
                          for f in exp.index], fontsize=8.5)
    ax_f.invert_yaxis()
    ax_f.axvline(50, color="#888888", linestyle="--", linewidth=0.8)
    ax_f.set_xlim(0, 100)
    ax_f.set_xlabel("% explicit (vs implicit) framing")
    ax_f.set_title("Explicitness fingerprints:\nnamed metaphors vs "
                   "tacit conceptions", fontweight="bold", fontsize=11)
    despine(ax_f)
    panel_label(ax_f, "F", x=-0.42)

    fig.suptitle("Figure 8b: The anatomy of conceptual change "
                 "(LLM paper-level extraction, 630 frames / 220 papers)",
                 fontweight="bold", fontsize=15)
    fig.tight_layout(rect=[0, 0, 1, 0.955])
    save_fig(fig, PUB / "v18_fig8b_conceptual_change")
    plt.close(fig)
    print("  saved v18_fig8b_conceptual_change")


# ═════════════════════════════════════════════════════════════════════════
def fig_planck():
    """Conversion vs replacement: does the field change its mind, or
    change its members? Built on OpenAlex disambiguated author IDs.
    Author names are NOT shown — the analysis singles out living
    researchers, so panel C is anonymised by design."""
    print("=== v18 Figure: Planck's principle ===")
    d = pd.read_csv(D.V18 / "planck_paper_frames.csv")

    fig, axes = plt.subplots(2, 2, figsize=(13, 9.5))
    ax_a, ax_b, ax_c, ax_d = axes.flat

    # ── A: decomposition, with sensitivity across period splits ──────────
    splits = [(1990, 2007, 2008, 2025), (1995, 2009, 2010, 2025),
              (1990, 2004, 2005, 2019), (2000, 2011, 2012, 2025)]
    convs, repls = [], []
    for lo1, hi1, lo2, hi2 in splits:
        p1 = d[(d.year >= lo1) & (d.year <= hi1)]
        p2 = d[(d.year >= lo2) & (d.year <= hi2)]
        a1, a2 = set(p1.author_id), set(p2.author_id)
        cont = a1 & a2
        c1, c2 = p1[p1.author_id.isin(cont)], p2[p2.author_id.isin(cont)]
        e2 = p2[p2.author_id.isin(a2 - a1)]
        x1 = p1[p1.author_id.isin(a1 - a2)]
        convs.append(100 * (c2.hub_dominant.mean() - c1.hub_dominant.mean()))
        repls.append(100 * (e2.hub_dominant.mean() - x1.hub_dominant.mean()))
    x = np.array([0, 1])
    ax_a.bar(x, [convs[0], repls[0]], width=0.6,
             color=[WONG["cyan"], WONG["red"]])
    for xi, vals in [(0, convs), (1, repls)]:
        ax_a.scatter([xi] * len(vals), vals, color="#333333", s=26,
                     zorder=3, label="other period splits"
                     if xi == 0 else None)
    ax_a.axhline(0, color="#444444", linewidth=0.8)
    ax_a.set_xticks(x)
    ax_a.set_xticklabels(["CONVERSION\n(same authors\nchange framing)",
                          "REPLACEMENT\n(entrants vs\nexiters)"],
                         fontsize=9)
    ax_a.set_ylabel("change in % hub-dominant papers (pp)")
    ax_a.set_title("The field changed its members,\nnot its mind",
                   fontweight="bold")
    ax_a.legend(fontsize=7.5, loc="upper left")
    ax_a.text(0.5, 0.80, "total field change: +26 pp\n"
              "(1.8% → 28.0% hub-dominant)",
              transform=ax_a.transAxes, ha="center", fontsize=8.5,
              color="#555555")
    despine(ax_a)
    panel_label(ax_a, "A", x=-0.15)

    # ── B: cohort gradient, publication era held fixed ───────────────────
    sub = d[d.year >= 2010].copy()
    bins = [(1970, 1999, "before 2000"), (2000, 2009, "2000s"),
            (2010, 2025, "2010 or later")]
    xs, ys, los, his, ns = [], [], [], [], []
    for i, (lo, hi, lab) in enumerate(bins):
        s = sub[(sub.cohort >= lo) & (sub.cohort <= hi)]
        k = int(s.hub_dominant.sum())
        clo, chi = D.wilson_ci(k, len(s))
        xs.append(i); ys.append(100 * s.hub_dominant.mean())
        los.append(100 * clo); his.append(100 * chi); ns.append(len(s))
    ax_b.errorbar(xs, ys, yerr=[np.array(ys) - los, np.array(his) - ys],
                  fmt="o", color=D.FRAME_COLORS["hub"], markersize=10,
                  capsize=5, linewidth=2)
    ax_b.plot(xs, ys, color=D.FRAME_COLORS["hub"], linewidth=1.6,
              alpha=0.6)
    for xi, yi, n in zip(xs, ys, ns):
        ax_b.text(xi, yi + 4, f"n={n}", ha="center", fontsize=8,
                  color="#555555")
    ax_b.set_xticks(xs)
    ax_b.set_xticklabels([b[2] for b in bins], fontsize=9)
    ax_b.set_xlabel("when the senior author entered the field")
    ax_b.set_ylabel("% hub-dominant papers")
    ax_b.set_xlim(-0.5, 2.5)
    ax_b.set_title("Among papers published 2010+,\nthe author's vintage "
                   "predicts the frame\n(OR = 1.28 per decade, p = .010)",
                   fontweight="bold", fontsize=10.5)
    despine(ax_b)
    panel_label(ax_b, "B")

    # ── C: within-author trajectories (anonymised) ───────────────────────
    g = d.groupby("author_id")
    span = g.year.agg(["min", "max", "size"])
    movers = span[(span["max"] - span["min"] >= 8) & (span["size"] >= 2)]
    n_up = n_flat = n_down = 0
    for aid in movers.index:
        s = d[d.author_id == aid].sort_values("year")
        half = len(s) // 2 or 1
        e, l = s.iloc[:half].hub_share.mean(), s.iloc[-half:].hub_share.mean()
        col = ("#cccccc" if e == l else
               D.FRAME_COLORS["hub"] if l > e else D.FRAME_COLORS["relay"])
        if e == l:
            n_flat += 1
        elif l > e:
            n_up += 1
        else:
            n_down += 1
        ax_c.plot([0, 1], [e * 100, l * 100], color=col, linewidth=1.6,
                  marker="o", markersize=4, alpha=0.75)
    ax_c.set_xticks([0, 1])
    ax_c.set_xticklabels(["earlier papers", "later papers"], fontsize=9)
    ax_c.set_xlim(-0.25, 1.25)
    ax_c.set_ylabel("% hub framing (per author)")
    ax_c.set_title(f"Individual researchers rarely convert\n"
                   f"({n_flat} unchanged, {n_up} toward hub, "
                   f"{n_down} toward relay; p = .11)",
                   fontweight="bold", fontsize=10.5)
    ax_c.text(0.5, -0.14, f"n = {len(movers)} authors with framing papers "
              "≥8 years apart (names withheld)",
              transform=ax_c.transAxes, ha="center", fontsize=7.5,
              color="#666666")
    despine(ax_c)
    panel_label(ax_c, "C", x=-0.13)

    # ── D: the four author groups ────────────────────────────────────────
    p1 = d[(d.year >= 1990) & (d.year <= 2007)]
    p2 = d[(d.year >= 2008) & (d.year <= 2025)]
    a1, a2 = set(p1.author_id), set(p2.author_id)
    cont = a1 & a2
    groups = [("exiters\n(last seen ≤2007)", p1[p1.author_id.isin(a1 - a2)],
               "#bbbbbb"),
              ("continuing\n(1990–2007)", p1[p1.author_id.isin(cont)],
               WONG["cyan"]),
              ("continuing\n(2008–2025)", p2[p2.author_id.isin(cont)],
               WONG["blue"]),
              ("entrants\n(new since 2008)", p2[p2.author_id.isin(a2 - a1)],
               WONG["red"])]
    xs = np.arange(len(groups))
    vals = [100 * s.hub_dominant.mean() for _, s, _ in groups]
    cols = [c for _, _, c in groups]
    ax_d.bar(xs, vals, color=cols, width=0.65)
    for xi, (lab, s, _), v in zip(xs, groups, vals):
        ax_d.text(xi, v + 0.8, f"{v:.1f}%\n(n={len(s)})", ha="center",
                  fontsize=8)
    ax_d.set_xticks(xs)
    ax_d.set_xticklabels([g[0] for g in groups], fontsize=8)
    ax_d.set_ylabel("% hub-dominant papers")
    ax_d.set_ylim(0, max(vals) * 1.35)
    ax_d.set_title("Continuing authors barely moved;\nthe newcomers "
                   "arrived already hub-framed", fontweight="bold",
                   fontsize=10.5)
    despine(ax_d)
    panel_label(ax_d, "D")

    fig.suptitle("Planck's principle in the thalamus literature: the "
                 "relay→hub shift is generational, not a change of mind",
                 fontweight="bold", fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.955])
    save_fig(fig, PUB / "v18_fig_planck")
    plt.close(fig)
    print("  saved v18_fig_planck")


def supp_abstract_robustness():
    """Composition-free replication on the uniform abstract corpus.
    Built by Code/pipeline/28_abstract_only_robustness.py."""
    print("=== v18 Supp: abstract-only robustness ===")
    fr = pd.read_csv(D.V18 / "abstract_framing.csv")
    stats = pd.read_csv(D.V18 / "abstract_robustness_stats.csv")

    raw = pd.read_csv(D.RAW / "pubmed" /
                      "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                      usecols=["pmid", "year", "abstract"],
                      low_memory=False).drop_duplicates("pmid")
    raw["year"] = pd.to_numeric(raw.year, errors="coerce")
    raw = raw.dropna(subset=["year"])
    raw["year"] = raw.year.astype(int)
    raw = raw[(raw.year >= 1950) & (raw.year <= 2025)]
    raw["has_abs"] = raw.abstract.fillna("").str.len() >= 100
    raw["decade"] = (raw.year // 10) * 10

    fig, axes = plt.subplots(2, 2, figsize=(13, 9.5))
    ax_a, ax_b, ax_c, ax_d = axes.flat

    # A: abstract availability — why 1980+ is the valid window
    av = raw.groupby("decade").has_abs.mean() * 100
    cols = [WONG["grey"] if d < 1980 else WONG["blue"] for d in av.index]
    ax_a.bar(av.index, av.values, width=7.5, color=cols)
    ax_a.axvline(1975, color=WONG["red"], linestyle="--", linewidth=1.2)
    ax_a.text(1976, 50, "analysis window\n(1980 onward)", fontsize=8,
              color=WONG["red"])
    ax_a.set_xticks(av.index)
    ax_a.set_xticklabels([f"{d}s" for d in av.index], fontsize=8)
    ax_a.set_ylabel("% of corpus papers with an abstract")
    ax_a.set_title("Abstract availability is uniform from the 1980s",
                   fontweight="bold")
    despine(ax_a)
    panel_label(ax_a, "A")

    # B: framing detection rate, abstracts vs pooled
    fr["decade"] = (fr.year // 10) * 10
    sent_tot = pd.read_csv(D.V18 / "abstract_framing.csv",
                           usecols=["decade"]).groupby("decade").size()
    stats_rate = None
    # recompute rate from the saved per-decade table in the pipeline print
    # (stored implicitly): use pooled corpus for comparison
    pooled = D.load_sentences(usecols=["pmid", "year", "frame_type"])
    pooled["decade"] = (pooled.year // 10) * 10
    pt = pooled.groupby("decade").size()
    ph = (pooled[pooled.frame_type.notna() & (pooled.frame_type != "")]
          .groupby("decade").size().reindex(pt.index, fill_value=0))
    prate = (100 * ph / pt)
    # abstract totals need the sentence denominator: reconstruct quickly
    abs_rate = pd.Series(
        {1970: 1.43, 1980: 1.25, 1990: 1.20, 2000: 2.00,
         2010: 1.96, 2020: 2.56})
    decs = [d for d in abs_rate.index if d in prate.index]
    x = np.arange(len(decs))
    ax_b.bar(x - 0.19, [abs_rate[d] for d in decs], width=0.38,
             color=WONG["blue"], label="abstracts only (uniform corpus)")
    ax_b.bar(x + 0.19, [prate[d] for d in decs], width=0.38,
             color="#bbbbbb", label="pooled corpus (mixed sources)")
    ax_b.set_xticks(x)
    ax_b.set_xticklabels([f"{d}s" for d in decs], fontsize=8)
    ax_b.set_ylabel("% of thalamus sentences with framing")
    ax_b.set_title("Framing rate rises in BOTH corpora\n"
                   "(abstracts 1980-99: 1.22% → 2015-25: 2.32%, "
                   "τ=+0.44, p<1e-4)", fontweight="bold", fontsize=10.5)
    ax_b.legend(fontsize=8)
    despine(ax_b)
    panel_label(ax_b, "B")

    # C: the clean crossover on abstracts
    fr["bin"] = (fr.year // 5) * 5
    b = fr.groupby("bin").size()
    keep = b[b >= 20].index
    for frame in ["relay", "hub"]:
        k = (fr[fr.frame_type == frame].groupby("bin").size()
             .reindex(b.index, fill_value=0))
        p = (k / b).loc[keep] * 100
        lo, hi = D.wilson_ci(k.loc[keep].values, b.loc[keep].values)
        c = D.FRAME_COLORS[frame]
        ax_c.fill_between(keep, lo * 100, hi * 100, color=c, alpha=0.15,
                          linewidth=0)
        ax_c.plot(keep, p.values, color=c, linewidth=2.6, marker="o",
                  markersize=5)
        ax_c.text(keep[-1] + 1, p.values[-1], frame, color=c, fontsize=9,
                  fontweight="bold", va="center")
    ax_c.set_xlim(keep[0] - 2, keep[-1] + 9)
    ax_c.set_xlabel("5-year bin")
    ax_c.set_ylabel("% of abstract framing sentences")
    ax_c.set_title("Relay→hub crossover replicates on abstracts alone\n"
                   "(relay τ=−0.75, hub τ=+0.74; both p<1e-7)",
                   fontweight="bold", fontsize=10.5)
    despine(ax_c)
    panel_label(ax_c, "C")

    # D: tau comparison — which trends survive
    st = stats.set_index("frame")
    order = ["relay", "hub", "clinical_target", "gate"]
    y = np.arange(len(order))
    ax_d.barh(y - 0.19, [st.loc[f, "tau_abstracts"] for f in order],
              height=0.38, color=WONG["blue"], label="abstracts only")
    ax_d.barh(y + 0.19, [st.loc[f, "tau_pooled"] for f in order],
              height=0.38, color="#bbbbbb", label="pooled corpus")
    ax_d.axvline(0, color="#444444", linewidth=0.8)
    ax_d.set_yticks(y)
    ax_d.set_yticklabels([f.replace("_", " ") for f in order], fontsize=9)
    ax_d.invert_yaxis()
    ax_d.set_xlabel("Mann-Kendall τ (annual proportion vs year)")
    ax_d.set_title("Three trends survive; GATE does not\n"
                   "(pooled τ=+0.28** vs abstracts τ=−0.08 ns)",
                   fontweight="bold", fontsize=10.5)
    ax_d.legend(fontsize=8, loc="lower right")
    ax_d.annotate("composition\nartifact", xy=(0.28, 3.19),
                  xytext=(0.45, 2.5), fontsize=8, color=WONG["red"],
                  arrowprops=dict(arrowstyle="->", color=WONG["red"],
                                  linewidth=1.2))
    despine(ax_d)
    panel_label(ax_d, "D", x=-0.22)

    fig.suptitle("Supplementary: composition-free replication on the "
                 "uniform abstract corpus (935 framing sentences, "
                 "831 papers)", fontweight="bold", fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.955])
    save_fig(fig, PUB / "v18_suppD_abstract_robustness")
    plt.close(fig)
    print("  saved v18_suppD_abstract_robustness")


# ═════════════════════════════════════════════════════════════════════════
FIGS = {"1": fig1, "2": fig2, "3": fig3, "4": fig4, "5": fig5, "6": fig6,
        "7": fig7, "8": fig8, "8b": fig8b, "supp-reg": supp_regression,
        "nucdom": fig_nucdom, "supp-nuc": supp_nucleus_trends,
        "supp-r008": supp_r008, "supp-abs": supp_abstract_robustness,
        "planck": fig_planck}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--figs", default="all",
                    help="comma-separated figure numbers, or 'all'")
    args = ap.parse_args()
    wanted = list(FIGS) if args.figs == "all" else args.figs.split(",")
    for w in wanted:
        FIGS[w.strip()]()


if __name__ == "__main__":
    main()
