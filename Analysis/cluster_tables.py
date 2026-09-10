"""
cluster_tables.py — Per-method cluster tables with stability scores.

For each of {SPECTER2, PubMedBERT, MPNet} at each resolution {0.005, 0.008, 0.012, 0.020}:
  - List clusters with: N, mean year, TF-IDF top terms, domain flags, stability

Stability for a cluster in config X = for each paper in the cluster, what fraction of
its cluster-mates in X are also together in OTHER configs, averaged.

Output:
  Data/processed/cluster_tables/{model}_res{N}.csv    (one CSV per config)
  Data/processed/cluster_tables/combined.csv          (all configs in one)
  Paper/cluster_tables.docx                           (nicely formatted)
  Graphs/pub/fig_cluster_table_comparison.png         (visual summary)
"""
import sys, io, os, re
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from collections import Counter

import faiss
import igraph as ig
import leidenalg
from sklearn.feature_extraction.text import TfidfVectorizer

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"
TABLE_DIR = PROC / "cluster_tables"
TABLE_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
CONFIGS = [
    ("SPECTER2", 0.005), ("SPECTER2", 0.008), ("SPECTER2", 0.012), ("SPECTER2", 0.020),
    ("PubMedBERT", 0.005), ("PubMedBERT", 0.008), ("PubMedBERT", 0.012), ("PubMedBERT", 0.020),
    ("MPNet", 0.005), ("MPNet", 0.008), ("MPNet", 0.012), ("MPNet", 0.020),
]
MIN_CLUSTER_SIZE = 50

MODELS = {
    "SPECTER2": PROC / "specter2_embeddings.npy",
    "PubMedBERT": PROC / "pubmedbert_embeddings.npy",
    "MPNet": PROC / "mpnet_embeddings.npy",
}

# Domain keyword detectors
DOMAIN_RE = {
    "VIS":  re.compile(r"\b(lgn|dlgn|lateral geniculate|visual cortex|visual thalam|retin\w+|v1|primary visual|photorecept|retinotop)\b", re.IGNORECASE),
    "AUD":  re.compile(r"\b(mgb|mgv|medial geniculate|auditory thalam|tonotop|cochlea|frequency tuning|inferior colliculus)\b", re.IGNORECASE),
    "SOM":  re.compile(r"\b(whisker|barrel|somatosensor|vpm|vpl|ventrobasal|trigeminal)\b", re.IGNORECASE),
    "PAIN": re.compile(r"\b(nociceptive|morphine|pain|allodynia|neuropathic|hyperalgesia)\b", re.IGNORECASE),
    "CLIN": re.compile(r"\b(schizophrenia|major depressive|bipolar|mdd|ocd|multiple sclerosis|parkinson|alzheimer)\b", re.IGNORECASE),
    "DBS":  re.compile(r"\b(DBS|deep brain stimulation|thalamotomy|mrgfus|vim)\b", re.IGNORECASE),
    "SLP":  re.compile(r"\b(sleep|nrem|rem sleep|slow wave|insomnia|wakefulness|arousal|vigilance)\b", re.IGNORECASE),
    "DEV":  re.compile(r"\b(embryonic|subplate|thalamocortical axon|developmental|preterm|progenitor)\b", re.IGNORECASE),
    "MEM":  re.compile(r"\b(memory|amnesia|fear conditioning|spatial memory|maze|extinction)\b", re.IGNORECASE),
    "EPI":  re.compile(r"\b(epilepsy|seizure|absence|spike.wave|gaers|swd|ictal)\b", re.IGNORECASE),
    "OSC":  re.compile(r"\b(oscillat|burst firing|pacemaker|spindle|rhythm|tonic|t-type|i\bt\b)\b", re.IGNORECASE),
    "TRAC": re.compile(r"\b(hrp|tract trac\w+|anterograde|retrograde|wga|fluorogold|cholera toxin|aav tracer)\b", re.IGNORECASE),
    "STRK": re.compile(r"\b(stroke|infarct|hemorrhage|hematoma|paramedian|aphasia)\b", re.IGNORECASE),
    "CIRC": re.compile(r"\b(corticothalamic|thalamocortical loop|reciprocal|circuit|connectivity|functional connectivity)\b", re.IGNORECASE),
}


# ═══════════════════════════════════════════════════════════════════════════
# LOAD + CLUSTER
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
papers = pd.read_csv(PROC / "paper_abstracts.csv", low_memory=False)
pmids = np.load(PROC / "specter2_pmids.npy")
N = len(papers)

# Precompute domain flags
text_all = papers["title"].fillna("") + " " + papers["abstract"].fillna("")
for domain, pat in DOMAIN_RE.items():
    papers[f"dom_{domain}"] = text_all.str.contains(pat)

dom_cols = [f"dom_{d}" for d in DOMAIN_RE.keys()]


def build_graph(embs, k=50):
    n, d = embs.shape
    index = faiss.IndexFlatIP(d)
    index.add(embs)
    D, I = index.search(embs, k + 1)
    edges, weights = [], []
    for i in range(n):
        for j_idx in range(1, k + 1):
            j = int(I[i, j_idx])
            if j != i:
                edges.append((i, j))
                weights.append(float(D[i, j_idx]))
    g = ig.Graph(n=n, edges=edges, directed=True)
    g.es["weight"] = weights
    return g.as_undirected(combine_edges="max")


print("Clustering all configs...")
memberships = {}
for model_name in set(m for m, _ in CONFIGS):
    embs = np.load(MODELS[model_name]).astype(np.float32)
    norms = np.linalg.norm(embs, axis=1, keepdims=True)
    embs = embs / (norms + 1e-10)
    g = build_graph(embs)
    for m, r in [c for c in CONFIGS if c[0] == model_name]:
        part = leidenalg.find_partition(
            g, leidenalg.CPMVertexPartition,
            resolution_parameter=r, weights="weight", seed=42,
        )
        memberships[(m, r)] = np.array(part.membership)
    print(f"  {model_name}: done")


# ═══════════════════════════════════════════════════════════════════════════
# COMPUTE STABILITY FOR EACH CLUSTER IN EACH CONFIG
# ═══════════════════════════════════════════════════════════════════════════
print("\nComputing stability scores...")


def best_jaccard(members_set, other_mem):
    """Best Jaccard overlap with any cluster in other_mem."""
    if len(members_set) == 0:
        return 0.0
    members_idx = list(members_set)
    other_clusters = Counter(other_mem[members_idx])
    best_j = 0.0
    for c, overlap in other_clusters.items():
        other_members = np.where(other_mem == c)[0]
        union = len(members_set | set(other_members))
        j = overlap / union if union > 0 else 0
        if j > best_j:
            best_j = j
    return best_j


def compute_cluster_table(cfg, all_mem):
    """Build a detailed table for one (model, res) configuration."""
    model_name, res = cfg
    mem = all_mem[cfg]
    sizes = Counter(mem)
    big = sorted([c for c, n in sizes.items() if n >= MIN_CLUSTER_SIZE],
                  key=lambda c: -sizes[c])

    # TF-IDF per cluster
    cluster_texts = []
    for c in big:
        mask = mem == c
        cluster_texts.append(" ".join(papers.loc[mask, "abstract"].fillna("").tolist()))

    vec = TfidfVectorizer(max_features=3000, stop_words="english",
                           ngram_range=(1, 2), min_df=2, max_df=0.5)
    X = vec.fit_transform(cluster_texts)
    feats = np.array(vec.get_feature_names_out())

    # For each cluster: compute stability as mean Jaccard vs OTHER configs
    other_cfgs = [c for c in CONFIGS if c != cfg]

    rows = []
    for i, c in enumerate(big):
        members = np.where(mem == c)[0]
        members_set = set(members.tolist())
        sub = papers.iloc[members]

        top_idx = X[i].toarray()[0].argsort()[-8:][::-1]
        terms = feats[top_idx].tolist()

        # Stability
        jaccards = []
        for oc in other_cfgs:
            jaccards.append(best_jaccard(members_set, all_mem[oc]))
        stab_mean = float(np.mean(jaccards))
        stab_min = float(np.min(jaccards))

        # Per-model stability breakdown
        jacc_spec = np.mean([best_jaccard(members_set, all_mem[(m, r)])
                             for m, r in CONFIGS if m == "SPECTER2" and (m, r) != cfg])
        jacc_pmb = np.mean([best_jaccard(members_set, all_mem[(m, r)])
                            for m, r in CONFIGS if m == "PubMedBERT" and (m, r) != cfg])
        jacc_mpn = np.mean([best_jaccard(members_set, all_mem[(m, r)])
                            for m, r in CONFIGS if m == "MPNet" and (m, r) != cfg])

        # Domain flags — fraction of cluster matching each domain
        dom_fracs = {}
        for domain in DOMAIN_RE.keys():
            dom_fracs[domain] = sub[f"dom_{domain}"].mean()

        # Primary domain (highest fraction, if >= 20%)
        primary = ", ".join(
            f"{d}:{int(f*100)}%" for d, f in sorted(dom_fracs.items(), key=lambda x: -x[1])
            if f >= 0.2
        )[:80]

        rows.append({
            "model": model_name,
            "resolution": res,
            "cluster": int(c),
            "n_papers": int(sizes[c]),
            "year_mean": round(sub["year"].mean(), 1),
            "year_std": round(sub["year"].std(), 1),
            "top_terms": " | ".join(terms[:6]),
            "domains": primary if primary else "—",
            **{f"pct_{d}": round(dom_fracs[d] * 100, 1) for d in DOMAIN_RE.keys()},
            "stability_mean": round(stab_mean, 3),
            "stability_min": round(stab_min, 3),
            "jacc_vs_SPECTER2": round(jacc_spec, 3),
            "jacc_vs_PubMedBERT": round(jacc_pmb, 3),
            "jacc_vs_MPNet": round(jacc_mpn, 3),
        })
    return pd.DataFrame(rows)


all_tables = {}
for cfg in CONFIGS:
    print(f"  {cfg[0]} @ res={cfg[1]}...")
    df = compute_cluster_table(cfg, memberships)
    all_tables[cfg] = df
    fname = f"{cfg[0]}_res{int(cfg[1]*1000):03d}.csv"
    df.to_csv(TABLE_DIR / fname, index=False)

# Combined master table
combined = pd.concat(all_tables.values(), ignore_index=True)
combined.to_csv(TABLE_DIR / "combined.csv", index=False)
print(f"\nSaved {len(all_tables)} per-config tables + combined ({len(combined)} rows)")

# ═══════════════════════════════════════════════════════════════════════════
# PRINT CONDENSED TABLE PER MODEL (for review)
# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "#" * 90)
print("PER-MODEL CLUSTER TABLES (res=0.008, sorted by stability)")
print("#" * 90)

for model in ["SPECTER2", "PubMedBERT", "MPNet"]:
    cfg = (model, 0.008)
    if cfg not in all_tables:
        continue
    df = all_tables[cfg].sort_values("stability_mean", ascending=False)

    print(f"\n{'='*90}")
    print(f"MODEL: {model}  @  res=0.008  ({len(df)} clusters)")
    print('='*90)
    print(f"{'C':>4} {'N':>5} {'Year':>6} {'Stab':>6} {'Domain':<25} {'Top terms':<40}")
    print("-" * 90)
    for _, r in df.iterrows():
        dom_short = r["domains"][:24] if r["domains"] != "—" else "—"
        terms_short = r["top_terms"][:40]
        print(f"{r['cluster']:>4} {r['n_papers']:>5} "
              f"{r['year_mean']:>6.0f} {r['stability_mean']:>6.2f}  "
              f"{dom_short:<25} {terms_short}")

# ═══════════════════════════════════════════════════════════════════════════
# BUILD VISUALIZATION
# ═══════════════════════════════════════════════════════════════════════════
print("\n\nGenerating visualization...")

# For each model @ res=0.008: show top 25 clusters with:
# - Name (top terms)
# - Size
# - Domain breakdown
# - Stability bar

fig, axes = plt.subplots(1, 3, figsize=(24, 14))

for ax_idx, model in enumerate(["SPECTER2", "PubMedBERT", "MPNet"]):
    ax = axes[ax_idx]
    cfg = (model, 0.008)
    df = all_tables[cfg].sort_values("stability_mean", ascending=True).tail(25)

    y_pos = np.arange(len(df))

    # Split bar: domain contributions (stacked) with stability marker
    # For simplicity: plot stability as main bar colored by primary domain
    def cluster_color(row):
        # Pick domain with highest % as color indicator
        dom_pcts = [(d, row[f"pct_{d}"]) for d in DOMAIN_RE.keys()]
        dom_pcts.sort(key=lambda x: -x[1])
        top_d, top_p = dom_pcts[0]
        if top_p < 15:
            return "#bbbbbb"
        color_map = {
            "VIS": "#1f77b4", "AUD": "#ff7f0e", "SOM": "#2ca02c",
            "PAIN": "#d62728", "CLIN": "#9467bd", "DBS": "#8c564b",
            "SLP": "#e377c2", "DEV": "#7f7f7f", "MEM": "#bcbd22",
            "EPI": "#17becf", "OSC": "#aec7e8", "TRAC": "#ffbb78",
            "STRK": "#98df8a", "CIRC": "#ff9896",
        }
        return color_map.get(top_d, "#bbbbbb")

    colors = [cluster_color(r) for _, r in df.iterrows()]

    # Background bar: cluster size (log-scale normalized)
    ax.barh(y_pos, df["stability_mean"], color=colors, alpha=0.85,
             edgecolor="black", linewidth=0.3)
    ax.axvline(0.5, color="black", linestyle="--", alpha=0.5, linewidth=1)

    # Labels: cluster name + size + primary domain
    labels = []
    for _, r in df.iterrows():
        terms = r["top_terms"].split(" | ")[:3]
        label = f"C{r['cluster']} (n={r['n_papers']}): {', '.join(terms)}"
        labels.append(label[:70])
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=6)
    ax.set_xlim(0, 1)
    ax.set_xlabel("Mean stability (Jaccard vs other configs)")
    ax.set_title(f"{model} @ res=0.008\n({len(all_tables[cfg])} clusters, "
                 f"mean stab={all_tables[cfg]['stability_mean'].mean():.2f})")

    # Annotate stability value
    for yi, sv in enumerate(df["stability_mean"].values):
        ax.text(sv + 0.01, yi, f"{sv:.2f}", fontsize=5, va="center")

# Domain legend
import matplotlib.patches as mpatches
color_map = {
    "VIS": "#1f77b4", "AUD": "#ff7f0e", "SOM": "#2ca02c",
    "PAIN": "#d62728", "CLIN": "#9467bd", "DBS": "#8c564b",
    "SLP": "#e377c2", "DEV": "#7f7f7f", "MEM": "#bcbd22",
    "EPI": "#17becf", "OSC": "#aec7e8", "TRAC": "#ffbb78",
    "STRK": "#98df8a", "CIRC": "#ff9896", "Mixed": "#bbbbbb",
}
handles = [mpatches.Patch(color=c, label=l) for l, c in color_map.items()]
fig.legend(handles=handles, loc="lower center", ncol=8, fontsize=8,
            bbox_to_anchor=(0.5, -0.02))

plt.suptitle("Cluster Comparison Across Models @ res=0.008 "
             "(sorted by stability, colored by primary domain)",
             fontsize=13, fontweight="bold")
plt.tight_layout(rect=[0, 0.02, 1, 0.97])
save_fig(fig, PUB / "fig_cluster_table_comparison.png")
print("  Saved fig_cluster_table_comparison.png")

# ═══════════════════════════════════════════════════════════════════════════
# BUILD DOCX TABLE
# ═══════════════════════════════════════════════════════════════════════════
print("\nGenerating DOCX tables...")

from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH

doc = Document()

# Margins
for section in doc.sections:
    section.top_margin = Inches(0.5)
    section.bottom_margin = Inches(0.5)
    section.left_margin = Inches(0.5)
    section.right_margin = Inches(0.5)
    section.page_width = Inches(14)
    section.page_height = Inches(11)


def h1(t): h = doc.add_heading(t, level=1); return h
def h2(t): h = doc.add_heading(t, level=2); return h


def add_table(df, cols=None, col_widths=None):
    """Add a DataFrame as a docx table."""
    if cols is None:
        cols = df.columns.tolist()
    table = doc.add_table(rows=1 + len(df), cols=len(cols))
    table.style = "Light Grid"

    # Header
    for j, c in enumerate(cols):
        cell = table.rows[0].cells[j]
        cell.text = c
        for run in cell.paragraphs[0].runs:
            run.bold = True
            run.font.size = Pt(8)

    # Data
    for i, row in df.iterrows():
        for j, c in enumerate(cols):
            cell = table.rows[i + 1].cells[j]
            val = row[c]
            if isinstance(val, float):
                val = f"{val:.2f}" if c.startswith("stab") or c.startswith("jacc") else f"{val:g}"
            cell.text = str(val)[:60]
            for run in cell.paragraphs[0].runs:
                run.font.size = Pt(7)


h1("Cluster Comparison Tables")
doc.add_paragraph(
    f"Detailed per-cluster tables for each (embedding model, resolution) "
    f"configuration. Stability scores are mean Jaccard overlap with the "
    f"best-matching cluster in each of the 11 other configurations. "
    f"Higher stability = more robust cluster identity across method choices."
)

# Summary cross-model comparison at res=0.008
h2("Summary: Model Comparison at res=0.008")
summary_rows = []
for model in ["SPECTER2", "PubMedBERT", "MPNet"]:
    cfg = (model, 0.008)
    df = all_tables[cfg]
    summary_rows.append({
        "Model": model,
        "n_clusters": len(df),
        "mean_stability": round(df["stability_mean"].mean(), 3),
        "median_stability": round(df["stability_mean"].median(), 3),
        "n_universal (stab>0.5)": (df["stability_mean"] >= 0.5).sum(),
        "largest cluster": df["n_papers"].max(),
        "median cluster size": int(df["n_papers"].median()),
        "year range": f"{df['year_mean'].min():.0f}-{df['year_mean'].max():.0f}",
    })
summary_df = pd.DataFrame(summary_rows)
add_table(summary_df)

# Detailed per-model tables at res=0.008
for model in ["SPECTER2", "PubMedBERT", "MPNet"]:
    cfg = (model, 0.008)
    df = all_tables[cfg].sort_values("stability_mean", ascending=False)
    h2(f"{model} @ res=0.008 ({len(df)} clusters)")
    display_cols = ["cluster", "n_papers", "year_mean", "stability_mean",
                    "jacc_vs_SPECTER2", "jacc_vs_PubMedBERT", "jacc_vs_MPNet",
                    "domains", "top_terms"]
    add_table(df[display_cols], display_cols)
    doc.add_paragraph()

# Summary across resolutions per model
for model in ["SPECTER2", "PubMedBERT", "MPNet"]:
    h2(f"{model} — across resolutions")
    for res in [0.005, 0.008, 0.012, 0.020]:
        cfg = (model, res)
        if cfg not in all_tables:
            continue
        df = all_tables[cfg].sort_values("stability_mean", ascending=False)
        p = doc.add_paragraph()
        p.add_run(f"res={res} ({len(df)} clusters, "
                  f"mean stability={df['stability_mean'].mean():.2f})").bold = True
        display_cols = ["cluster", "n_papers", "stability_mean",
                        "domains", "top_terms"]
        add_table(df[display_cols], display_cols)
        doc.add_paragraph()

out_docx = ROOT / "Paper" / "cluster_tables.docx"
doc.save(str(out_docx))
print(f"  Saved: {out_docx}")
print("\nDone.")
