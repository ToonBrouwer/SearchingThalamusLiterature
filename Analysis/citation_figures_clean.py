"""
citation_figures_clean.py — Clean, interpretable per-decade citation figures.

Shows top ~150 nodes per decade, Leiden-clustered, with readable labels.
Each cluster gets a descriptive name based on its content, not just frame type.
"""
import sys, io, os, json, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx
import igraph as ig
import leidenalg
from pathlib import Path
from collections import Counter

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"

CLUSTER_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#17becf", "#bcbd22", "#aec7e8",
    "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5", "#c49c94",
]

# ═══════════════════════════════════════════════════════════════════════════
# LOAD
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
cit = pd.read_csv(PROC / "citation_data.csv")
cit["decade"] = (cit["year"] // 10) * 10
G_full = nx.read_graphml(str(PROC / "citation_graph.graphml"))

pmid_data = {}
for _, row in cit.iterrows():
    pmid_data[str(int(row["pmid"]))] = {
        "year": int(row["year"]) if pd.notna(row["year"]) else 0,
        "title": str(row["title"]) if pd.notna(row["title"]) else "",
        "cited_by_count": int(row["cited_by_count"]),
        "frame": str(row["dominant_frame"]) if pd.notna(row["dominant_frame"]) else "",
    }

DECADES = [1980, 1990, 2000, 2010, 2020]
TOP_N = 150  # nodes to show per decade


def short_label(title, year, max_len=35):
    """Create a short author-year-ish label from title."""
    title = str(title)
    # Remove common prefixes
    for prefix in ["The ", "A ", "An "]:
        if title.startswith(prefix):
            title = title[len(prefix):]
    title = title[:max_len]
    if len(str(title)) >= max_len:
        title = title[:max_len-3] + "..."
    return f"{title} ({year})"


def cluster_topic_label(members, pmid_data):
    """Generate a descriptive label for a cluster based on title keywords."""
    # Collect title words
    words = Counter()
    stop = {"the", "and", "for", "with", "from", "that", "this", "its",
            "are", "was", "were", "has", "been", "not", "but", "study",
            "role", "effects", "evidence", "using", "during", "after",
            "between", "into", "neurons", "nucleus", "nuclei", "rat",
            "cats", "monkey", "human", "mice", "brain", "neural", "new"}

    for m in members:
        data = pmid_data.get(m, {})
        title = str(data.get("title", "")).lower()
        for w in re.findall(r'[a-z]{4,}', title):
            if w not in stop and w != "thalamic" and w != "thalamus":
                words[w] += 1

    # Top 2-3 distinctive words
    top = [w for w, _ in words.most_common(5)]
    # Pick the most informative 2
    if len(top) >= 2:
        return f"{top[0]}/{top[1]}"
    elif top:
        return top[0]
    return "mixed"


def nx_to_igraph(G_nx):
    nodes = list(G_nx.nodes())
    node_idx = {n: i for i, n in enumerate(nodes)}
    edges = [(node_idx[u], node_idx[v]) for u, v in G_nx.edges()
             if u in node_idx and v in node_idx]
    g = ig.Graph(n=len(nodes), edges=edges, directed=False)
    g.vs["name"] = nodes
    return g, nodes


# ═══════════════════════════════════════════════════════════════════════════
# GENERATE ONE CLEAN FIGURE PER DECADE
# ═══════════════════════════════════════════════════════════════════════════

for decade in DECADES:
    print(f"\n{'='*60}")
    print(f"  {decade}s")
    print(f"{'='*60}")

    # Get decade papers
    decade_pmids = set(str(int(row["pmid"])) for _, row in cit.iterrows()
                       if int(row.get("decade", 0)) == decade)

    # Build subgraph (decade papers + cited references within ±15 years)
    relevant = set()
    for pmid in decade_pmids:
        if G_full.has_node(pmid):
            relevant.add(pmid)
            for _, target in G_full.out_edges(pmid):
                t_year = pmid_data.get(target, {}).get("year", 0)
                if t_year >= decade - 15:
                    relevant.add(target)
            for source, _ in G_full.in_edges(pmid):
                s_year = pmid_data.get(source, {}).get("year", 0)
                if s_year <= decade + 15:
                    relevant.add(source)

    G_sub = G_full.subgraph(relevant).copy()
    G_undir = G_sub.to_undirected()
    components = list(nx.connected_components(G_undir))
    largest_cc = max(components, key=len)
    G_cc = G_undir.subgraph(largest_cc).copy()

    # PageRank on directed subgraph
    pagerank = nx.pagerank(G_sub, alpha=0.85)

    # Leiden clustering
    g_ig, node_list = nx_to_igraph(G_cc)
    partition = leidenalg.find_partition(g_ig, leidenalg.ModularityVertexPartition,
                                         seed=42)
    node_cluster = {node_list[i]: partition.membership[i]
                    for i in range(len(node_list))}
    n_clusters = len(partition)

    # Select top N nodes by PageRank
    top_nodes = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    top_set = set(n for n, _ in top_nodes)

    # Build display subgraph
    G_disp = G_cc.subgraph(top_set).copy()

    # Characterize clusters (only those with nodes in top_set)
    cluster_members = {}
    for n in top_set:
        cl = node_cluster.get(n, 0)
        if cl not in cluster_members:
            cluster_members[cl] = []
        cluster_members[cl].append(n)

    # Generate cluster labels
    cluster_labels = {}
    for cl, members in sorted(cluster_members.items(),
                               key=lambda x: len(x[1]), reverse=True):
        # Get all members of this cluster (not just top nodes)
        all_members = [n for n, c in node_cluster.items() if c == cl]
        topic = cluster_topic_label(all_members, pmid_data)

        # Top paper in this cluster
        top_in_cl = max(members, key=lambda m: pagerank.get(m, 0))
        top_title = pmid_data.get(top_in_cl, {}).get("title", "")[:30]

        cluster_labels[cl] = f"{topic} ({len(all_members)})"

    # ── Layout ──
    # Use Kamada-Kawai for cleaner layout on smaller graph
    pos = nx.kamada_kawai_layout(G_disp)

    # ── Figure ──
    fig, ax = plt.subplots(figsize=(16, 14))

    # Node properties
    node_colors = []
    node_sizes = []
    node_alphas = []
    for n in G_disp.nodes():
        cl = node_cluster.get(n, 0)
        node_colors.append(CLUSTER_COLORS[cl % len(CLUSTER_COLORS)])
        pr = pagerank.get(n, 0)
        node_sizes.append(max(pr * 80000, 15))
        # Decade papers slightly brighter
        node_alphas.append(0.9 if n in decade_pmids else 0.5)

    # Draw edges
    nx.draw_networkx_edges(G_disp, pos, ax=ax, alpha=0.06,
                           edge_color="grey", width=0.4)

    # Draw nodes
    for i, n in enumerate(G_disp.nodes()):
        ax.scatter(pos[n][0], pos[n][1],
                   s=node_sizes[i], c=node_colors[i],
                   alpha=node_alphas[i],
                   edgecolors="black" if n in decade_pmids else "none",
                   linewidths=0.3 if n in decade_pmids else 0,
                   zorder=3)

    # Label top 3 papers per cluster
    labeled = set()
    for cl, members in sorted(cluster_members.items(),
                               key=lambda x: len(x[1]), reverse=True)[:8]:
        # Top 3 by PageRank in this cluster
        cl_sorted = sorted(members, key=lambda m: pagerank.get(m, 0), reverse=True)
        for m in cl_sorted[:3]:
            if m in labeled:
                continue
            data = pmid_data.get(m, {})
            label = short_label(data.get("title", ""), data.get("year", ""))
            gc = data.get("cited_by_count", 0)
            frame = data.get("frame", "")
            if frame and frame != "nan":
                label = f"[{frame}] {label}"

            ax.annotate(label, pos[m],
                        fontsize=5.5, fontweight="bold",
                        color=CLUSTER_COLORS[cl % len(CLUSTER_COLORS)],
                        textcoords="offset points", xytext=(8, 4),
                        arrowprops=dict(arrowstyle="-", color="grey",
                                        lw=0.4, alpha=0.5),
                        zorder=5)
            labeled.add(m)

    # Legend: top 8 clusters
    patches = []
    for cl in sorted(cluster_members.keys(),
                     key=lambda c: len(cluster_members[c]), reverse=True)[:8]:
        label = cluster_labels.get(cl, f"C{cl}")
        patches.append(mpatches.Patch(
            color=CLUSTER_COLORS[cl % len(CLUSTER_COLORS)],
            label=label))

    legend = ax.legend(handles=patches, fontsize=8, loc="upper left",
                       title=f"Leiden Clusters ({n_clusters} total)",
                       title_fontsize=9, framealpha=0.9,
                       edgecolor="grey")

    ax.set_title(f"Citation Network — {decade}s\n"
                 f"Top {len(G_disp)} papers by PageRank | "
                 f"{n_clusters} Leiden clusters | "
                 f"modularity = {partition.modularity:.2f}",
                 fontsize=13, fontweight="bold")
    ax.axis("off")

    plt.tight_layout()
    save_fig(fig, PUB / f"fig_citation_clean_{decade}.png")
    plt.close(fig)
    print(f"  Saved fig_citation_clean_{decade}.png")

# ═══════════════════════════════════════════════════════════════════════════
# OVERVIEW: 5 panels side by side
# ═══════════════════════════════════════════════════════════════════════════
print("\nGenerating overview panel...")
fig, axes = plt.subplots(1, 5, figsize=(35, 8))

for dec_idx, decade in enumerate(DECADES):
    ax = axes[dec_idx]

    decade_pmids = set(str(int(row["pmid"])) for _, row in cit.iterrows()
                       if int(row.get("decade", 0)) == decade)
    relevant = set()
    for pmid in decade_pmids:
        if G_full.has_node(pmid):
            relevant.add(pmid)
            for _, target in G_full.out_edges(pmid):
                t_year = pmid_data.get(target, {}).get("year", 0)
                if t_year >= decade - 15:
                    relevant.add(target)

    G_sub_d = G_full.subgraph(relevant).copy()
    G_undir_d = G_sub_d.to_undirected()
    comps = list(nx.connected_components(G_undir_d))
    largest = max(comps, key=len)
    G_cc_d = G_undir_d.subgraph(largest).copy()

    pr = nx.pagerank(G_sub_d, alpha=0.85)
    g_ig_d, nl_d = nx_to_igraph(G_cc_d)
    part_d = leidenalg.find_partition(g_ig_d, leidenalg.ModularityVertexPartition, seed=42)
    nc_d = {nl_d[i]: part_d.membership[i] for i in range(len(nl_d))}

    # Top 100 for overview
    top100 = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:100]
    top_set_d = set(n for n, _ in top100)
    G_d = G_cc_d.subgraph(top_set_d).copy()
    pos_d = nx.kamada_kawai_layout(G_d)

    colors_d = [CLUSTER_COLORS[nc_d.get(n, 0) % len(CLUSTER_COLORS)] for n in G_d.nodes()]
    sizes_d = [max(pr.get(n, 0) * 30000, 8) for n in G_d.nodes()]

    nx.draw_networkx_edges(G_d, pos_d, ax=ax, alpha=0.08, edge_color="grey", width=0.3)
    nx.draw_networkx_nodes(G_d, pos_d, ax=ax, node_color=colors_d,
                           node_size=sizes_d, alpha=0.7, edgecolors="none")

    # Label top 5
    top5 = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:5]
    for pmid, _ in top5:
        if pmid in pos_d:
            data = pmid_data.get(pmid, {})
            t = str(data.get("title", ""))[:20]
            ax.annotate(t, pos_d[pmid], fontsize=4.5, fontweight="bold",
                        textcoords="offset points", xytext=(5, 3))

    ax.set_title(f"{decade}s\n{len(G_cc_d):,} nodes | {len(part_d)} clusters",
                 fontsize=11, fontweight="bold")
    ax.axis("off")

fig.suptitle("Citation Network Evolution (Leiden Clustering, top 100 nodes per decade)",
             fontsize=14, fontweight="bold", y=1.02)
plt.tight_layout()
save_fig(fig, PUB / "fig_citation_decades_clean.png")
plt.close(fig)
print("Saved fig_citation_decades_clean.png")

print("\nDone.")
