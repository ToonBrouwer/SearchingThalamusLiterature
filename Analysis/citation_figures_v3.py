"""
citation_figures_v3.py — Clean per-decade citation networks.

Improvements:
  - Edge width scales with citation weight
  - Labels as "FirstAuthor et al. (year)" instead of title
  - Cluster overview table
  - Interactive HTML plots (plotly)
  - Only top nodes shown (PageRank filter)
"""
import sys, io, os, re, json
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
RAW  = ROOT / "Data" / "raw" / "pubmed"

CLUSTER_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#17becf", "#bcbd22", "#aec7e8",
    "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5", "#c49c94",
]

DECADES = [1980, 1990, 2000, 2010, 2020]
TOP_N = 120

# Load author labels
AUTHOR_FILE = PROC / "paper_authors.csv"
author_lookup = {}
if AUTHOR_FILE.exists():
    _au = pd.read_csv(AUTHOR_FILE)
    for _, row in _au.iterrows():
        author_lookup[str(int(row["pmid"]))] = row["author_label"]
    print(f"  Author labels: {len(author_lookup):,}")


# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA + EXTRACT FIRST AUTHORS
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
cit = pd.read_csv(PROC / "citation_data.csv")
cit["decade"] = (cit["year"] // 10) * 10
G_full = nx.read_graphml(str(PROC / "citation_graph.graphml"))

# Build pmid lookup
pmid_data = {}
for _, row in cit.iterrows():
    pmid_data[str(int(row["pmid"]))] = {
        "year": int(row["year"]) if pd.notna(row["year"]) else 0,
        "title": str(row["title"]) if pd.notna(row["title"]) else "",
        "cited_by_count": int(row["cited_by_count"]),
        "frame": str(row["dominant_frame"]) if pd.notna(row["dominant_frame"]) else "",
    }


def extract_first_author(title):
    """Heuristic: extract a short label from title.
    Since we don't have author data, use first 2-3 key words."""
    title = str(title)
    # Remove common leading articles
    for p in ["The ", "A ", "An ", "On "]:
        if title.startswith(p):
            title = title[len(p):]
    # Take first few meaningful words
    words = title.split()[:4]
    short = " ".join(words)
    if len(short) > 25:
        short = short[:22] + "..."
    return short


def author_label(pmid):
    """Create 'Author et al. (year)' label, fall back to short title."""
    if pmid in author_lookup:
        return author_lookup[pmid]
    data = pmid_data.get(pmid, {})
    title = extract_first_author(data.get("title", ""))
    year = data.get("year", "")
    return f"{title} ({year})"


def cluster_topic_label(members, pmid_data):
    words = Counter()
    stop = {"the", "and", "for", "with", "from", "that", "this", "its",
            "are", "was", "were", "has", "been", "not", "but", "study",
            "role", "effects", "evidence", "using", "during", "after",
            "between", "into", "neurons", "nucleus", "nuclei", "rat",
            "cats", "monkey", "human", "mice", "brain", "neural", "new",
            "thalamic", "thalamus", "thalamocortical", "cortical", "cortex"}
    for m in members:
        data = pmid_data.get(m, {})
        title = str(data.get("title", "")).lower()
        for w in re.findall(r'[a-z]{4,}', title):
            if w not in stop:
                words[w] += 1
    top = [w for w, _ in words.most_common(3)]
    return " / ".join(top[:2]) if len(top) >= 2 else (top[0] if top else "mixed")


def nx_to_igraph(G_nx):
    nodes = list(G_nx.nodes())
    node_idx = {n: i for i, n in enumerate(nodes)}
    edges = [(node_idx[u], node_idx[v]) for u, v in G_nx.edges()
             if u in node_idx and v in node_idx]
    g = ig.Graph(n=len(nodes), edges=edges, directed=False)
    g.vs["name"] = nodes
    return g, nodes


# ═══════════════════════════════════════════════════════════════════════════
# PER-DECADE FIGURES
# ═══════════════════════════════════════════════════════════════════════════

cluster_table_rows = []

for decade in DECADES:
    print(f"\n{'='*60}")
    print(f"  {decade}s")
    print(f"{'='*60}")

    decade_pmids = set(str(int(row["pmid"])) for _, row in cit.iterrows()
                       if int(row.get("decade", 0)) == decade)

    # Build subgraph
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
    comps = list(nx.connected_components(G_undir))
    largest = max(comps, key=len)
    G_cc = G_undir.subgraph(largest).copy()

    pagerank = nx.pagerank(G_sub, alpha=0.85)
    in_deg = dict(G_sub.in_degree())

    # Leiden
    g_ig, node_list = nx_to_igraph(G_cc)
    partition = leidenalg.find_partition(g_ig, leidenalg.ModularityVertexPartition, seed=42)
    node_cluster = {node_list[i]: partition.membership[i] for i in range(len(node_list))}
    n_clusters = len(partition)

    # Top N nodes
    top_nodes = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    top_set = set(n for n, _ in top_nodes)
    G_disp = G_cc.subgraph(top_set).copy()

    # Cluster membership for display nodes
    cluster_members = {}
    for n in top_set:
        cl = node_cluster.get(n, 0)
        cluster_members.setdefault(cl, []).append(n)

    # Cluster labels + table data
    cluster_labels = {}
    for cl, members in sorted(cluster_members.items(),
                               key=lambda x: len(x[1]), reverse=True):
        all_members = [n for n, c in node_cluster.items() if c == cl]
        topic = cluster_topic_label(all_members, pmid_data)

        # Dominant frame
        frames = [pmid_data.get(m, {}).get("frame", "") for m in all_members]
        frames = [f for f in frames if f and f != "nan"]
        frame_dist = Counter(frames).most_common(2)
        frame_str = ", ".join(f"{f}({c})" for f, c in frame_dist) if frame_dist else "—"

        # Top paper
        top_m = max(members, key=lambda m: pagerank.get(m, 0))
        top_data = pmid_data.get(top_m, {})

        cluster_labels[cl] = topic

        cluster_table_rows.append({
            "decade": decade,
            "cluster": cl,
            "n_papers": len(all_members),
            "topic": topic,
            "frames": frame_str,
            "top_paper": author_label(top_m),
            "top_citations": top_data.get("cited_by_count", 0),
        })

    # ── Layout ──
    pos = nx.kamada_kawai_layout(G_disp)

    # ── Compute edge weights for display ──
    # Count shared citations between connected nodes
    edge_weights = {}
    for u, v in G_disp.edges():
        # Use in-degree sum as proxy for edge importance
        w = in_deg.get(u, 0) + in_deg.get(v, 0)
        edge_weights[(u, v)] = w

    max_ew = max(edge_weights.values()) if edge_weights else 1

    # ── Figure ──
    fig, ax = plt.subplots(figsize=(16, 14))

    # Draw edges with scaled width
    for (u, v), w in edge_weights.items():
        if u in pos and v in pos:
            width = (w / max_ew) * 3.0 + 0.2
            alpha = min(0.05 + (w / max_ew) * 0.4, 0.45)
            ax.plot([pos[u][0], pos[v][0]], [pos[u][1], pos[v][1]],
                    color="grey", linewidth=width, alpha=alpha, zorder=1)

    # Draw nodes
    for n in G_disp.nodes():
        cl = node_cluster.get(n, 0)
        color = CLUSTER_COLORS[cl % len(CLUSTER_COLORS)]
        pr = pagerank.get(n, 0)
        size = max(pr * 100000, 20)
        is_decade = n in decade_pmids
        ax.scatter(pos[n][0], pos[n][1], s=size, c=color,
                   alpha=0.85 if is_decade else 0.45,
                   edgecolors="black" if is_decade else "none",
                   linewidths=0.5 if is_decade else 0, zorder=3)

    # Label top 3 per cluster
    labeled = set()
    for cl in sorted(cluster_members.keys(),
                     key=lambda c: len(cluster_members[c]), reverse=True)[:8]:
        members = cluster_members[cl]
        cl_sorted = sorted(members, key=lambda m: pagerank.get(m, 0), reverse=True)
        for m in cl_sorted[:3]:
            if m in labeled:
                continue
            label = author_label(m)
            data = pmid_data.get(m, {})
            gc = data.get("cited_by_count", 0)
            frame = data.get("frame", "")
            if frame and frame != "nan":
                label = f"[{frame}] {label}"

            ax.annotate(label, pos[m], fontsize=6, fontweight="bold",
                        color=CLUSTER_COLORS[cl % len(CLUSTER_COLORS)],
                        textcoords="offset points", xytext=(8, 4),
                        arrowprops=dict(arrowstyle="-",
                                        color=CLUSTER_COLORS[cl % len(CLUSTER_COLORS)],
                                        lw=0.8, alpha=0.7),
                        bbox=dict(boxstyle="round,pad=0.15", fc="white",
                                  ec="none", alpha=0.7),
                        zorder=5)
            labeled.add(m)

    # Legend
    patches = []
    for cl in sorted(cluster_members.keys(),
                     key=lambda c: len(cluster_members[c]), reverse=True)[:8]:
        all_n = len([n for n, c in node_cluster.items() if c == cl])
        label = f"{cluster_labels.get(cl, 'C'+str(cl))} ({all_n})"
        patches.append(mpatches.Patch(
            color=CLUSTER_COLORS[cl % len(CLUSTER_COLORS)], label=label))

    ax.legend(handles=patches, fontsize=8, loc="upper left",
              title=f"Leiden Clusters", title_fontsize=9,
              framealpha=0.95, edgecolor="grey")

    ax.set_title(f"Citation Network — {decade}s\n"
                 f"Top {len(G_disp)} nodes | {n_clusters} clusters | "
                 f"modularity = {partition.modularity:.2f}",
                 fontsize=14, fontweight="bold")
    ax.axis("off")
    plt.tight_layout()
    save_fig(fig, PUB / f"fig_citation_v3_{decade}.png")
    plt.close(fig)
    print(f"  Saved fig_citation_v3_{decade}.png")

# ═══════════════════════════════════════════════════════════════════════════
# CLUSTER OVERVIEW TABLE
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("CLUSTER OVERVIEW TABLE")
print("=" * 70)

table = pd.DataFrame(cluster_table_rows)
table = table.sort_values(["decade", "n_papers"], ascending=[True, False])

# Print top 5 clusters per decade
for dec in DECADES:
    dec_t = table[table["decade"] == dec].head(5)
    print(f"\n  {dec}s:")
    print(f"  {'Cluster':>8} {'N':>6} {'Topic':20s} {'Frames':30s} {'Top Paper':40s}")
    print(f"  {'-'*8} {'-'*6} {'-'*20} {'-'*30} {'-'*40}")
    for _, row in dec_t.iterrows():
        print(f"  {row['cluster']:>8} {row['n_papers']:>6} "
              f"{str(row['topic'])[:20]:20s} {str(row['frames'])[:30]:30s} "
              f"{str(row['top_paper'])[:40]:40s}")

table.to_csv(PROC / "citation_cluster_table.csv", index=False)
print(f"\nSaved citation_cluster_table.csv")

# ═══════════════════════════════════════════════════════════════════════════
# INTERACTIVE HTML PLOT (one per decade)
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("GENERATING INTERACTIVE PLOTS")
print("=" * 70)

try:
    import plotly.graph_objects as go

    for decade in DECADES:
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

        G_sub = G_full.subgraph(relevant).copy()
        G_undir = G_sub.to_undirected()
        comps = list(nx.connected_components(G_undir))
        largest = max(comps, key=len)
        G_cc = G_undir.subgraph(largest).copy()

        pr = nx.pagerank(G_sub, alpha=0.85)

        # Leiden
        g_ig, nl = nx_to_igraph(G_cc)
        part = leidenalg.find_partition(g_ig, leidenalg.ModularityVertexPartition, seed=42)
        nc = {nl[i]: part.membership[i] for i in range(len(nl))}

        # Top 200 for interactive
        top200 = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:200]
        top_set = set(n for n, _ in top200)
        G_d = G_cc.subgraph(top_set).copy()
        pos = nx.kamada_kawai_layout(G_d)

        # Edge traces
        edge_x, edge_y = [], []
        for u, v in G_d.edges():
            if u in pos and v in pos:
                x0, y0 = pos[u]
                x1, y1 = pos[v]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])

        edge_trace = go.Scatter(x=edge_x, y=edge_y, mode="lines",
                                line=dict(width=0.3, color="rgba(150,150,150,0.3)"),
                                hoverinfo="none")

        # Node traces per cluster
        node_traces = []
        clusters_in_disp = set(nc.get(n, 0) for n in G_d.nodes())

        for cl in sorted(clusters_in_disp):
            cl_nodes = [n for n in G_d.nodes() if nc.get(n, 0) == cl]
            if not cl_nodes:
                continue

            x_vals = [pos[n][0] for n in cl_nodes]
            y_vals = [pos[n][1] for n in cl_nodes]
            sizes = [max(pr.get(n, 0) * 5000, 5) for n in cl_nodes]

            hover_texts = []
            for n in cl_nodes:
                data = pmid_data.get(n, {})
                frame = data.get("frame", "")
                if not frame or frame == "nan":
                    frame = "none"
                auth = author_lookup.get(n, "")
                title_short = data.get('title', 'Unknown')[:60]
                hover_texts.append(
                    f"<b>{auth or title_short}</b><br>"
                    f"<i>{title_short}</i><br>"
                    f"Year: {data.get('year', '?')}<br>"
                    f"Citations: {data.get('cited_by_count', 0):,}<br>"
                    f"Frame: {frame}<br>"
                    f"PMID: {n}<br>"
                    f"PageRank: {pr.get(n, 0):.5f}"
                )

            # Get topic label for this cluster
            all_cl = [n for n, c in nc.items() if c == cl]
            topic = cluster_topic_label(all_cl, pmid_data)

            node_traces.append(go.Scatter(
                x=x_vals, y=y_vals, mode="markers",
                marker=dict(
                    size=sizes,
                    color=CLUSTER_COLORS[cl % len(CLUSTER_COLORS)],
                    line=dict(width=0.5, color="black"),
                    opacity=0.8,
                ),
                text=hover_texts, hoverinfo="text",
                name=f"C{cl}: {topic} ({len(all_cl)})",
            ))

        fig_plotly = go.Figure(data=[edge_trace] + node_traces)
        fig_plotly.update_layout(
            title=f"Citation Network — {decade}s (interactive, top 200 nodes)",
            showlegend=True,
            legend=dict(font=dict(size=10)),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            width=1200, height=900,
            template="plotly_white",
        )

        html_path = PUB / f"fig_citation_interactive_{decade}.html"
        fig_plotly.write_html(str(html_path))
        print(f"  Saved {html_path.name}")

except ImportError:
    print("  plotly not installed — skipping interactive plots")
    print("  Install with: pip install plotly")

print("\nDone.")
