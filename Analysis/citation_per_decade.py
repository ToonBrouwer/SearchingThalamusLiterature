"""
citation_per_decade.py — Per-decade citation network analysis with Leiden clustering.

For each decade:
  - Build the within-corpus citation subgraph
  - Leiden clustering to find intellectual communities
  - Identify key nodes (PageRank, in-degree, betweenness)
  - Characterize each cluster (dominant frame, top papers, topic)
  - Visualize the network with clusters colored

Output:
  Graphs/pub/fig_citation_decade_1980.png ... fig_citation_decade_2020.png
  Graphs/pub/fig_citation_decades_overview.png (summary panel)
  Data/processed/citation_communities_per_decade.csv
"""
import sys, io, os, json
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

# ═══════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading data...")
cit = pd.read_csv(PROC / "citation_data.csv")
cit["decade"] = (cit["year"] // 10) * 10

G_full = nx.read_graphml(str(PROC / "citation_graph.graphml"))
print(f"  Full graph: {G_full.number_of_nodes():,} nodes, {G_full.number_of_edges():,} edges")

# Build pmid lookup
pmid_data = {}
for _, row in cit.iterrows():
    pmid_data[str(int(row["pmid"]))] = {
        "year": int(row["year"]) if pd.notna(row["year"]) else 0,
        "title": str(row["title"]) if pd.notna(row["title"]) else "",
        "cited_by_count": int(row["cited_by_count"]),
        "frame": str(row["dominant_frame"]) if pd.notna(row["dominant_frame"]) else "",
    }

DECADES = [1980, 1990, 2000, 2010, 2020]

# Cluster colors (10 distinct colors)
CLUSTER_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
]


def nx_to_igraph(G_nx):
    """Convert NetworkX graph to igraph for Leiden."""
    nodes = list(G_nx.nodes())
    node_idx = {n: i for i, n in enumerate(nodes)}
    edges = [(node_idx[u], node_idx[v]) for u, v in G_nx.edges()
             if u in node_idx and v in node_idx]
    g = ig.Graph(n=len(nodes), edges=edges, directed=False)
    g.vs["name"] = nodes
    return g, nodes


def get_decade_subgraph(decade):
    """Extract subgraph of papers from this decade + papers they cite."""
    # Papers published in this decade
    decade_pmids = set(str(int(row["pmid"])) for _, row in cit.iterrows()
                       if int(row.get("decade", 0)) == decade)

    # Build subgraph: include decade papers + any paper they cite or that cites them
    # (within ±1 decade for context)
    relevant = set()
    for pmid in decade_pmids:
        if G_full.has_node(pmid):
            relevant.add(pmid)
            # Papers this decade cites (predecessors in directed graph)
            for _, target in G_full.out_edges(pmid):
                t_year = pmid_data.get(target, {}).get("year", 0)
                if t_year >= decade - 15:  # cited papers up to 15 years before
                    relevant.add(target)
            # Papers that cite this decade (successors)
            for source, _ in G_full.in_edges(pmid):
                s_year = pmid_data.get(source, {}).get("year", 0)
                if s_year <= decade + 15:
                    relevant.add(source)

    G_sub = G_full.subgraph(relevant).copy()
    return G_sub, decade_pmids


# ═══════════════════════════════════════════════════════════════════════════
# PER-DECADE ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════
all_community_data = []

# Big summary figure: one panel per decade
fig_overview, axes_ov = plt.subplots(1, 5, figsize=(30, 7))

for dec_idx, decade in enumerate(DECADES):
    print(f"\n{'='*70}")
    print(f"DECADE: {decade}s")
    print("=" * 70)

    G_sub, decade_pmids = get_decade_subgraph(decade)
    G_undir = G_sub.to_undirected()

    # Keep only largest connected component
    if G_undir.number_of_nodes() < 10:
        print(f"  Too few nodes ({G_undir.number_of_nodes()}), skipping")
        continue

    components = list(nx.connected_components(G_undir))
    largest_cc = max(components, key=len)
    G_cc = G_undir.subgraph(largest_cc).copy()
    print(f"  Subgraph: {G_sub.number_of_nodes():,} nodes, {G_sub.number_of_edges():,} edges")
    print(f"  Largest CC: {len(largest_cc):,} nodes")

    # ── Leiden clustering ──
    g_ig, node_list = nx_to_igraph(G_cc)
    partition = leidenalg.find_partition(g_ig, leidenalg.ModularityVertexPartition,
                                         seed=42)
    n_clusters = len(partition)
    membership = partition.membership
    node_cluster = {node_list[i]: membership[i] for i in range(len(node_list))}

    print(f"  Leiden clusters: {n_clusters}")
    print(f"  Modularity: {partition.modularity:.3f}")

    # ── Key metrics ──
    in_deg = dict(G_sub.in_degree())
    pagerank = nx.pagerank(G_sub, alpha=0.85)

    # ── Characterize each cluster ──
    cluster_info = {}
    for c_id in range(min(n_clusters, 12)):
        members = [n for n, c in node_cluster.items() if c == c_id]
        if len(members) < 3:
            continue

        # How many are from this decade vs cited references?
        n_decade = sum(1 for m in members if m in decade_pmids)
        n_refs = len(members) - n_decade

        # Frames
        frames = [pmid_data.get(m, {}).get("frame", "") for m in members]
        frames = [f for f in frames if f and f != "nan"]
        frame_dist = Counter(frames).most_common(3)

        # Top paper by PageRank
        top_pr = max(members, key=lambda m: pagerank.get(m, 0))
        top_title = pmid_data.get(top_pr, {}).get("title", "")[:55]
        top_year = pmid_data.get(top_pr, {}).get("year", "")
        top_cit = pmid_data.get(top_pr, {}).get("cited_by_count", 0)

        # Mean year of cluster
        years = [pmid_data.get(m, {}).get("year", 0) for m in members]
        years = [y for y in years if y > 1950]
        mean_year = np.mean(years) if years else 0

        cluster_info[c_id] = {
            "size": len(members), "n_decade": n_decade, "n_refs": n_refs,
            "mean_year": mean_year, "frames": frame_dist,
            "top_paper": top_title, "top_year": top_year,
            "top_cit": top_cit, "top_pmid": top_pr,
        }

        frame_str = ", ".join(f"{f}({c})" for f, c in frame_dist) if frame_dist else "none"
        print(f"\n  Cluster {c_id}: {len(members)} papers "
              f"({n_decade} from {decade}s, {n_refs} refs), ~{mean_year:.0f}")
        print(f"    Frames: {frame_str}")
        print(f"    Landmark: [{top_cit}] {top_title} ({top_year})")

        # Save for CSV
        for m in members:
            all_community_data.append({
                "pmid": m, "decade_analyzed": decade,
                "cluster": c_id, "cluster_size": len(members),
            })

    # ── Top 10 key nodes for this decade ──
    print(f"\n  TOP 10 KEY NODES ({decade}s — by PageRank):")
    decade_nodes_pr = {n: pr for n, pr in pagerank.items() if n in decade_pmids}
    top10 = sorted(decade_nodes_pr.items(), key=lambda x: x[1], reverse=True)[:10]
    for rank, (pmid, pr) in enumerate(top10):
        data = pmid_data.get(pmid, {})
        frame = data.get("frame", "")
        if not frame or frame == "nan":
            frame = "—"
        title = data.get("title", "")[:50]
        gc = data.get("cited_by_count", 0)
        cl = node_cluster.get(pmid, -1)
        print(f"    {rank+1:>2}. [{gc:>5} cit] C{cl} {frame:12s} | {title}")

    # ── Per-decade figure ──
    fig_dec, ax_dec = plt.subplots(figsize=(14, 12))

    # Layout — use only decade papers for positioning
    pos = nx.spring_layout(G_cc, seed=42, k=1.5/max(np.sqrt(len(G_cc)), 1),
                           iterations=80)

    # Node colors by cluster
    node_colors = []
    node_sizes = []
    for n in G_cc.nodes():
        cl = node_cluster.get(n, 0)
        node_colors.append(CLUSTER_COLORS[cl % len(CLUSTER_COLORS)])
        # Size by PageRank
        pr = pagerank.get(n, 0)
        node_sizes.append(max(pr * 50000, 5))

    # Draw edges (light)
    nx.draw_networkx_edges(G_cc, pos, ax=ax_dec, alpha=0.03,
                           edge_color="grey", width=0.3)

    # Draw nodes
    nx.draw_networkx_nodes(G_cc, pos, ax=ax_dec,
                           node_color=node_colors, node_size=node_sizes,
                           alpha=0.7, edgecolors="black", linewidths=0.2)

    # Label top 15 nodes by PageRank
    top15 = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:15]
    labels = {}
    for pmid, _ in top15:
        data = pmid_data.get(pmid, {})
        # Short author-year label
        title = data.get("title", "")
        year = data.get("year", "")
        # Extract first author surname from title (heuristic)
        short = f"{str(title)[:25]}... ({year})"
        labels[pmid] = short

    nx.draw_networkx_labels(G_cc, pos, labels=labels, ax=ax_dec,
                            font_size=6, font_weight="bold")

    # Legend for clusters
    shown_clusters = sorted(cluster_info.keys())[:8]
    patches = []
    for cl in shown_clusters:
        info = cluster_info[cl]
        frame_label = info["frames"][0][0] if info["frames"] else "mixed"
        patches.append(mpatches.Patch(
            color=CLUSTER_COLORS[cl % len(CLUSTER_COLORS)],
            label=f"C{cl}: {info['size']} papers, {frame_label} "
                  f"(~{info['mean_year']:.0f})"
        ))
    ax_dec.legend(handles=patches, fontsize=8, loc="upper left",
                  title="Leiden Clusters", title_fontsize=9)

    ax_dec.set_title(f"Citation Network — {decade}s\n"
                     f"({len(G_cc):,} nodes, {G_cc.number_of_edges():,} edges, "
                     f"{n_clusters} clusters, modularity={partition.modularity:.2f})",
                     fontsize=13, fontweight="bold")
    ax_dec.axis("off")

    save_fig(fig_dec, PUB / f"fig_citation_decade_{decade}.png")
    plt.close(fig_dec)
    print(f"\n  Saved fig_citation_decade_{decade}.png")

    # ── Overview panel ──
    ax_ov = axes_ov[dec_idx]
    # Simplified version for overview
    nx.draw_networkx_edges(G_cc, pos, ax=ax_ov, alpha=0.02,
                           edge_color="grey", width=0.2)
    nx.draw_networkx_nodes(G_cc, pos, ax=ax_ov,
                           node_color=node_colors,
                           node_size=[s * 0.3 for s in node_sizes],
                           alpha=0.6, edgecolors="none")
    # Label top 5 only
    top5_labels = {pmid: pmid_data.get(pmid, {}).get("title", "")[:20]
                   for pmid, _ in top15[:5]}
    nx.draw_networkx_labels(G_cc, pos, labels=top5_labels, ax=ax_ov,
                            font_size=4, font_weight="bold")

    ax_ov.set_title(f"{decade}s\n{len(G_cc):,} nodes, {n_clusters} clusters",
                    fontsize=11, fontweight="bold")
    ax_ov.axis("off")

# Save overview
fig_overview.suptitle("Citation Network Evolution Across Decades (Leiden Clustering)",
                      fontsize=14, fontweight="bold", y=1.02)
fig_overview.tight_layout()
save_fig(fig_overview, PUB / "fig_citation_decades_overview.png")
plt.close(fig_overview)
print(f"\nSaved fig_citation_decades_overview.png")

# ═══════════════════════════════════════════════════════════════════════════
# SAVE COMMUNITY DATA
# ═══════════════════════════════════════════════════════════════════════════
if all_community_data:
    comm_df = pd.DataFrame(all_community_data)
    comm_df.to_csv(PROC / "citation_communities_per_decade.csv", index=False)
    print(f"Saved citation_communities_per_decade.csv ({len(comm_df):,} rows)")

# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY: KEY TRANSITION PAPERS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TRANSITION ANALYSIS: Papers bridging decades")
print("=" * 70)

# Find papers that are highly cited by BOTH their own decade and the next
for i in range(len(DECADES) - 1):
    d1, d2 = DECADES[i], DECADES[i+1]
    d1_pmids = set(str(int(row["pmid"])) for _, row in cit.iterrows()
                   if int(row.get("decade", 0)) == d1)

    bridge_scores = {}
    for pmid in d1_pmids:
        if not G_full.has_node(pmid):
            continue
        # Count citations from own decade and next decade
        own_dec_cit = 0
        next_dec_cit = 0
        for source, _ in G_full.in_edges(pmid):
            s_year = pmid_data.get(source, {}).get("year", 0)
            if s_year > 0:
                s_dec = (s_year // 10) * 10
                if s_dec == d1:
                    own_dec_cit += 1
                elif s_dec == d2:
                    next_dec_cit += 1
        if own_dec_cit >= 5 and next_dec_cit >= 5:
            bridge_scores[pmid] = own_dec_cit + next_dec_cit

    if bridge_scores:
        top_bridges = sorted(bridge_scores.items(), key=lambda x: x[1], reverse=True)[:5]
        print(f"\n  Bridging {d1}s → {d2}s:")
        for pmid, score in top_bridges:
            data = pmid_data.get(pmid, {})
            frame = data.get("frame", "—")
            if not frame or frame == "nan": frame = "—"
            title = data.get("title", "")[:55]
            print(f"    [{score:>4} cross-dec cit] {frame:12s} | {title}")

print("\nDone.")
