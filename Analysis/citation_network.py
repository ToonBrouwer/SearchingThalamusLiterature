"""
citation_network.py — Analysis 10: Citation network analysis.

Uses OpenAlex API (free, no key needed) to fetch citation data:
  - cited_by_count per paper
  - referenced_works (outgoing citations)
  - Build directed citation graph
  - Per-decade: top-cited papers with frame + nucleus annotations
  - Betweenness centrality (bridging papers)
  - Louvain community detection

Output:
  Data/processed/citation_data.csv
  Data/processed/citation_graph.graphml
  Graphs/pub/fig_citation_network.png
"""
import sys, io, os, re, time, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import requests
from pathlib import Path
from collections import Counter

from plotting import apply_style, save_fig, panel_label, despine, WONG

apply_style()

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
PUB  = ROOT / "Graphs" / "pub"
PUB.mkdir(parents=True, exist_ok=True)

OPENALEX_BASE = "https://api.openalex.org"
EMAIL = "e.a.brouwer2@uva.nl"  # polite pool
RATE_LIMIT = 0.12  # ~8 req/s (OpenAlex allows 10/s with email)
CACHE_FILE = PROC / "openalex_cache.json"
CITATION_CSV = PROC / "citation_data.csv"

# ═══════════════════════════════════════════════════════════════════════════
# LOAD CORPUS METADATA
# ═══════════════════════════════════════════════════════════════════════════
print("Loading corpus metadata...")
raw = pd.read_csv(
    ROOT / "Data" / "raw" / "pubmed" / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
    low_memory=False
)
# Load DOIs from download list
doi_df = pd.read_csv(PROC / "scihub_download_list.csv", usecols=["pmid", "doi"])
raw = raw.merge(doi_df, on="pmid", how="left")

# Load framing data for annotation
df = pd.read_csv(PROC / "all_sentences_final.csv",
                 usecols=["pmid", "frame_type", "year"], low_memory=False)
# Get dominant frame per paper
frame_per_paper = (
    df[df["frame_type"].notna() & (df["frame_type"] != "")]
    .groupby("pmid")["frame_type"]
    .agg(lambda x: x.value_counts().index[0])
    .rename("dominant_frame")
)

print(f"  Papers: {len(raw):,}")
print(f"  With DOI: {raw['doi'].notna().sum():,}")
print(f"  With frame annotation: {len(frame_per_paper):,}")

# ═══════════════════════════════════════════════════════════════════════════
# FETCH FROM OPENALEX API (with cache)
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("FETCHING CITATION DATA FROM OPENALEX")
print("=" * 70)

# Load cache
if CACHE_FILE.exists():
    cache = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    print(f"  Cache: {len(cache):,} entries")
else:
    cache = {}

session = requests.Session()
session.headers.update({"User-Agent": f"ThalFraming/1.0 (mailto:{EMAIL})"})


def fetch_openalex(pmid, doi=None):
    """Fetch paper metadata from OpenAlex by PMID or DOI."""
    key = str(pmid)
    if key in cache:
        return cache[key]

    # Try PMID first
    url = f"{OPENALEX_BASE}/works/pmid:{pmid}?mailto={EMAIL}"
    try:
        resp = session.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            result = _parse_openalex(data)
            cache[key] = result
            return result
    except Exception:
        pass

    # Fallback to DOI
    if doi and pd.notna(doi):
        url = f"{OPENALEX_BASE}/works/doi:{doi}?mailto={EMAIL}"
        try:
            resp = session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                result = _parse_openalex(data)
                cache[key] = result
                return result
        except Exception:
            pass

    cache[key] = None
    return None


def _parse_openalex(data):
    """Extract relevant fields from OpenAlex response."""
    return {
        "openalex_id": data.get("id", ""),
        "cited_by_count": data.get("cited_by_count", 0),
        "referenced_works": data.get("referenced_works", []),
        "publication_year": data.get("publication_year"),
        "cited_by_api_url": data.get("cited_by_api_url", ""),
    }


# Fetch all papers (with progress)
t0 = time.time()
results = []
n_found = 0
n_miss = 0

for i, (_, row) in enumerate(raw.iterrows()):
    pmid = int(row["pmid"])
    doi = row.get("doi")

    result = fetch_openalex(pmid, doi)
    if result:
        results.append({
            "pmid": pmid,
            "year": row["year"],
            "title": row["title"],
            "cited_by_count": result["cited_by_count"],
            "openalex_id": result["openalex_id"],
            "n_references": len(result["referenced_works"]),
        })
        n_found += 1
    else:
        results.append({
            "pmid": pmid,
            "year": row["year"],
            "title": row["title"],
            "cited_by_count": 0,
            "openalex_id": "",
            "n_references": 0,
        })
        n_miss += 1

    # Rate limit (only for non-cached requests)
    if str(pmid) not in cache or cache[str(pmid)] is None:
        time.sleep(RATE_LIMIT)

    if (i + 1) % 500 == 0:
        elapsed = time.time() - t0
        rate = (i + 1) / elapsed * 60
        print(f"  {i+1:,}/{len(raw):,} ({n_found:,} found, {n_miss:,} miss, "
              f"{rate:.0f}/min)")

        # Save cache periodically
        CACHE_FILE.write_text(json.dumps(cache, default=str), encoding="utf-8")

# Final cache save
CACHE_FILE.write_text(json.dumps(cache, default=str), encoding="utf-8")
elapsed = time.time() - t0
print(f"\n  Done: {n_found:,} found, {n_miss:,} missing ({elapsed/60:.1f} min)")

# ═══════════════════════════════════════════════════════════════════════════
# BUILD CITATION DATAFRAME
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("BUILDING CITATION ANALYSIS")
print("=" * 70)

cit_df = pd.DataFrame(results)
cit_df["decade"] = (cit_df["year"] // 10) * 10

# Add frame annotation
cit_df = cit_df.merge(frame_per_paper.reset_index(), on="pmid", how="left")

# Save
cit_df.to_csv(CITATION_CSV, index=False)
print(f"  Saved citation_data.csv ({len(cit_df):,} rows)")

# Stats
print(f"\n  Citation counts:")
print(f"    Total citations across corpus: {cit_df['cited_by_count'].sum():,}")
print(f"    Mean citations/paper: {cit_df['cited_by_count'].mean():.1f}")
print(f"    Median: {cit_df['cited_by_count'].median():.0f}")
print(f"    Max: {cit_df['cited_by_count'].max():,}")
print(f"    Papers with >100 citations: {(cit_df['cited_by_count'] > 100).sum():,}")
print(f"    Papers with 0 citations: {(cit_df['cited_by_count'] == 0).sum():,}")

# ═══════════════════════════════════════════════════════════════════════════
# TOP PAPERS PER DECADE
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("TOP-CITED PAPERS PER DECADE")
print("=" * 70)

DECADES = [1980, 1990, 2000, 2010, 2020]
top_papers_per_decade = {}

for dec in DECADES:
    dec_papers = cit_df[cit_df["decade"] == dec].nlargest(10, "cited_by_count")
    top_papers_per_decade[dec] = dec_papers
    print(f"\n  {dec}s — top 5:")
    for _, row in dec_papers.head(5).iterrows():
        frame = row.get("dominant_frame", "—")
        title = str(row["title"])[:60]
        print(f"    [{row['cited_by_count']:>5}] {title}... ({frame})")

# ═══════════════════════════════════════════════════════════════════════════
# BUILD CITATION GRAPH (within-corpus citations)
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("BUILDING WITHIN-CORPUS CITATION GRAPH")
print("=" * 70)

# Map OpenAlex IDs to PMIDs for within-corpus links
oa_to_pmid = {}
for key, val in cache.items():
    if val and val.get("openalex_id"):
        oa_to_pmid[val["openalex_id"]] = int(key)

G = nx.DiGraph()

# Add nodes
for _, row in cit_df.iterrows():
    G.add_node(row["pmid"], year=row["year"], decade=row["decade"],
               cited_by_count=row["cited_by_count"],
               frame=row.get("dominant_frame", ""),
               title=str(row["title"])[:80])

# Add edges (within-corpus citations only)
n_edges = 0
for key, val in cache.items():
    if not val or not val.get("referenced_works"):
        continue
    pmid = int(key)
    for ref_id in val["referenced_works"]:
        if ref_id in oa_to_pmid:
            ref_pmid = oa_to_pmid[ref_id]
            if ref_pmid != pmid and G.has_node(ref_pmid):
                G.add_edge(pmid, ref_pmid)
                n_edges += 1

print(f"  Nodes: {G.number_of_nodes():,}")
print(f"  Within-corpus edges: {n_edges:,}")

# Save graph
nx.write_graphml(G, str(PROC / "citation_graph.graphml"))

# ═══════════════════════════════════════════════════════════════════════════
# CENTRALITY ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("CENTRALITY ANALYSIS")
print("=" * 70)

# In-degree (most cited within corpus)
in_deg = dict(G.in_degree())
top_in = sorted(in_deg.items(), key=lambda x: x[1], reverse=True)[:20]
print("\n  Top 20 most cited within corpus:")
for pmid, deg in top_in:
    node = G.nodes[pmid]
    frame = str(node.get('frame', '—') or '—')
    title = str(node.get('title', '') or '')[:50]
    print(f"    [{deg:>4}] PMID {pmid}  {node.get('year','')}  "
          f"frame={frame:15s}  {title}")

# Betweenness centrality (bridging papers) — on largest connected component
if n_edges > 100:
    print("\n  Computing betweenness centrality (may take a moment)...")
    # Use undirected version for betweenness
    G_undir = G.to_undirected()
    components = list(nx.connected_components(G_undir))
    largest = max(components, key=len)
    G_sub = G_undir.subgraph(largest)
    print(f"  Largest component: {len(largest):,} nodes")

    bc = nx.betweenness_centrality(G_sub, k=min(500, len(largest)))
    top_bc = sorted(bc.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\n  Top 10 bridging papers (betweenness centrality):")
    for pmid, cent in top_bc:
        node = G.nodes[pmid]
        frame = str(node.get('frame', '—') or '—')
        title = str(node.get('title', '') or '')[:50]
        print(f"    [{cent:.4f}] PMID {pmid}  {node.get('year','')}  "
              f"frame={frame:15s}  {title}")

# ═══════════════════════════════════════════════════════════════════════════
# COMMUNITY DETECTION (Louvain)
# ═══════════════════════════════════════════════════════════════════════════
if n_edges > 100:
    print(f"\n{'='*70}")
    print("COMMUNITY DETECTION (Louvain)")
    print("=" * 70)

    import community as community_louvain
    partition = community_louvain.best_partition(G_sub, random_state=42)
    n_communities = len(set(partition.values()))
    print(f"  Communities found: {n_communities}")

    # Characterize each community
    comm_frames = {}
    comm_decades = {}
    for comm_id in range(min(n_communities, 10)):
        members = [n for n, c in partition.items() if c == comm_id]
        frames = [G.nodes[n].get("frame", "") for n in members if G.nodes[n].get("frame")]
        decades = [G.nodes[n].get("decade", 0) for n in members if G.nodes[n].get("decade")]
        top_frame = Counter(frames).most_common(1)[0] if frames else ("—", 0)
        mean_decade = np.mean(decades) if decades else 0
        print(f"    Community {comm_id}: {len(members):,} papers, "
              f"dominant frame={top_frame[0]} ({top_frame[1]}), "
              f"mean decade={mean_decade:.0f}")

# ═══════════════════════════════════════════════════════════════════════════
# FIGURE: 4-panel citation analysis
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print("GENERATING FIGURES")
print("=" * 70)

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("Citation Network Analysis", fontsize=14, fontweight="bold", y=0.98)

# Panel A: Citation distribution per decade
ax = axes[0, 0]
for dec in DECADES:
    dec_data = cit_df[cit_df["decade"] == dec]["cited_by_count"]
    if len(dec_data) > 0:
        # Log scale, clip at 1
        dec_data = dec_data.clip(lower=1)
        ax.boxplot(dec_data.values, positions=[dec], widths=6,
                   showfliers=False, medianprops={"color": "red"})
ax.set_yscale("log")
ax.set_xlabel("Decade")
ax.set_ylabel("Citations (log scale)")
ax.set_title("A. Citation distribution per decade")
despine(ax)
panel_label(ax, "A")

# Panel B: Mean citations by frame type
ax = axes[0, 1]
frame_cit = cit_df[cit_df["dominant_frame"].notna()].groupby(
    "dominant_frame")["cited_by_count"].mean().sort_values(ascending=True)
frame_cit = frame_cit.tail(12)  # top 12
y_pos = np.arange(len(frame_cit))
ax.barh(y_pos, frame_cit.values, color=WONG["blue"], alpha=0.8)
ax.set_yticks(y_pos)
ax.set_yticklabels(frame_cit.index, fontsize=8)
ax.set_xlabel("Mean citations")
ax.set_title("B. Mean citations by dominant frame")
despine(ax)
panel_label(ax, "B")

# Panel C: Top-cited papers timeline
ax = axes[1, 0]
top100 = cit_df.nlargest(100, "cited_by_count")
colors = [WONG.get("blue") if f == "relay"
          else WONG.get("red") if f == "hub"
          else WONG.get("orange") if f == "gate"
          else WONG.get("grey")
          for f in top100["dominant_frame"].fillna("")]
ax.scatter(top100["year"], top100["cited_by_count"], c=colors,
           s=30, alpha=0.7, edgecolors="none")
ax.set_xlabel("Year")
ax.set_ylabel("Citations")
ax.set_title("C. Top 100 most-cited papers\n(blue=relay, red=hub, orange=gate)")
ax.set_yscale("log")
despine(ax)
panel_label(ax, "C")

# Panel D: Within-corpus citation density per decade
ax = axes[1, 1]
edges_per_decade = Counter()
for u, v in G.edges():
    src_dec = G.nodes[u].get("decade", 0)
    edges_per_decade[src_dec] += 1
papers_per_dec = cit_df.groupby("decade").size()
for dec in DECADES:
    density = edges_per_decade.get(dec, 0) / max(papers_per_dec.get(dec, 1), 1)
    ax.bar(dec, density, width=8, color=WONG["green"], alpha=0.85)
ax.set_xlabel("Decade")
ax.set_ylabel("Within-corpus citations per paper")
ax.set_title("D. Citation density within thalamus corpus")
despine(ax)
panel_label(ax, "D")

plt.tight_layout(rect=[0, 0, 1, 0.96])
save_fig(fig, PUB / "fig_citation_network.png")
print("  Saved fig_citation_network.png")

elapsed = time.time() - t0
print(f"\nDone in {elapsed/60:.1f} minutes.")
