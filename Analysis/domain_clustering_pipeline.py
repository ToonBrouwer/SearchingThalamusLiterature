"""
domain_clustering_pipeline.py — Paper-level domain clustering for thalamus abstracts.

Follows the methodology of "The Evolving Landscape of Neuroscience":
  1. Prepare abstracts (title + abstract concatenation)
  2. Embed with SPECTER2 (AllenAI, 768-dim scientific paper embedding)
  3. Train contrastive neural network (1024→512→256→128→64 with InfoNCE)
     Adapted to SPECTER2's 768 input dim.
  4. Build K-NN semantic similarity graph with FAISS (top 50)
  5. Leiden community detection with Constant Potts Model (CPM)
  6. Characterize clusters

Runs both RAW (768-dim SPECTER2) and REFINED (64-dim contrastive) clustering
for comparison.

Outputs:
  Data/processed/paper_abstracts.csv
  Data/processed/specter2_embeddings.npy
  Data/processed/specter2_refined_embeddings.npy
  Data/processed/domain_clusters_raw.csv
  Data/processed/domain_clusters_refined.csv
  Graphs/pub/fig_domain_*.png
"""
import sys, io, os, time, argparse
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROC = ROOT / "Data" / "processed"
RAW  = ROOT / "Data" / "raw" / "pubmed"
PUB  = ROOT / "Graphs" / "pub"
PROC.mkdir(parents=True, exist_ok=True)
PUB.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════════════
# PHASE 2: PREPARE ABSTRACTS
# ═══════════════════════════════════════════════════════════════════════════

def prepare_abstracts():
    print("=" * 70)
    print("PHASE 2: PREPARING ABSTRACTS")
    print("=" * 70)

    out_path = PROC / "paper_abstracts.csv"
    if out_path.exists():
        print(f"  Loading cached: {out_path}")
        return pd.read_csv(out_path, low_memory=False)

    raw = pd.read_csv(RAW / "pubmed_broad_title_only_1950_2026_2026-03-26.csv",
                       low_memory=False)
    print(f"  Total papers: {len(raw):,}")

    # Filter: must have abstract and title
    mask = (raw["abstract"].notna() &
            (raw["abstract"].str.len() >= 50) &
            raw["title"].notna())
    papers = raw[mask].copy()
    print(f"  Papers with valid title+abstract: {len(papers):,}")

    # Clean text
    papers["title"] = papers["title"].str.strip()
    papers["abstract"] = papers["abstract"].str.strip()

    # SPECTER2 format: title [SEP] abstract
    papers["text"] = papers["title"] + " [SEP] " + papers["abstract"]

    # Truncate very long texts (SPECTER2 max 512 tokens ≈ 2000 chars safe)
    papers["text"] = papers["text"].str[:2000]

    keep_cols = ["pmid", "year", "journal", "title", "abstract", "text"]
    keep_cols = [c for c in keep_cols if c in papers.columns]
    papers[keep_cols].to_csv(out_path, index=False)
    print(f"  Saved: {out_path}")
    return papers[keep_cols]


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 3: SPECTER2 EMBEDDING
# ═══════════════════════════════════════════════════════════════════════════

def embed_with_specter2(papers, batch_size=32):
    print("\n" + "=" * 70)
    print("PHASE 3: SPECTER2 EMBEDDING")
    print("=" * 70)

    emb_path = PROC / "specter2_embeddings.npy"
    pmid_path = PROC / "specter2_pmids.npy"

    if emb_path.exists() and pmid_path.exists():
        print(f"  Loading cached embeddings: {emb_path}")
        embeddings = np.load(emb_path)
        pmids = np.load(pmid_path)
        print(f"  Shape: {embeddings.shape}")
        return embeddings, pmids

    import torch
    from transformers import AutoTokenizer
    from adapters import AutoAdapterModel

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"  Device: {device}")

    print("  Loading SPECTER2 model...")
    tokenizer = AutoTokenizer.from_pretrained("allenai/specter2_base")
    model = AutoAdapterModel.from_pretrained("allenai/specter2_base")
    model.load_adapter("allenai/specter2", source="hf", load_as="specter2",
                        set_active=True)
    model.to(device)
    model.eval()

    print(f"  Embedding {len(papers):,} papers in batches of {batch_size}...")
    t0 = time.time()
    all_emb = []
    pmids = papers["pmid"].values

    with torch.no_grad():
        for i in range(0, len(papers), batch_size):
            batch_texts = papers["text"].iloc[i:i+batch_size].tolist()
            inputs = tokenizer(batch_texts, padding=True, truncation=True,
                                return_tensors="pt", max_length=512)
            inputs = {k: v.to(device) for k, v in inputs.items()}
            output = model(**inputs)
            # SPECTER2: use [CLS] token (first token) as embedding
            emb = output.last_hidden_state[:, 0, :].cpu().numpy()
            all_emb.append(emb)

            if (i + batch_size) % (batch_size * 20) == 0:
                elapsed = time.time() - t0
                rate = (i + batch_size) / elapsed
                eta = (len(papers) - i - batch_size) / rate
                print(f"    {i+batch_size:,} / {len(papers):,} "
                      f"({rate:.0f}/s, ETA {eta:.0f}s)")

    embeddings = np.vstack(all_emb).astype(np.float32)
    # L2-normalize for cosine similarity
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    embeddings = embeddings / (norms + 1e-10)

    np.save(emb_path, embeddings)
    np.save(pmid_path, pmids)
    print(f"  Saved: {emb_path} (shape {embeddings.shape})")
    print(f"  Total time: {(time.time()-t0)/60:.1f} min")

    return embeddings, pmids


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 4: CONTRASTIVE NEURAL NETWORK
# ═══════════════════════════════════════════════════════════════════════════

class ContrastiveEmbedder:
    """PyTorch neural network for contrastive embedding refinement."""

    def __init__(self, input_dim=768, hidden_dims=(512, 256, 128), output_dim=64):
        import torch
        import torch.nn as nn

        self.torch = torch
        self.nn = nn

        class Net(nn.Module):
            def __init__(self):
                super().__init__()
                self.fc1 = nn.Linear(input_dim, hidden_dims[0])
                self.bn1 = nn.BatchNorm1d(hidden_dims[0])
                self.fc2 = nn.Linear(hidden_dims[0], hidden_dims[1])
                self.bn2 = nn.BatchNorm1d(hidden_dims[1])
                self.fc3 = nn.Linear(hidden_dims[1], hidden_dims[2])
                self.fc4 = nn.Linear(hidden_dims[2], output_dim)
                self.elu = nn.ELU()
                self.dropout = nn.Dropout(0.1)

            def forward(self, x):
                x = self.elu(self.bn1(self.fc1(x)))
                x = self.dropout(x)
                x = self.elu(self.bn2(self.fc2(x)))
                x = self.dropout(x)
                x = self.elu(self.fc3(x))
                x = self.fc4(x)
                # L2 normalize
                x = x / (x.norm(dim=1, keepdim=True) + 1e-10)
                return x

        self.Net = Net


def train_contrastive(raw_embeddings, pmids, n_epochs=2000,
                      batch_size=256, lr=1e-4, temperature=0.1,
                      pos_thresh=0.85, neg_thresh=0.75,
                      corr_weight=0.1, l2_weight=0.01):
    """Train contrastive NN following the paper's methodology."""
    print("\n" + "=" * 70)
    print("PHASE 4: CONTRASTIVE NEURAL NETWORK")
    print("=" * 70)

    out_path = PROC / "specter2_refined_embeddings.npy"
    if out_path.exists():
        print(f"  Loading cached refined embeddings: {out_path}")
        return np.load(out_path)

    import torch
    from torch.utils.data import DataLoader, TensorDataset

    device = "cuda" if torch.cuda.is_available() else "cpu"
    input_dim = raw_embeddings.shape[1]
    print(f"  Device: {device}, input_dim: {input_dim}")
    print(f"  Training {n_epochs} epochs, batch={batch_size}, lr={lr}")

    # Build model
    embedder = ContrastiveEmbedder(input_dim=input_dim)
    model = embedder.Net().to(device)

    # Pre-compute similarity matrix for pair definition (chunked for memory)
    # We need sim(x_i, x_j) >= 0.85 for positives, < 0.75 for negatives
    # Rather than the full N×N matrix (too large), we'll compute on-the-fly per batch

    # Train/val split
    n = len(raw_embeddings)
    rng = np.random.RandomState(42)
    perm = rng.permutation(n)
    n_val = int(n * 0.05)
    val_idx = perm[:n_val]
    train_idx = perm[n_val:]
    print(f"  Train: {len(train_idx):,}, Val: {len(val_idx):,}")

    # Convert to tensors
    raw_t = torch.from_numpy(raw_embeddings.astype(np.float32))
    train_x = raw_t[train_idx].to(device)
    val_x = raw_t[val_idx].to(device)

    # Optimizer
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=l2_weight)
    scheduler = torch.optim.lr_scheduler.ExponentialLR(optimizer, gamma=0.995)

    # Grokfast — amplify slow gradients
    grokfast_ema = None
    grokfast_alpha = 0.98
    grokfast_lamb = 2.0

    t0 = time.time()
    best_val_loss = float("inf")

    def info_nce_loss(refined_batch, raw_batch):
        """Self-supervised multi-positive InfoNCE.
        Positive pairs: raw similarity >= pos_thresh
        Negative pairs: raw similarity < neg_thresh
        Intermediate: additive stability term
        """
        # Cosine similarity in raw space (defines positives/negatives)
        raw_sim = raw_batch @ raw_batch.t()  # already L2-normalized
        # Cosine similarity in refined space
        refined_sim = refined_batch @ refined_batch.t() / temperature

        # Positive mask: raw_sim >= pos_thresh, exclude self
        pos_mask = (raw_sim >= pos_thresh).float()
        pos_mask = pos_mask - torch.eye(pos_mask.size(0), device=pos_mask.device)
        pos_mask = torch.clamp(pos_mask, min=0)

        # Negative mask: raw_sim < neg_thresh
        neg_mask = (raw_sim < neg_thresh).float()

        # Compute per-sample InfoNCE
        exp_sim = torch.exp(refined_sim)
        # Numerator: sum over positives
        pos_term = (exp_sim * pos_mask).sum(dim=1)
        # Denominator: sum over negatives + stability term
        neg_term = (exp_sim * neg_mask).sum(dim=1) + 1.0  # +1 stability

        # Loss: -log(pos / (pos + neg))
        # Only for samples with at least one positive
        has_pos = pos_mask.sum(dim=1) > 0
        if has_pos.sum() == 0:
            return torch.tensor(0.0, device=refined_sim.device, requires_grad=True)
        loss = -torch.log((pos_term + 1e-10) /
                          (pos_term + neg_term + 1e-10))
        loss = (loss * has_pos.float()).sum() / has_pos.sum()
        return loss

    def correlation_loss(refined):
        """Penalize correlation between output dimensions."""
        r = refined - refined.mean(dim=0, keepdim=True)
        cov = r.t() @ r / (r.size(0) - 1)
        std = r.std(dim=0)
        corr = cov / (std.unsqueeze(0) * std.unsqueeze(1) + 1e-10)
        # Off-diagonal correlation magnitude
        n_dim = corr.size(0)
        mask = 1.0 - torch.eye(n_dim, device=corr.device)
        return (corr.abs() * mask).sum() / (n_dim * (n_dim - 1))

    print(f"  Starting training loop...")
    for epoch in range(n_epochs):
        model.train()
        # Sample random batch
        batch_idx = torch.randperm(len(train_x))[:batch_size]
        raw_batch = train_x[batch_idx]

        refined = model(raw_batch)
        l_contrast = info_nce_loss(refined, raw_batch)
        l_corr = correlation_loss(refined)
        loss = l_contrast + corr_weight * l_corr

        optimizer.zero_grad()
        loss.backward()

        # Grokfast
        if grokfast_ema is None:
            grokfast_ema = {n: p.grad.clone() for n, p in model.named_parameters()
                            if p.grad is not None}
        else:
            for name, param in model.named_parameters():
                if param.grad is None: continue
                grokfast_ema[name] = (grokfast_alpha * grokfast_ema[name] +
                                       (1 - grokfast_alpha) * param.grad)
                param.grad = param.grad + grokfast_lamb * grokfast_ema[name]

        optimizer.step()
        scheduler.step()

        # Log every 100 epochs
        if (epoch + 1) % 100 == 0 or epoch == 0:
            model.eval()
            with torch.no_grad():
                # Validation
                val_idx_b = torch.arange(min(batch_size, len(val_x)))
                val_batch = val_x[val_idx_b]
                val_refined = model(val_batch)
                val_loss = info_nce_loss(val_refined, val_batch).item()

            elapsed = time.time() - t0
            lr_now = optimizer.param_groups[0]["lr"]
            print(f"    Epoch {epoch+1}/{n_epochs}: "
                  f"train_loss={loss.item():.4f}, val_loss={val_loss:.4f}, "
                  f"lr={lr_now:.5f} ({elapsed:.0f}s)")
            if val_loss < best_val_loss:
                best_val_loss = val_loss

    print(f"  Training complete ({(time.time()-t0)/60:.1f} min)")
    print(f"  Best val loss: {best_val_loss:.4f}")

    # Generate refined embeddings for all papers
    print("  Generating refined embeddings for all papers...")
    model.eval()
    all_refined = []
    with torch.no_grad():
        for i in range(0, len(raw_t), batch_size):
            batch = raw_t[i:i+batch_size].to(device)
            refined = model(batch).cpu().numpy()
            all_refined.append(refined)
    refined_embeddings = np.vstack(all_refined)

    np.save(out_path, refined_embeddings)
    print(f"  Saved: {out_path} (shape {refined_embeddings.shape})")

    # Save model weights
    torch.save(model.state_dict(), PROC / "contrastive_model.pt")

    return refined_embeddings


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 5: LEIDEN CLUSTERING
# ═══════════════════════════════════════════════════════════════════════════

def leiden_cluster(embeddings, pmids, name="refined", k=50):
    """K-NN graph + Leiden clustering with CPM resolution sweep."""
    print("\n" + "=" * 70)
    print(f"PHASE 5: LEIDEN CLUSTERING ({name})")
    print("=" * 70)

    out_path = PROC / f"domain_clusters_{name}.csv"
    if out_path.exists():
        print(f"  Loading cached: {out_path}")
        return pd.read_csv(out_path)

    import faiss
    import igraph as ig
    import leidenalg

    # Ensure L2 normalized
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    embs = embeddings / (norms + 1e-10)
    embs = embs.astype(np.float32)
    n, d = embs.shape
    print(f"  Embedding shape: {embs.shape}")

    # FAISS K-NN search
    print(f"  Building FAISS index (IndexFlatIP)...")
    index = faiss.IndexFlatIP(d)  # Inner product on normalized = cosine
    index.add(embs)
    print(f"  Searching top-{k} neighbors for each paper...")
    t0 = time.time()
    D, I = index.search(embs, k + 1)  # +1 because first result is self
    print(f"  Search took {time.time()-t0:.0f}s")

    # Build edge list (skip self-loops)
    print(f"  Building igraph...")
    edges = []
    weights = []
    for i in range(n):
        for j_idx in range(1, k + 1):  # skip [0] which is self
            j = I[i, j_idx]
            if j != i:
                edges.append((i, int(j)))
                weights.append(float(D[i, j_idx]))

    g = ig.Graph(n=n, edges=edges, directed=True)
    g.es["weight"] = weights
    print(f"  Directed graph: {g.vcount()} nodes, {g.ecount()} edges")

    # Symmetrize (add bidirectional edges)
    g = g.as_undirected(combine_edges="max")
    print(f"  Undirected: {g.vcount()} nodes, {g.ecount()} edges")

    # Leiden with CPM at multiple resolutions
    print(f"  Running Leiden CPM resolution sweep...")
    resolutions = np.linspace(0.001, 1.0, 1000)
    best_mod = -np.inf
    best_partition = None
    best_res = None
    mod_history = []
    downward_count = 0

    t0 = time.time()
    for ri, res in enumerate(resolutions):
        partition = leidenalg.find_partition(
            g, leidenalg.CPMVertexPartition,
            resolution_parameter=res,
            weights="weight", seed=42,
        )
        mod = partition.modularity
        mod_history.append((res, mod, len(partition)))

        if mod > best_mod:
            best_mod = mod
            best_partition = partition
            best_res = res
            downward_count = 0
        else:
            downward_count += 1

        if ri % 50 == 0:
            print(f"    res={res:.4f}: mod={mod:.4f}, "
                  f"n_clusters={len(partition)} "
                  f"(best: res={best_res:.4f}, mod={best_mod:.4f})")

        # Early stopping: 5 consecutive drops
        if downward_count >= 30:  # More lenient
            print(f"    Early stop at res={res:.4f} (30 non-improving steps)")
            break

    print(f"  Resolution sweep took {time.time()-t0:.0f}s")
    print(f"  BEST: resolution={best_res:.4f}, "
          f"modularity={best_mod:.4f}, "
          f"n_clusters={len(best_partition)}")

    # Build output DataFrame
    memberships = best_partition.membership
    cluster_df = pd.DataFrame({
        "pmid": pmids,
        f"cluster_{name}": memberships,
    })
    cluster_df.to_csv(out_path, index=False)
    print(f"  Saved: {out_path}")

    # Cluster sizes
    sizes = pd.Series(memberships).value_counts()
    print(f"\n  Cluster size distribution:")
    print(f"    Total clusters: {len(sizes)}")
    print(f"    Largest 10 sizes: {sizes.head(10).tolist()}")
    print(f"    Clusters with >10 papers: {(sizes > 10).sum()}")
    print(f"    Clusters with >50 papers: {(sizes > 50).sum()}")

    # Save modularity history
    mod_df = pd.DataFrame(mod_history, columns=["resolution", "modularity", "n_clusters"])
    mod_df.to_csv(PROC / f"leiden_sweep_{name}.csv", index=False)

    return cluster_df


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main(phase=None):
    print("=" * 70)
    print("DOMAIN CLUSTERING PIPELINE")
    print("=" * 70)

    # Phase 2: Abstracts
    papers = prepare_abstracts()

    # Phase 3: SPECTER2 embedding
    if phase and phase < 3:
        return
    raw_emb, pmids = embed_with_specter2(papers)

    # Phase 4: Contrastive NN
    if phase and phase < 4:
        return
    refined_emb = train_contrastive(raw_emb, pmids)

    # Phase 5: Leiden clustering (both versions)
    if phase and phase < 5:
        return
    cluster_raw = leiden_cluster(raw_emb, pmids, name="raw")
    cluster_refined = leiden_cluster(refined_emb, pmids, name="refined")

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", type=int, default=None,
                        help="Stop after phase N (2=abstracts, 3=embed, "
                             "4=contrastive, 5=cluster)")
    args = parser.parse_args()
    main(phase=args.phase)
