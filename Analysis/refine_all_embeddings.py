"""
refine_all_embeddings.py — Apply the contrastive NN to all three embedding models.

Reuses `train_contrastive()` from domain_clustering_pipeline.py but exposes it
as a loop over {SPECTER2, PubMedBERT, MPNet}. Each refined embedding set is
cached to `{model}_refined_embeddings.npy` so reruns are cheap.

Produces:
  Data/processed/{specter2,pubmedbert,mpnet}_refined_embeddings.npy   (14,947 × 64)
  Data/processed/contrastive_model_{model}.pt                          (weights)
  Data/processed/contrastive_training_{model}.csv                      (loss curves)

Usage:
    python analysis/refine_all_embeddings.py                # all three models
    python analysis/refine_all_embeddings.py --models mpnet # subset
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

MODELS = ["specter2", "pubmedbert", "mpnet"]


def train_contrastive_for(model_tag, n_epochs=2000, batch_size=256, lr=1e-4,
                           temperature=0.1, pos_thresh=0.85, neg_thresh=0.75,
                           corr_weight=0.1, l2_weight=0.01, log_every=50):
    """Train contrastive NN on one model's raw embeddings.

    Architecture and loss are identical to domain_clustering_pipeline.train_contrastive,
    but exposed here per-model with a per-run loss log saved to CSV.
    """
    raw_path = PROC / f"{model_tag}_embeddings.npy"
    out_path = PROC / f"{model_tag}_refined_embeddings.npy"
    weights_path = PROC / f"contrastive_model_{model_tag}.pt"
    log_path = PROC / f"contrastive_training_{model_tag}.csv"

    if not raw_path.exists():
        print(f"  [SKIP {model_tag}] missing {raw_path.name}")
        return None
    if out_path.exists():
        print(f"  [CACHED {model_tag}] {out_path.name} already exists "
              f"(shape {np.load(out_path).shape})")
        return np.load(out_path)

    import torch
    import torch.nn as nn

    raw_embeddings = np.load(raw_path).astype(np.float32)
    norms = np.linalg.norm(raw_embeddings, axis=1, keepdims=True)
    raw_embeddings = raw_embeddings / (norms + 1e-10)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    input_dim = raw_embeddings.shape[1]
    print(f"\n  [{model_tag}] device={device} input_dim={input_dim} "
          f"n_papers={len(raw_embeddings):,}")
    print(f"  [{model_tag}] training {n_epochs} epochs, bs={batch_size}, lr={lr}")

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.fc1 = nn.Linear(input_dim, 512)
            self.bn1 = nn.BatchNorm1d(512)
            self.fc2 = nn.Linear(512, 256)
            self.bn2 = nn.BatchNorm1d(256)
            self.fc3 = nn.Linear(256, 128)
            self.fc4 = nn.Linear(128, 64)
            self.elu = nn.ELU()
            self.dropout = nn.Dropout(0.1)

        def forward(self, x):
            x = self.elu(self.bn1(self.fc1(x)))
            x = self.dropout(x)
            x = self.elu(self.bn2(self.fc2(x)))
            x = self.dropout(x)
            x = self.elu(self.fc3(x))
            x = self.fc4(x)
            return x / (x.norm(dim=1, keepdim=True) + 1e-10)

    model = Net().to(device)
    n = len(raw_embeddings)
    rng = np.random.RandomState(42)
    perm = rng.permutation(n)
    n_val = int(n * 0.05)
    val_idx = perm[:n_val]
    train_idx = perm[n_val:]

    raw_t = torch.from_numpy(raw_embeddings)
    train_x = raw_t[train_idx].to(device)
    val_x = raw_t[val_idx].to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=l2_weight)
    scheduler = torch.optim.lr_scheduler.ExponentialLR(optimizer, gamma=0.995)

    grokfast_ema = None
    grokfast_alpha = 0.98
    grokfast_lamb = 2.0

    def info_nce_loss(refined_batch, raw_batch):
        raw_sim = raw_batch @ raw_batch.t()
        refined_sim = refined_batch @ refined_batch.t() / temperature
        pos_mask = (raw_sim >= pos_thresh).float()
        pos_mask = pos_mask - torch.eye(pos_mask.size(0), device=pos_mask.device)
        pos_mask = torch.clamp(pos_mask, min=0)
        neg_mask = (raw_sim < neg_thresh).float()
        exp_sim = torch.exp(refined_sim)
        pos_term = (exp_sim * pos_mask).sum(dim=1)
        neg_term = (exp_sim * neg_mask).sum(dim=1) + 1.0
        has_pos = pos_mask.sum(dim=1) > 0
        if has_pos.sum() == 0:
            return torch.tensor(0.0, device=refined_sim.device, requires_grad=True)
        loss = -torch.log((pos_term + 1e-10) / (pos_term + neg_term + 1e-10))
        return (loss * has_pos.float()).sum() / has_pos.sum()

    def correlation_loss(refined):
        r = refined - refined.mean(dim=0, keepdim=True)
        cov = r.t() @ r / (r.size(0) - 1)
        std = r.std(dim=0)
        corr = cov / (std.unsqueeze(0) * std.unsqueeze(1) + 1e-10)
        n_dim = corr.size(0)
        mask = 1.0 - torch.eye(n_dim, device=corr.device)
        return (corr.abs() * mask).sum() / (n_dim * (n_dim - 1))

    t0 = time.time()
    best_val = float("inf")
    history = []

    for epoch in range(n_epochs):
        model.train()
        batch_idx = torch.randperm(len(train_x))[:batch_size]
        raw_batch = train_x[batch_idx]
        refined = model(raw_batch)
        l_contrast = info_nce_loss(refined, raw_batch)
        l_corr = correlation_loss(refined)
        loss = l_contrast + corr_weight * l_corr

        optimizer.zero_grad()
        loss.backward()

        if grokfast_ema is None:
            grokfast_ema = {n: p.grad.clone() for n, p in model.named_parameters()
                            if p.grad is not None}
        else:
            for name, param in model.named_parameters():
                if param.grad is None:
                    continue
                grokfast_ema[name] = (grokfast_alpha * grokfast_ema[name] +
                                      (1 - grokfast_alpha) * param.grad)
                param.grad = param.grad + grokfast_lamb * grokfast_ema[name]

        optimizer.step()
        scheduler.step()

        if (epoch + 1) % log_every == 0 or epoch == 0:
            model.eval()
            with torch.no_grad():
                vb = val_x[:batch_size]
                vr = model(vb)
                vl = info_nce_loss(vr, vb).item()
            history.append({
                "epoch": epoch + 1,
                "train_loss": float(loss.item()),
                "train_contrast": float(l_contrast.item()),
                "train_corr": float(l_corr.item()),
                "val_loss": float(vl),
                "lr": float(optimizer.param_groups[0]["lr"]),
            })
            if vl < best_val:
                best_val = vl
            if (epoch + 1) % (log_every * 4) == 0 or epoch == 0:
                print(f"    [{model_tag}] epoch {epoch+1}/{n_epochs}: "
                      f"train={loss.item():.4f} val={vl:.4f} "
                      f"(best={best_val:.4f}, {time.time()-t0:.0f}s)")

    print(f"  [{model_tag}] training done ({(time.time()-t0)/60:.1f} min, "
          f"best val={best_val:.4f})")

    model.eval()
    all_refined = []
    with torch.no_grad():
        for i in range(0, len(raw_t), batch_size):
            batch = raw_t[i:i+batch_size].to(device)
            all_refined.append(model(batch).cpu().numpy())
    refined_embeddings = np.vstack(all_refined).astype(np.float32)

    np.save(out_path, refined_embeddings)
    torch.save(model.state_dict(), weights_path)
    pd.DataFrame(history).to_csv(log_path, index=False)
    print(f"  [{model_tag}] saved {out_path.name} (shape {refined_embeddings.shape})")
    print(f"  [{model_tag}] saved weights {weights_path.name}, log {log_path.name}")
    return refined_embeddings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=MODELS,
                        choices=MODELS, help="Which models to refine")
    parser.add_argument("--epochs", type=int, default=2000)
    args = parser.parse_args()

    print("=" * 70)
    print(f"CONTRASTIVE REFINEMENT: {args.models}")
    print("=" * 70)
    for tag in args.models:
        train_contrastive_for(tag, n_epochs=args.epochs)
    print("\nAll done.")


if __name__ == "__main__":
    main()
