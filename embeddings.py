"""
Sentence-transformer embedding utilities with NumPy caching.

Usage:
    from embeddings import embed_texts, load_embeddings
    vecs = embed_texts(texts, cache_path="Data/processed/embeddings.npy")
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Sequence

import numpy as np

# Lazy-loaded model cache (avoids import-time GPU allocation)
_MODEL_CACHE: dict[str, object] = {}


def get_model(model_name: str = "all-MiniLM-L6-v2"):
    """
    Lazy-load and cache a SentenceTransformer model.

    Returns the cached instance on subsequent calls with the same name.
    """
    if model_name not in _MODEL_CACHE:
        from sentence_transformers import SentenceTransformer
        _MODEL_CACHE[model_name] = SentenceTransformer(model_name)
    return _MODEL_CACHE[model_name]


def embed_texts(
    texts: Sequence[str],
    model_name: str = "all-MiniLM-L6-v2",
    cache_path: Optional[str | Path] = None,
    batch_size: int = 256,
) -> np.ndarray:
    """
    Encode *texts* into dense vectors, with optional on-disk caching.

    If *cache_path* exists and its first dimension matches ``len(texts)``,
    the cached array is returned immediately.  Otherwise the model encodes
    the texts, saves the result to *cache_path* (when given), and returns it.

    Parameters
    ----------
    texts      : sequence of strings to embed
    model_name : SentenceTransformer model identifier
    cache_path : .npy file for caching (None = no caching)
    batch_size : encoding batch size

    Returns
    -------
    np.ndarray of shape (len(texts), embedding_dim)
    """
    if cache_path is not None:
        cache_path = Path(cache_path)
        if cache_path.exists():
            cached = np.load(cache_path)
            if cached.shape[0] == len(texts):
                return cached

    model = get_model(model_name)
    embeddings = model.encode(
        list(texts),
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
    )
    embeddings = np.asarray(embeddings, dtype=np.float32)

    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(cache_path, embeddings)
        print(f"  Saved embeddings {embeddings.shape} -> {cache_path}")

    return embeddings


def load_embeddings(path: str | Path) -> np.ndarray:
    """Load a NumPy embedding matrix from *path*."""
    return np.load(Path(path))


def save_embeddings(embeddings: np.ndarray, path: str | Path) -> None:
    """Save an embedding matrix to *path* (.npy)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.save(path, embeddings)
