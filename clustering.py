"""
BERTopic / HDBSCAN clustering utilities with sensible defaults.

Usage:
    from clustering import run_bertopic, get_topics_over_time
    model, topics, probs = run_bertopic(docs, embeddings)
"""

from __future__ import annotations
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
from umap import UMAP
from hdbscan import HDBSCAN
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer

# ── Default hyper-parameters ──────────────────────────────────────────────

UMAP_DEFAULTS = dict(
    n_components=5,
    n_neighbors=30,
    min_dist=0.05,
    metric="cosine",
    random_state=42,
)

HDBSCAN_DEFAULTS = dict(
    min_cluster_size=30,
    min_samples=5,
    metric="euclidean",
    cluster_selection_method="eom",
)


# ── Core fitting ──────────────────────────────────────────────────────────

def _make_model(
    min_topic_size: int = 30,
    nr_topics: str | int = "auto",
    seed_topics: Optional[list[list[str]]] = None,
) -> BERTopic:
    """Build a BERTopic instance with project defaults."""
    umap_model = UMAP(**UMAP_DEFAULTS)
    hdbscan_model = HDBSCAN(
        **{**HDBSCAN_DEFAULTS, "min_cluster_size": min_topic_size},
        prediction_data=True,
    )
    vectorizer = CountVectorizer(stop_words="english")

    kwargs = dict(
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer,
        min_topic_size=min_topic_size,
        nr_topics=nr_topics,
        calculate_probabilities=True,
        verbose=True,
    )
    if seed_topics is not None:
        kwargs["seed_topic_list"] = seed_topics

    return BERTopic(**kwargs)


def run_bertopic(
    docs: Sequence[str],
    embeddings: np.ndarray,
    min_topic_size: int = 30,
    nr_topics: str | int = "auto",
    seed_topics: Optional[list[list[str]]] = None,
    y: Optional[np.ndarray] = None,
) -> tuple[BERTopic, list[int], np.ndarray]:
    """
    Fit BERTopic with pre-computed embeddings.

    Parameters
    ----------
    docs           : list of document strings
    embeddings     : pre-computed embedding matrix (n_docs x dim)
    min_topic_size : minimum documents per topic / HDBSCAN cluster
    nr_topics      : target number of topics ("auto" or int)
    seed_topics    : optional seed topic word lists for guided modelling
    y              : optional document labels for semi-supervised mode

    Returns
    -------
    (topic_model, topics, probs)
    """
    model = _make_model(min_topic_size, nr_topics, seed_topics)
    topics, probs = model.fit_transform(docs, embeddings=embeddings, y=y)
    return model, topics, probs


def run_bertopic_semisupervised(
    docs: Sequence[str],
    embeddings: np.ndarray,
    labels: np.ndarray,
    min_topic_size: int = 30,
) -> tuple[BERTopic, list[int], np.ndarray]:
    """
    Semi-supervised BERTopic using the *y* parameter.

    *labels* should be an integer array where -1 means unlabelled.
    """
    return run_bertopic(
        docs, embeddings,
        min_topic_size=min_topic_size,
        nr_topics="auto",
        y=labels,
    )


# ── Post-hoc analyses ────────────────────────────────────────────────────

def get_topics_over_time(
    topic_model: BERTopic,
    docs: Sequence[str],
    timestamps: Sequence,
    nr_bins: int = 20,
):
    """
    Wrapper for ``topic_model.topics_over_time()``.

    Returns a DataFrame with columns [Topic, Words, Frequency, Timestamp].
    """
    return topic_model.topics_over_time(
        docs, timestamps, nr_bins=nr_bins,
    )


def get_hierarchical_topics(topic_model: BERTopic):
    """
    Wrapper for ``topic_model.hierarchical_topics()``.

    Returns a DataFrame describing the topic merge tree.
    """
    return topic_model.hierarchical_topics(topic_model.original_documents_)


# ── Persistence ───────────────────────────────────────────────────────────

def save_topic_model(model: BERTopic, path: str | Path) -> None:
    """Save a fitted BERTopic model to *path*."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    model.save(str(path))
    print(f"  Saved topic model -> {path}")


def load_topic_model(path: str | Path) -> BERTopic:
    """Load a previously saved BERTopic model from *path*."""
    return BERTopic.load(str(path))
