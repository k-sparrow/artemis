from langchain_qdrant import FastEmbedSparse
from langchain_qdrant.sparse_embeddings import SparseEmbeddings

__all__ = [
    "FastEmbedSparseEmbeddings",
    "SparseEmbeddings",
]


class FastEmbedSparseEmbeddings(FastEmbedSparse):
    """BM25 sparse embeddings running in-process via fastembed.

    Uses the Qdrant/bm25 model — a lookup-based BM25 implementation with no
    GPU requirement and sub-millisecond latency per query.
    """

    def __init__(self) -> None:
        super().__init__(model_name="Qdrant/bm25")
