from __future__ import annotations

from dataclasses import dataclass, field

from qdrant_client import models

__all__ = [
    "PayloadIndexSpec",
    "QdrantCollectionConfig",
    "MULTI_TENANT_DENSE",
    "MULTI_TENANT_HYBRID",
    "MULTI_TENANT_MULTI_STAGE",
]


@dataclass(frozen=True)
class PayloadIndexSpec:
    field_name: str
    schema: models.PayloadSchemaType


@dataclass(frozen=True)
class QdrantCollectionConfig:
    distance: models.Distance = models.Distance.COSINE
    hnsw_config: models.HnswConfigDiff = field(
        default_factory=lambda: models.HnswConfigDiff(payload_m=16, m=0)
    )
    payload_indexes: tuple[PayloadIndexSpec, ...] = field(default_factory=tuple)
    sparse_vectors: dict[str, models.SparseVectorParams] = field(default_factory=dict)
    # Named multi-vectors (e.g. ColBERT). When non-empty, the dense vector is also
    # named ("dense") so Qdrant can hold multiple named vector spaces in one collection.
    multi_vectors: dict[str, models.VectorParams] = field(default_factory=dict)


_NAMESPACE_TENANT_INDEX = PayloadIndexSpec(
    field_name="metadata.namespace_id",
    schema=models.UuidIndexParams(
        type=models.UuidIndexType.UUID,
        is_tenant=True,
    ),
)

MULTI_TENANT_DENSE = QdrantCollectionConfig(
    payload_indexes=(_NAMESPACE_TENANT_INDEX,),
)

# Extends MULTI_TENANT_DENSE with a BM25 sparse vector index.
MULTI_TENANT_HYBRID = QdrantCollectionConfig(
    payload_indexes=(_NAMESPACE_TENANT_INDEX,),
    sparse_vectors={
        "sparse": models.SparseVectorParams(
            modifier=models.Modifier.IDF,
            index=models.SparseIndexParams(on_disk=False),
        ),
    },
)

# Three-vector two-stage retrieval: sparse+dense→RRF fusion (Stage 1) →
# ColBERT MaxSim reranking (Stage 2). ColBERTv2.0 produces 128-dim token vectors.
MULTI_TENANT_MULTI_STAGE = QdrantCollectionConfig(
    payload_indexes=(_NAMESPACE_TENANT_INDEX,),
    sparse_vectors={
        "sparse": models.SparseVectorParams(
            modifier=models.Modifier.IDF,
            index=models.SparseIndexParams(on_disk=False),
        ),
    },
    multi_vectors={
        "colbert": models.VectorParams(
            size=128,
            distance=models.Distance.COSINE,
            multivector_config=models.MultiVectorConfig(
                comparator=models.MultiVectorComparator.MAX_SIM,
            ),
        ),
    },
)
