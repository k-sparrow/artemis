from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from qdrant_client import models


__all__ = [
    "RetrievalParams",
    "build_qdrant_filter",
]


@dataclass(frozen=True)
class RetrievalParams:
    """Per-request retrieval configuration.

    ``namespace_id`` is mandatory — it scopes the query to a single tenant
    namespace.  ``group_id`` and ``doc_id`` are optional additional filters that
    narrow the result set further.  ``k`` controls how many chunks are returned.
    """

    namespace_id: UUID | str
    k: int = 10
    group_id: UUID | str | None = None
    doc_id: UUID | str | None = None


def build_qdrant_filter(params: RetrievalParams) -> models.Filter:
    """Build a Qdrant ``Filter`` from retrieval params.

    Always includes ``namespace_id``; optionally adds ``group_id`` and
    ``doc_id`` conditions when provided.
    """
    conditions: list[models.FieldCondition] = [
        models.FieldCondition(
            key="metadata.namespace_id",
            match=models.MatchValue(value=str(params.namespace_id)),
        )
    ]
    if params.group_id is not None:
        conditions.append(
            models.FieldCondition(
                key="metadata.group_id",
                match=models.MatchValue(value=str(params.group_id)),
            )
        )
    if params.doc_id is not None:
        conditions.append(
            models.FieldCondition(
                key="metadata.obj_id",
                match=models.MatchValue(value=str(params.doc_id)),
            )
        )
    return models.Filter(must=conditions)
