"""MinIO intermediate store for parsed document chunks.

Used by the Celery worker chain to hand off ``List[ParsedChunk]`` between
the ``fetch_and_parse`` and ``index`` tasks without routing large payloads
through the Postgres result backend.

Design contract:
- ``save`` serialises chunks to JSON and uploads to MinIO; returns the object key.
- ``load`` downloads and deserialises; raises on any MinIO or validation error.
- ``delete`` removes the object after successful indexing.  On task failure the
  object is intentionally left in place for dead-letter inspection / replay.
"""

from __future__ import annotations

import io
import json
from typing import List

from minio import Minio
from pydantic import TypeAdapter

from src.lib.core.ingestion.types import ParsedChunk

__all__ = ["ParsedChunkStore"]

_adapter: TypeAdapter[List[ParsedChunk]] = TypeAdapter(List[ParsedChunk])


class ParsedChunkStore:
    """Thin wrapper around a MinIO bucket for ``List[ParsedChunk]`` payloads."""

    def __init__(self, client: Minio, bucket: str) -> None:
        self._client = client
        self._bucket = bucket
        self._ensure_bucket()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, chunks: List[ParsedChunk], task_id: str) -> str:
        """Serialise *chunks* to JSON and upload to MinIO.

        Returns the object key that can be passed to :meth:`load` or
        :meth:`delete`.
        """
        key = f"parsed-chunks/{task_id}.json"
        data = json.dumps([c.model_dump(mode="json") for c in chunks]).encode()
        self._client.put_object(
            self._bucket,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )
        return key

    def load(self, key: str) -> List[ParsedChunk]:
        """Download and deserialise a previously saved chunk list."""
        response = self._client.get_object(self._bucket, key)
        return _adapter.validate_json(response.read())

    def delete(self, key: str) -> None:
        """Remove the object from MinIO.  Called after successful indexing."""
        self._client.remove_object(self._bucket, key)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)
