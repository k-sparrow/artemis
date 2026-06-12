# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""MinIO-backed :class:`BlobStore`.

Generalises the byte handling previously hard-coded in ``ParsedChunkStore`` so
parsing and indexing can read/write arbitrary artifacts directly by key.
"""

from __future__ import annotations

import io

from minio import Minio
from minio.error import S3Error

from src.lib.core.adapters.stores.base.blob import BlobStore

__all__ = ["MinioBlobStore"]


class MinioBlobStore(BlobStore):
    """A :class:`BlobStore` over a single MinIO bucket."""

    def __init__(self, client: Minio, bucket: str) -> None:
        # Construction is pure (no I/O) so it is cheap to build per request in a
        # FastAPI dependency. Call :meth:`ensure_bucket` once at app startup for
        # writer buckets the service owns.
        self._client = client
        self._bucket = bucket

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist (call once at startup)."""
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def get(self, key: str) -> bytes:
        response = self._client.get_object(self._bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def put(
        self, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> None:
        self._client.put_object(
            self._bucket,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )

    def delete(self, key: str) -> None:
        self._client.remove_object(self._bucket, key)

    def exists(self, key: str) -> bool:
        try:
            self._client.stat_object(self._bucket, key)
            return True
        except S3Error:
            return False
