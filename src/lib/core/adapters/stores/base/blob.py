# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""BlobStore: the key → bytes claim-check data plane.

Producers (the parsing service) write artifacts under a key; consumers (the
indexing service) read them back by key — so large payloads never cross the
control plane (Celery messages / service HTTP bodies), only the small key does.

Sync methods are the contract: the MinIO client is synchronous. The async
variants simply offload to a worker thread so FastAPI handlers don't block the
event loop on blob I/O.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Callable

__all__ = ["BlobStore", "BlobStoreFactory"]


class BlobStore(ABC):
    """Abstract key → bytes object store."""

    @abstractmethod
    def get(self, key: str) -> bytes:
        """Return the bytes stored at *key*. Raises if absent."""

    @abstractmethod
    def put(
        self, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> None:
        """Store *data* at *key*, overwriting any existing object."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove *key*. Idempotent — a missing key is not an error."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return whether *key* is present."""

    # -- async convenience: offload the sync client to a thread -------------

    async def aget(self, key: str) -> bytes:
        return await asyncio.to_thread(self.get, key)

    async def aput(
        self, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> None:
        await asyncio.to_thread(self.put, key, data, content_type=content_type)

    async def adelete(self, key: str) -> None:
        await asyncio.to_thread(self.delete, key)

    async def aexists(self, key: str) -> bool:
        return await asyncio.to_thread(self.exists, key)


# A factory that yields a :class:`BlobStore` bound to a given bucket. Injected as
# a FastAPI dependency so source-read and artifact-write both go through one
# overridable seam — a unit test swaps in a fake for both with a single override.
BlobStoreFactory = Callable[[str], BlobStore]
