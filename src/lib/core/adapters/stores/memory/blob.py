# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""In-memory :class:`BlobStore` for unit tests and local runs."""

from __future__ import annotations

from src.lib.core.adapters.stores.base.blob import BlobStore

__all__ = ["InMemoryBlobStore"]


class InMemoryBlobStore(BlobStore):
    """Dict-backed :class:`BlobStore`.

    Lets a unit test inject ``{key: bytes}`` and exercise a key-based endpoint
    with no MinIO mocking — the claim-check analogue of ``files={...}``.
    """

    def __init__(self, initial: dict[str, bytes] | None = None) -> None:
        self._data: dict[str, bytes] = dict(initial or {})

    def get(self, key: str) -> bytes:
        try:
            return self._data[key]
        except KeyError:
            raise KeyError(f"blob not found: {key!r}") from None

    def put(
        self, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> None:
        self._data[key] = data

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def exists(self, key: str) -> bool:
        return key in self._data
