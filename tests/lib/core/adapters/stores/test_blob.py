# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Unit tests for the BlobStore port via the in-memory implementation.

These cover the contract every BlobStore must satisfy (round-trip, overwrite,
idempotent delete, missing-key behaviour, and the async wrappers). The MinIO
impl is exercised separately at the integration layer.
"""

from __future__ import annotations

import asyncio

import pytest

from src.lib.core.adapters.stores.memory.blob import InMemoryBlobStore


class TestInMemoryBlobStore:
    def test_put_get_round_trip(self) -> None:
        store = InMemoryBlobStore()
        store.put("k", b"payload", content_type="application/json")
        assert store.get("k") == b"payload"

    def test_exists(self) -> None:
        store = InMemoryBlobStore()
        assert store.exists("k") is False
        store.put("k", b"x")
        assert store.exists("k") is True

    def test_overwrite(self) -> None:
        store = InMemoryBlobStore()
        store.put("k", b"one")
        store.put("k", b"two")
        assert store.get("k") == b"two"

    def test_delete_is_idempotent(self) -> None:
        store = InMemoryBlobStore()
        store.put("k", b"x")
        store.delete("k")
        assert not store.exists("k")
        store.delete("k")  # absent key → no error

    def test_get_missing_raises_keyerror(self) -> None:
        with pytest.raises(KeyError):
            InMemoryBlobStore().get("missing")

    def test_seed_from_initial(self) -> None:
        store = InMemoryBlobStore({"seed": b"data"})
        assert store.get("seed") == b"data"


class TestAsyncWrappers:
    """The async variants delegate to the sync methods via a worker thread."""

    def test_aput_aget_round_trip(self) -> None:
        store = InMemoryBlobStore()
        asyncio.run(store.aput("k", b"payload"))
        assert asyncio.run(store.aget("k")) == b"payload"

    def test_aexists_and_adelete(self) -> None:
        store = InMemoryBlobStore()
        asyncio.run(store.aput("k", b"x"))
        assert asyncio.run(store.aexists("k")) is True
        asyncio.run(store.adelete("k"))
        assert asyncio.run(store.aexists("k")) is False
