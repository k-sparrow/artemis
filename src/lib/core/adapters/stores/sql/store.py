# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""SQL-backed DocumentIndex.

:class:`SQLDocumentIndex` extends :class:`StoreDocumentIndex` with SQL
schema management (create / drop) and engine-based factory constructors.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, TypeVar, Union

from langchain_community.storage.sql import SQLStore
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from src.lib.core.adapters.stores.base import StoreDocumentIndex

__all__ = ["SQLDocumentIndex"]


IDX = TypeVar("IDX", bound="SQLDocumentIndex")


class SQLDocumentIndex(StoreDocumentIndex):
    """A :class:`StoreDocumentIndex` backed by an :class:`SQLStore`.

    Adds schema lifecycle methods (``create_schema``, ``acreate_schema``,
    ``drop``) and convenience constructors (:meth:`from_engine`,
    :meth:`from_db_uri`).

    Schema creation is called automatically before every mutating operation
    via the :meth:`_setup` / :meth:`_asetup` hooks, so callers do not need
    to invoke it explicitly.

    Attributes:
        store: The underlying ``SQLStore`` (narrows ``StoreDocumentIndex.store``).
        id_kwd: Metadata key used to resolve document IDs.  Defaults to
            ``"doc_id"``.
    """

    store: SQLStore  # type: ignore[assignment]  # narrows ByteStore → SQLStore

    # ------------------------------------------------------------------
    # Subclass hooks — ensure SQL schema exists before every operation
    # ------------------------------------------------------------------

    def _setup(self) -> None:
        self.store.create_schema()

    async def _asetup(self) -> None:
        await self.store.acreate_schema()

    # ------------------------------------------------------------------
    # Schema lifecycle helpers (explicit API for callers that need them)
    # ------------------------------------------------------------------

    def create_schema(self) -> None:
        """Create the SQL table if it does not already exist."""
        self.store.create_schema()

    async def acreate_schema(self) -> None:
        """Async version of :meth:`create_schema`."""
        await self.store.acreate_schema()

    def drop(self) -> None:
        """Drop the SQL table and all its contents.  Primarily for tests."""
        self.store.drop()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_engine(
        cls: type[IDX],
        namespace: str,
        engine: Engine | AsyncEngine,
        id_kwd: str = "doc_id",
    ) -> IDX:
        """Construct from an existing SQLAlchemy engine.

        Args:
            namespace: Table namespace passed to :class:`SQLStore`.
            engine: Sync or async SQLAlchemy engine.
            id_kwd: Metadata key used to resolve document IDs.
        """
        store = SQLStore(
            namespace=namespace,
            engine=engine,
            async_mode=isinstance(engine, AsyncEngine),
        )
        return cls(store=store, id_kwd=id_kwd)

    @classmethod
    def from_db_uri(
        cls: type[IDX],
        namespace: str,
        db_url: Optional[Union[str, Path]],
        engine_kwargs: Optional[Dict[str, Any]] = None,
        async_mode: Optional[bool] = None,
        id_kwd: str = "doc_id",
    ) -> IDX:
        """Construct from a database URL string.

        Args:
            namespace: Table namespace passed to :class:`SQLStore`.
            db_url: SQLAlchemy-compatible database URL.
            engine_kwargs: Extra kwargs forwarded to the engine constructor.
            async_mode: Force sync or async mode.  Inferred from the URL
                driver when ``None``.
            id_kwd: Metadata key used to resolve document IDs.
        """
        store = SQLStore(
            namespace=namespace,
            db_url=db_url,
            engine_kwargs=engine_kwargs,
            async_mode=async_mode,
        )
        return cls(store=store, id_kwd=id_kwd)
