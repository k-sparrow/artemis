"""SQLAlchemy model for the intake service's content-addressed dedup ledger."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from src.lib.backend.db.base import Base


class IntakeDedupLedger(Base):
    """One row per (namespace_id, canonical path, content hash) ever ingested.

    See dedup.py for the claim lifecycle and 0009_add_intake_dedup_ledger.py
    for why the key includes path (not just sha256).
    """

    __tablename__ = "intake_dedup_ledger"

    namespace_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True
    )
    path: Mapped[str] = mapped_column(
        sa.Text,
        primary_key=True,
        comment="Canonical (symlink-resolved) filesystem path",
    )
    sha256: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    task_id: Mapped[uuid.UUID | None] = mapped_column(
        sa.UUID(as_uuid=True),
        nullable=True,
        comment="Set after the claiming request's upload succeeds; NULL while in-flight",
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
