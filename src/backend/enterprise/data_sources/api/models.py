"""SQLAlchemy model for the data_source control plane table."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from src.lib.backend.db.base import Base

# Stable RFC 4122 DNS namespace — same UUID used by storage service and enterprise ingest.
# Must never change once data exists.
ARTEMIS_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


class DataSource(Base):
    """Persisted record of a registered data source connector.

    One row per Kafka Connect connector. The connector_name is the authoritative
    handle used for all Kafka Connect API calls.

    namespace_id is a logical reference only — no FK to the storage service DB.
    """

    __tablename__ = "data_source"

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    name: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        comment="Human-readable display label",
    )
    source_type: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        comment="'filesystem' | 'github-pr' | 'github-pr-comment'",
    )
    connector_name: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        unique=True,
        comment="Kafka Connect connector name: artemis-{id}",
    )
    namespace_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        nullable=False,
        comment="Logical ref to namespace on storage service — no hard FK",
    )
    org_name: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        comment="Used to derive owner_id in the enterprise ingest sink",
    )
    config: Mapped[dict] = mapped_column(
        sa.JSON,
        nullable=False,
        comment="Source-type-specific connector params (path, repo, etc.)",
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        comment="Soft-delete timestamp",
    )
