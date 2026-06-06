"""SQLAlchemy models for the namespace data model.

Ownership hierarchy
-------------------
Owner (1) ─< Namespace (1) ─< IngestedFile
  │
  └── populated lazily on namespace creation (simple mode)
      or via JDBC sink (enterprise mode)

Deletion lifecycle
------------------
1. Soft-delete: ``namespace.deleted_at`` set by storage service or DB trigger.
2. Debezium CDC detects the column change (enterprise) or the storage service dispatches
   directly (simple).
3. Worker hard-deletes rows in order:
       IngestedFile → Namespace (→ Owner if no remaining refs).

``IngestedFile`` is written exclusively by the JDBC sink (CDC from Celery result table).
The storage service only reads from it.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import StrEnum

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.lib.backend.db.base import Base

# Deterministic UUID5 namespace used to derive surrogate IDs from natural keys.
# Stable across all deployments — must never change once data exists.
ARTEMIS_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # RFC 4122 DNS namespace


class NamespaceType(StrEnum):
    PRIVATE = "private"
    SHARED = "shared"


class IngestionTaskType(StrEnum):
    CREATE = "create"
    MODIFY = "modify"
    DELETE = "delete"


class Owner(Base):
    """Thin ownership anchor.

    Not a user registry. Exists solely to:
    - provide an FK target for ``namespace.owner_id`` (``ON DELETE RESTRICT``)
    - serve as the Debezium CDC trigger point for the owner-deletion cascade

    Simple mode: upserted by the storage service on namespace creation.
    Enterprise mode: populated by a JDBC sink connector from IdP CDC events.
    """

    __tablename__ = "owner"

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        primary_key=True,
        comment="External IdP subject or org UUID — no FK to any identity table",
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        comment="Soft-delete; DB trigger cascades deleted_at to all owned namespaces",
    )

    namespaces: Mapped[list[Namespace]] = relationship(back_populates="owner")


class Namespace(Base):
    """Core data-separation unit.

    PRIVATE namespaces use a random UUID4 and represent a single user's chat session.
    SHARED namespaces use UUID5 derived from the natural project name and represent an
    enterprise project accessible to multiple users.
    """

    __tablename__ = "namespace"

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        primary_key=True,
        comment="UUID4 for PRIVATE; UUID5(ARTEMIS_NS, name) for SHARED",
    )
    name: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        comment=(
            "Display label. UUID5 seed for SHARED."
            "Auto-generated for PRIVATE if omitted."
        ),
    )
    type: Mapped[NamespaceType] = mapped_column(
        sa.Enum(
            NamespaceType,
            name="namespace_type",
            values_callable=lambda obj: [e.value for e in obj],
        ),
        nullable=False,
    )
    owner_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey("owner.id", ondelete="RESTRICT"),
        nullable=False,
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=True,
        comment=(
            "Soft-delete. Debezium watches this column "
            "to trigger downstream cleanup."
        ),
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

    owner: Mapped[Owner] = relationship(back_populates="namespaces")
    # passive_deletes=True: FK to namespace was dropped (0004 migration) so the DB
    # won't cascade. Without this flag the ORM would issue UPDATE…SET namespace_id=NULL
    # before the parent DELETE, which the NOT NULL constraint rejects. Orphaned rows
    # are intentional — Celery cleans them up asynchronously via the tombstone pipeline.
    objects: Mapped[list["IngestedObject"]] = relationship(
        back_populates="namespace", passive_deletes=True
    )


class IngestedObject(Base):
    """Object registry — one row per successfully ingested object.

    Written exclusively by the JDBC sink (SUCCESS events only, upsert on id).
    The storage service reads from this table; it never writes to it.
    """

    __tablename__ = "ingested_objects"

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="obj_id: uuid5(namespace_id, source) — stable object identity",
    )
    namespace_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey("namespace.id"),
        nullable=False,
        index=True,
    )
    source: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        comment="Display label: filename for user uploads, full fs path for enterprise",
    )
    object_type: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        comment="'file' for user uploads; open-ended for enterprise ingestion",
    )
    content_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    size_bytes: Mapped[int | None] = mapped_column(sa.BigInteger, nullable=True)
    group_id: Mapped[uuid.UUID | None] = mapped_column(
        sa.UUID(as_uuid=True),
        nullable=True,
        index=True,
        comment=(
            "Logical group owner: connector_id (enterprise) or namespace_id (private)"
        ),
    )

    namespace: Mapped[Namespace] = relationship(back_populates="objects")


class IngestionTask(Base):
    """Task history — one row per completed ingestion task (SUCCESS or FAILURE).

    Written exclusively by the JDBC sink (CDC from ``apollo_celery_taskmeta``).
    The storage service reads from this table; it never writes to it.
    """

    __tablename__ = "ingestion_tasks"

    task_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        primary_key=True,
        comment="Links to apollo_celery_taskmeta.task_id",
    )
    obj_id: Mapped[uuid.UUID | None] = mapped_column(
        sa.UUID(as_uuid=True),
        nullable=True,
        index=True,
        comment="NULL on FAILURE tasks that produced no object",
    )
    namespace_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        sa.ForeignKey("namespace.id"),
        nullable=False,
        index=True,
        comment=(
            "Denormalised from obj_id for efficient per-namespace "
            "queries and FAILURE lookup"
        ),
    )
    status: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        comment="'SUCCESS' or 'FAILURE'",
    )
    failure_reason: Mapped[str | None] = mapped_column(
        sa.Text,
        nullable=True,
        comment="Traceback excerpt on failure; NULL on SUCCESS",
    )
    completed_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        nullable=False,
        comment="date_done from Celery result",
    )
    operation: Mapped[str] = mapped_column(
        sa.Text,
        nullable=False,
        comment="'CREATE', 'MODIFY', or 'DELETE'",
    )

    namespace: Mapped[Namespace] = relationship()


# ---------------------------------------------------------------------------
# DB trigger DDL — soft-cascade Owner DELETE → Namespace.deleted_at
# ---------------------------------------------------------------------------
# This DDL is emitted by Alembic migrations in production.
# For local development (create_all), it is attached as a DDL event below.

_SOFT_CASCADE_DDL = [
    sa.DDL(
        """
        CREATE OR REPLACE FUNCTION soft_cascade_namespace_on_owner_delete()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE namespace
            SET deleted_at = now()
            WHERE owner_id = OLD.id
              AND deleted_at IS NULL;
            RETURN OLD;
        END;
        $$
        """
    ),
    sa.DDL("DROP TRIGGER IF EXISTS owner_soft_cascade ON owner"),
    sa.DDL(
        """
        CREATE TRIGGER owner_soft_cascade
        AFTER DELETE ON owner
        FOR EACH ROW EXECUTE FUNCTION soft_cascade_namespace_on_owner_delete()
        """
    ),
]

for _ddl in _SOFT_CASCADE_DDL:
    sa.event.listen(
        Base.metadata,
        "after_create",
        _ddl.execute_if(dialect="postgresql"),
    )
