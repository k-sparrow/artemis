"""Shared SQLAlchemy models used by more than one backend service.

Home for any model that needs to be read (and/or written) from multiple
services — the SQLAlchemy counterpart of ``contract.py``'s shared Pydantic
types (``IngestionTaskDetails``/``SourceDetails``). A model that only ever
has one owning service belongs in that service's own ``models.py`` instead.
"""

from __future__ import annotations

import sqlalchemy as sa

from src.lib.backend.db.base import Base

__all__ = [
    "IngestionStatus",
]


class IngestionStatus(Base):
    """One row per contract task_id, updated in place through the pipeline.

    Written exclusively by the controller worker (see
    ``src.backend.controller.worker.backend.outbox`` for the
    create/update/mark-terminal helpers — only that module writes). Read
    directly by the storage service (task-state visibility, Epic 22) as a
    deliberate, scoped exception to this codebase's usual CDC-only
    cross-service read boundary — both services share the same physical
    Postgres database.
    """

    __tablename__ = "ingestion_status"

    task_id = sa.Column(sa.UUID(as_uuid=True), primary_key=True)
    namespace_id = sa.Column(sa.UUID(as_uuid=True), nullable=False)
    obj_id = sa.Column(sa.UUID(as_uuid=True), nullable=True)
    source = sa.Column(sa.Text, nullable=True)
    object_type = sa.Column(sa.Text, nullable=True)
    content_type = sa.Column(sa.Text, nullable=True)
    size_bytes = sa.Column(sa.BigInteger, nullable=True)
    group_id = sa.Column(sa.UUID(as_uuid=True), nullable=True)
    operation = sa.Column(sa.Text, nullable=False)
    stage = sa.Column(sa.Text, nullable=False)
    status = sa.Column(sa.Text, nullable=False)
    failure_reason = sa.Column(sa.Text, nullable=True)
    # Deliberately timezone-less (unlike parse_stage_state, which is never
    # CDC'd) — Debezium encodes a plain TIMESTAMP as epoch-microsecond BIGINT
    # (io.debezium.time.MicroTimestamp), which artemis_init.ksql's
    # FROM_UNIXTIME(x / 1000) pattern already handles correctly (the same
    # encoding the old apollo_celery_taskmeta.date_done used). TIMESTAMPTZ
    # would encode as an ISO-8601 STRING instead (ZonedTimestamp) — a
    # different, unproven downstream path for this pipeline.
    created_at = sa.Column(
        sa.DateTime(), nullable=False, server_default=sa.text("now()")
    )
    updated_at = sa.Column(
        sa.DateTime(), nullable=False, server_default=sa.text("now()")
    )
