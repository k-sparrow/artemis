"""Transactional outbox for task-state visibility.

``ingestion_status`` is the single source of truth for both task-state
visibility (``ingestion_tasks``) and object visibility (``ingested_objects``)
via CDC — see ``tools/oci/images/ksqldb/artemis_init.ksql``. One row per
*contract* ``task_id`` — the id the storage service itself generates and
returns to its caller (see ``ingest()``'s own docstring for the full
provenance chain) — created once by ``ingest()``, updated in place by every
task via ``OutboxTask``'s ``before_start``/``on_success``/``on_failure``
hooks (see ``tasks.py``).

Unlike ``parse_stage_state`` (ephemeral, claim-and-delete), rows here are
never deleted — the same permanent-history lifecycle as today's
``ingestion_tasks`` table.
"""

from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy.orm import Session

from src.lib.backend.db.base import Base

__all__ = [
    "IngestionStatus",
    "create_status_row",
    "update_stage",
    "mark_success",
    "mark_failure",
]

# Bound on failure_reason so a multi-KB traceback message never bloats the
# column / CDC payload — the full traceback still lives in worker logs.
_FAILURE_REASON_MAX = 2000


class IngestionStatus(Base):
    """One row per contract task_id, updated in place through the pipeline."""

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


def create_status_row(
    session: Session,
    *,
    task_id: str,
    namespace_id: uuid.UUID,
    obj_id: uuid.UUID | None,
    source: str | None,
    object_type: str | None,
    content_type: str | None,
    size_bytes: int | None,
    group_id: uuid.UUID | None,
    operation: str,
) -> None:
    """The sole INSERT for a pipeline run — called once by ``ingest()``,
    before dispatching ``parse``/``delete_document``. Every later task only
    ever UPDATEs this row (``update_stage``/``mark_success``/``mark_failure``
    below) — see ``OutboxTask`` in ``tasks.py``.
    """
    session.execute(
        sa.insert(IngestionStatus).values(
            task_id=task_id,
            namespace_id=namespace_id,
            obj_id=obj_id,
            source=source,
            object_type=object_type,
            content_type=content_type,
            size_bytes=size_bytes,
            group_id=group_id,
            operation=operation,
            stage="tasks.ingest",
            status="running",
        )
    )
    session.commit()


def update_stage(session: Session, *, task_id: str, stage: str) -> None:
    """Advance ``stage`` to the calling task's own name.

    Called from ``OutboxTask.before_start``, gated there on
    ``self.request.retries == 0`` — the primary defense against turning
    ``poll_parse``/``poll_chunk``'s up-to-1440-retry budget into 1440 writes
    per document. The ``IS DISTINCT FROM`` guard here is a defensive
    backstop only (covers ``acks_late`` redelivery of a crashed first
    attempt, same ``retries == 0`` on redelivery), not the primary mechanism
    — it prevents the WAL write/CDC event, not the round-trip itself.
    """
    session.execute(
        sa.update(IngestionStatus)
        .where(
            IngestionStatus.task_id == task_id,
            IngestionStatus.stage.is_distinct_from(stage),
        )
        .values(stage=stage, updated_at=sa.func.now())
    )
    session.commit()


def mark_success(session: Session, *, task_id: str) -> None:
    """Terminal SUCCESS write — called only from tasks using
    ``TerminalOutboxTask`` (``index``, ``delete_document``)."""
    session.execute(
        sa.update(IngestionStatus)
        .where(IngestionStatus.task_id == task_id)
        .values(status="success", updated_at=sa.func.now())
    )
    session.commit()


def mark_failure(session: Session, *, task_id: str, failure_reason: str) -> None:
    """Terminal FAILURE write — called from ``OutboxTask.on_failure``/
    ``record_failure`` on every task using the outbox base."""
    session.execute(
        sa.update(IngestionStatus)
        .where(IngestionStatus.task_id == task_id)
        .values(
            status="failure",
            failure_reason=failure_reason[:_FAILURE_REASON_MAX],
            updated_at=sa.func.now(),
        )
    )
    session.commit()
