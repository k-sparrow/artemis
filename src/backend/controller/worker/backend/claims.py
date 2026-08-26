"""Postgres claim-arbitration table for the parse-conversion callback race.

Once docling-serve can report a conversion result via an HTTP callback, both
``poll_parse`` (self-driven polling) and ``advance_from_callback``
(docling-serve-driven) can independently observe a terminal outcome for the
same document. ``parse_stage_state`` is an atomic claim row — exactly one of
the two ever wins :func:`try_claim` for a given ``(obj_id, stage)``, so the
rest of the pipeline is dispatched (or the failure recorded) exactly once no
matter which side gets there first, or whether only one of them fires at all.

Row lifecycle: absent -> unclaimed -> claimed (briefly) -> deleted by its own
winner, right after it finishes acting on the claim (see :mod:`tasks`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.backend.controller.lib.schemas import S3Details, SourceDetails
from src.lib.backend.db.base import Base

logger = logging.getLogger(__name__)

__all__ = [
    "ParseStageState",
    "ResumeContext",
    "ensure_stage_row",
    "backfill_docling_task_id",
    "try_claim",
    "delete_stage_row",
]


class ResumeContext(BaseModel):
    """Everything the tail chain (resolve_parse → submit_chunk → poll_chunk →
    index) needs to dispatch, captured once by submit_parse and carried in
    ``ParseStageState.resume_context`` — the winning claimant (poll_parse or
    advance_from_callback) has no chain kwargs of its own to fall back on.
    """

    namespace_id: str
    group_id: str | None = None
    task_id: str | None = None
    operation: str | None = None
    # Always "source" from this worker's own client today (call_parse_submit
    # always sends source_ref, never an inline file upload) — kept as a field
    # rather than hardcoded downstream so resolve_parse's request shape stays
    # self-contained in this one record.
    mode: str
    source: SourceDetails
    s3: S3Details


class ParseStageState(Base):
    """One in-flight parse stage awaiting either poll or callback dispatch."""

    __tablename__ = "parse_stage_state"

    obj_id = sa.Column(sa.Text, primary_key=True)
    stage = sa.Column(sa.Text, primary_key=True)
    docling_task_id = sa.Column(sa.Text, nullable=True)
    resume_context = sa.Column(JSONB, nullable=False)
    created_at = sa.Column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    claimed_at = sa.Column(sa.DateTime(timezone=True), nullable=True)
    claimed_by = sa.Column(sa.Text, nullable=True)


def ensure_stage_row(
    session: Session,
    *,
    obj_id: str,
    stage: str,
    resume_context: ResumeContext,
) -> None:
    """Idempotently insert the claim row. No-op if it already exists.

    Called once, from ``submit_parse``, before the docling-serve HTTP call —
    the sole write of ``resume_context``, closing the race where a very fast
    document's callback could otherwise fire before this row exists.
    """
    stmt = pg_insert(ParseStageState).values(
        obj_id=obj_id,
        stage=stage,
        resume_context=resume_context.model_dump(mode="json"),
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["obj_id", "stage"])
    session.execute(stmt)
    session.commit()


def backfill_docling_task_id(
    session: Session, *, obj_id: str, stage: str, docling_task_id: str
) -> None:
    """Fill in ``docling_task_id`` on ``poll_parse``'s first pass.

    Purely observational (the callback path gets its own copy fresh from the
    callback body, never reads this column) — a no-op UPDATE affecting zero
    rows if the claim row is somehow missing is not an error here; the
    subsequent :func:`try_claim` call is what surfaces that anomaly.
    """
    session.execute(
        sa.update(ParseStageState)
        .where(
            ParseStageState.obj_id == obj_id,
            ParseStageState.stage == stage,
            ParseStageState.docling_task_id.is_(None),
        )
        .values(docling_task_id=docling_task_id)
    )
    session.commit()


def try_claim(
    session: Session, *, obj_id: str, stage: str, claimant: str
) -> ResumeContext | None:
    """Atomically claim the row for ``(obj_id, stage)``; ``None`` if already claimed.

    Returns a validated ``ResumeContext`` (not the ORM row) — callers never
    need any other column, and returning the mapped entity itself would hand
    back an object bound to *this* function's session, which the caller's
    ``session_cleanup`` closes right after this returns; touching an expired
    attribute on it afterward raises ``DetachedInstanceError``. Selecting
    just the column via ``.returning(ParseStageState.resume_context)`` sides
    steps the whole ORM-instance-lifecycle question, and validating the raw
    JSONB dict back through the same model it was written with also catches
    a malformed/stale row with a clear Pydantic error instead of a bare
    ``KeyError`` deep inside whichever task dispatches the tail chain.

    ``UPDATE ... WHERE claimed_at IS NULL RETURNING`` is a Postgres-native
    compare-and-swap — exactly one concurrent caller ever gets a non-``None``
    result back for the same ``(obj_id, stage)``, regardless of whether the
    competing caller is another poll attempt or the callback path.

    ``claimant`` (the calling task's own ``self.request.id``, stable across a
    Celery ``self.retry()`` — retries reuse the same task id by default) also
    matches an ALREADY-claimed row when ``claimed_by`` equals the same value,
    so a task that wins the claim and then hits a transient failure before it
    can act on it re-affirms (not re-attempts) its own claim on retry, rather
    than reading the row as already lost to a different claimant.
    """
    stmt = (
        sa.update(ParseStageState)
        .where(
            ParseStageState.obj_id == obj_id,
            ParseStageState.stage == stage,
            sa.or_(
                ParseStageState.claimed_at.is_(None),
                ParseStageState.claimed_by == claimant,
            ),
        )
        .values(claimed_at=datetime.now(timezone.utc), claimed_by=claimant)
        .returning(ParseStageState.resume_context)
    )
    raw_resume_context = session.execute(stmt).scalars().one_or_none()
    session.commit()

    if raw_resume_context is None:
        # Ambiguous on its own: already claimed by the other path, or the row
        # is already gone. Both are expected, normal operation — the callback
        # path is almost always faster than poll_parse's ~60s cadence, so by
        # the time poll's next check runs the callback has typically already
        # won the claim AND deleted the row, making no_row the dominant
        # outcome here, not already_claimed. A no-op either way; only
        # observability differs.
        existing = session.execute(
            sa.select(ParseStageState.obj_id).where(
                ParseStageState.obj_id == obj_id, ParseStageState.stage == stage
            )
        ).scalar_one_or_none()
        if existing is not None:
            logger.info(
                "parse_stage_claim=already_claimed obj_id=%s stage=%s", obj_id, stage
            )
        else:
            logger.info("parse_stage_claim=no_row obj_id=%s stage=%s", obj_id, stage)
        return None

    return ResumeContext.model_validate(raw_resume_context)


def delete_stage_row(session: Session, *, obj_id: str, stage: str) -> None:
    """Delete the row after its winning claimant has finished acting on it."""
    session.execute(
        sa.delete(ParseStageState).where(
            ParseStageState.obj_id == obj_id, ParseStageState.stage == stage
        )
    )
    session.commit()
