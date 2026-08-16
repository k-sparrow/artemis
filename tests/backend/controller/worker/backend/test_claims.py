"""Per-service integration tests: claims.py's atomic claim arbitration against
a real Postgres. No other service image — just this worker's own claims table.

This is the layer that matters most for backend/claims.py: try_claim's whole
purpose is to guarantee exactly one winner when poll_parse and
advance_from_callback race on the same (obj_id, stage) row, and that guarantee
only means anything under real concurrent transactions, not a mocked-out
session. See tasks.py's module docstring and claims.py's own docstring for the
race this arbitrates.
"""

from __future__ import annotations

import threading
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker

from src.backend.controller.worker.backend import claims

pytestmark = pytest.mark.integration

_STAGE = "convert"


def _resume_context(obj_id: str) -> claims.ResumeContext:
    return claims.ResumeContext(
        namespace_id=str(uuid.uuid4()),
        group_id=None,
        task_id=str(uuid.uuid4()),
        operation="CREATE",
        mode="source",
        source={
            "source": "doc.pdf",
            "content_type": "application/pdf",
            "obj_id": obj_id,
            "object_type": "file",
        },
        s3={"bucket": "docs", "object": f"files/{obj_id}.pdf", "size": 10},
    )


def _fetch_row(session: Session, obj_id: str) -> claims.ParseStageState | None:
    return session.execute(
        sa.select(claims.ParseStageState).where(
            claims.ParseStageState.obj_id == obj_id,
            claims.ParseStageState.stage == _STAGE,
        )
    ).scalar_one_or_none()


class TestEnsureStageRow:
    def test_creates_the_row(self, session_factory: sessionmaker[Session]) -> None:
        obj_id = str(uuid.uuid4())
        resume_context = _resume_context(obj_id)
        session = session_factory()
        try:
            claims.ensure_stage_row(
                session, obj_id=obj_id, stage=_STAGE, resume_context=resume_context
            )
            row = _fetch_row(session, obj_id)
            assert row is not None
            assert row.resume_context["task_id"] == resume_context.task_id
            assert row.claimed_at is None
            assert row.claimed_by is None
        finally:
            session.close()

    def test_second_call_is_a_no_op_not_an_overwrite(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        """submit_parse's own retry re-runs this insert — it must never clobber
        resume_context on a second call for the same (obj_id, stage)."""
        obj_id = str(uuid.uuid4())
        original = _resume_context(obj_id)
        session = session_factory()
        try:
            claims.ensure_stage_row(
                session, obj_id=obj_id, stage=_STAGE, resume_context=original
            )
            different = _resume_context(obj_id)
            assert different.task_id != original.task_id
            claims.ensure_stage_row(
                session, obj_id=obj_id, stage=_STAGE, resume_context=different
            )

            row = _fetch_row(session, obj_id)
            assert row.resume_context["task_id"] == original.task_id
        finally:
            session.close()


class TestBackfillDoclingTaskId:
    def test_sets_it_once(self, session_factory: sessionmaker[Session]) -> None:
        obj_id = str(uuid.uuid4())
        session = session_factory()
        try:
            claims.ensure_stage_row(
                session,
                obj_id=obj_id,
                stage=_STAGE,
                resume_context=_resume_context(obj_id),
            )
            claims.backfill_docling_task_id(
                session, obj_id=obj_id, stage=_STAGE, docling_task_id="conv-task-1"
            )
            row = _fetch_row(session, obj_id)
            assert row.docling_task_id == "conv-task-1"
        finally:
            session.close()

    def test_does_not_overwrite_an_existing_value(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        """poll_parse calls this on every pass; a later call (e.g. a retried
        poll) must not clobber a value already set."""
        obj_id = str(uuid.uuid4())
        session = session_factory()
        try:
            claims.ensure_stage_row(
                session,
                obj_id=obj_id,
                stage=_STAGE,
                resume_context=_resume_context(obj_id),
            )
            claims.backfill_docling_task_id(
                session, obj_id=obj_id, stage=_STAGE, docling_task_id="conv-task-1"
            )
            claims.backfill_docling_task_id(
                session, obj_id=obj_id, stage=_STAGE, docling_task_id="conv-task-2"
            )
            row = _fetch_row(session, obj_id)
            assert row.docling_task_id == "conv-task-1"
        finally:
            session.close()

    def test_missing_row_is_a_silent_noop(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        """No row ever existed for this obj_id — an anomaly try_claim is what
        surfaces, not this call; it must not raise."""
        session = session_factory()
        try:
            claims.backfill_docling_task_id(
                session,
                obj_id=str(uuid.uuid4()),
                stage=_STAGE,
                docling_task_id="conv-task-1",
            )
        finally:
            session.close()


class TestTryClaim:
    def test_wins_when_unclaimed(self, session_factory: sessionmaker[Session]) -> None:
        obj_id = str(uuid.uuid4())
        resume_context = _resume_context(obj_id)
        session = session_factory()
        try:
            claims.ensure_stage_row(
                session, obj_id=obj_id, stage=_STAGE, resume_context=resume_context
            )
            won = claims.try_claim(
                session, obj_id=obj_id, stage=_STAGE, claimant="task-1"
            )
            assert won is not None
            assert won.task_id == resume_context.task_id
        finally:
            session.close()

    def test_second_distinct_claimant_loses(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        obj_id = str(uuid.uuid4())
        session = session_factory()
        try:
            claims.ensure_stage_row(
                session,
                obj_id=obj_id,
                stage=_STAGE,
                resume_context=_resume_context(obj_id),
            )
            first = claims.try_claim(
                session, obj_id=obj_id, stage=_STAGE, claimant="task-1"
            )
            assert first is not None

            second = claims.try_claim(
                session, obj_id=obj_id, stage=_STAGE, claimant="task-2"
            )
            assert second is None
        finally:
            session.close()

    def test_same_claimant_reaffirms_rather_than_losing_on_retry(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        """Simulates a Celery self.retry(): the same task id calls try_claim
        again after already winning (e.g. the tail-chain dispatch failed
        transiently before the row was deleted). It must see itself as still
        holding the claim, not read its own row as already lost."""
        obj_id = str(uuid.uuid4())
        session = session_factory()
        try:
            claims.ensure_stage_row(
                session,
                obj_id=obj_id,
                stage=_STAGE,
                resume_context=_resume_context(obj_id),
            )
            first = claims.try_claim(
                session, obj_id=obj_id, stage=_STAGE, claimant="task-1"
            )
            assert first is not None

            retried = claims.try_claim(
                session, obj_id=obj_id, stage=_STAGE, claimant="task-1"
            )
            assert retried is not None

            other = claims.try_claim(
                session, obj_id=obj_id, stage=_STAGE, claimant="task-2"
            )
            assert other is None
        finally:
            session.close()

    def test_no_row_returns_none_without_raising(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        """The anomalous case: no submit_parse write ever happened. A no-op,
        not an error — only the WARNING log level differs from the
        already-claimed case (see claims.try_claim's docstring)."""
        session = session_factory()
        try:
            result = claims.try_claim(
                session, obj_id=str(uuid.uuid4()), stage=_STAGE, claimant="task-1"
            )
            assert result is None
        finally:
            session.close()

    def test_exactly_one_of_two_concurrent_callers_wins(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        """The actual mechanism this whole feature depends on: poll_parse and
        advance_from_callback racing on the same row, for real, under
        Postgres's own row-level locking — not a mocked claims module."""
        obj_id = str(uuid.uuid4())
        resume_context = _resume_context(obj_id)
        setup_session = session_factory()
        try:
            claims.ensure_stage_row(
                setup_session,
                obj_id=obj_id,
                stage=_STAGE,
                resume_context=resume_context,
            )
        finally:
            setup_session.close()

        results: list[claims.ResumeContext | None] = [None, None]
        errors: list[BaseException] = []
        barrier = threading.Barrier(2)

        def _race(index: int, claimant: str) -> None:
            session = session_factory()
            try:
                barrier.wait(timeout=5)
                results[index] = claims.try_claim(
                    session, obj_id=obj_id, stage=_STAGE, claimant=claimant
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced via `errors`
                errors.append(exc)
            finally:
                session.close()

        threads = [
            threading.Thread(target=_race, args=(0, "claimant-A")),
            threading.Thread(target=_race, args=(1, "claimant-B")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"try_claim raised under concurrency: {errors}"
        winners = [r for r in results if r is not None]
        assert len(winners) == 1, f"expected exactly one winner, got {results}"
        assert winners[0].task_id == resume_context.task_id

    def test_many_concurrent_callers_still_produce_exactly_one_winner(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        """Same guarantee under more contention than the two-path race this
        table is designed for — a stronger form of the same assertion."""
        obj_id = str(uuid.uuid4())
        resume_context = _resume_context(obj_id)
        setup_session = session_factory()
        try:
            claims.ensure_stage_row(
                setup_session,
                obj_id=obj_id,
                stage=_STAGE,
                resume_context=resume_context,
            )
        finally:
            setup_session.close()

        n = 8
        results: list[claims.ResumeContext | None] = [None] * n
        errors: list[BaseException] = []
        barrier = threading.Barrier(n)

        def _race(index: int) -> None:
            session = session_factory()
            try:
                barrier.wait(timeout=5)
                results[index] = claims.try_claim(
                    session, obj_id=obj_id, stage=_STAGE, claimant=f"claimant-{index}"
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced via `errors`
                errors.append(exc)
            finally:
                session.close()

        threads = [threading.Thread(target=_race, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"try_claim raised under concurrency: {errors}"
        winners = [r for r in results if r is not None]
        assert (
            len(winners) == 1
        ), f"expected exactly one winner among {n}, got {results}"


class TestDeleteStageRow:
    def test_removes_the_row(self, session_factory: sessionmaker[Session]) -> None:
        obj_id = str(uuid.uuid4())
        session = session_factory()
        try:
            claims.ensure_stage_row(
                session,
                obj_id=obj_id,
                stage=_STAGE,
                resume_context=_resume_context(obj_id),
            )
            claims.delete_stage_row(session, obj_id=obj_id, stage=_STAGE)
            assert _fetch_row(session, obj_id) is None
        finally:
            session.close()

    def test_missing_row_is_a_silent_noop(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        session = session_factory()
        try:
            claims.delete_stage_row(session, obj_id=str(uuid.uuid4()), stage=_STAGE)
        finally:
            session.close()
