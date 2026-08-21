"""Content-addressed dedup ledger for the enterprise intake service.

Defense-in-depth against duplicate ingestion caused by connector-level
flakiness (idempotent-repository eviction, task restarts, tasks.max>1,
Kafka Connect's own at-least-once redelivery) — see
0009_add_intake_dedup_ledger.py for the full rationale and why the key is
(namespace_id, path, sha256) rather than sha256 alone.

Claim lifecycle
----------------
1. claim_or_get_existing() atomically inserts a row for the key. Exactly one
   caller ever wins (Postgres ON CONFLICT DO NOTHING) — the winner proceeds
   with the real upload; a loser gets back the existing row's task_id.
2. The winner calls backfill_task_id() once its upload succeeds — task_id is
   only known after the fact (storage issues it, not intake), so it can't be
   part of the atomic insert itself.
3. A loser arriving while the winner is still mid-flight sees task_id=NULL.
   wait_for_task_id() polls briefly for the backfill; if it never arrives
   (the window is normally a single HTTP round trip), the caller is expected
   to fail open and perform its own upload rather than block indefinitely —
   this ledger is a best-effort optimisation, not a correctness guarantee.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.backend.enterprise.intake.api.intake.models import IntakeDedupLedger

logger = logging.getLogger(__name__)

__all__ = [
    "ClaimResult",
    "claim_or_get_existing",
    "backfill_task_id",
    "wait_for_task_id",
]


@dataclass
class ClaimResult:
    won: bool
    existing_task_id: uuid.UUID | None = None


async def claim_or_get_existing(
    session: AsyncSession, *, namespace_id: uuid.UUID, path: str, sha256: str
) -> ClaimResult:
    """Atomically claim (namespace_id, path, sha256), or report the existing claim."""
    stmt = (
        pg_insert(IntakeDedupLedger)
        .values(namespace_id=namespace_id, path=path, sha256=sha256)
        .on_conflict_do_nothing(index_elements=["namespace_id", "path", "sha256"])
        .returning(IntakeDedupLedger.namespace_id)
    )
    result = await session.execute(stmt)
    won = result.scalar_one_or_none() is not None
    await session.commit()
    if won:
        return ClaimResult(won=True)

    result = await session.execute(
        sa.select(IntakeDedupLedger.task_id).where(
            IntakeDedupLedger.namespace_id == namespace_id,
            IntakeDedupLedger.path == path,
            IntakeDedupLedger.sha256 == sha256,
        )
    )
    return ClaimResult(won=False, existing_task_id=result.scalar_one_or_none())


async def backfill_task_id(
    session: AsyncSession,
    *,
    namespace_id: uuid.UUID,
    path: str,
    sha256: str,
    task_id: uuid.UUID,
) -> None:
    """Fill in task_id after the claiming request's upload succeeds."""
    await session.execute(
        sa.update(IntakeDedupLedger)
        .where(
            IntakeDedupLedger.namespace_id == namespace_id,
            IntakeDedupLedger.path == path,
            IntakeDedupLedger.sha256 == sha256,
        )
        .values(task_id=task_id)
    )
    await session.commit()


async def wait_for_task_id(
    session: AsyncSession,
    *,
    namespace_id: uuid.UUID,
    path: str,
    sha256: str,
    timeout: float = 3.0,
    interval: float = 0.2,
) -> uuid.UUID | None:
    """Poll briefly for a concurrent winner to backfill task_id.

    Returns None (not an error) if the window elapses without a backfill —
    see module docstring: the caller is expected to fail open at that point.
    """
    attempts = max(1, int(timeout / interval))
    for _ in range(attempts):
        result = await session.execute(
            sa.select(IntakeDedupLedger.task_id).where(
                IntakeDedupLedger.namespace_id == namespace_id,
                IntakeDedupLedger.path == path,
                IntakeDedupLedger.sha256 == sha256,
            )
        )
        task_id = result.scalar_one_or_none()
        if task_id is not None:
            return task_id
        await asyncio.sleep(interval)
    logger.warning(
        "intake_dedup_backfill_timeout namespace_id=%s path=%s", namespace_id, path
    )
    return None
