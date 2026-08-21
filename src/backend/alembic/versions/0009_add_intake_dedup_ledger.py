"""Add intake_dedup_ledger: content-addressed dedup for the enterprise intake service.

Defense-in-depth against duplicate ingestion caused by connector-level flakiness
(Camel's idempotent repository evicting under an oversized tree, task restarts
losing in-memory dedup state, tasks.max>1 double-scanning, Kafka Connect's own
at-least-once redelivery) — all of which redeliver the same (path, content) pair
to the intake service. Keyed on (namespace_id, path, sha256), with path resolved
to its canonical filesystem identity (symlinks followed) before insert, so a
symlink and its target collapse to the same row instead of being treated as two
distinct files. Deliberately NOT keyed on sha256 alone: two independently
authored files that happen to share byte-identical content, at different real
paths, must stay distinct — see src/backend/enterprise/intake/api/intake/dedup.py.

Revision ID: 0009
Revises: 0008
Create Date: 2026-08-21
"""

import sqlalchemy as sa

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "intake_dedup_ledger",
        sa.Column("namespace_id", sa.UUID(as_uuid=True), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("sha256", sa.Text(), nullable=False),
        # Filled in after the upload succeeds (storage issues the task_id, not
        # intake) — NULL briefly between claiming the row and finishing the
        # upload. See dedup.py's claim()/backfill_task_id() docstrings.
        sa.Column("task_id", sa.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("namespace_id", "path", "sha256"),
    )


def downgrade() -> None:
    op.drop_table("intake_dedup_ledger")
