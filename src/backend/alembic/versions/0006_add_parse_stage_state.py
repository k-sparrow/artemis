"""Add parse_stage_state: claim-arbitration table for the docling-serve
conversion callback race (poll_parse vs. advance_from_callback).

Revision ID: 0006
Revises: 0005
Create Date: 2026-08-16
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "parse_stage_state",
        sa.Column("obj_id", sa.Text(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("docling_task_id", sa.Text(), nullable=True),
        sa.Column("resume_context", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("obj_id", "stage"),
    )
    # This table is NOT Celery-internal (unlike apollo_celery_taskmeta/
    # tasksetmeta, which 0001 created the "celery" role for) — it's plain
    # application state, owned by backend/claims.py alone. It's granted to
    # "celery" here only because that happens to be the Postgres role the
    # worker process itself authenticates as in this environment: migrations
    # run as the schema-owning user (e.g. postgres), so without this the
    # worker — which reads/writes this table directly, unlike the CDC-only
    # tables — would inherit no privileges on a table it didn't create.
    op.execute("ALTER TABLE public.parse_stage_state OWNER TO celery")


def downgrade() -> None:
    op.drop_table("parse_stage_state")
