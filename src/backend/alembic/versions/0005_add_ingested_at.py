"""Add ingested_at column to ingested_objects.

Required for ordering objects by ingestion time (TUI "last ingested" sidebar).
DEFAULT NOW() backfills existing rows with the migration timestamp.

Revision ID: 0005
Revises: 0004
Create Date: 2026-06-07
"""

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ingested_objects",
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_column("ingested_objects", "ingested_at")
