"""data_source: add namespace_name column.

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-05
"""

import sqlalchemy as sa

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "data_source",
        sa.Column("namespace_name", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("data_source", "namespace_name")
