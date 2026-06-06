"""Drop FK constraints from ingested_objects and ingestion_tasks to namespace.

The ingestion pipeline (MinIO → Kafka → Celery) processes tombstone events
asynchronously. The namespace row may be hard-deleted before Celery lands its
completion writes, so referential integrity on namespace_id is impossible to
maintain without coordination. namespace_id becomes a plain UUID identifier.

Revision ID: 0004
Revises: 0003
Create Date: 2026-06-06
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "ingested_objects_namespace_id_fkey", "ingested_objects", type_="foreignkey"
    )
    op.drop_constraint(
        "ingestion_tasks_namespace_id_fkey", "ingestion_tasks", type_="foreignkey"
    )


def downgrade() -> None:
    import sqlalchemy as sa

    op.create_foreign_key(
        "ingestion_tasks_namespace_id_fkey",
        "ingestion_tasks",
        "namespace",
        ["namespace_id"],
        ["id"],
    )
    op.create_foreign_key(
        "ingested_objects_namespace_id_fkey",
        "ingested_objects",
        "namespace",
        ["namespace_id"],
        ["id"],
    )
