"""Drop ingestion_tasks — retired in favor of a direct read of ingestion_status.

Epic 22 (live ingestion stage visibility): the storage service's task-status
endpoints (GET /tasks, GET /tasks/{task_id}) now read ingestion_status
directly instead of the CDC-fed, terminal-only ingestion_tasks table — see
src/backend/storage/api/files/service.py. ingestion_status is a strict
superset (live `stage` + terminal `status`/`failure_reason`, never deleted),
so ingestion_tasks has no remaining query pattern to serve. Its CDC leg
(ksqlDB CSAS fan-out B/C + the debezium-jdbc-sink-ingestion-tasks connector)
is retired alongside this in the same change — see
tools/oci/images/ksqldb/artemis_init.ksql and tools/oci/images/BUILD.bazel.

Unlike apollo_celery_taskmeta (0008), ingestion_tasks was never a Debezium
CDC *source* — only a JDBC sink *target* — so there's no publication/GRANT
to reverse here, just the table and its indexes (pattern otherwise follows
0008_drop_celery_taskmeta_cdc_infra.py).

Also adds the ingestion_status.namespace_id index the worker never needed
(it only ever looks up by task_id) but list_tasks now does, now that it
queries ingestion_status per-namespace instead of ingestion_tasks (which
already had this index — see 0002_application_schema.py).

Revision ID: 0010
Revises: 0009
Create Date: 2026-08-31
"""

import sqlalchemy as sa
from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_ingestion_tasks_namespace_id", table_name="ingestion_tasks")
    op.drop_index("ix_ingestion_tasks_obj_id", table_name="ingestion_tasks")
    op.drop_table("ingestion_tasks")
    op.create_index(
        "ix_ingestion_status_namespace_id", "ingestion_status", ["namespace_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_ingestion_status_namespace_id", table_name="ingestion_status")
    op.create_table(
        "ingestion_tasks",
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("obj_id", sa.UUID(), nullable=True),
        sa.Column("namespace_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("task_id"),
    )
    op.create_index("ix_ingestion_tasks_obj_id", "ingestion_tasks", ["obj_id"])
    op.create_index(
        "ix_ingestion_tasks_namespace_id", "ingestion_tasks", ["namespace_id"]
    )
