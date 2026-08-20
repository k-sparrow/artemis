"""Add ingestion_status: the transactional outbox for task-state visibility.

Replaces apollo_celery_taskmeta as the CDC source of truth for both
ingestion_tasks (task-state visibility) and ingested_objects (object
visibility) — see tools/oci/images/ksqldb/artemis_init.ksql. One row per
contract task_id (the id the storage service itself generates — see
ingest()'s own docstring for the full provenance chain), created once by
ingest(), updated in place by every task via OutboxTask's
before_start/on_success/on_failure hooks.

The CDC/replication infra specific to the OLD table (apollo_celery_taskmeta,
celery_results_publication, celery_replication_group) is dropped separately,
in 0008 — kept apart so this table can be verified end-to-end before the old
one is torn out.

Revision ID: 0007
Revises: 0006
Create Date: 2026-08-17
"""

import sqlalchemy as sa
from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ingestion_status",
        sa.Column("task_id", sa.UUID(as_uuid=True), nullable=False),
        sa.Column("namespace_id", sa.UUID(as_uuid=True), nullable=False),
        sa.Column("obj_id", sa.UUID(as_uuid=True), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
        sa.Column("object_type", sa.Text(), nullable=True),
        sa.Column("content_type", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("group_id", sa.UUID(as_uuid=True), nullable=True),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        # Deliberately timezone-less — see outbox.py's IngestionStatus model
        # for why (Debezium encoding compatible with this pipeline's
        # existing FROM_UNIXTIME(x / 1000) ksqlDB pattern).
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("task_id"),
    )
    # Same ownership pattern as parse_stage_state (0006): migrations run as
    # the schema-owning user, the worker itself connects as the more
    # restricted "celery" role and reads/writes this table directly.
    op.execute("ALTER TABLE public.ingestion_status OWNER TO celery")
    # REPLICA IDENTITY DEFAULT (the implicit default, no ALTER needed) is
    # sufficient here — task_id is the real primary key, unlike
    # apollo_celery_taskmeta where the PK was a surrogate `id` and task_id a
    # separate unique column (see 0008's docstring for why that table needed
    # REPLICA IDENTITY FULL and this one doesn't).
    op.execute("GRANT SELECT ON public.ingestion_status TO debezium")
    # The Debezium PostgreSQL source connector reads from a publication it
    # does NOT auto-create (publication.autocreate.mode=disabled, same as
    # the old celery_results_publication) — it must already exist.
    op.execute(
        "CREATE PUBLICATION ingestion_status_publication FOR TABLE public.ingestion_status"  # noqa: E501
    )


def downgrade() -> None:
    op.execute("DROP PUBLICATION IF EXISTS ingestion_status_publication")
    op.execute("REVOKE SELECT ON public.ingestion_status FROM debezium")
    op.drop_table("ingestion_status")
