"""Drop apollo_celery_taskmeta's CDC/replication infra.

ingestion_status (0007) is now the CDC source of truth — apollo_celery_taskmeta
is no longer read by anything. This drops only the parts of 0001's setup that
were specific to CDC-ing that one table: the table itself, its sequence, the
publication, and the celery_replication_group role.

Deliberately NOT touched here:
  - The "celery"/"debezium" roles themselves — "celery" still needs
    GRANT CREATE ON SCHEMA public (0001) for the stock Celery result
    backend's own database_create_tables_at_setup auto-creation of
    celery_taskmeta/celery_tasksetmeta (new tables, Celery-owned, no
    migration needed for them going forward — see celery.py), and
    "debezium" still needs its role for ingestion_status (0007).
  - apollo_celery_tasksetmeta — Celery's group-result table, never CDC'd,
    orthogonal to this cleanup; left alone (also superseded at runtime by
    celery_tasksetmeta under the stock backend, but that's celery.py's
    concern, not a migration's).

Revision ID: 0008
Revises: 0007
Create Date: 2026-08-17
"""

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("REVOKE SELECT ON public.apollo_celery_taskmeta FROM debezium")
    op.execute("DROP PUBLICATION IF EXISTS celery_results_publication")
    op.execute(
        "ALTER TABLE public.apollo_celery_taskmeta OWNER TO celery_replication_group"
    )
    op.drop_table("apollo_celery_taskmeta")
    op.execute("DROP SEQUENCE IF EXISTS public.apollo_task_id_sequence")
    op.execute("DROP ROLE IF EXISTS celery_replication_group")


def downgrade() -> None:
    import sqlalchemy as sa

    op.execute(
        """
        CREATE SEQUENCE public.apollo_task_id_sequence
            INCREMENT 1 START 1 MINVALUE 1
            MAXVALUE 9223372036854775807 CACHE 1
        """
    )
    op.create_table(
        "apollo_celery_taskmeta",
        sa.Column(
            "id",
            sa.Integer(),
            sa.Sequence("apollo_task_id_sequence"),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("task_id", sa.String(155), unique=True, nullable=True),
        sa.Column("status", sa.String(50), nullable=True),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("date_done", sa.DateTime(), nullable=True),
        sa.Column("traceback", sa.Text(), nullable=True),
        sa.Column("name", sa.String(155), nullable=True),
        sa.Column("args", sa.LargeBinary(), nullable=True),
        sa.Column("kwargs", sa.LargeBinary(), nullable=True),
        sa.Column("worker", sa.String(155), nullable=True),
        sa.Column("retries", sa.Integer(), nullable=True),
        sa.Column("queue", sa.String(155), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute("ALTER TABLE public.apollo_celery_taskmeta REPLICA IDENTITY FULL")
    op.execute(
        "CREATE PUBLICATION celery_results_publication FOR TABLE public.apollo_celery_taskmeta"
    )
    op.execute("CREATE ROLE celery_replication_group")
    op.execute("GRANT celery_replication_group TO postgres")
    op.execute("GRANT celery_replication_group TO debezium")
    op.execute("GRANT celery_replication_group TO celery")
    op.execute(
        "ALTER TABLE public.apollo_celery_taskmeta OWNER TO celery_replication_group"
    )
    op.execute(
        "GRANT USAGE, SELECT, UPDATE ON SEQUENCE public.apollo_task_id_sequence "
        "TO celery_replication_group"
    )
    op.execute("GRANT SELECT ON public.apollo_celery_taskmeta TO debezium")
