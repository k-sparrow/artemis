"""Celery result tables + Debezium/Celery users and CDC setup.

Replaces the init.sql bootstrap that was baked into the artemis/postgres image.

Revision ID: 0001
Revises:
Create Date: 2026-05-05
"""

import sqlalchemy as sa

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- Users ---
    op.execute("CREATE USER debezium WITH ENCRYPTED PASSWORD 'debezium'")
    op.execute("ALTER USER debezium REPLICATION")
    op.execute("CREATE USER celery WITH ENCRYPTED PASSWORD 'celery'")
    op.execute("ALTER USER celery REPLICATION")
    # PostgreSQL 15 removed the default CREATE grant on the public schema.
    # Celery's result backend calls create_all() which needs CREATE to make
    # its own sequences and tables.
    op.execute("GRANT CREATE ON SCHEMA public TO celery")

    # --- apollo_celery_taskmeta ---
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
        "GRANT USAGE, SELECT, UPDATE ON SEQUENCE public.apollo_task_id_sequence TO celery_replication_group"
    )
    op.execute("GRANT SELECT ON public.apollo_celery_taskmeta TO debezium")

    # --- apollo_celery_tasksetmeta ---
    op.execute(
        """
        CREATE SEQUENCE public.apollo_taskset_id_sequence
            INCREMENT 1 START 1 MINVALUE 1
            MAXVALUE 9223372036854775807 CACHE 1
        """
    )
    op.create_table(
        "apollo_celery_tasksetmeta",
        sa.Column(
            "id",
            sa.Integer(),
            sa.Sequence("apollo_taskset_id_sequence"),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("taskset_id", sa.String(155), unique=True, nullable=True),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("date_done", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute("ALTER TABLE IF EXISTS public.apollo_celery_tasksetmeta OWNER TO celery")
    op.execute(
        "GRANT USAGE, SELECT, UPDATE ON SEQUENCE public.apollo_taskset_id_sequence TO celery"
    )


def downgrade() -> None:
    op.drop_table("apollo_celery_tasksetmeta")
    op.execute("DROP SEQUENCE IF EXISTS public.apollo_taskset_id_sequence")
    op.execute("DROP PUBLICATION IF EXISTS celery_results_publication")
    op.drop_table("apollo_celery_taskmeta")
    op.execute("DROP SEQUENCE IF EXISTS public.apollo_task_id_sequence")
    op.execute("DROP ROLE IF EXISTS celery_replication_group")
    op.execute("DROP USER IF EXISTS celery")
    op.execute("DROP USER IF EXISTS debezium")
