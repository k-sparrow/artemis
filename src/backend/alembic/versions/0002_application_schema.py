"""Application schema: owner, namespace, ingested_objects, ingestion_tasks, data_source.

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-05
"""

import sqlalchemy as sa

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- owner ---
    op.create_table(
        "owner",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # --- namespace ---
    op.create_table(
        "namespace",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column(
            "type",
            sa.Enum("private", "shared", name="namespace_type"),
            nullable=False,
        ),
        sa.Column("owner_id", sa.UUID(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["owner_id"], ["owner.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )

    # DB trigger: soft-cascade owner DELETE → namespace.deleted_at
    op.execute(
        """
        CREATE OR REPLACE FUNCTION soft_cascade_namespace_on_owner_delete()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE namespace
            SET deleted_at = now()
            WHERE owner_id = OLD.id
              AND deleted_at IS NULL;
            RETURN OLD;
        END;
        $$
        """
    )
    op.execute("DROP TRIGGER IF EXISTS owner_soft_cascade ON owner")
    op.execute(
        """
        CREATE TRIGGER owner_soft_cascade
        AFTER DELETE ON owner
        FOR EACH ROW EXECUTE FUNCTION soft_cascade_namespace_on_owner_delete()
        """
    )

    # --- ingested_objects ---
    op.create_table(
        "ingested_objects",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("namespace_id", sa.UUID(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("object_type", sa.Text(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("group_id", sa.UUID(), nullable=True),
        sa.ForeignKeyConstraint(["namespace_id"], ["namespace.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ingested_objects_namespace_id", "ingested_objects", ["namespace_id"]
    )
    op.create_index("ix_ingested_objects_group_id", "ingested_objects", ["group_id"])

    # --- ingestion_tasks ---
    op.create_table(
        "ingestion_tasks",
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("obj_id", sa.UUID(), nullable=True),
        sa.Column("namespace_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["namespace_id"], ["namespace.id"]),
        sa.PrimaryKeyConstraint("task_id"),
    )
    op.create_index("ix_ingestion_tasks_obj_id", "ingestion_tasks", ["obj_id"])
    op.create_index(
        "ix_ingestion_tasks_namespace_id", "ingestion_tasks", ["namespace_id"]
    )

    # --- data_source ---
    op.create_table(
        "data_source",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("connector_name", sa.Text(), nullable=False),
        sa.Column("namespace_id", sa.UUID(), nullable=False),
        sa.Column("org_name", sa.Text(), nullable=False),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("connector_name"),
    )


def downgrade() -> None:
    op.drop_table("data_source")
    op.drop_index("ix_ingestion_tasks_namespace_id", table_name="ingestion_tasks")
    op.drop_index("ix_ingestion_tasks_obj_id", table_name="ingestion_tasks")
    op.drop_table("ingestion_tasks")
    op.drop_index("ix_ingested_objects_group_id", table_name="ingested_objects")
    op.drop_index("ix_ingested_objects_namespace_id", table_name="ingested_objects")
    op.drop_table("ingested_objects")
    op.execute("DROP TRIGGER IF EXISTS owner_soft_cascade ON owner")
    op.execute("DROP FUNCTION IF EXISTS soft_cascade_namespace_on_owner_delete")
    op.drop_table("namespace")
    op.drop_table("owner")
    sa.Enum(name="namespace_type").drop(op.get_bind())
