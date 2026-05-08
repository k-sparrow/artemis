"""Integration tests: verify the schema produced by alembic upgrade head."""

EXPECTED_TABLES = {
    "alembic_version",
    "apollo_celery_taskmeta",
    "apollo_celery_tasksetmeta",
    "data_source",
    "ingested_objects",
    "ingestion_tasks",
    "namespace",
    "owner",
}


def test_all_tables_exist(conn):
    rows = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    ).fetchall()
    assert {r[0] for r in rows} == EXPECTED_TABLES


def test_alembic_version(conn):
    row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    assert row[0] == "0002"


def test_debezium_and_celery_users_exist_with_replication(conn):
    rows = conn.execute(
        "SELECT rolname, rolreplication FROM pg_roles"
        " WHERE rolname IN ('debezium', 'celery') ORDER BY rolname"
    ).fetchall()
    assert {r[0]: r[1] for r in rows} == {"celery": True, "debezium": True}


def test_celery_replication_group_role_exists(conn):
    row = conn.execute(
        "SELECT 1 FROM pg_roles WHERE rolname = 'celery_replication_group'"
    ).fetchone()
    assert row is not None


def test_celery_results_publication_exists(conn):
    row = conn.execute(
        "SELECT pubname FROM pg_publication WHERE pubname = 'celery_results_publication'"
    ).fetchone()
    assert row is not None


def test_publication_covers_taskmeta(conn):
    row = conn.execute(
        """
        SELECT c.relname
        FROM pg_publication_rel pr
        JOIN pg_publication p ON pr.prpubid = p.oid
        JOIN pg_class c ON pr.prrelid = c.oid
        WHERE p.pubname = 'celery_results_publication'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "apollo_celery_taskmeta"


def test_taskmeta_replica_identity_full(conn):
    row = conn.execute(
        "SELECT relreplident FROM pg_class WHERE relname = 'apollo_celery_taskmeta'"
    ).fetchone()
    assert row[0] == "f"  # 'f' = FULL


def test_taskmeta_owned_by_replication_group(conn):
    row = conn.execute(
        """
        SELECT pg_get_userbyid(relowner)
        FROM pg_class
        WHERE relname = 'apollo_celery_taskmeta' AND relkind = 'r'
        """
    ).fetchone()
    assert row[0] == "celery_replication_group"


def test_debezium_has_select_on_taskmeta(conn):
    row = conn.execute(
        "SELECT has_table_privilege('debezium', 'public.apollo_celery_taskmeta', 'SELECT')"  # noqa: E501
    ).fetchone()
    assert row[0] is True
