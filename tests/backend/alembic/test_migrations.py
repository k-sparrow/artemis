"""Integration tests: verify the schema produced by alembic upgrade head."""

EXPECTED_TABLES = {
    "alembic_version",
    # Celery's own group-result table — never CDC'd, orthogonal to the
    # apollo_celery_taskmeta cleanup (0008), deliberately left alone.
    "apollo_celery_tasksetmeta",
    "data_source",
    "ingested_objects",
    "ingestion_status",
    "intake_dedup_ledger",
    "namespace",
    "owner",
    "parse_stage_state",
}


def test_all_tables_exist(conn):
    rows = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    ).fetchall()
    assert {r[0] for r in rows} == EXPECTED_TABLES


def test_alembic_version(conn):
    row = conn.execute("SELECT version_num FROM alembic_version").fetchone()
    assert row[0] == "0010"


def test_debezium_and_celery_users_exist_with_replication(conn):
    rows = conn.execute(
        "SELECT rolname, rolreplication FROM pg_roles"
        " WHERE rolname IN ('debezium', 'celery') ORDER BY rolname"
    ).fetchall()
    assert {r[0]: r[1] for r in rows} == {"celery": True, "debezium": True}


def test_apollo_celery_taskmeta_cdc_infra_is_gone(conn):
    """0008 drops apollo_celery_taskmeta itself and everything specific to
    CDC-ing it (the table, its sequence, celery_results_publication,
    celery_replication_group) — ingestion_status is the CDC source of truth
    now. The "celery"/"debezium" roles themselves survive (still needed —
    see 0008's own docstring)."""
    row = conn.execute(
        "SELECT 1 FROM pg_publication WHERE pubname = 'celery_results_publication'"
    ).fetchone()
    assert row is None

    row = conn.execute(
        "SELECT 1 FROM pg_roles WHERE rolname = 'celery_replication_group'"
    ).fetchone()
    assert row is None


def test_ingestion_status_publication_exists(conn):
    row = conn.execute(
        "SELECT pubname FROM pg_publication WHERE pubname = 'ingestion_status_publication'"  # noqa: E501
    ).fetchone()
    assert row is not None


def test_publication_covers_ingestion_status(conn):
    row = conn.execute(
        """
        SELECT c.relname
        FROM pg_publication_rel pr
        JOIN pg_publication p ON pr.prpubid = p.oid
        JOIN pg_class c ON pr.prrelid = c.oid
        WHERE p.pubname = 'ingestion_status_publication'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "ingestion_status"


def test_ingestion_status_owned_by_celery(conn):
    """No celery_replication_group indirection this time (unlike the old
    apollo_celery_taskmeta) — a direct OWNER TO celery grant, same pattern
    as parse_stage_state below."""
    row = conn.execute(
        """
        SELECT pg_get_userbyid(relowner)
        FROM pg_class
        WHERE relname = 'ingestion_status' AND relkind = 'r'
        """
    ).fetchone()
    assert row[0] == "celery"


def test_debezium_has_select_on_ingestion_status(conn):
    row = conn.execute(
        "SELECT has_table_privilege('debezium', 'public.ingestion_status', 'SELECT')"
    ).fetchone()
    assert row[0] is True


def test_parse_stage_state_owned_by_celery(conn):
    """The worker (SQL_DB_USER=celery in this test env) reads/writes this
    table directly — unlike the old apollo_celery_taskmeta, it is not
    Celery-internal, but 0006 still transfers ownership to the "celery" role
    since migrations run as a different (schema-owning) user. Regression
    test for a real bug: without this transfer, submit_parse fails with
    "permission denied for table parse_stage_state" on every attempt.
    """
    row = conn.execute(
        """
        SELECT pg_get_userbyid(relowner)
        FROM pg_class
        WHERE relname = 'parse_stage_state' AND relkind = 'r'
        """
    ).fetchone()
    assert row[0] == "celery"
