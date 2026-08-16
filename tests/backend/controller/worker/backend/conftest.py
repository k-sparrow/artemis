"""Session fixtures: postgres + migrations container + SQLAlchemy session factory.

Mirrors tests/backend/alembic/conftest.py's postgres+migrations setup, but
exposes a SQLAlchemy session factory (not a raw psycopg connection) — claims.py's
functions take a Session, and the point of this suite is to exercise them
directly against a real Postgres, not just inspect the resulting schema.
"""

from __future__ import annotations

import secrets

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer

PG_IMAGE = "postgres:16"
MIGRATIONS_IMAGE = "artemis/db-migrations:dev"
PG_ALIAS = "postgres"


@pytest.fixture(scope="session")
def pg_credentials() -> dict[str, str]:
    return {
        "username": "postgres",
        "password": secrets.token_urlsafe(16),
        "dbname": "test_documents",
    }


@pytest.fixture(scope="session")
def network():
    with Network() as net:
        yield net


@pytest.fixture(scope="session")
def pg(network, pg_credentials):
    with (
        PostgresContainer(image=PG_IMAGE, driver="psycopg", **pg_credentials)
        .with_network(network)
        .with_network_aliases(PG_ALIAS)
        .with_command(
            "postgres "
            "-c wal_level=logical "
            "-c hot_standby=on "
            "-c max_wal_senders=10 "
            "-c max_replication_slots=10 "
            "-c hot_standby_feedback=on"
        )
    ) as container:
        yield container


@pytest.fixture(scope="session")
def migrated_db(network, pg, pg_credentials):
    creds = pg_credentials
    db_url = (
        f"postgresql+psycopg://{creds['username']}:{creds['password']}"
        f"@{PG_ALIAS}:5432/{creds['dbname']}"
    )
    with (
        DockerContainer(MIGRATIONS_IMAGE)
        .with_network(network)
        .with_env("SQL_DB_URL", db_url)
    ) as migrations:
        exit_code = migrations.get_wrapped_container().wait()["StatusCode"]
        if exit_code != 0:
            raise RuntimeError(f"migrations container exited with code {exit_code}")


@pytest.fixture(scope="session")
def session_factory(
    pg: PostgresContainer, pg_credentials: dict[str, str], migrated_db: None
) -> sessionmaker[Session]:
    """Connects as the "celery" role — the same Postgres identity the real
    worker process uses, and the role parse_stage_state is owned by
    (migration 0006's ALTER TABLE ... OWNER TO celery). Using the real
    least-privilege role here, rather than the postgres superuser, is what
    makes this suite an actual regression test for that ownership grant.
    """
    host = pg.get_container_host_ip()
    port = pg.get_exposed_port(5432)
    dbname = pg_credentials["dbname"]
    url = f"postgresql+psycopg://celery:celery@{host}:{port}/{dbname}"
    engine = create_engine(url)
    return sessionmaker(bind=engine)
