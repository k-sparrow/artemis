"""Session fixtures: postgres + migrations container + db connection."""

from __future__ import annotations

import secrets

import psycopg
import pytest
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
def conn(pg, pg_credentials, migrated_db):
    creds = pg_credentials
    host = pg.get_container_host_ip()
    port = pg.get_exposed_port(5432)
    with psycopg.connect(
        f"postgresql://{creds['username']}:{creds['password']}@{host}:{port}/{creds['dbname']}"  # noqa: E501
    ) as c:
        yield c
