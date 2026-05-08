"""Shared helper: run the artemis-db-migrations container before tests."""

from __future__ import annotations

from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

_MIGRATIONS_IMAGE = "artemis/db-migrations:dev"


def run_migrations_on_network(
    network: Network,
    username: str,
    password: str,
    dbname: str,
    alias: str = "postgres",
) -> None:
    """Run migrations over a shared Docker bridge network using *alias* as the host."""
    db_url = f"postgresql+psycopg://{username}:{password}@{alias}:5432/{dbname}"
    with (
        DockerContainer(_MIGRATIONS_IMAGE)
        .with_network(network)
        .with_env("SQL_DB_URL", db_url)
    ) as c:
        code = c.get_wrapped_container().wait()["StatusCode"]
        if code != 0:
            raise RuntimeError(f"Migrations container exited with code {code}")


def run_migrations_host(
    username: str,
    password: str,
    host: str,
    port: int | str,
    dbname: str,
) -> None:
    """
    Run migrations using host networking (for tests that expose postgres on localhost).
    """
    db_url = f"postgresql+psycopg://{username}:{password}@{host}:{port}/{dbname}"
    with (
        DockerContainer(_MIGRATIONS_IMAGE)
        .with_kwargs(network_mode="host")
        .with_env("SQL_DB_URL", db_url)
    ) as c:
        code = c.get_wrapped_container().wait()["StatusCode"]
        if code != 0:
            raise RuntimeError(f"Migrations container exited with code {code}")
