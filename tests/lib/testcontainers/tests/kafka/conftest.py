# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Shared fixtures for Kafka ecosystem integration tests.

These fixtures start real Docker containers, so they are slow.
They are all session-scoped to avoid restarting the stack for every test.

Stack topology:
    Network → Kafka (KRaft) → KafkaConnect
                            → KSQLDbServer
"""

from __future__ import annotations

import io
import tarfile
import zipfile
from collections.abc import Generator
from pathlib import Path

import pytest
import requests
from testcontainers.core.network import Network

from tests.lib.testcontainers.kafka import (
    KafkaContainer,
    KafkaConnectContainer,
    KSQLDbContainer,
)

# ---------------------------------------------------------------------------
# Network alias constants (used both for .with_network_aliases() and in URLs)
# ---------------------------------------------------------------------------

_KAFKA_ALIAS = "broker"
_CONNECT_ALIAS = "connect"
_KSQLDB_ALIAS = "ksqldb-server"

# ---------------------------------------------------------------------------
# Network and Kafka broker  (shared by all integration tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_network() -> Generator[Network, None, None]:
    """A Docker network shared by all containers in the integration stack."""
    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def kafka_broker(kafka_network: Network) -> Generator[KafkaContainer, None, None]:
    """A single-node KRaft Kafka broker on the shared network."""
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(kafka_network)
        .with_network_aliases(_KAFKA_ALIAS)
    )
    container.start()
    yield container
    container.stop()


# ---------------------------------------------------------------------------
# Kafka Connect
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_connect(
    kafka_network: Network,
    kafka_broker: KafkaContainer,
) -> Generator[KafkaConnectContainer, None, None]:
    """A Kafka Connect worker on the shared network."""
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka_broker.get_internal_bootstrap_server()
        )
        .with_network(kafka_network)
        .with_network_aliases(_CONNECT_ALIAS)
    )
    container.start()
    yield container
    container.stop()


# ---------------------------------------------------------------------------
# KSQLDb server
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ksqldb_server(
    kafka_network: Network,
    kafka_broker: KafkaContainer,
) -> Generator[KSQLDbContainer, None, None]:
    """A ksqlDB server on the shared network, connected to the Kafka broker."""
    container = (
        KSQLDbContainer(bootstrap_servers=kafka_broker.get_internal_bootstrap_server())
        .with_network(kafka_network)
        .with_network_aliases(_KSQLDB_ALIAS)
    )
    container.start()
    yield container
    container.stop()


# ---------------------------------------------------------------------------
# Connector tar fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def datagen_connector_tar(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Download the Confluent Datagen connector and package it as a tar archive.

    The tar uses paths relative to the container root (no leading ``/``), so
    ``with_connector_tar()`` can apply it as a Docker layer directly:

        usr/share/java/datagen-connector/<jar-files>

    The Datagen connector (``io.confluent.kafka.connect.datagen.DatagenConnector``)
    is NOT present in the base ``cp-kafka-connect`` image, making it a clean
    canary for verifying that ``with_connector_tar()`` actually works.
    """
    url = (
        "https://api.hub.confluent.io/api/plugins/"
        "confluentinc/kafka-connect-datagen/versions/0.6.5/archive"
    )
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    tmp_dir = tmp_path_factory.mktemp("datagen-connector")
    plugin_dir = tmp_dir / "usr" / "share" / "java" / "datagen-connector"
    plugin_dir.mkdir(parents=True)

    # The Confluent Hub archive is a ZIP whose lib/ directory contains the JARs
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for member in zf.namelist():
            if "/lib/" in member and member.endswith(".jar"):
                jar_name = Path(member).name
                with zf.open(member) as src:
                    (plugin_dir / jar_name).write_bytes(src.read())

    # Create a tar with paths relative to the container root (no leading /)
    # This is the same layout that Bazel's mtree_mutate + tar rules produce
    tar_path = tmp_dir / "datagen-connector.tar"
    with tarfile.open(tar_path, "w") as tar:
        tar.add(plugin_dir, arcname="usr/share/java/datagen-connector")

    return tar_path


@pytest.fixture(scope="session")
def kafka_connect_with_datagen(
    kafka_network: Network,
    kafka_broker: KafkaContainer,
    datagen_connector_tar: Path,
) -> Generator[KafkaConnectContainer, None, None]:
    """A Kafka Connect worker with the Datagen connector baked in as an image layer."""
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka_broker.get_internal_bootstrap_server()
        )
        .with_network(kafka_network)
        .with_connector_tar(datagen_connector_tar)
    )
    container.start()
    yield container
    container.stop()
