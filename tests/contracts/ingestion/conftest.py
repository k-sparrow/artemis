"""Shared session fixtures for ingestion contract tests.

Stack: Network → Kafka (KRaft) → KSQLDbContainer

The `streams` fixture creates the `artemis-ingestion-storage-s3` source stream
and the `artemis-ingestion-celery-tasks` CSAS directly via the ksqlDB REST API,
decoupling these contract tests from any specific init OCI image.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from tests.lib.testcontainers.kafka import KSQLDbContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_KAFKA_ALIAS = "broker"
_KSQLDB_ALIAS = "ksqldb-server"

_INPUT_TOPIC = "artemis.ingestion.storage.s3"
_OUTPUT_TOPIC = "artemis.ingestion.celery.tasks"


# ---------------------------------------------------------------------------
# Infrastructure: Network → Kafka → KSQLDb
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def network() -> Generator[Network, None, None]:
    with Network() as n:
        yield n


@pytest.fixture(scope="session")
def kafka(network: Network) -> Generator[KafkaContainer, None, None]:
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(network)
        .with_network_aliases(_KAFKA_ALIAS)
        .with_kwargs(hostname=_KAFKA_ALIAS)
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def bootstrap_server(kafka: KafkaContainer) -> str:
    return kafka.get_bootstrap_server()


@pytest.fixture(scope="session")
def ksqldb(
    network: Network, kafka: KafkaContainer
) -> Generator[KSQLDbContainer, None, None]:
    container = (
        KSQLDbContainer(bootstrap_servers=kafka.get_internal_bootstrap_server())
        .with_network(network)
        .with_network_aliases(_KSQLDB_ALIAS)
        .with_log_level("WARN")
    )
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def ksqldb_url(ksqldb: KSQLDbContainer) -> str:
    return ksqldb.get_url()


# ---------------------------------------------------------------------------
# Topic pre-creation
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def topics(bootstrap_server: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=[bootstrap_server])
    existing = set(admin.list_topics())
    new = [
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in (_INPUT_TOPIC, _OUTPUT_TOPIC)
        if t not in existing
    ]
    if new:
        try:
            admin.create_topics(new)
        except TopicAlreadyExistsError:
            pass
    admin.close()


# ---------------------------------------------------------------------------
# Stream creation via artemis/ksqldb-init sidecar
# ---------------------------------------------------------------------------

_INIT_IMAGE = "artemis/ksqldb-init:latest"


class _NoWait:
    """No-op wait strategy: start returns immediately; we block on .wait() instead."""

    def wait_until_ready(self, container: DockerContainer) -> None:
        pass


@pytest.fixture(scope="session")
def streams(
    network: Network,
    ksqldb: KSQLDbContainer,
    topics: None,
) -> None:
    """Run the artemis/ksqldb-init sidecar to create the ingestion streams.

    Uses the same image that production deploys — artemis_init.ksql is baked
    in, no file mounting required.

    Prerequisite: bazel run //tools/oci/images/ksqldb:artemis_init_tarball
    """
    internal_url = f"http://{_KSQLDB_ALIAS}:{KSQLDbContainer.HTTP_PORT}"
    container = (
        DockerContainer(_INIT_IMAGE)
        .with_network(network)
        .with_env("KSQLDB_SERVER_URL", internal_url)
        .waiting_for(_NoWait())
    )
    container.start()
    result = container.get_wrapped_container().wait(timeout=120)
    if result["StatusCode"] != 0:
        stdout, stderr = container.get_logs()
        container.stop()
        logs = f"stdout:\n{stdout.decode()}\nstderr:\n{stderr.decode()}"
        print(f"\n--- artemis/ksqldb-init logs ---\n{logs}\n---")
        raise RuntimeError(
            f"artemis/ksqldb-init exited with {result['StatusCode']}\n{logs}"
        )
    container.stop()
