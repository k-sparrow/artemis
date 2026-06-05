"""Fixtures for the enterprise intake subsystem e2e test.

Full pipeline (all containers session-scoped):

  POST /data-sources              [data_sources control plane]
      → KafkaConnect deploys FileSource connector
      → FileSource (Camel, /watch)
      → artemis.datasource.filesystem          [Kafka topic, flat message + headers]
      → ksqlDB transformation                  [reads headers, shapes IntakeRequest JSON]
      → artemis.datasource.filesystem.intake   [Kafka topic, IntakeRequest-shaped JSON]
      → HTTP sink (Aiven)                      [deployed by http_sink_connector fixture]
      → POST /intake                           [intake service container]
      → GET /namespaces/{id}                   [WireMock — storage service stub]
      → POST /namespaces/{id}/objects          [WireMock — storage service stub]

Containers:
  - KafkaContainer          (KRaft broker, alias: broker)
  - KSQLDbContainer         (alias: ksqldb)
  - KafkaConnectContainer   (artemis/cp-kafka-connect:latest, alias: kafka-connect)
  - PostgresContainer       (alias: postgres, backing data_sources service)
  - WireMockContainer       (alias: wiremock)
      Stubs: POST /namespaces          → 201 (namespace creation by data_sources)
             GET  /namespaces/*        → 200 (namespace check by intake)
             POST /namespaces/*/objects→ 202 (file upload by intake)
             DELETE /namespaces/*      → 202 (soft-delete on data source DELETE)
  - DockerContainer         (artemis/backend-data-sources:dev, alias: data-sources)
  - DockerContainer         (artemis/backend-enterprise-intake:dev, alias: intake)
      Mounts session_watch_dir at /watch (read-only).

Pre-requisites:
    bazel run //src/backend/enterprise/data_sources/api:tarball.dev
    bazel run //src/backend/enterprise/intake/api:tarball.dev
    (artemis/cp-kafka-connect:latest must also be built)

Test isolation
--------------
All containers are session-scoped.  Tests write files with unique names to
``session_watch_dir``; the WireMock request journal is cleared before each
test.  The FileSource connector is created once via POST /data-sources in the
``data_source`` session fixture and removed at session teardown.  The
idempotent flag on the FileSource connector ensures files from previous tests
are never redelivered.
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path
from typing import Iterator

import httpx
import pytest
from fastapi import status
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.postgres import PostgresContainer
from wiremock.testing.testcontainer import WireMockContainer

from tests.lib.testcontainers.kafka import KafkaConnectContainer, KSQLDbContainer
from tests.lib.testcontainers.kafka.kafka import KafkaContainer

_NAMESPACE = "pipeline-test-ns"
_NAMESPACE_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_LIFECYCLE_NAMESPACE_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
_ORG_NAME = "pipeline-test-org"

_FILESYSTEM_TOPIC = "artemis.datasource.filesystem"

_KC_IMAGE = "artemis/cp-kafka-connect:latest"
_DS_IMAGE = "artemis/backend-data-sources:dev"
_DS_PORT = 9500
_INTAKE_IMAGE = "artemis/backend-enterprise-intake:dev"
_INTAKE_PORT = 9000
_WIREMOCK_INTERNAL_PORT = 8080

# ---------------------------------------------------------------------------
# ksqlDB transformation SQL
#
# Reads Camel FileSource messages from ``artemis.datasource.filesystem``.
# The FileSource injects artemis.* and CamelHeader.* Kafka record headers.
# ksqlDB promotes those headers to value fields and reshapes the record into
# the IntakeRequest JSON schema consumed by POST /intake.
# ---------------------------------------------------------------------------
_KSQL_CREATE_SOURCE_STREAM = """\
CREATE OR REPLACE STREAM `artemis-enterprise-datasource-filesystem` (
    `payload`      BYTES,
    `path`         BYTES HEADER('CamelHeader.CamelFileAbsolutePath'),
    `display_name` BYTES HEADER('CamelHeader.CamelFileName'),
    `namespace`    BYTES HEADER('artemis.namespace'),
    `namespace_id` BYTES HEADER('artemis.namespace_id'),
    `org_name`     BYTES HEADER('artemis.org_name'),
    `group_id`     BYTES HEADER('artemis.group_id'),
    `owner_id`     BYTES HEADER('artemis.owner_id')
)
WITH (
    KAFKA_TOPIC  = 'artemis.datasource.filesystem',
    VALUE_FORMAT = 'KAFKA',
    PARTITIONS   = 1
);
"""

_KSQL_CREATE_INTAKE_STREAM = """\
CREATE OR REPLACE STREAM `artemis-enterprise-datasource-filesystem-intake`
WITH (
    KAFKA_TOPIC  = 'artemis.datasource.filesystem.intake',
    VALUE_FORMAT = 'JSON',
    PARTITIONS   = 1
)
AS SELECT
    STRUCT(
        `type` := 'filesystem',
        `path` := FROM_BYTES(`path`, 'utf8')
    ) AS `source`,
    FROM_BYTES(`display_name`, 'utf8') AS `display_name`,
    FROM_BYTES(`namespace_id`, 'utf8') AS `namespace_id`,
    FROM_BYTES(`group_id`, 'utf8') AS `group_id`,
    FROM_BYTES(`owner_id`, 'utf8') AS `owner_id`
FROM `artemis-enterprise-datasource-filesystem`
EMIT CHANGES;
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wait_for_http(base_url: str, path: str, timeout: int = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(f"{base_url}{path}", timeout=3.0)
            if resp.status_code == status.HTTP_200_OK:
                return
        except httpx.RequestError:
            pass
        time.sleep(2)
    raise TimeoutError(f"{base_url}{path} did not become ready within {timeout}s")


def _post_ksql(ksqldb_url: str, statement: str) -> None:
    for attempt in range(10):
        resp = httpx.post(
            f"{ksqldb_url}/ksql",
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            json={
                "ksql": statement,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            },
            timeout=30.0,
        )
        if resp.status_code < 500:
            break
        print(
            f"  [ksqlDB retry {attempt + 1}/10] {resp.status_code}: {resp.text[:200]}"
        )
        time.sleep(3)
    if not resp.is_success:
        raise RuntimeError(
            f"ksqlDB statement failed ({resp.status_code}):\n"
            f"Statement: {statement.strip()}\n"
            f"Response:  {resp.text}"
        )


# ---------------------------------------------------------------------------
# Shared watch directory (session-scoped)
# ---------------------------------------------------------------------------


def _clear_dir(d: Path) -> None:
    """Remove all contents of *d* without deleting the directory itself."""
    if not d.exists():
        return
    for child in d.iterdir():
        if child.is_file():
            child.unlink()
        else:
            shutil.rmtree(child)


@pytest.fixture(scope="session")
def session_watch_dir(request: pytest.FixtureRequest) -> Path:
    """Persistent temp directory mounted into KafkaConnect and the intake container."""
    d = Path(tempfile.mkdtemp(prefix="intake-subsystem-watch-"))
    _clear_dir(d)
    request.addfinalizer(lambda: _clear_dir(d))
    request.addfinalizer(lambda: shutil.rmtree(d, ignore_errors=True))
    return d


# ---------------------------------------------------------------------------
# Docker network (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def docker_network(request: pytest.FixtureRequest) -> Network:
    network = Network()
    network.create()
    request.addfinalizer(network.remove)
    return network


# ---------------------------------------------------------------------------
# Kafka broker (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> KafkaContainer:
    container = (
        KafkaContainer()
        .with_kraft()
        .with_network(docker_network)
        .with_network_aliases("broker")
        .with_kwargs(hostname="broker")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def kafka_bootstrap_server(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def kafka_topic(kafka_bootstrap_server: str) -> str:
    """Pre-create the filesystem topic so ksqlDB can declare the source stream."""
    admin = KafkaAdminClient(bootstrap_servers=[kafka_bootstrap_server])
    try:
        admin.create_topics(
            [NewTopic(_FILESYSTEM_TOPIC, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    admin.close()
    return _FILESYSTEM_TOPIC


# ---------------------------------------------------------------------------
# ksqlDB (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ksqldb_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    kafka_topic: str,
) -> KSQLDbContainer:
    container = (
        KSQLDbContainer(
            bootstrap_servers=kafka_container.get_internal_bootstrap_server()
        )
        .with_network(docker_network)
        .with_network_aliases("ksqldb")
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def ksqldb_url(ksqldb_container: KSQLDbContainer) -> str:
    return ksqldb_container.get_url()


@pytest.fixture(scope="session")
def ksqldb_transformation(ksqldb_url: str, kafka_topic: str) -> None:
    """POST the two CREATE STREAM statements that build the intake pipeline."""
    _post_ksql(ksqldb_url, _KSQL_CREATE_SOURCE_STREAM)
    _post_ksql(ksqldb_url, _KSQL_CREATE_INTAKE_STREAM)


# ---------------------------------------------------------------------------
# Postgres — backing the data_sources service (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def postgres_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> PostgresContainer:
    container = (
        PostgresContainer(
            image="postgres:16-alpine",
            username="postgres",
            password="postgres",
            dbname="data_sources_test",
        )
        .with_network(docker_network)
        .with_network_aliases("postgres")
        .with_command(
            "postgres "
            "-c wal_level=logical "
            "-c max_wal_senders=10 "
            "-c max_replication_slots=10"
        )
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def migrations_container(
    docker_network: Network,
    postgres_container: PostgresContainer,
) -> None:
    from tests.lib.testcontainers.migrations import run_migrations_on_network

    run_migrations_on_network(
        docker_network,
        postgres_container.username,
        postgres_container.password,
        postgres_container.dbname,
    )


# ---------------------------------------------------------------------------
# WireMock — stubs all storage service calls (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def wiremock(
    request: pytest.FixtureRequest,
    docker_network: Network,
) -> WireMockContainer:
    wm = WireMockContainer(secure=False)
    wm.with_network(docker_network)
    wm.with_network_aliases("wiremock")
    _owner_id_required = {"X-Owner-Id": {"matches": ".+"}}

    wm.with_mapping(
        "namespace-create.json",
        {
            # Priority 1: matches only the session-scoped namespace by name.
            "priority": 1,
            "request": {
                "method": "POST",
                "url": "/namespaces",
                "headers": _owner_id_required,
                "bodyPatterns": [
                    {"matchesJsonPath": f"$[?(@.name == '{_NAMESPACE}')]"}
                ],
            },
            "response": {
                "status": 201,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {"id": _NAMESPACE_ID, "type": "shared", "name": _NAMESPACE},
            },
        },
    )
    wm.with_mapping(
        "namespace-create-lifecycle.json",
        {
            # Priority 2: catch-all for lifecycle connectors — returns a distinct ID
            # so they don't appear as siblings of the session-scoped connector in the DB.
            "priority": 2,
            "request": {
                "method": "POST",
                "url": "/namespaces",
                "headers": _owner_id_required,
            },
            "response": {
                "status": 201,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {
                    "id": _LIFECYCLE_NAMESPACE_ID,
                    "type": "shared",
                    "name": "lifecycle-ns",
                },
            },
        },
    )
    wm.with_mapping(
        "namespace-get.json",
        {
            "request": {
                "method": "GET",
                "urlPattern": "/namespaces/.*",
                "headers": _owner_id_required,
            },
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {"id": _NAMESPACE_ID, "type": "shared", "name": _NAMESPACE},
            },
        },
    )
    wm.with_mapping(
        "namespace-upload.json",
        {
            "request": {
                "method": "POST",
                "urlPathPattern": "/namespaces/.*/objects",
                "headers": _owner_id_required,
            },
            "response": {
                "status": 202,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {
                    "task_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
                },
            },
        },
    )
    wm.with_mapping(
        "namespace-delete.json",
        {
            "request": {
                "method": "DELETE",
                "urlPattern": "/namespaces/.*",
                "headers": _owner_id_required,
            },
            "response": {"status": 202},
        },
    )
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


@pytest.fixture(scope="session")
def wiremock_host_url(wiremock: WireMockContainer) -> str:
    host = wiremock.get_container_host_ip()
    port = wiremock.get_exposed_port(_WIREMOCK_INTERNAL_PORT)
    return f"http://{host}:{port}"


# ---------------------------------------------------------------------------
# KafkaConnect — session-scoped; mounts session_watch_dir at /watch
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_connect(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_container: KafkaContainer,
    session_watch_dir: Path,
) -> KafkaConnectContainer:
    container = (
        KafkaConnectContainer(
            bootstrap_servers=kafka_container.get_internal_bootstrap_server(),
            image=_KC_IMAGE,
        )
        .with_network(docker_network)
        .with_network_aliases("kafka-connect")
        .with_volume_mapping(str(session_watch_dir), "/watch", "ro")
        .with_log_level("WARN")
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def kafka_connect_url(kafka_connect: KafkaConnectContainer) -> str:
    return kafka_connect.get_url()


# ---------------------------------------------------------------------------
# Data sources service — session-scoped
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def data_sources_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    kafka_connect: KafkaConnectContainer,
    postgres_container: PostgresContainer,
    wiremock: WireMockContainer,
    migrations_container: None,
) -> DockerContainer:
    container = (
        DockerContainer(_DS_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("data-sources")
        .with_exposed_ports(_DS_PORT)
        .with_env(
            "SQL_DB_URL",
            f"postgresql+asyncpg://{postgres_container.username}:{postgres_container.password}"  # noqa: E501
            f"@postgres:5432/{postgres_container.dbname}",
        )
        .with_env("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
        .with_env("STORAGE_SERVICE_URL", f"http://wiremock:{_WIREMOCK_INTERNAL_PORT}")
        .with_env("DEBUG", "true")
    )
    container.start()
    request.addfinalizer(container.stop)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(_DS_PORT)
    try:
        _wait_for_http(f"http://{host}:{port}", "/health/liveness")
    except TimeoutError:
        stdout, stderr = container.get_logs()
        print("\n=== data-sources stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== data-sources stderr ===\n", stderr.decode(errors="replace"))
        raise
    return container


@pytest.fixture(scope="session")
def data_sources_base_url(data_sources_container: DockerContainer) -> str:
    host = data_sources_container.get_container_host_ip()
    port = data_sources_container.get_exposed_port(_DS_PORT)
    return f"http://{host}:{port}"


# ---------------------------------------------------------------------------
# Intake service container — session-scoped
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def intake_container(
    request: pytest.FixtureRequest,
    docker_network: Network,
    ksqldb_transformation: None,
    wiremock: WireMockContainer,
    session_watch_dir: Path,
) -> DockerContainer:
    container = (
        DockerContainer(_INTAKE_IMAGE)
        .with_network(docker_network)
        .with_network_aliases("intake")
        .with_exposed_ports(_INTAKE_PORT)
        .with_env("STORAGE_SERVICE_URL", f"http://wiremock:{_WIREMOCK_INTERNAL_PORT}")
        .with_env("DEBUG", "true")
        .with_volume_mapping(str(session_watch_dir), "/watch", "ro")
    )
    container.start()
    request.addfinalizer(container.stop)
    host = container.get_container_host_ip()
    port = container.get_exposed_port(_INTAKE_PORT)
    try:
        _wait_for_http(f"http://{host}:{port}", "/health/liveness")
    except TimeoutError:
        stdout, stderr = container.get_logs()
        print("\n=== intake stdout ===\n", stdout.decode(errors="replace"))
        print("\n=== intake stderr ===\n", stderr.decode(errors="replace"))
        raise
    return container


# ---------------------------------------------------------------------------
# HTTP sink connector — deployed after intake is healthy so the consumer group
# commits its ``latest`` offset before any file is produced.
# ---------------------------------------------------------------------------

_HTTP_SINK_CONNECTOR_NAME = "AivenHttpSinkConnector__FilesystemIntake"
_INTAKE_TOPIC = "artemis.datasource.filesystem.intake"


@pytest.fixture(scope="session")
def http_sink_connector(
    kafka_connect: KafkaConnectContainer,
    intake_container: DockerContainer,
) -> None:
    """Deploy the Aiven HTTP sink connector via the Kafka Connect REST API."""
    intake_internal_url = f"http://intake:{_INTAKE_PORT}/intake"
    config = {
        "connector.class": "io.aiven.kafka.connect.http.HttpSinkConnector",
        "tasks.max": "1",
        "topics": _INTAKE_TOPIC,
        "http.url": intake_internal_url,
        "http.authorization.type": "none",
        "batching.enabled": "false",
        "consumer.override.auto.offset.reset": "latest",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "http.headers.content.type": "application/json",
        "errors.tolerance": "all",
        "errors.deadletterqueue.topic.name": f"{_INTAKE_TOPIC}.dlq",
        "errors.deadletterqueue.topic.replication.factor": "1",
        "errors.deadletterqueue.context.headers.enable": "true",
    }
    kc_url = kafka_connect.get_url()
    with httpx.Client(base_url=kc_url, timeout=15.0) as client:
        resp = client.put(
            f"/connectors/{_HTTP_SINK_CONNECTOR_NAME}/config", json=config
        )
        resp.raise_for_status()

    # Wait for RUNNING before tests produce any messages.
    deadline = time.monotonic() + 60
    with httpx.Client(base_url=kc_url, timeout=10.0) as client:
        while time.monotonic() < deadline:
            r = client.get(f"/connectors/{_HTTP_SINK_CONNECTOR_NAME}/status")
            if r.is_success:
                s = r.json()
                tasks = s.get("tasks", [])
                if (
                    s.get("connector", {}).get("state") == "RUNNING"
                    and tasks
                    and all(t["state"] == "RUNNING" for t in tasks)
                ):
                    return
            time.sleep(2)
    pytest.fail("HTTP sink connector did not reach RUNNING within 60s")


# ---------------------------------------------------------------------------
# Data source — session-scoped, created via data_sources REST API
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def data_source(
    http_sink_connector: None,
    data_sources_base_url: str,
) -> Iterator[dict]:
    """Create one data source via the control plane; wait for connector RUNNING."""
    with httpx.Client(base_url=data_sources_base_url, timeout=10.0) as client:
        resp = client.post(
            "/data-sources",
            json={
                "display_name": "Pipeline Test Docs",
                "path": "/watch",
                "namespace": _NAMESPACE,
                "org_name": _ORG_NAME,
                "recursive": False,
            },
        )
        assert resp.status_code == status.HTTP_201_CREATED, resp.text
        body = resp.json()

        source_id = body["id"]
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            poll = client.get(f"/data-sources/{source_id}")
            assert poll.status_code == status.HTTP_200_OK
            if poll.json().get("kafka_status", {}).get("state") == "RUNNING":
                break
            time.sleep(2)
        else:
            pytest.fail(
                f"FileSource connector for data_source {source_id} did not reach "
                f"RUNNING within 90s"
            )

        yield body

        client.delete(f"/data-sources/{source_id}")


# ---------------------------------------------------------------------------
# Per-test WireMock request log reset
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_wiremock_requests(wiremock_host_url: str) -> Iterator[None]:
    httpx.delete(f"{wiremock_host_url}/__admin/requests")
    yield


# ---------------------------------------------------------------------------
# Log dump on test failure
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def dump_logs_on_failure(
    request: pytest.FixtureRequest,
    intake_container: DockerContainer,
    data_sources_container: DockerContainer,
    kafka_connect: KafkaConnectContainer,
    ksqldb_container: KSQLDbContainer,
) -> Iterator[None]:
    yield
    if getattr(getattr(request.node, "rep_call", None), "failed", False):
        for name, container in [
            ("intake", intake_container),
            ("data-sources", data_sources_container),
            ("kafka-connect", kafka_connect),
            ("ksqldb", ksqldb_container),
        ]:
            stdout, stderr = container.get_logs()
            print(f"\n=== {name} stdout ===\n", stdout.decode(errors="replace"))
            print(f"\n=== {name} stderr ===\n", stderr.decode(errors="replace"))


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):  # noqa: ARG001
    outcome = yield
    rep = outcome.get_result()
    setattr(item, f"rep_{rep.when}", rep)
