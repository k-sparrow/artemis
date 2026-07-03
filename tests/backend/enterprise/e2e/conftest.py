"""Session fixtures for the enterprise → ingestion full e2e test.

Full pipeline under test:
  POST /data-sources              [backend-enterprise-data-sources]
      → KafkaConnect deploys FileSource connector
      → FileSource (Camel) watches a host directory mounted into kafka-connect
      → artemis.datasource.filesystem          [Kafka]
      → ksqlDB transformation
      → artemis.datasource.filesystem.intake   [Kafka]
      → HTTP sink                              [deployed by artemis-kafka-connect-init]
      → POST /intake                           [backend-enterprise-intake]
      → POST /namespaces/{id}/objects          [backend-storage]
      → Celery task (controller worker)
      → Docling parse + TEI embed
      → Qdrant vectors

The docker-compose.test.yaml artefact is produced by:
    bazel build //tools/docker:docker_compose_test

and passed to this conftest via the --compose-file pytest argument, set in
BUILD.bazel using $(location //tools/docker:docker_compose_test).

Profiles started: infra, backend, enterprise, ai, ai-tei, mcp
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as sa
from qdrant_client import QdrantClient
from testcontainers.compose import DockerCompose
from tests.lib.polling import (
    poll_until,
    wait_for_celery_workers,
    wait_for_http,
    wait_for_kc_connector,
)
from tests.lib.testcontainers.vllm import VLLMContainer


# ---------------------------------------------------------------------------
# Helpers
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


def _wait_connector_running(
    data_sources_url: str, source_id: str, timeout: int = 120
) -> None:
    import httpx

    def _is_running() -> bool:
        resp = httpx.get(f"{data_sources_url}/data-sources/{source_id}", timeout=5.0)
        return (
            resp.is_success
            and resp.json().get("kafka_status", {}).get("state") == "RUNNING"
        )

    try:
        poll_until(_is_running, timeout=timeout, interval=3.0)
    except TimeoutError:
        raise TimeoutError(
            f"Connector for data source {source_id} "
            f"did not reach RUNNING within {timeout}s"
        )


# ---------------------------------------------------------------------------
# pytest option
# ---------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--compose-file",
        required=True,
        help=(
            "Absolute path to the generated docker-compose.test.yaml "
            "(provided by Bazel via $(location))."
        ),
    )


# ---------------------------------------------------------------------------
# Session fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def compose_file(pytestconfig: pytest.Config) -> Path:
    return Path(pytestconfig.getoption("--compose-file"))


@pytest.fixture(
    scope="session",
    params=["dense", "hybrid"],
    ids=["dense", "hybrid"],
)
def retrieval_mode(request: pytest.FixtureRequest) -> str:
    """Parametrize the full pipeline over CPU-only retrieval modes.

    Sets RETRIEVAL_MODE in the process environment before the compose stack
    starts so docker-compose substitutes it into backend-indexing-ingestion.
    multi_stage is excluded here — its ai-colbert compose profile requires a
    GPU-backed vLLM container; it is covered by the indexing smoke tests
    (tagged local) which use VLLMContainer directly.
    """
    mode = request.param
    os.environ["RETRIEVAL_MODE"] = mode
    return mode


@pytest.fixture(
    scope="session",
    params=["none", "colbert"],
    ids=["no-reranker", "colbert"],
)
def reranker_mode(request: pytest.FixtureRequest) -> str:
    """Parametrize the full pipeline over ColBERT reranker on/off.

    When "colbert": starts a VLLMContainer serving colbert-ir/colbertv2.0,
    sets COLBERT_RERANKER_URL / COLBERT_MODEL_NAME / COLBERT_MAX_TOKENS_PER_DOC
    so docker-compose wires the reranker into backend-indexing-ingestion.
    Must be requested before the ``compose`` fixture.

    GPU + NVIDIA Docker runtime required for the "colbert" variant.
    """
    mode = request.param
    if mode == "colbert":
        hf_cache = Path.home() / ".cache/huggingface"
        container = VLLMContainer("colbert-ir/colbertv2.0")
        container.with_hf_cache(str(hf_cache))
        container.start()
        request.addfinalizer(container.stop)
        port = container.get_exposed_port(VLLMContainer.HTTP_PORT)
        os.environ["COLBERT_RERANKER_URL"] = f"http://host.docker.internal:{port}"
        os.environ["COLBERT_MODEL_NAME"] = "colbert-ir/colbertv2.0"
        os.environ["COLBERT_MAX_TOKENS_PER_DOC"] = "511"
    else:
        os.environ.pop("COLBERT_RERANKER_URL", None)
    return mode


@pytest.fixture(scope="session")
def watch_dir(request: pytest.FixtureRequest) -> Path:
    """Host directory mounted into kafka-connect and intake as /watch.

    Uses tempfile.mkdtemp() (resolves to a real /tmp path) rather than
    pytest's tmp_path_factory, which creates directories inside Bazel's
    sandbox that Docker cannot reach as volume mounts.

    Sets ARTEMIS_WATCH_DIR before the compose stack starts so Docker Compose
    substitutes it into the volume declarations in docker-compose.test.yaml.
    Must be requested before the ``compose`` fixture.
    """
    d = Path(tempfile.mkdtemp(prefix="artemis-e2e-watch-"))
    _clear_dir(d)
    request.addfinalizer(lambda: _clear_dir(d))
    request.addfinalizer(lambda: shutil.rmtree(d, ignore_errors=True))
    os.environ["ARTEMIS_WATCH_DIR"] = str(d)
    return d


_DIAGNOSTIC_SERVICES = [
    "backend-storage",
    "backend-enterprise-data-sources",
    "backend-enterprise-intake",
    "backend-indexing-ingestion",
    "backend-controller-parse-worker",
    "backend-controller-index-worker",
    "backend-parsing",
    "backend-mcp",
    "kafka-connect",
    "artemis-kafka-connect-init",
    "ksqldb-server",
    "artemis-ksqldb-init",
    "ksqldb-enterprise-init",
    "tei",
]


@pytest.fixture(scope="session", autouse=True)
def mcp_enable_upload() -> None:
    """Enable the upload_file MCP tool for e2e tests.

    The tool is off by default (requires gateway auth); e2e tests run against
    the full compose stack where ownership is not yet enforced end-to-end,
    so enabling it here is safe for test purposes.
    """
    os.environ["MCP_ENABLE_UPLOAD"] = "true"


@pytest.fixture(scope="session")
def compose(
    compose_file: Path,
    watch_dir: Path,
    retrieval_mode: str,  # sets RETRIEVAL_MODE env var before compose starts
    reranker_mode: str,  # sets COLBERT_RERANKER_URL (or nothing) before compose starts
    mcp_enable_upload: None,  # sets MCP_ENABLE_UPLOAD before compose starts
    request: pytest.FixtureRequest,
) -> DockerCompose:
    dc = DockerCompose(
        context=str(compose_file.parent),
        compose_file_name=[compose_file.name],
        profiles=["infra", "backend", "enterprise", "ai", "ai-tei", "mcp"],
        pull=False,
        build=False,
    )
    try:
        dc.start()
    except Exception:
        # docker compose up --wait exits 1 when any container exits, even
        # successfully-completed init containers (e.g. ksqldb-init exits 0).
        # We verify actual readiness ourselves via HTTP health checks below.
        pass

    # Wait for the services the test actively drives.
    # Docling and TEI are included in the ai profile and can be slow to start.
    host_storage, port_storage = dc.get_service_host_and_port("backend-storage", 7000)
    host_ds, port_ds = dc.get_service_host_and_port(
        "backend-enterprise-data-sources", 9500
    )
    host_intake, port_intake = dc.get_service_host_and_port(
        "backend-enterprise-intake", 9000
    )
    host_ingestion, port_ingestion = dc.get_service_host_and_port(
        "backend-indexing-ingestion", 10000
    )
    host_parsing, port_parsing = dc.get_service_host_and_port("backend-parsing", 10001)

    host_kc, port_kc = dc.get_service_host_and_port("kafka-connect", 8083)
    kc_url = f"http://{host_kc}:{port_kc}"

    host_tei, port_tei = dc.get_service_host_and_port("tei", 80)

    try:
        # TEI downloads the model on first start and can take several minutes.
        wait_for_http(f"http://{host_tei}:{port_tei}/info", timeout=360)
        wait_for_http(f"http://{host_storage}:{port_storage}/health/readiness")
        wait_for_http(f"http://{host_ds}:{port_ds}/health/liveness")
        wait_for_http(f"http://{host_intake}:{port_intake}/health/liveness")
        wait_for_http(f"http://{host_ingestion}:{port_ingestion}/health/readiness")
        # backend-parsing depends on docling-serve and can be slow; the Celery
        # worker calls it synchronously so it must be up before any file is dropped.
        wait_for_http(f"http://{host_parsing}:{port_parsing}/health/readiness")
        # MCP server is stateless — just wait for its liveness probe.
        host_mcp, port_mcp = dc.get_service_host_and_port("backend-mcp", 11000)
        wait_for_http(f"http://{host_mcp}:{port_mcp}/health/liveness")
        # Both system connectors must be RUNNING before any file is dropped.
        # auto.offset.reset=latest means messages produced before the connector
        # is up would be lost.
        wait_for_kc_connector(kc_url, "AivenHttpSinkConnector__FilesystemIntake")
        wait_for_kc_connector(
            kc_url, "CamelRabbitMQSinkConnector__CeleryTasksIngestion"
        )
        # CDC pipeline: Debezium source + two JDBC sinks
        wait_for_kc_connector(
            kc_url, "DebeziumPostgresSourceConnector__CeleryResultBackendPublish"
        )
        wait_for_kc_connector(
            kc_url, "DebeziumJdbcSinkConnector__CeleryResultToIngestedObjects"
        )
        wait_for_kc_connector(
            kc_url, "DebeziumJdbcSinkConnector__CeleryResultToIngestionTasks"
        )
        # Both Celery workers must be consuming before any file is dropped.
        # Workers connect to RabbitMQ asynchronously after compose starts;
        # tasks dispatched before they're ready would queue up but not be lost,
        # however a failed worker start (e.g. bad env) would cause a silent hang.
        rabbit_host, rabbit_port = dc.get_service_host_and_port("rabbitmq", 5672)
        broker_url = f"amqp://artemis:artemis@{rabbit_host}:{rabbit_port}/artemis"
        wait_for_celery_workers(
            broker_url,
            required_queues=["artemis.ingestion.parse", "artemis.ingestion.index"],
            timeout=120,
        )
    except Exception:
        _dump_service_logs(dc)
        dc.stop()
        raise

    yield dc

    if request.session.testsfailed:
        _dump_service_logs(dc)
    dc.stop()


def _dump_service_logs(dc: DockerCompose) -> None:
    for svc in _DIAGNOSTIC_SERVICES:
        try:
            stdout, stderr = dc.get_logs(svc)
            if stdout.strip() or stderr.strip():
                print(f"\n=== {svc} ===\n{stdout}\n{stderr}")
        except Exception as e:
            print(f"\n=== {svc}: could not retrieve logs: {e} ===")


@pytest.fixture(scope="session")
def storage_url(compose: DockerCompose) -> str:
    host, port = compose.get_service_host_and_port("backend-storage", 7000)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def data_sources_url(compose: DockerCompose) -> str:
    host, port = compose.get_service_host_and_port(
        "backend-enterprise-data-sources", 9500
    )
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def qdrant_client(compose: DockerCompose) -> QdrantClient:
    host, port = compose.get_service_host_and_port("vectorstore", 6333)
    return QdrantClient(host=host, port=int(port))


@pytest.fixture(scope="session")
def indexing_url(compose: DockerCompose) -> str:
    host, port = compose.get_service_host_and_port("backend-indexing-ingestion", 10000)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def mcp_url(compose: DockerCompose) -> str:
    host, port = compose.get_service_host_and_port("backend-mcp", 11000)
    # MCP SDK rejects "0.0.0.0" as an invalid Host header (DNS rebinding protection).
    # The port is mapped to all host interfaces, so localhost is always reachable.
    if host == "0.0.0.0":
        host = "localhost"
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def postgres_engine(compose: DockerCompose) -> sa.Engine:
    host, port = compose.get_service_host_and_port("postgres", 5432)
    engine = sa.create_engine(
        f"postgresql+psycopg://postgres:testpass@{host}:{port}/documents"
    )
    yield engine
    engine.dispose()
