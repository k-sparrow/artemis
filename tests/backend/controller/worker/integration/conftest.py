"""Tier 3 integration test fixtures.

Infrastructure (session-scoped):
  - RabbitMQ testcontainer   — broker
  - Postgres testcontainer   — result backend
  - MinIO testcontainer      — S3 source + parse-artifact (claim-check) intermediate

HTTP service stubs — one WireMock container, two path-scoped views (session-scoped):
  - parsing_stub   — stubs the 4-step async chain under /v1/parse/*:
                     POST /v1/parse/submit → SubmitResult
                     GET  /v1/parse/status/* → ParseStatus "success"
                     POST /v1/parse/resolve → ResolveResult
                     POST /v1/parse/finalize → BlobRef (pre-seeded artifact in MinIO)
  - indexing_stub  — returns UpsertResult on POST /ingest, 204 on DELETE /ingest
  Each ``WireMockStub`` filters the shared WireMock request journal by its path,
  keeping the old StubServer surface (``requests``/``push_response``/``clear``).

Worker container (session-scoped):
  - Real Celery worker running inside the pre-built Docker image with host
    networking so it can reach all testcontainer ports and the WireMock stub.
  - Requires the image to be loaded before the test run:
      bazel run //src/backend/controller/worker:image.tarball

Per-test helpers:
  - reset_stubs (autouse)        - resets the WireMock journal + scenarios
                                   between tests
  - s3_source_bucket (autouse)   - ensures "ingest-source" bucket exists;
                                   cleans up objects after
"""

from __future__ import annotations

import base64
import io
import json
import uuid
from typing import Any

import httpx
import psycopg
import pytest
from minio import Minio
from testcontainers.core.container import DockerContainer
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.rabbitmq import RabbitMqContainer
from wiremock.testing.testcontainer import WireMockContainer

from tests.lib.polling import poll_until

_WIREMOCK_PORT = 8080

_IMAGE_TAG = "artemis/backend-controller-worker:latest"

# ---------------------------------------------------------------------------
# Configurable stub HTTP server
# ---------------------------------------------------------------------------

_STUB_OBJ_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
_STUB_CONV_TASK_ID = "stub-conv-task-id"
_STUB_CHUNK_TASK_ID = "stub-chunk-task-id"

_SAMPLE_ARTIFACT = {
    "pages": [
        {
            "obj_id": _STUB_OBJ_ID,
            "page_no": 1,
            "markdown": "# hello world\n\n| a | b |",
        }
    ],
    "chunks": [
        {
            "page_content": "hello world",
            "source": "test.md",
            "type": "text",
            "obj_id": _STUB_OBJ_ID,
            "page_no": 1,
        },
        {
            "page_content": "| a | b |",
            "source": "test.md",
            "type": "table",
            "obj_id": _STUB_OBJ_ID,
            "page_no": 1,
        },
    ],
}

# Claim-check: the parsing stub returns a BlobRef pointing at an artifact the
# seed_artifact fixture pre-writes to MinIO (mirrors the real parsing service
# writing the artifact). The index task reads/cleans it up by this key.
_ARTIFACT_BUCKET = "parsed-chunks"
_ARTIFACT_KEY = f"parse/{_STUB_OBJ_ID}.json"
_PARSE_RESPONSE = {"bucket": _ARTIFACT_BUCKET, "key": _ARTIFACT_KEY}

# Async-parse chain stubs:
# submit → status (success) → resolve → status (success) → finalize
_SUBMIT_RESULT = {
    "parsing_task_id": _STUB_CONV_TASK_ID,
    "obj_id": _STUB_OBJ_ID,
}
_PARSE_STATUS_SUCCESS = {
    "status": "success",
    "num_processed": 1,
    "num_total": 1,
    "error_message": None,
}
_RESOLVE_RESULT = {
    "chunking_task_id": _STUB_CHUNK_TASK_ID,
    "obj_id": _STUB_OBJ_ID,
}

_UPSERT_RESULT = {
    "num_added": 2,
    "num_updated": 0,
    "num_skipped": 0,
    "num_deleted": 0,
    "ids": ["vec-id-1", "vec-id-2"],
}


class WireMockStub:
    """A ``StubServer``-compatible facade over a WireMock container, scoped to one
    URL path.

    The parsing and indexing services share a single WireMock instance; each
    ``WireMockStub`` filters the shared request journal by its own path so callers
    keep two independent views. The public surface (``url``, ``requests``,
    ``push_response``, ``clear``) matches the old in-process stub so the tests are
    unchanged. ``requests`` entries keep the old shape — ``method``/``path``/
    ``headers``/``body`` with ``body`` as **bytes** (decoded from the journal's
    ``bodyAsBase64``), so existing ``b"..." in body`` and ``json.loads(body)``
    assertions still work.
    """

    def __init__(self, base_url: str, path: str) -> None:
        self.url = base_url  # the worker points PARSING/INGESTION_SERVICE_URL here
        self._admin = f"{base_url}/__admin"
        self._path = path  # "/v1/parse" or "/ingest"
        self._oneshot_mapping_ids: list[str] = []

    @property
    def requests(self) -> list[dict]:
        resp = httpx.get(f"{self._admin}/requests", timeout=5.0)
        resp.raise_for_status()
        out: list[dict] = []
        for entry in resp.json()["requests"]:
            req = entry["request"]
            url = req.get("url", "")
            if not url.split("?")[0].startswith(self._path):
                continue
            if req.get("bodyAsBase64"):
                body = base64.b64decode(req["bodyAsBase64"])
            else:
                body = (req.get("body") or "").encode()
            out.append(
                {
                    "method": req.get("method", ""),
                    "path": url,
                    "headers": req.get("headers", {}),
                    "body": body,
                }
            )
        return out

    def push_response(self, status: int, body: Any) -> None:
        """Make the NEXT POST to this path return *status*, then fall back to the
        default mapping. Implemented as a one-shot WireMock scenario transition."""
        scenario = f"oneshot-{self._path.strip('/').replace('/', '-')}"
        mapping = {
            "scenarioName": scenario,
            "requiredScenarioState": "Started",
            "newScenarioState": "served",
            "priority": 1,  # outrank the default mapping while in the Started state
            "request": {"method": "POST", "urlPath": self._path},
            "response": {
                "status": status,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": body,
            },
        }
        resp = httpx.post(f"{self._admin}/mappings", json=mapping, timeout=5.0)
        resp.raise_for_status()
        self._oneshot_mapping_ids.append(resp.json()["id"])

    def clear(self) -> None:
        for mapping_id in self._oneshot_mapping_ids:
            httpx.delete(f"{self._admin}/mappings/{mapping_id}", timeout=5.0)
        self._oneshot_mapping_ids.clear()
        httpx.post(f"{self._admin}/scenarios/reset", timeout=5.0)
        httpx.post(f"{self._admin}/requests/reset", timeout=5.0)


# ---------------------------------------------------------------------------
# Infrastructure containers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def rabbitmq_container(request: pytest.FixtureRequest) -> RabbitMqContainer:
    container = RabbitMqContainer(image="rabbitmq:4.1.2-management-alpine")
    container.start()
    request.addfinalizer(container.stop)
    container.get_connection_params
    return container


@pytest.fixture(scope="session")
def postgres_container(request: pytest.FixtureRequest) -> PostgresContainer:
    container = PostgresContainer(
        image="postgres:16-alpine",
        username="postgres",
        password="postgres",
        dbname="celery_results",
    ).with_command(
        "postgres "
        "-c wal_level=logical "
        "-c max_wal_senders=10 "
        "-c max_replication_slots=10"
    )
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def migrations_container(postgres_container: PostgresContainer) -> None:
    from tests.lib.testcontainers.migrations import run_migrations_host

    run_migrations_host(
        username=postgres_container.username,
        password=postgres_container.password,
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        dbname=postgres_container.dbname,
    )


@pytest.fixture(scope="session")
def minio_container(request: pytest.FixtureRequest) -> MinioContainer:
    container = MinioContainer()
    container.start()
    request.addfinalizer(container.stop)
    return container


# ---------------------------------------------------------------------------
# WireMock — stubs the parsing + indexing HTTP services (session-scoped)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def wiremock(request: pytest.FixtureRequest) -> WireMockContainer:
    """One WireMock stubbing both HTTP deps of the chain, matched by path.

    Parsing (async 4-step chain):
      POST /v1/parse/submit   → SubmitResult
      GET  /v1/parse/status/* → ParseStatus "success"
      POST /v1/parse/resolve  → ResolveResult
      POST /v1/parse/finalize → BlobRef (same as old /v1/parse claim-check)

    Indexing:
      POST   /ingest → UpsertResult
      DELETE /ingest → 204
    """
    wm = WireMockContainer(secure=False)
    wm.with_mapping(
        "parsing-submit.json",
        {
            "request": {"method": "POST", "urlPath": "/v1/parse/submit"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": _SUBMIT_RESULT,
            },
        },
    )
    wm.with_mapping(
        "parsing-status.json",
        {
            "request": {"method": "GET", "urlPattern": "/v1/parse/status/.*"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": _PARSE_STATUS_SUCCESS,
            },
        },
    )
    wm.with_mapping(
        "parsing-resolve.json",
        {
            "request": {"method": "POST", "urlPath": "/v1/parse/resolve"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": _RESOLVE_RESULT,
            },
        },
    )
    wm.with_mapping(
        "parsing-finalize.json",
        {
            "request": {"method": "POST", "urlPath": "/v1/parse/finalize"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": _PARSE_RESPONSE,
            },
        },
    )
    wm.with_mapping(
        "indexing-ingest.json",
        {
            "request": {"method": "POST", "urlPath": "/ingest"},
            "response": {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": _UPSERT_RESULT,
            },
        },
    )
    wm.with_mapping(
        "indexing-delete.json",
        {
            "request": {"method": "DELETE", "urlPath": "/ingest"},
            "response": {"status": 204},
        },
    )
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


@pytest.fixture(scope="session")
def _wiremock_base_url(wiremock: WireMockContainer) -> str:
    host = wiremock.get_container_host_ip()
    port = wiremock.get_exposed_port(_WIREMOCK_PORT)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def parsing_stub(_wiremock_base_url: str) -> WireMockStub:
    return WireMockStub(_wiremock_base_url, "/v1/parse")


@pytest.fixture(scope="session")
def indexing_stub(_wiremock_base_url: str) -> WireMockStub:
    return WireMockStub(_wiremock_base_url, "/ingest")


# ---------------------------------------------------------------------------
# Worker subprocess
# ---------------------------------------------------------------------------


def _worker_env(
    rabbitmq_container: RabbitMqContainer,
    postgres_container: PostgresContainer,
    minio_container: MinioContainer,
    parsing_stub: WireMockStub,
    indexing_stub: WireMockStub,
) -> dict:
    minio_host = minio_container.get_container_host_ip()
    minio_port = minio_container.get_exposed_port(9000)
    rabbit_host = rabbitmq_container.get_container_host_ip()
    rabbit_port = rabbitmq_container.get_exposed_port(5672)
    pg_host = postgres_container.get_container_host_ip()
    pg_port = postgres_container.get_exposed_port(5432)

    return {
        "S3_ENDPOINT": f"{minio_host}:{minio_port}",
        "S3_ACCESS_KEY": "minioadmin",
        "S3_SECRET_KEY": "minioadmin",
        "S3_SECURE": "false",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_HOST": rabbit_host,
        "RABBITMQ_PORT": str(rabbit_port),
        "RABBITMQ_VHOST": "",
        "SQL_DB_HOST": pg_host,
        "SQL_DB_PORT": str(pg_port),
        "SQL_DB_USER": "celery",
        "SQL_DB_PASSWORD": "celery",
        "SQL_DB_DATABASE": "celery_results",
        "SQL_DRIVER": "postgresql+psycopg",
        "PARSING_SERVICE_URL": parsing_stub.url,
        "INGESTION_SERVICE_URL": indexing_stub.url,
        "EXCHANGE_NAME": "artemis",
        "PARSED_CHUNKS_BUCKET": "parsed-chunks",
        "HTTPX_TIMEOUT": "30.0",
    }


def _wait_for_worker(broker_url: str, timeout: int = 60) -> None:
    from celery import Celery as _Celery

    check = _Celery(broker=broker_url)

    def _ready() -> bool:
        # inspect() raises while the broker is still coming up; active_queues()
        # returns None until a worker replies. poll_until retries on both.
        return bool(check.control.inspect(timeout=2).active_queues())

    try:
        poll_until(_ready, timeout=timeout, interval=2.0)
    except TimeoutError:
        raise TimeoutError(
            "Celery worker did not become ready within timeout"
        ) from None


@pytest.fixture(scope="session")
def worker_container(
    rabbitmq_container: RabbitMqContainer,
    postgres_container: PostgresContainer,
    minio_container: MinioContainer,
    parsing_stub: WireMockStub,
    indexing_stub: WireMockStub,
    migrations_container: None,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    env = _worker_env(
        rabbitmq_container,
        postgres_container,
        minio_container,
        parsing_stub,
        indexing_stub,
    )
    rabbit_host = rabbitmq_container.get_container_host_ip()
    rabbit_port = rabbitmq_container.get_exposed_port(5672)
    broker_url = f"amqp://guest:guest@{rabbit_host}:{rabbit_port}/"

    container = DockerContainer(_IMAGE_TAG)
    container.with_kwargs(network_mode="host")
    for key, value in env.items():
        container.with_env(key, value)
    container.start()

    try:
        _wait_for_worker(broker_url, timeout=60)
    except TimeoutError:
        logs = container.get_logs()
        container.stop()
        raise TimeoutError(
            f"Celery worker container did not become ready within timeout\n"
            f"broker_url = {broker_url!r}\n"
            f"Worker logs:\n{logs}"
        ) from None

    def _stop_and_dump():
        if request.session.testsfailed:
            stdout, stderr = container.get_logs()
            print(
                "\n=== Worker container stdout ===\n"
                + stdout.decode(errors="replace")
                + "\n=== Worker container stderr ===\n"
                + stderr.decode(errors="replace"),
                flush=True,
            )
        container.stop()

    request.addfinalizer(_stop_and_dump)
    return container


# ---------------------------------------------------------------------------
# Dispatch client (test-side Celery app — does NOT import tasks.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def dispatch_app(
    rabbitmq_container: RabbitMqContainer,
    worker_container: DockerContainer,
):
    from celery import Celery
    from kombu import Exchange, Queue

    rabbit_host = rabbitmq_container.get_container_host_ip()
    rabbit_port = rabbitmq_container.get_exposed_port(5672)
    broker_url = f"amqp://guest:guest@{rabbit_host}:{rabbit_port}/"

    _exchange = Exchange("artemis", type="direct")
    app = Celery(broker=broker_url)
    app.conf.task_serializer = "json"
    app.conf.accept_content = ["json"]
    app.conf.task_queues = [
        Queue(
            name="artemis.ingestion.parse",
            exchange=_exchange,
            routing_key="parse",
            durable=True,
        ),
        Queue(
            name="artemis.ingestion.index",
            exchange=_exchange,
            routing_key="index",
            durable=True,
        ),
    ]
    return app


# ---------------------------------------------------------------------------
# MinIO client (test-side)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def minio_client(minio_container: MinioContainer) -> Minio:
    return minio_container.get_client()


# ---------------------------------------------------------------------------
# Per-test helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_stubs(parsing_stub: WireMockStub, indexing_stub: WireMockStub):
    parsing_stub.clear()
    indexing_stub.clear()
    yield
    parsing_stub.clear()
    indexing_stub.clear()


@pytest.fixture(autouse=True)
def s3_source_bucket(minio_client: Minio):
    bucket = "ingest-source"
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
    yield bucket
    for obj in minio_client.list_objects(bucket, recursive=True):
        minio_client.remove_object(bucket, obj.object_name)


@pytest.fixture(autouse=True)
def seed_artifact(minio_client: Minio):
    """Pre-write the parse artifact the parsing stub points at, so the chain's
    artifact read (indexing) and cleanup (index task delete) operate on a real
    object — mirroring the parsing service writing the artifact."""
    if not minio_client.bucket_exists(_ARTIFACT_BUCKET):
        minio_client.make_bucket(_ARTIFACT_BUCKET)
    data = json.dumps(_SAMPLE_ARTIFACT).encode()
    minio_client.put_object(
        _ARTIFACT_BUCKET,
        _ARTIFACT_KEY,
        io.BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    yield
    for obj in minio_client.list_objects(_ARTIFACT_BUCKET, recursive=True):
        minio_client.remove_object(_ARTIFACT_BUCKET, obj.object_name)


@pytest.fixture
def namespace_id() -> uuid.UUID:
    return uuid.uuid4()


# ---------------------------------------------------------------------------
# Helpers for tests
# ---------------------------------------------------------------------------


def upload_file(
    minio_client: Minio,
    bucket: str,
    key: str,
    content: bytes = b"# Hello World\n\nSome content.\n",
) -> None:
    minio_client.put_object(
        bucket, key, io.BytesIO(content), len(content), content_type="text/markdown"
    )


def wait_until_stub_called(
    stub: WireMockStub, timeout: int = 60, method: str = "POST"
) -> None:
    """Block until the stub receives at least one request with *method*."""
    poll_until(
        lambda: any(r["method"] == method for r in stub.requests),
        timeout=timeout,
        interval=0.5,
    )


def wait_until_stub_called_n_times(
    stub: WireMockStub,
    n: int,
    timeout: int = 60,
    method: str = "POST",
) -> None:
    """Block until the stub receives at least *n* requests with *method*."""
    poll_until(
        lambda: sum(1 for r in stub.requests if r["method"] == method) >= n,
        timeout=timeout,
        interval=0.5,
    )


def wait_until_minio_empty(minio_client: Minio, bucket: str, timeout: int = 30) -> None:
    """Block until the bucket has no objects (cleanup complete)."""

    def _empty() -> bool:
        try:
            return not list(minio_client.list_objects(bucket, recursive=True))
        except Exception:
            return False

    poll_until(_empty, timeout=timeout, interval=0.5)


def _pg_connstr(postgres_container: PostgresContainer) -> str:
    host = postgres_container.get_container_host_ip()
    port = postgres_container.get_exposed_port(5432)
    return f"host={host} port={port} dbname=celery_results user=celery password=celery"


def wait_for_task(
    task_id: str,
    postgres_container: PostgresContainer,
    timeout: int = 60,
) -> str:
    """Poll apollo_celery_taskmeta until the task reaches SUCCESS or FAILURE.

    Returns the final status string. Raises TimeoutError if not done within *timeout*s.
    """
    connstr = _pg_connstr(postgres_container)

    def _terminal_status() -> str | None:
        with psycopg.connect(connstr) as conn:
            row = conn.execute(
                "SELECT status FROM apollo_celery_taskmeta WHERE task_id = %s",
                (task_id,),
            ).fetchone()
        if row and row[0] in ("SUCCESS", "FAILURE"):
            return row[0]
        return None

    return poll_until(_terminal_status, timeout=timeout, interval=1.0)


def wait_for_task_result(
    task_id: str,
    postgres_container: PostgresContainer,
    timeout: int = 60,
) -> dict:
    """Poll until *task_id* reaches SUCCESS; return its parsed JSON result.

    Used to read a dispatcher task's own return value (e.g. ``ingest``'s
    ``{"chain_id": ...}``) — distinct from ``wait_for_task``, which only reports
    the terminal status of a task whose result payload isn't needed.
    """
    connstr = _pg_connstr(postgres_container)

    def _result() -> dict | None:
        with psycopg.connect(connstr) as conn:
            row = conn.execute(
                "SELECT status, result FROM apollo_celery_taskmeta WHERE task_id = %s",
                (task_id,),
            ).fetchone()
        if row and row[0] == "SUCCESS":
            return json.loads(row[1])
        return None

    return poll_until(_result, timeout=timeout, interval=1.0)
