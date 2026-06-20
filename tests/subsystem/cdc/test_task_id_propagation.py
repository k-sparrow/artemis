"""Subsystem test: the contract task_id propagates to ``ingestion_tasks``.

This is the suite that was *missing* — the seam the bug fell through.  The
existing ``test_cdc_pipeline`` injects **synthetic** rows into
``apollo_celery_taskmeta`` and is source-agnostic, so it never reproduces the
real Celery id linkage.  The worker-integration suite runs the **real** chain
but knows nothing about CDC.  Neither owns *identity propagation* across the
storage→worker→CDC→storage round-trip.

This test joins the two: it reuses the full CDC stack from ``conftest.py``
(Kafka, Schema Registry, Postgres+migrations, ksqlDB running ``artemis_init.ksql``,
Kafka Connect + the three connectors) and adds the **real Celery worker** (with
parsing and indexing **stubbed** — so the chain reaches SUCCESS with no
Docling/TEI/Qdrant/GPU).  It dispatches ``tasks.ingest`` with an explicit
``task_id`` — exactly as the storage service does when it returns that id to the
caller — and asserts the caller can resolve their upload by that id in
``ingestion_tasks``.

**This test is xfail(strict) today.**  The ksqlDB topology keys
``ingestion_tasks`` by the *index subtask* id (``<C>``, the ``tasks.index``
task_id), not the *contract* id (``<A>``, the ``tasks.ingest`` task_id the caller
holds — see ``artemis_init.ksql`` §6 ``PARTITION BY STRUCT(task_id := task_id)``
over ``WHERE name = 'tasks.index'``).  So the contract-id lookup finds nothing.
When the fix (a ksqlDB JOIN that recovers ``<A>`` and keys the sink by it) lands,
this test XPASSes — strict xfail then turns that into a failure, forcing the
marker to be removed.  Remove the ``xfail`` as the final commit of the fix.

Prerequisites (in addition to the ``test_cdc_pipeline`` ones):
    bazel run //src/backend/controller/worker:image.tarball
"""

from __future__ import annotations

import io
import json
import uuid

import pytest
import sqlalchemy as sa
from celery import Celery
from kombu import Exchange, Queue
from testcontainers.core.container import DockerContainer
from wiremock.testing.testcontainer import WireMockContainer

# Reuse the worker-integration artifact/response constants + worker bring-up so
# the two suites stay in lock-step on the chain's wire contract. The HTTP deps
# themselves are stubbed with WireMock here (the house convention), not the
# worker suite's in-process StubServer.
from tests.backend.controller.worker.integration.conftest import (  # noqa: PLC2701
    _ARTIFACT_BUCKET,
    _ARTIFACT_KEY,
    _IMAGE_TAG,
    _PARSE_RESPONSE,
    _SAMPLE_ARTIFACT,
    _UPSERT_RESULT,
    _wait_for_worker,
    upload_file,
)
from tests.lib.polling import poll_until

_WIREMOCK_PORT = 8080

_SOURCE_BUCKET = "ingest-source"
_FILENAME = "test.md"

# Generous CDC budget: Debezium WAL capture → ksqlDB transform → JDBC sink upsert.
_CDC_POLL_TIMEOUT_S = 120
_CDC_POLL_INTERVAL_S = 3


# ---------------------------------------------------------------------------
# Worker-side fixtures (localised here so the source-agnostic CDC tests in
# test_cdc_pipeline.py never pay for the worker stack).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def rabbitmq_container(request: pytest.FixtureRequest):
    from testcontainers.rabbitmq import RabbitMqContainer

    container = RabbitMqContainer(image="rabbitmq:4.1.2-management-alpine")
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def minio_container(request: pytest.FixtureRequest):
    from testcontainers.minio import MinioContainer

    container = MinioContainer()
    container.start()
    request.addfinalizer(container.stop)
    return container


@pytest.fixture(scope="session")
def minio_client(minio_container):
    return minio_container.get_client()


@pytest.fixture(scope="session")
def wiremock(request: pytest.FixtureRequest) -> WireMockContainer:
    """One WireMock stubbing BOTH HTTP deps of the chain — the parsing service
    (``POST /v1/parse`` → claim-check BlobRef) and the indexing service
    (``POST /ingest`` → UpsertResult). Matched by path, so a single instance
    backs both PARSING_SERVICE_URL and INGESTION_SERVICE_URL."""
    wm = WireMockContainer(secure=False)
    wm.with_mapping(
        "parsing-parse.json",
        {
            "request": {"method": "POST", "urlPath": "/v1/parse"},
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
    wm.start()
    request.addfinalizer(wm.stop)
    return wm


@pytest.fixture(scope="session")
def wiremock_url(wiremock: WireMockContainer) -> str:
    host = wiremock.get_container_host_ip()
    port = wiremock.get_exposed_port(_WIREMOCK_PORT)
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def worker_container(
    rabbitmq_container,
    minio_container,
    wiremock_url: str,
    # CDC stack (cdc/conftest.py): `connectors` brings up the whole pipeline AND
    # implies migrations + postgres; depending on it guarantees Debezium's
    # replication slot exists before the worker writes any taskmeta row.
    postgres,
    connectors: None,
    request: pytest.FixtureRequest,
) -> DockerContainer:
    """The real Celery worker, wired at the SHARED CDC Postgres so the taskmeta
    rows it writes are the ones Debezium captures."""
    minio_host = minio_container.get_container_host_ip()
    minio_port = minio_container.get_exposed_port(9000)
    rabbit_host = rabbitmq_container.get_container_host_ip()
    rabbit_port = rabbitmq_container.get_exposed_port(5672)
    pg_host = postgres.get_container_host_ip()
    pg_port = postgres.get_exposed_port(5432)
    broker_url = f"amqp://guest:guest@{rabbit_host}:{rabbit_port}/"

    env = {
        "S3_ENDPOINT": f"{minio_host}:{minio_port}",
        "S3_ACCESS_KEY": "minioadmin",
        "S3_SECRET_KEY": "minioadmin",
        "S3_SECURE": "false",
        "RABBITMQ_USER": "guest",
        "RABBITMQ_PASSWORD": "guest",
        "RABBITMQ_HOST": rabbit_host,
        "RABBITMQ_PORT": str(rabbit_port),
        "RABBITMQ_VHOST": "",
        # Result backend → the SHARED CDC Postgres (db `documents`), where
        # apollo_celery_taskmeta lives and Debezium is watching.
        "SQL_DB_HOST": pg_host,
        "SQL_DB_PORT": str(pg_port),
        "SQL_DB_USER": postgres.username,
        "SQL_DB_PASSWORD": postgres.password,
        "SQL_DB_DATABASE": postgres.dbname,
        "SQL_DRIVER": "postgresql+psycopg",
        "PARSING_SERVICE_URL": wiremock_url,
        "INGESTION_SERVICE_URL": wiremock_url,
        "EXCHANGE_NAME": "artemis",
        "PARSED_CHUNKS_BUCKET": "parsed-chunks",
        "HTTPX_TIMEOUT": "30.0",
    }

    container = DockerContainer(_IMAGE_TAG).with_kwargs(network_mode="host")
    for key, value in env.items():
        container.with_env(key, value)
    container.start()

    try:
        _wait_for_worker(broker_url, timeout=60)
    except TimeoutError:
        logs = container.get_logs()
        container.stop()
        raise TimeoutError(
            f"Celery worker did not become ready.\nbroker_url={broker_url!r}\n"
            f"Worker logs:\n{logs}"
        ) from None

    def _stop_and_dump():
        if request.session.testsfailed:
            stdout, stderr = container.get_logs()
            print(
                "\n=== Worker stdout ===\n"
                + stdout.decode(errors="replace")
                + "\n=== Worker stderr ===\n"
                + stderr.decode(errors="replace"),
                flush=True,
            )
        container.stop()

    request.addfinalizer(_stop_and_dump)
    return container


@pytest.fixture(scope="session")
def dispatch_app(rabbitmq_container, worker_container: DockerContainer) -> Celery:
    """Test-side Celery client that dispatches `tasks.ingest` the way storage does."""
    rabbit_host = rabbitmq_container.get_container_host_ip()
    rabbit_port = rabbitmq_container.get_exposed_port(5672)
    broker_url = f"amqp://guest:guest@{rabbit_host}:{rabbit_port}/"

    exchange = Exchange("artemis", type="direct")
    app = Celery(broker=broker_url)
    app.conf.task_serializer = "json"
    app.conf.accept_content = ["json"]
    app.conf.task_queues = [
        Queue(
            "artemis.ingestion.fetch-and-parse",
            exchange=exchange,
            routing_key="fetch-and-parse",
            durable=True,
        ),
        Queue(
            "artemis.ingestion.index",
            exchange=exchange,
            routing_key="index",
            durable=True,
        ),
    ]
    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_minio(minio_client) -> None:
    """Seed the source object + the parse artifact the parsing stub points at, so
    the real chain can run end-to-end (fetch → parse-ref → index → cleanup)."""
    if not minio_client.bucket_exists(_SOURCE_BUCKET):
        minio_client.make_bucket(_SOURCE_BUCKET)
    upload_file(minio_client, _SOURCE_BUCKET, _FILENAME)

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


def _fetch_one(engine: sa.Engine, query: str, params: dict):
    with engine.connect() as conn:
        return conn.execute(sa.text(query), params).fetchone()


def _dispatch_ingest(
    dispatch_app: Celery,
    namespace_id: uuid.UUID,
    *,
    task_id: uuid.UUID | None = None,
) -> None:
    """Dispatch ``tasks.ingest`` the way the storage service does (optionally with
    an explicit contract ``task_id``)."""
    dispatch_app.send_task(
        "tasks.ingest",
        kwargs={
            "s3": {"bucket": _SOURCE_BUCKET, "object": _FILENAME, "size": 1},
            "source": {
                "source": _FILENAME,
                "content_type": "text/markdown",
                "obj_id": str(uuid.uuid5(namespace_id, _FILENAME)),
                "object_type": "file",
            },
            "upload_action": "CREATE",
            "info": {"namespace_id": str(namespace_id)},
        },
        queue="artemis.ingestion.fetch-and-parse",
        routing_key="fetch-and-parse",
        task_id=str(task_id) if task_id is not None else None,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_real_worker_result_reaches_ingestion_tasks(
    dispatch_app: Celery,
    worker_container: DockerContainer,
    minio_client,
    postgres_engine: sa.Engine,
    namespace_row: uuid.UUID,
) -> None:
    """The REAL worker's result reaches ``ingestion_tasks`` via CDC.

    This is the harness guard (and genuinely new real-source coverage vs the
    synthetic ``test_cdc_pipeline``): it proves the worker → apollo_celery_taskmeta
    → Debezium → ksqlDB → JDBC sink path produces a SUCCESS row for the namespace.
    If this fails, the pipeline is broken — so a miss in the contract-id test below
    is unambiguously a *keying* problem, not a dead pipeline.
    """
    _seed_minio(minio_client)
    _dispatch_ingest(dispatch_app, namespace_row)

    try:
        row = poll_until(
            lambda: _fetch_one(
                postgres_engine,
                "SELECT status FROM ingestion_tasks WHERE namespace_id = :ns",
                {"ns": namespace_row},
            ),
            timeout=_CDC_POLL_TIMEOUT_S,
            interval=_CDC_POLL_INTERVAL_S,
        )
    except TimeoutError:
        pytest.fail(
            f"real worker → CDC produced no ingestion_tasks row for namespace "
            f"{namespace_row} within {_CDC_POLL_TIMEOUT_S}s — pipeline is broken."
        )
    assert row.status == "SUCCESS", f"expected SUCCESS, got {row.status!r}"


@pytest.mark.integration
def test_contract_task_id_resolves_in_ingestion_tasks(
    dispatch_app: Celery,
    worker_container: DockerContainer,
    minio_client,
    postgres_engine: sa.Engine,
    namespace_row: uuid.UUID,
) -> None:
    """A caller can resolve their upload's status by the task_id storage returned.

    Mirrors the storage service: dispatch ``tasks.ingest`` with an explicit
    ``task_id`` (the contract id ``<A>`` storage hands back), run the real chain
    to completion, then resolve ``<A>`` in ``ingestion_tasks`` — proving the id
    was registered AND reached SUCCESS. Liveness of the worker→CDC path is proven
    by ``test_real_worker_result_reaches_ingestion_tasks``, so a miss here is a
    keying failure.

    Regression guard for issue/task_id_propagation_fix: ``ingestion_tasks`` used to
    be keyed by the per-subtask Celery id (``tasks.index``), which the caller never
    sees. The contract id is now carried in ``IngestionResult.task_id`` and the
    ksqlDB ``ingestion_tasks`` fan-out keys the row by it.
    """
    _seed_minio(minio_client)

    contract_task_id = uuid.uuid4()  # <A> — the id storage returns to the caller
    _dispatch_ingest(dispatch_app, namespace_row, task_id=contract_task_id)

    # The row must be resolvable by the CONTRACT task_id the caller holds.
    try:
        row = poll_until(
            lambda: _fetch_one(
                postgres_engine,
                "SELECT status FROM ingestion_tasks WHERE task_id = :tid",
                {"tid": contract_task_id},
            ),
            timeout=_CDC_POLL_TIMEOUT_S,
            interval=_CDC_POLL_INTERVAL_S,
        )
    except TimeoutError:
        pytest.fail(
            f"ingestion_tasks has no row for the contract task_id {contract_task_id} "
            "that storage returned to the caller — the upload's status is unresolvable."
        )
    assert row.status == "SUCCESS", f"expected SUCCESS, got {row.status!r}"
