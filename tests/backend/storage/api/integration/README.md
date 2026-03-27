# Integration Tests — Storage Service (Tier 2)

HTTP-level integration tests for the storage service.
Source under test: all of `src/backend/storage/api/` — routers, services, models.

---

## What these tests cover

| Concern | Covered |
|---|---|
| Full HTTP request → router → service → DB round-trip | Yes |
| Namespace persistence and retrieval from real Postgres | Yes |
| Shared namespace idempotency (UUID5 get-or-create) | Yes |
| Soft-delete: row marked deleted, excluded from listings and GET | Yes |
| Rename: private allowed, shared returns 409 | Yes |
| File upload: S3 object created with correct metadata (`task_id`, `task_type=CREATE`) | Yes |
| File reingest: S3 object overwritten with `task_type=MODIFY` | Yes |
| File tombstone: zero-byte S3 object written with `task_type=DELETE` | Yes |
| List files: scoped to namespace, excludes other namespaces | Yes |
| 404 on missing resources | Yes |
| 422 on schema validation failures (e.g. shared namespace without name) | Yes |
| 400 on invalid owner UUID | Yes |
| Service business logic (DB queries, constraint handling) | Yes — no mocks |
| Kafka notification on MinIO upload | **No** — see below |
| Celery task dispatch | **No** — see below |

---

## Test structure

```
tests/backend/storage/api/integration/
├── conftest.py     ← infrastructure fixtures (containers, cleanup)
└── test_api.py     ← 18 HTTP-level test cases
```

The integration suite sits alongside the unit suites but does not share their
conftest layers (no mock session, no mock MinIO). It only inherits the top-level
`api/conftest.py` for the env-var defaults and the lifespan mock (see below).

---

## Fixture layering

```
tests/backend/storage/api/conftest.py          ← autouse: env vars + lifespan mock
tests/backend/storage/api/integration/conftest.py ← real infrastructure
tests/backend/storage/api/integration/test_api.py
```

### Layer 1 — `api/conftest.py` (inherited, autouse)

Sets env vars at module import time so `StorageSettings` can be instantiated, and
replaces `app.router.lifespan_context` with a no-op. The lifespan is still mocked
here even in integration tests because `link_s3_bucket_with_kafka_event` requires a
Kafka ARN registered in MinIO — not available in a plain `MinioContainer`. Schema
creation and bucket setup are handled explicitly by the integration fixtures instead.

### Layer 2 — `integration/conftest.py`

#### Infrastructure (session-scoped)

```
PostgresContainer  (postgres:16-alpine)
MinioContainer     (minio/minio default image)
```

Both containers start once per test session and are stopped via `request.addfinalizer`.
This keeps container startup overhead (a few seconds) out of individual test timing.

`storage_session_factory` creates a SQLAlchemy async engine against the Postgres
container and runs `Base.metadata.create_all` to build the schema. It is declared
as an `async` fixture (pytest-asyncio manages the session-scoped event loop).

`test_minio_client` returns a `Minio` client pointed at the container and ensures the
`S3_ARTEMIS_BUCKET` bucket exists.

#### Per-test cleanup (autouse)

```python
# After each test:
TRUNCATE owner, namespace, ingested_file CASCADE
# + remove all objects from the S3 bucket
```

Tests are order-independent: each starts with a clean DB and empty bucket.

#### Client fixture (function-scoped)

```python
@pytest.fixture
def client(async_db_url, test_minio_client, storage_session_factory) -> TestClient:
    engine = create_async_engine(async_db_url)   # fresh engine per test
    factory = async_sessionmaker(engine, ...)
    app.dependency_overrides[get_db_session] = _get_session
    app.dependency_overrides[get_minio_client] = lambda: test_minio_client
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
    engine.sync_engine.dispose()                 # synchronous dispose
```

**Why a fresh engine per test?**

`TestClient` uses `anyio` internally and runs each HTTP request in its own event loop.
`asyncpg` connections are bound to the loop that created them. If the client shared
the session-scoped engine, its anyio-loop connections would leak back into the pool
and `clean_db` (running on pytest-asyncio's session loop) would hit:

```
RuntimeError: Task ... got Future attached to a different loop
```

Creating a fresh engine per test and calling `engine.sync_engine.dispose()` (not
`await engine.dispose()`) before returning control to `clean_db` ensures no
anyio-loop connections survive in the shared pool.

---

## Seed helper

Tests that need a pre-existing `IngestedFile` row (reingest, delete, list) use
`_seed_ingested_file(session_factory, namespace_id)`, which inserts a row directly
via the session factory and returns the file's UUID. This bypasses the upload endpoint
to keep tests focused on the operation under test.

```python
file_id = _seed_ingested_file(storage_session_factory, ns_id)
response = client.put(f"/namespaces/{ns_id}/files/{file_id}", files=...)
```

---

## What was skipped and why

### Kafka/MinIO notification link

`link_s3_bucket_with_kafka_event` in the real lifespan registers a MinIO bucket
notification pointing at a Kafka topic. This requires a Kafka broker and a
correctly registered ARN in MinIO's notification config — not available in a
plain `MinioContainer`. Testing this path requires an e2e test with the full
docker-compose stack. Skipped at this tier; covered by e2e tests.

### Celery task dispatch

The upload/reingest/delete endpoints write S3 objects with metadata (`task_id`,
`task_type`) that MinIO's Kafka notification forwards to the controller. The
controller dispatches a Celery task. This entire path is async and out-of-process —
integration tests only verify the S3 side (object exists, metadata correct).
Full pipeline coverage lives in the e2e subsystem tests.

### Service-layer integration tests

A `test_namespace_service.py` / `test_files_service.py` tier (calling service
functions directly against real Postgres, no HTTP) was considered and deferred.
The HTTP integration tests already exercise the same service code paths; a separate
service tier would add coverage only for edge cases better handled at the unit level
with mocks. Revisit if service logic grows significantly more complex.

### Storage container in e2e

A `storage_container` fixture (running the storage service as a Docker image in
`tests/e2e/conftest.py`) was deferred pending the storage service Docker image build.
Will be added as part of Epic 2.

---

## Running the tests

```bash
# Via Bazel (recommended)
bazel test //tests/backend/storage/api/integration:test_api

# Direct pytest (requires containers to be available on PATH)
pytest tests/backend/storage/api/integration/
```

Docker must be available. The test session starts and stops the containers
automatically via Testcontainers.
