# Controller Worker

**Type:** Celery worker (no HTTP port)  
**Broker:** RabbitMQ via AMQP  
**Result backend:** PostgreSQL (`apollo_celery_taskmeta`)  
**Source:** `src/backend/controller/`

The controller worker orchestrates the two-step ingestion pipeline: fetch-and-parse, then
index. It is the only service that calls both the parsing service and the indexing service.
It does not serve HTTP requests.

---

## Task Queues

| Queue | Concurrency | Tasks |
|-------|-------------|-------|
| `gpu_bound` | 1 | `tasks.fetch_and_parse`, `tasks.ingest`, `tasks.delete_document`, `tasks.delete_namespace` |
| `io_bound` | scalable | `tasks.index` |

`gpu_bound` is serial (concurrency=1) to prevent concurrent Docling jobs from competing
for the GPU. `io_bound` scales horizontally — the indexing service calls TEI and Qdrant
which are both network-bound.

---

## Task Chain

### CREATE / MODIFY

```
tasks.ingest(upload_action=CREATE|MODIFY)
  │
  └──► chain(
         fetch_and_parse.s(task_id, s3_details, source_details, info),
         index.s(task_id, source_details, info)
       )
```

`fetch_and_parse` receives the full `IngestionTaskDetails` from the Celery kwargs (delivered
by ksqlDB + RabbitMQ). It calls the parsing service with a `BlobRef` pointing to the file
in MinIO, and returns `BlobRef {bucket: "parsed-chunks", key: "<uuid4>.json"}`.

`index` receives the `BlobRef` from `fetch_and_parse` (chained via `Signature.s()`) and the
remaining kwargs from `tasks.ingest`. It calls the indexing service to embed chunks and write
to Qdrant. Returns `IngestionResult`.

### DELETE

```
tasks.ingest(upload_action=DELETE)
  │
  └──► delete_document.s(task_id, obj_id, namespace_id)
```

`delete_document` calls `DELETE /ingest?namespace=<id>&obj_id=<id>` on the indexing service.
Returns a minimal `IngestionResult` with `num_added=0, num_skipped=0`.

### Namespace deletion

```
tasks.delete_namespace(namespace_id)
  │
  └──► Calls DELETE /ingest?namespace=<id> (no obj_id — clears the entire namespace)
```

Dispatched by the storage service when a namespace is soft-deleted.

---

## acks_late and GPU Serialisation

`tasks.fetch_and_parse` is decorated with `acks_late=True`:
- The RabbitMQ message is NOT acknowledged until the task completes
- If the worker crashes during parsing, the message is redelivered
- Combined with `concurrency=1`, at most one Docling job runs at a time

This is the current stopgap for heavy-PDF stability. The full solution (async Docling
polling via `self.retry`) is planned in Epic 18.

---

## Retry Policy

Both `fetch_and_parse` and `index` use:
```python
autoretry_for=(Exception,)
max_retries=5
retry_backoff=True          # exponential backoff
retry_backoff_max=120       # seconds
retry_jitter=True
```

`tasks.ingest` and `tasks.delete_document` autoretry only on `CircuitBreakerError`
(which is thrown when downstream services are overwhelmed). Other failures propagate
immediately to the `on_failure` handler.

---

## Circuit Breakers

Two `pybreaker.CircuitBreaker` instances are module-level singletons:

| Breaker | Downstream | Opens after | Resets after |
|---------|-----------|-------------|-------------|
| `parsing_breaker` | Parsing service | 3 failures | 60 seconds |
| `indexing_breaker` | Indexing service | 3 failures | 60 seconds |

When a breaker opens, all tasks that call through it raise `CircuitBreakerError`. The
autoretry policy catches this and schedules a retry with exponential backoff. Once the
reset timeout elapses, the next retry attempt closes the breaker.

A `_LoggingListener` logs state transitions (CLOSED → OPEN → HALF_OPEN) as WARNING
messages; monitor these for downstream service health signals.

---

## FailureRecordingTask

`fetch_and_parse` and `index` inherit from `FailureRecordingTask`. When a task fails after
exhausting retries, `on_failure()` is called:

```python
def on_failure(self, exc, task_id, args, kwargs, einfo):
    failure_record = FailureRecord(
        task_id=kwargs["task_id"],       # contract task_id from IngestionTaskDetails
        namespace_id=kwargs["info"]["namespace_id"],
        failure_reason=str(exc)[:4096],
    )
    _db_backend.store_result(
        task_id,
        result=failure_record.model_dump(mode="json"),
        status=states.FAILURE,
        traceback=None,                  # suppress raw traceback
        request=self.request,            # preserves name/args/kwargs/worker columns
    )
```

The `FailureRecord` JSON is what Debezium reads from the WAL to populate
`ingestion_tasks.status = 'FAILURE'` with the correct contract `task_id` as the key.
`traceback=None` prevents the raw Python exception string from leaking into the CDC
pipeline's `EXTRACTJSONFIELD` call.

`store_result(..., request=self.request)` is required to preserve the extended columns
(`name`, `args`, `kwargs`, `worker`). Omitting `request=` nulls those columns and breaks
the ksqlDB `name IN (...)` filter for the failure fan-out.

---

## Result Backend

`src/backend/controller/worker/backend.py` defines `DatabaseBackend`, a custom Celery
result backend that:
- Writes to `apollo_celery_taskmeta` (same PostgreSQL database as the rest of the stack)
- Sets `result_extended=True` to persist `name`, `args`, `kwargs`, `worker` columns
- Uses a single shared connection pool (`_db_backend` module-level singleton)

---

## HTTPX Timeout

```
HTTPX_TIMEOUT=86400   # seconds (24 hours)
```

Aligned to RabbitMQ's `consumer_timeout = 86400000ms`. Both must be raised together —
raising one without the other still causes timeout failures on heavy PDFs.

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `RABBITMQ_HOST` | `rabbitmq` | |
| `RABBITMQ_PORT` | `5672` | |
| `RABBITMQ_USER` | `artemis` | |
| `RABBITMQ_PASS` | `artemis` | |
| `RABBITMQ_VHOST` | `artemis` | |
| `EXCHANGE_NAME` | `ingestion_test0` | Must match the RabbitMQ Sink connector |
| `SQL_DB_HOST` | `postgres` | Result backend |
| `SQL_DB_PORT` | `5432` | |
| `SQL_DB_USER` | `postgres` | |
| `SQL_DB_PASSWORD` | `testpass` | |
| `SQL_DB_DATABASE` | `documents` | |
| `SQL_DRIVER` | `postgresql+psycopg` | Synchronous driver for result backend |
| `S3_ENDPOINT_URL` | `http://minio:9000` | |
| `S3_ACCESS_KEY` | `minioadmin` | |
| `S3_SECRET_KEY` | `minioadmin` | |
| `PARSED_CHUNKS_BUCKET` | `parsed-chunks` | Passed in BlobRef to indexing |
| `PARSING_SERVICE_URL` | `http://backend-parsing:10001` | |
| `INGESTION_SERVICE_URL` | `http://backend-indexing:10000` | |
| `HTTPX_TIMEOUT` | `86400` | Must match RabbitMQ consumer_timeout in seconds |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-controller-worker` | |
