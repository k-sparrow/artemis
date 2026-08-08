# Controller Worker

**Type:** Celery worker (no HTTP port)  
**Broker:** RabbitMQ via AMQP  
**Result backend:** PostgreSQL (`apollo_celery_taskmeta`)  
**Source:** `src/backend/controller/`

The controller worker orchestrates the ingestion pipeline: async parse (scatter-gather),
then index. It is the only service that calls both the parsing service and the indexing
service. It does not serve HTTP requests.

---

## Task Queues

| Queue | Worker | Concurrency | Tasks |
|-------|--------|-------------|-------|
| `artemis.ingestion.parse` | `backend-controller-parse-worker` | 3 | `tasks.ingest`, `tasks.parse`, `tasks.submit_parse`, `tasks.poll_parse`, `tasks.resolve_parse`, `tasks.submit_chunk`, `tasks.poll_chunk` |
| `artemis.ingestion.index` | `backend-controller-index-worker` | 1 | `tasks.index`, `tasks.delete_document`, `tasks.delete_namespace` |

The parse worker runs at `concurrency=3` with `prefetch_multiplier=1` — three Celery
threads can each hold a task, but each thread either makes a single short HTTP call
(submit, status, resolve, chunk submit, chunk finalize) or is sleeping in `self.retry()`.
No thread blocks for the full duration of a Docling job.

The index worker remains serial (`concurrency=1`) because the indexing service calls
the TEI embedding model, which is GPU-bound and not safely parallelised from a single host.

---

## Task Chain

### CREATE / MODIFY

```
tasks.ingest(upload_action=CREATE|MODIFY)
  │
  └──► chain(
         parse.s(s3, source=..., namespace_id=..., group_id=..., task_id=..., operation=...),
         index.s(namespace_id=..., upload_action=..., group_id=..., source=..., s3=..., task_id=...)
       )
```

`tasks.parse` uses `self.replace()` to substitute itself with the internal async sub-chain:

```
parse  →self.replace()→  chain(
                           submit_parse    → SubmitResult dict
                           poll_parse      → SubmitResult dict (pass-through after conversion terminal)
                           resolve_parse   → ResolveResult dict {obj_id}
                           submit_chunk    → ChunkSubmitResult dict {chunking_task_id, obj_id}
                           poll_chunk      → calls chunk finalize → BlobRef dict
                         )
```

Chunking is a fully decoupled stage from conversion (Epic 21): `resolve_parse` only downloads
and caches the conversion result — it never submits a chunk job itself. `submit_chunk` is a
separate task, mirroring `submit_parse`'s retry structure, so the two stages can fail and
retry independently instead of sharing one task's error handling.

`index` receives the `BlobRef` dict from `poll_chunk` (via `self.replace()`) as its
first positional argument — identical interface to the old `fetch_and_parse`.

Every sub-task receives `task_id` as a kwarg so that `FailureRecordingTask.on_failure`
can always recover the contract `task_id` for CDC records regardless of which sub-task fails.

### DELETE

```
tasks.ingest(upload_action=DELETE)
  │
  └──► delete_document.s(task_id, obj_id, namespace_id)
```

`delete_document` calls `DELETE /ingest?namespace=<id>&obj_id=<id>` on the indexing service.

### Namespace deletion

```
tasks.delete_namespace(namespace_id)
  │
  └──► Calls DELETE /ingest?namespace=<id>  (no obj_id — clears the entire namespace)
```

Dispatched by the storage service when a namespace is soft-deleted.

---

## Parse Sub-chain Tasks

### `submit_parse`

Fetches source bytes from MinIO and calls `POST /v1/parse/submit` on the parsing service,
which submits a single async conversion to Docling Serve regardless of document size — large
PDFs are fanned out into page slices and converted concurrently server-side by Docling
Serve's Ray engine (see TODOs.md Epic 21), not by any client-side splitting. Returns
`SubmitResult {parsing_task_id, obj_id}`.

### `poll_parse`

Calls `GET /v1/parse/status/{parsing_task_id}`. Retries with a 60–75 s countdown while
`status == "processing"`. On `"failure"` raises `DocumentConversionError` (permanent).
On `"success"` passes `SubmitResult` through unchanged to `resolve_parse`.

`max_retries=1440` (≈ 24 h ceiling at 60 s/retry).

### `resolve_parse`

Calls `POST /v1/parse/resolve`. The parsing service downloads the conversion result
(`GET /v1/result/{parsing_task_id}`), writes the replay cache, and derives + caches pages —
it does not submit a chunk job. Returns `ResolveResult {obj_id}`.

### `submit_chunk`

Calls `POST /v1/chunk/submit`. The parsing service hands docling-serve the replay-cache S3
location directly (no document bytes cross this task or the parsing-service call). Same
retry structure as `submit_parse` (`max_retries=20`). Returns
`ChunkSubmitResult {chunking_task_id, obj_id}`.

### `poll_chunk`

Calls `GET /v1/chunk/status/{chunking_task_id}`. Same retry pattern as `poll_parse`.
On `"success"` calls `POST /v1/chunk/finalize`, which fetches the chunk result, merges it
with the pages `resolve_parse` already cached, and writes the `ParseArtifact` to MinIO.
Returns the `BlobRef` dict that `index` consumes.

---

## Resiliency Layers

The worker uses two co-operating layers to handle downstream service failures.

### Layer 1 — Celery retry (first line of defence)

All tasks use manual `try/except` blocks. Each exception type has a different backoff:

| Exception | Behaviour | Countdown | Max retries |
|-----------|-----------|-----------|-------------|
| `httpx.HTTPStatusError` 5xx | Transient upstream failure — retry | `min(120, 2^n) + jitter([0, backoff])` | 20 (parse sub-tasks), scalable for poll tasks |
| `pybreaker.CircuitBreakerError` | Breaker open — wait for reset window | `reset_timeout + 5s + jitter([0, 30s])` = 125–155 s | same as above |
| `httpx.HTTPStatusError` 4xx | Permanent failure — propagate | — | — |

`poll_parse` and `poll_chunk` use `max_retries=1440` (≈ 24 h). All other parse tasks
use `max_retries=20`.

### Layer 2 — Circuit breakers (second line of defence)

Two `pybreaker.CircuitBreaker` singletons guard outbound HTTP calls:

| Breaker | Wraps | Opens after | Probes after |
|---------|-------|-------------|--------------|
| `parsing_breaker` | all `/v1/parse/*` and `/v1/chunk/*` calls | 3 consecutive failures | 120 s (HALF-OPEN) |
| `indexing_breaker` | `call_indexing_service`, `call_delete_service` | 3 consecutive failures | 120 s (HALF-OPEN) |

Once a downstream is known to be failing, the circuit breaker fast-fails subsequent calls
without making a network request. State transitions are logged at WARNING level by
`_LoggingListener`; monitor `circuit_breaker=<name> state=<old>-><new>` log lines.

---

## acks_late and GPU Serialisation

Every task in the parse sub-chain (`parse`, `submit_parse`, `poll_parse`, `resolve_parse`,
`submit_chunk`, `poll_chunk`) is decorated with `acks_late=True`. Between retries a task is
"parked" in the worker's in-memory timer waiting for its ETA, not sitting acked-and-forgotten
in RabbitMQ — a worker crash during that park window leaves the message unacked so RabbitMQ
redelivers it, rather than dropping it permanently.

---

## FailureRecordingTask

`parse`, `submit_parse`, `poll_parse`, `resolve_parse`, `submit_chunk`, `poll_chunk`, and
`index` inherit from `FailureRecordingTask`. On terminal failure:

```python
def on_failure(self, exc, task_id, args, kwargs, einfo):
    failure_record = FailureRecord(
        task_id=kwargs["task_id"],       # contract task_id from IngestionTaskDetails
        namespace_id=kwargs["namespace_id"],
        failure_reason=str(exc)[:4096],
    )
    _db_backend.store_result(
        task_id,
        result=failure_record.model_dump(mode="json"),
        status=states.FAILURE,
        traceback=None,
        request=self.request,            # preserves name/args/kwargs/worker columns
    )
```

`store_result(..., request=self.request)` is required. Omitting it nulls the extended
columns and breaks the ksqlDB `name IN (...)` filter for the failure fan-out.

---

## Result Backend

`src/backend/controller/worker/backend.py` defines `DatabaseBackend`, a custom Celery
result backend that writes to `apollo_celery_taskmeta` (shared PostgreSQL) with
`result_extended=True` to persist `name`, `args`, `kwargs`, `worker` columns.

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `RABBITMQ_HOST` | `rabbitmq` | |
| `RABBITMQ_PORT` | `5672` | |
| `RABBITMQ_USER` | `artemis` | |
| `RABBITMQ_PASSWORD` | `artemis` | |
| `RABBITMQ_VHOST` | `artemis` | |
| `EXCHANGE_NAME` | *(required)* | Must match the RabbitMQ Sink connector |
| `SQL_DB_HOST` | `postgres` | Result backend |
| `SQL_DB_PORT` | `5432` | |
| `SQL_DB_USER` | `postgres` | |
| `SQL_DB_PASSWORD` | `testpass` | |
| `SQL_DB_DATABASE` | `documents` | |
| `SQL_DRIVER` | `postgresql+psycopg` | |
| `S3_ENDPOINT` | `http://minio:9000` | |
| `S3_ACCESS_KEY` | `minioadmin` | |
| `S3_SECRET_KEY` | `minioadmin` | |
| `S3_SECURE` | `false` | |
| `PARSED_CHUNKS_BUCKET` | `parsed-chunks` | |
| `PARSING_SERVICE_URL` | `http://backend-parsing:10001` | |
| `INGESTION_SERVICE_URL` | `http://backend-indexing:10000` | |
| `HTTPX_TIMEOUT` | `600` | For indexing/delete calls only; parse calls use per-endpoint timeouts |
| `PARSING_SUBMIT_TIMEOUT` | `120.0` | Source download/forward + HTTP POST |
| `PARSING_STATUS_TIMEOUT` | `30.0` | Single status GET (parse or chunk task) |
| `PARSING_RESOLVE_TIMEOUT` | `120.0` | Conversion result fetch + replay/pages cache write |
| `PARSING_CHUNK_SUBMIT_TIMEOUT` | `30.0` | Chunk job submission (S3-direct, no bytes) |
| `PARSING_FINALIZE_TIMEOUT` | `60.0` | Chunk result fetch + artifact write |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-controller-worker` | |
