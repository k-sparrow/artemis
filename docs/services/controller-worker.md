# Controller Worker

**Type:** Celery worker (no HTTP port)  
**Broker:** RabbitMQ via AMQP  
**Task state:** PostgreSQL (`ingestion_status`, written directly by the worker)  
**Result backend:** PostgreSQL (stock Celery tables, informational only)  
**Source:** `src/backend/controller/`

The controller worker orchestrates the ingestion pipeline: async parse (scatter-gather),
then index. It is the only service that calls both the parsing service and the indexing
service. It does not serve HTTP requests.

---

## Workers

The controller runs as three compose services. All three use the
`artemis/backend-controller-worker` image and differ only in their `command`: the queue they
consume and how many slots they have. `task_routes` in
`src/backend/controller/worker/celery.py` decides which queue each task goes to.

| Service | Queue | Slots | Longest single call | What it is for |
|---------|-------|-------|---------------------|----------------|
| `backend-controller-parse-worker` | `artemis.ingestion.parse` | 3 | 300 s | Moves each document through the parse sub-chain |
| `backend-controller-parse-long-worker` | `artemis.ingestion.parse.long` | 1 | 1500 s | Starts chunk jobs, which can block for minutes on a large document |
| `backend-controller-index-worker` | `artemis.ingestion.index` | 1 | 1800 s | Adds documents to the indexing service and deletes them from it |

A slot is one prefork child process, which runs one task at a time. All three services use
`prefetch_multiplier=1`. A task waiting on a `self.retry()` countdown does not hold a slot:
the worker keeps the message in memory and only gives it a slot once the countdown ends.

### Parse worker

Runs the whole parse path except `submit_chunk`: `tasks.ingest`, `tasks.parse`,
`tasks.submit_parse`, `tasks.poll_parse`, `tasks.advance_from_callback`,
`tasks.resolve_parse` and `tasks.poll_chunk`. `tasks.ingest` is the entry point from the
RabbitMQ sink. It writes the `ingestion_status` row, then starts a parse or sends a delete to
the index worker. `tasks.advance_from_callback` is published by the parsing service when
docling-serve reports a finished conversion, and races `poll_parse` for that document.

Each task makes at most one short call to the parsing service, and Docling does the slow work
elsewhere. A document spends most of its time waiting on a 60–75 s poll countdown, which holds
no slot, so three slots can follow many more than three documents.

This is the worker to scale if `artemis.ingestion.parse` backs up. The parsing service's 429
backpressure still limits how much reaches docling-serve.

### Parse-long worker

Runs only `tasks.submit_chunk`. The task sends no document bytes, but the parsing service reads
the replay cache and uploads it to docling-serve inline, because no chunk endpoint takes an S3
source. The call's length therefore grows with the document, and it can block for minutes on a
very large one.

It has its own worker for two reasons: polls never wait behind an upload, and only one upload
runs at a time. A second slot or replica gives up the one-at-a-time limit, so confirm
docling-serve can take parallel uploads before adding one.

### Index worker

Runs `tasks.index`, `tasks.delete_document` and `tasks.delete_namespace`: every task that
changes what the indexing service holds. `tasks.index` passes the indexing service a `BlobRef`,
waits while it embeds and stores the chunks, then deletes the artifact from MinIO.

It keeps one slot because embedding goes through the GPU-bound TEI model. Deletes share that
slot, so a delete can wait behind an indexing call of up to `HTTPX_TIMEOUT` (1800 s).

### Why three services

`--concurrency` applies to a whole worker, and Celery cannot limit slots per task or per queue
inside one, so each limit needs its own worker. Separate services also let each queue scale on
its own, keep each worker's output in its own `docker logs` stream, and let one restart without
touching the others.

Running them in one container would need a process supervisor as the main process and would
tie the queues together: a replica added for parsing would also add `submit_chunk` and index
slots.

---

## Task Chain

### CREATE / MODIFY

```
tasks.ingest(upload_action=CREATE|MODIFY)
  │  inserts the ingestion_status row
  │
  └──► parse.apply_async(s3, source=..., namespace_id=..., group_id=..., task_id=..., operation=...)
```

`index` is not chained here. `tasks.parse` uses `self.replace()` to substitute itself with the
first half of the async sub-chain:

```
parse  →self.replace()→  chain(
                           submit_parse    → SubmitResult dict
                           poll_parse      → polls until the conversion is terminal
                         )
```

`submit_parse` writes a `parse_stage_state` row before it submits the conversion. From then on
two paths can see the conversion finish: `poll_parse`, and docling-serve's HTTP callback, which
the parsing service publishes as `tasks.advance_from_callback`. Both try to claim that row, and
only the winner dispatches the rest of the pipeline:

```
chain(
  resolve_parse   → ResolveResult dict {obj_id}
  submit_chunk    → ChunkSubmitResult dict {chunking_task_id, obj_id}
  poll_chunk      → calls chunk finalize → BlobRef dict
  index
)
```

`_build_and_dispatch_tail_chain` builds this chain when the claim is won. It cannot be chained
from `parse`, because a Celery chain moves to the next task on any success and cannot respect
the claim.

Chunking is a fully decoupled stage from conversion (Epic 21): `resolve_parse` only downloads
and caches the conversion result — it never submits a chunk job itself. `submit_chunk` is a
separate task, mirroring `submit_parse`'s retry structure, so the two stages can fail and
retry independently instead of sharing one task's error handling.

`index` receives the `BlobRef` dict from `poll_chunk` as its first positional argument.

Every sub-task receives `task_id` as a kwarg so that `OutboxTask` can always recover the
contract `task_id` for `ingestion_status` regardless of which sub-task fails.
`advance_from_callback` is the exception: it recovers the id from the claimed row's
`resume_context`.

### DELETE

```
tasks.ingest(upload_action=DELETE|AUTO_DELETE)
  │  inserts the ingestion_status row
  │
  └──► delete_document.apply_async(source=..., namespace_id=..., task_id=..., operation=...)
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

Writes the `parse_stage_state` row, then calls `POST /v1/parse/submit` on the parsing service
with a `BlobRef` to the source, never its bytes. The parsing service submits a single async
conversion to Docling Serve regardless of document size — large PDFs are fanned out into page
slices and converted concurrently server-side by Docling Serve's Ray engine (see TODOs.md
Epic 21), not by any client-side splitting. Returns
`SubmitResult {parsing_task_id, obj_id, mode}`.

### `poll_parse`

Calls `GET /v1/parse/status/{parsing_task_id}`. Retries with a 60–75 s countdown while
`status == "processing"`. On a terminal status it tries to claim the `parse_stage_state` row.
If it wins, it dispatches the tail chain on `"success"`, or raises `DocumentConversionError`
(permanent) on `"failure"`. If it loses, the callback path already handled the document and
this task does nothing.

`max_retries=1440` (≈ 24 h ceiling at 60 s/retry).

### `advance_from_callback`

Published by the parsing service when docling-serve calls `POST /v1/parse/callback/{obj_id}`
with a terminal result. It races `poll_parse` for the same claim. If it wins, it dispatches the
tail chain on success or records the failure on `ingestion_status`. If it loses, it does
nothing. `poll_parse` stays on as the safety net because callback delivery is not guaranteed,
so this task retries database errors only 20 times and never records a failure for them.

### `resolve_parse`

Calls `POST /v1/parse/resolve`. The parsing service downloads the conversion result
(`GET /v1/result/{parsing_task_id}`), writes the replay cache, and derives + caches pages —
it does not submit a chunk job. Returns `ResolveResult {obj_id}`.

### `submit_chunk`

Calls `POST /v1/chunk/submit`. No document bytes cross this task, but the parsing service
reads the replay cache and uploads it to docling-serve inline (no chunk endpoint takes an S3
source), so the call's duration grows with document size — hence its own
`artemis.ingestion.parse.long` queue. Same retry structure as `submit_parse`
(`max_retries=20`). Returns `ChunkSubmitResult {chunking_task_id, obj_id}`.

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
| `httpx.TransportError` (timeout, refused or dropped connection) | Retry on the 5xx schedule — every parse sub-task | `min(120, 2^n) + jitter([0, backoff])` | same as above |
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

## acks_late

Every task in the parse sub-chain (`parse`, `submit_parse`, `poll_parse`,
`advance_from_callback`, `resolve_parse`, `submit_chunk`, `poll_chunk`) is decorated with
`acks_late=True`. Between retries a task is "parked" in the worker's in-memory timer waiting
for its ETA, not sitting acked-and-forgotten in RabbitMQ — a worker crash during that park
window leaves the message unacked so RabbitMQ redelivers it, rather than dropping it
permanently.

`ingest`, `index`, `delete_document` and `delete_namespace` use the global default and ack when
they start, so if a worker dies during one of those calls, RabbitMQ does not redeliver it.

---

## Task State

`ingestion_status` holds one row per contract `task_id`, and it is the source of truth for
task state. `tasks.ingest` inserts the row, the helpers in
`src/backend/controller/worker/backend/outbox.py` own every write, and task base classes in
`tasks.py` call them from Celery's lifecycle hooks:

| Hook | Base class | Write |
|------|-----------|-------|
| `before_start` | `OutboxTask` | Sets `stage` to this task's name, on the first delivery only (`retries == 0`) |
| `on_failure` | `OutboxTask` | Sets `status='failure'` and `failure_reason` |
| `on_success` | `TerminalOutboxTask` (`index`, `delete_document`) | Sets `status='success'` |

The other parse sub-chain tasks use `OutboxTask`. A failed stage write is logged and ignored,
because the next task's write corrects it. Terminal writes retry locally up to three times,
because `self.retry()` is not available in `on_success` and `on_failure`.

`advance_from_callback` has no `task_id` kwarg, so these hooks do nothing for it, and it calls
`record_failure` itself once it has the id from `resume_context`. `tasks.ingest` and
`delete_namespace` use a plain `Task`. A namespace deletion has no `ingestion_status` row.

The storage service reads this table directly for task status. CDC turns its terminal successes
into `ingested_objects` writes (see [Event Topology](../architecture/event-topology.md)).

---

## Result Backend

The worker uses Celery's stock `DatabaseBackend` (the `db+` URL in `celery.py`), which creates
its own `celery_taskmeta` and `celery_tasksetmeta` tables. It is informational only: nothing
reads it for task state or CDC.

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
| `HTTPX_TIMEOUT` | `1800.0` | For indexing/delete calls only; parse calls use per-endpoint timeouts |
| `PARSING_SUBMIT_TIMEOUT` | `180.0` | Source download/forward + HTTP POST |
| `PARSING_STATUS_TIMEOUT` | `45.0` | Single status GET (parse or chunk task) |
| `PARSING_RESOLVE_TIMEOUT` | `300.0` | S3 server-side copy of the conversion result + pages cache write. Keep above the parsing service's `DOCLING_RESOLVE_TIMEOUT` |
| `PARSING_CHUNK_SUBMIT_TIMEOUT` | `1500.0` | Replay-cache read + inline upload to docling-serve. Scales with document size; keep above `DOCLING_CHUNK_SUBMIT_TIMEOUT` |
| `PARSING_FINALIZE_TIMEOUT` | `180.0` | Chunk result fetch + artifact write. Keep above `DOCLING_FINALIZE_TIMEOUT` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-controller-worker` | |
