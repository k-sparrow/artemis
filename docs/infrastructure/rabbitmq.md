# RabbitMQ

**Version:** RabbitMQ 4.1.2 (management-alpine)  
**Role:** AMQP message broker for Celery task queues. The Camel Spring RabbitMQ Sink
Connector delivers Kafka messages to RabbitMQ, and the Celery worker consumes from RabbitMQ.

---

## Exchange and Queue Layout

**Exchange:** `artemis.ingestion` (configurable via `EXCHANGE_NAME` env var; default in
dev compose: `ingestion_test0`)

| Queue | Celery task(s) | Worker concurrency | Notes |
|-------|---------------|-------------------|-------|
| `artemis.ingestion.parse` | `tasks.ingest`, `tasks.parse`, `tasks.submit_parse`, `tasks.poll_parse`, `tasks.advance_from_callback`, `tasks.resolve_parse`, `tasks.poll_chunk` | 3 | Short HTTP calls and countdown polls |
| `artemis.ingestion.parse.long` | `tasks.submit_chunk` | 1 | The parsing service uploads the whole DoclingDocument inline; one call can take minutes |
| `artemis.ingestion.index` | `tasks.index`, `tasks.delete_document`, `tasks.delete_namespace` | 1 | TEI + Qdrant + Postgres |

**Task routing** is `task_routes` in `src/backend/controller/worker/celery.py`.

---

## consumer_timeout

RabbitMQ's `consumer_timeout` is set to **2 hours** (from `rabbitmq.conf`):

```ini
consumer_timeout = 7200000   # milliseconds
```

A message counts against it for as long as it is unacknowledged, and that includes a
countdown retry (`self.retry(countdown=...)`) parked in a worker waiting for a free slot. On
a one-slot worker that wait can be two of the longest calls: the one running, then one that
takes the slot first. So it must exceed **2 × the longest call on any worker + its longest
retry countdown**:

| Worker | Longest call | Longest countdown | Needs more than |
|--------|-------------|-------------------|-----------------|
| parse | 300 s (`PARSING_RESOLVE_TIMEOUT`) | ~330 s (429 backpressure) | ~16 min |
| parse.long | 1500 s (`PARSING_CHUNK_SUBMIT_TIMEOUT`) | ~330 s | ~56 min |
| index | 1800 s (`HTTPX_TIMEOUT`) | ~240 s (5xx backoff) | ~64 min |

On a breach RabbitMQ closes the channel and requeues every unacknowledged message on it. For
`acks_late=True` tasks (the whole parse sub-chain) that includes tasks still running, which
are then delivered and run a second time. Recheck this table whenever a worker timeout
changes.

---

## Connection Configuration

```
RABBITMQ_HOST    rabbitmq
RABBITMQ_PORT    5672
RABBITMQ_USER    artemis
RABBITMQ_PASS    artemis
RABBITMQ_VHOST   artemis
```

The Celery broker URL is constructed as:
`amqp://{user}:{pass}@{host}:{port}/{vhost}`

---

## `acks_late=True` on fetch_and_parse

`tasks.fetch_and_parse` is decorated with `acks_late=True`. This means the message is
NOT acknowledged until the task completes. Combined with `concurrency=1` on the
`artemis.ingestion.fetch-and-parse` queue, this ensures:

1. At most one Docling job runs at a time (GPU exclusivity)
2. If the worker crashes mid-parse, RabbitMQ redelivers the message to another worker
   (no silent data loss)

Without `acks_late`, a crash after the message is dequeued but before parsing completes
would drop the job silently.

---

## Dead Letter Exchange (Planned)

DLQ support is not yet configured. After a task exhausts its retries (`max_retries=20`),
Celery marks it as FAILURE and the CDC pipeline captures the failure reason. The failed
message is not rerouted anywhere — it remains visible in `ingestion_tasks` with
`status='FAILURE'`.

Epic 17 will add:
- DLX `artemis.ingestion.dlx`
- DLQ `artemis.ingestion.dlq` bound to the DLX
- `x-dead-letter-exchange` queue argument on task queues

---

## Management UI

Available at `http://localhost:15672` when the `infra` profile is running.

Default credentials: `artemis` / `artemis` (from `RABBITMQ_DEFAULT_USER/PASS` in compose).

Useful for:
- Inspecting queue depths during load testing
- Checking unacknowledged message counts
- Verifying the exchange/queue bindings created by Celery
