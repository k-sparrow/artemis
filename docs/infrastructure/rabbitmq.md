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
| `artemis.ingestion.fetch-and-parse` | `tasks.ingest`, `tasks.fetch_and_parse` | 1 | `acks_late=True`; heavy-PDF stability requires serial execution |
| `artemis.ingestion.index` | `tasks.index`, `tasks.delete_document`, `tasks.delete_namespace` | Scalable (I/O-bound) | TEI + Qdrant + Postgres are network I/O |

**Task routing** (from `src/backend/controller/worker/celery.py`):

```python
task_routes = {
    "tasks.ingest":           {"queue": "artemis.ingestion.fetch-and-parse"},
    "tasks.fetch_and_parse":  {"queue": "artemis.ingestion.fetch-and-parse"},
    "tasks.index":            {"queue": "artemis.ingestion.index"},
    "tasks.delete_document":  {"queue": "artemis.ingestion.index"},
    "tasks.delete_namespace": {"queue": "artemis.ingestion.index"},
}
```

---

## consumer_timeout

RabbitMQ's `consumer_timeout` is set to **24 hours** (from `rabbitmq.conf`):

```ini
consumer_timeout = 86400000   # milliseconds
```

This is required because heavy PDFs can take 3–5 hours to parse. Without this, RabbitMQ
would close the channel and requeue the message while Docling is still running, causing
infinite re-delivery loops.

The controller worker's `HTTPX_TIMEOUT` is aligned to the same value:
```
HTTPX_TIMEOUT=86400   # seconds
```

Both must be raised together. Raising one without the other still causes timeouts — either
the broker closes the channel or the HTTP client gives up before the parse finishes.

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
