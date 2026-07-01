# Observability

Artemis ships an optional observability stack (distributed tracing + metrics) via the
`observability` Docker Compose profile. All services are instrumented with OpenTelemetry
and emit traces to a central collector when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

---

## Starting the Observability Stack

```bash
docker-compose -f deployment/docker/docker-compose.dev.yaml \
  --profile infra --profile observability up -d
```

The `observability` profile starts three additional services:

| Service | Host port | Purpose |
|---------|-----------|---------|
| OTel Collector | 4317 (gRPC), 4318 (HTTP) | Receives OTLP spans; fans out to Jaeger + Prometheus |
| Jaeger UI | 16686 | Trace search and waterfall view |
| Prometheus | 9091 | Metrics scraped from OTel Collector's Prometheus exporter |

Then set `OTEL_EXPORTER_OTLP_ENDPOINT` for every service you want to trace:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

All services are no-ops when this env var is unset, so existing local dev runs and
tests are unaffected.

---

## Instrumented Services

Every Artemis service is instrumented with the same three auto-instrumentors:

| Library | What it traces |
|---------|----------------|
| `FastAPIInstrumentor` | Incoming HTTP requests (span per route, status code, method) |
| `HTTPXClientInstrumentor` | Outgoing HTTP calls to downstream services |
| `SQLAlchemyInstrumentor` | Database queries |

The controller worker additionally uses `CeleryInstrumentor` (span per task execution).

| Service name in Jaeger | Source |
|------------------------|--------|
| `backend-storage` | Storage service |
| `backend-parsing` | Parsing service |
| `backend-indexing` | Indexing service |
| `controller-worker` | Celery worker |
| `backend-enterprise-intake` | Enterprise intake |
| `backend-enterprise-data-sources` | Enterprise data sources |
| `backend-mcp` | MCP server |
| `tei` | TEI (text embeddings inference) |
| `vllm` | vLLM / ColBERT reranker |

TEI and vLLM emit traces automatically when `OTEL_EXPORTER_OTLP_ENDPOINT` is set in
the compose environment — no code change required.

---

## Custom Span Attributes

Artemis stamps a consistent set of domain attributes on ingestion-path spans to make
cross-service traces queryable by business key:

| Attribute | Type | Set by | Meaning |
|-----------|------|--------|---------|
| `artemis.task_id` | string (UUID) | Storage service upload handler, `tasks.ingest`, `tasks.fetch_and_parse`, `tasks.index` | Contract task ID from `IngestionTaskDetails` — the same UUID the caller polls via `GET /tasks/{task_id}` |
| `artemis.namespace_id` | string (UUID) | Storage service upload handler, `tasks.fetch_and_parse`, `tasks.index` | Namespace the document belongs to |
| `artemis.obj_id` | string (UUID) | Storage service upload handler, `tasks.fetch_and_parse`, `tasks.index` | Deterministic object ID (`uuid5(namespace_id, filename)`) |

`artemis.task_id` is the primary correlation key. Searching Jaeger for a specific
`task_id` value returns every span involved in that ingestion: the upload HTTP request,
the three Celery tasks, and all downstream HTTP calls to the parsing and indexing
services.

---

## Worker Spans

The controller worker emits two kinds of spans:

**Auto-instrumented (CeleryInstrumentor):** one span per task at the Celery level,
covering message receive → task return. Span name matches the registered task name
(e.g. `tasks.ingest`).

**Manual spans inside fetch_and_parse and index:** the tasks open a child span
(`tasks.fetch_and_parse`, `tasks.index`) that covers only the HTTP call to the
downstream service. This is where `artemis.*` attributes are set and where you see
the actual parse/index latency.

---

## Jaeger UI

Open `http://localhost:16686`.

- **Search by service:** select e.g. `backend-storage` to see all upload requests
- **Search by tag:** use `artemis.task_id = <uuid>` to trace a single ingestion
  end-to-end across all services
- **Trace view:** the waterfall shows the full chain — upload → Celery task → parsing
  HTTP → Docling → MinIO write → index HTTP → TEI embedding → Qdrant upsert

Jaeger uses in-memory storage (max 100,000 traces). Traces are lost on container
restart. For persistent storage, replace the `memstore` backend in the Jaeger compose
config with a Badger or Cassandra backend.

---

## Prometheus

Open `http://localhost:9091`.

The OTel Collector's Prometheus exporter exposes metrics at `:8889/metrics` (scraped
by Prometheus). All metrics are prefixed with `artemis_` (set by the `namespace`
field in the collector config).

Metrics include auto-instrumented span counters and latency histograms from the
FastAPI, HTTPX, and Celery instrumentors.

---

## Adding Telemetry to a New Service

```python
from src.lib.backend.telemetry import is_telemetry_enabled, setup_telemetry
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

app = FastAPI(...)

if is_telemetry_enabled():
    setup_telemetry(
        "my-service-name",      # appears as the service in Jaeger
        HTTPXClientInstrumentor(),
    )
    FastAPIInstrumentor.instrument_app(app)
```

`setup_telemetry` must be called at **module level** (before the app is imported by
uvicorn), not inside a lifespan handler. FastAPIInstrumentor must wrap the app before
Starlette builds its middleware stack.
