# Architecture Overview

Artemis is a RAG document ingestion system. Its job is to accept files (via REST upload or
enterprise filesystem/connector sources), parse them into structured chunks, embed them into
a vector store, and serve semantic search queries. The system is event-driven: a file upload
triggers a Kafka notification, which flows through a stream-processing layer into an async
Celery task chain that handles parsing and indexing independently.

---

## System Overview

### Indexing

```mermaid
flowchart LR
    FS[("File System")]
    PrivateClient(["Private\nClient"])

    subgraph enterprise[ ]
        e["&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\nEnterprise Subsystem\n&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"]
    end

    subgraph dispatch[ ]
        d["&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\nDispatch Subsystem\n&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"]
    end

    subgraph queue[ ]
        q["Task Queue"]
    end

    subgraph ingestion[ ]
        i["&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\nIngestion Subsystem\n&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"]
    end

    style e fill:none,stroke:none
    style d fill:none,stroke:none
    style q fill:none,stroke:none,font-size:13px
    style i fill:none,stroke:none

    FS --> enterprise
    PrivateClient --> dispatch
    enterprise --> dispatch
    dispatch --> queue
    queue --> ingestion
    ingestion --> PG[("PostgreSQL")]
    ingestion --> Qdrant[("Qdrant")]
    ingestion --> S3[("MinIO S3")]

    click enterprise href "./enterprise-subsystem.md"
    click dispatch href "./dispatch-subsystem.md"
    click queue href "./task-queue.md"
    click ingestion href "./ingestion-subsystem.md"
```

A file upload enters the system through one of two paths: private clients POST directly to
the storage service, while enterprise sources are watched by Kafka Connect FileSource
connectors managed by the data sources service. Both paths converge at the dispatch
subsystem, which writes the file to MinIO and emits a notification event. The event flows
through the task queue to the ingestion subsystem, where a Celery worker orchestrates a
parse → embed → store chain: the parsing service drives Docling to extract structured
chunks, the indexing service embeds them via TEI and writes vectors to Qdrant, page-level
text to MinIO S3, and task metadata to PostgreSQL.

### Retrieval

```mermaid
flowchart LR
    QueryClient(["Query\nClient"])

    subgraph retrieval[ ]
        r["&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\nRetrieval Subsystem\n&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"]
    end

    style r fill:none,stroke:none

    Qdrant[("Qdrant")] -->|"chunks"| retrieval
    PG[("PostgreSQL")] --> retrieval
    S3[("MinIO S3")] -->|"parent pages"| retrieval
    retrieval --> QueryClient

    click retrieval href "./retrieval-subsystem.md"
```

A query client sends a natural-language query to the retrieval subsystem. The indexing
service embeds the query and searches Qdrant for the most relevant chunks. Retrieved chunks are reordered using a reranker model.
Optionally, chunk `parent_id` references are resolved against the MinIO S3 doc store to
return full page-level context rather than just the matched chunk. PostgreSQL is consulted
for task and namespace metadata.

---

## Service Port Table

| Service | Port | Type |
|---------|------|------|
| Storage service | 7000 | FastAPI HTTP |
| Enterprise intake | 9000 | FastAPI HTTP |
| Indexing service | 10000 | FastAPI HTTP (LangServe) |
| Parsing service | 10001 | FastAPI HTTP |
| Enterprise data sources | 9500 | FastAPI HTTP |
| MCP server | 11000 | FastAPI + MCP streamable HTTP |
| Controller worker | — | Celery (no HTTP surface) |
| MinIO API | 9000 | S3-compatible |
| MinIO console | 9090 | Web UI |
| PostgreSQL | 5432 | Postgres wire protocol |
| Qdrant | 6333 | HTTP + gRPC |
| RabbitMQ broker | 5672 | AMQP |
| RabbitMQ management | 15672 | Web UI |
| Kafka broker (internal) | 9092 | Kafka protocol |
| Kafka broker (external) | 19092 | Kafka protocol |
| Schema Registry | 8081 | HTTP |
| Kafka Connect | 8083 | HTTP |
| ksqlDB | 8088 | HTTP |
| TEI (internal) | 80 | HTTP |
| TEI (host) | 11435 | HTTP |
| Docling Serve | 5001 | HTTP |
| ColBERT/vLLM (internal) | 8000 | HTTP |
| ColBERT/vLLM (host) | 11436 | HTTP |
| APISIX proxy | 9080 | HTTP |
| APISIX admin | 9180 | HTTP |

---

## Docker Compose Profiles

| Profile | Contents | When to use |
|---------|----------|-------------|
| `infra` | PostgreSQL, MinIO, Qdrant, RabbitMQ, Kafka, Schema Registry, `artemis-db-migrations` | Always — foundational data layer |
| `backend` | Storage, Parsing, Indexing, Controller Worker, ksqlDB, `artemis-ksqldb-init` | Core ingestion and retrieval |
| `enterprise` | Kafka Connect, Enterprise Intake, Enterprise Data Sources, `artemis-kafka-connect-init`, `ksqldb-enterprise-init` | Enterprise connector ingestion |
| `ai` | Docling Serve (GPU) | Document parsing |
| `ai-tei` | TEI (HuggingFace text embeddings) | Dense embedding |
| `ai-colbert` | vLLM + ColBERT model | Multi-stage reranking |
| `gateway` | APISIX, etcd, `artemis-apisix-init` | API gateway routing |
| `observability` | OTel Collector, Jaeger, Prometheus | Distributed tracing and metrics |
| `dev-tools` | pgAdmin, Kafka UI, ksqlDB CLI | Local development tooling |

**Common combinations:**

```bash
# Minimal private ingestion + retrieval
docker compose --profile infra --profile backend --profile ai --profile ai-tei up

# Full enterprise stack
docker compose --profile infra --profile backend --profile enterprise \
               --profile ai --profile ai-tei --profile gateway up

# With observability
docker compose --profile infra --profile backend --profile ai --profile ai-tei \
               --profile observability up
```

See [Deployment Guide](../guides/deployment.md) for startup order and env vars.

---

## Key Cross-Cutting Concerns

- **Multi-tenancy**: every object is scoped by `namespace_id`; see [Tenancy](tenancy.md)
- **Claim-check pattern**: large payloads never cross service boundaries as bytes — services
  exchange `BlobRef {bucket, key}` and read/write directly to MinIO
- **Deduplication**: LangChain RecordManager keyed by `obj_id = uuid5(namespace_id, source)` —
  re-uploading the same file skips re-embedding unchanged content
- **Observability**: OTel SDK in every service; `artemis.task_id` span attribute links all
  spans in an ingestion round trip; Jaeger UI on port 16686 (observability profile)
