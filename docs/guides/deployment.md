# Deployment Guide

This guide covers running Artemis with Docker Compose. For building images and working
with the source, see [Development](development.md).

---

## Compose Files

| File | Purpose |
|------|---------|
| `deployment/docker/docker-compose.dev.yaml` | Development — images tagged `:dev`, all services exposed on host ports |
| `deployment/docker/docker-compose.release.yaml` | Release — images pinned to `${ARTEMIS_VERSION}`, gateway-only ingress |

Both files are **generated** from `tools/docker/docker-compose.tmpl.yaml`. After changing
the template, regenerate with:

```bash
bazel run //deployment/docker:docker_compose.update
```

---

## Profiles

Start only what you need by combining profiles:

| Profile | Services included |
|---------|-------------------|
| `infra` | PostgreSQL, MinIO, Qdrant, RabbitMQ, Kafka, Schema Registry, `artemis-db-migrations` |
| `backend` | Storage, Parsing, Indexing, Controller Worker, ksqlDB, `artemis-ksqldb-init` |
| `enterprise` | Kafka Connect, Enterprise Intake, Enterprise Data Sources, `artemis-kafka-connect-init`, `ksqldb-enterprise-init` |
| `ai` | Docling Serve (GPU, CUDA 12.8) |
| `ai-tei` | TEI text embedding inference (Alibaba gte-large-en-v1.5) |
| `ai-colbert` | vLLM + ColBERT (jinaai/jina-colbert-v2) for multi-stage reranking |
| `gateway` | APISIX, etcd, `artemis-apisix-init` |
| `observability` | OTel Collector, Jaeger, Prometheus |
| `dev-tools` | pgAdmin (5444), Kafka UI (18080), ksqlDB CLI |

---

## Common Startup Combinations

```bash
# Minimal private ingestion + retrieval (no enterprise, no GPU-based parsing)
docker compose -f deployment/docker/docker-compose.dev.yaml \
  --profile infra --profile backend --profile ai-tei up -d

# With Docling parsing (requires NVIDIA GPU)
docker compose -f deployment/docker/docker-compose.dev.yaml \
  --profile infra --profile backend --profile ai --profile ai-tei up -d

# Full enterprise stack (connectors, gateway)
docker compose -f deployment/docker/docker-compose.dev.yaml \
  --profile infra --profile backend --profile enterprise \
  --profile ai --profile ai-tei --profile gateway up -d

# With observability
docker compose -f deployment/docker/docker-compose.dev.yaml \
  --profile infra --profile backend --profile ai --profile ai-tei \
  --profile observability up -d

# With local dev tools
docker compose -f deployment/docker/docker-compose.dev.yaml \
  --profile infra --profile dev-tools up -d
```

---

## Service Startup Order

`depends_on` in the compose file enforces this order automatically. Manual reference:

```
1. PostgreSQL (infra)
2. MinIO, Qdrant, RabbitMQ, Kafka, Schema Registry (infra, parallel)
3. artemis-db-migrations  ←  waits for postgres: service_healthy
4. Storage Service         ←  waits for minio + postgres + db-migrations
5. Parsing Service         ←  waits for docling-serve (ai profile)
6. Indexing Service        ←  waits for qdrant + postgres + db-migrations + tei
7. Controller Worker       ←  waits for rabbitmq + postgres + db-migrations + minio
8. ksqlDB                 ←  waits for broker + schemaregistry
9. artemis-ksqldb-init    ←  waits for ksqldb: service_healthy
   (enterprise)
10. Kafka Connect          ←  waits for broker + schemaregistry + postgres
11. artemis-kafka-connect-init  ←  waits for kafka-connect + storage
12. Enterprise Intake, Data Sources  ←  wait for storage
13. APISIX, etcd          ←  gateway profile
14. artemis-apisix-init   ←  waits for apisix
```

---

## Environment Variables

Each service reads configuration from environment variables. In the compose files these are
set inline; in production use `.env` files or secrets management.

### All services (optional)
```
OTEL_EXPORTER_OTLP_ENDPOINT   # OTel collector gRPC endpoint (e.g. http://otel-collector:4317)
OTEL_SERVICE_NAME             # Set automatically per service in compose
```

### Storage service (port 7000)
```
SQL_DB_URL           postgresql+asyncpg://user:pass@host:5432/documents
S3_ENDPOINT_URL      minio:9000
S3_ACCESS_KEY        minioadmin
S3_SECRET_KEY        minioadmin
S3_ARTEMIS_BUCKET    artemis
S3_ARTEMIS_BUCKET_KAFKA_EVENT   ARTEMIS   # must match MinIO notification event type
```

### Parsing service (port 10001)
```
DOCLING_SERVE_URI          http://docling-serve:5001
LOADER_TYPE                docling   # or pymupdf4llm
S3_ENDPOINT                minio:9000
S3_ACCESS_KEY              minioadmin
S3_SECRET_KEY              minioadmin
S3_SECURE                  false
PARSED_ARTIFACTS_BUCKET    parsed-chunks     # default
REPLAY_CACHE_BUCKET        docling-replay    # default
```

### Indexing service (port 10000)
```
QDRANT_HOST_URL            http://vectorstore:6333
QDRANT_COLLECTION_NAME     artemis
TEI_HOST_URL               http://tei:80
RETRIEVAL_MODE             dense                         # dense | hybrid | multi_stage
COLBERT_RERANKER_URL       http://colbert:8000            # enables mode-agnostic reranking
COLBERT_HOST_URL           http://colbert:8000            # multi_stage only (not currently used)
COLBERT_MODEL_NAME         colbert-ir/colbertv2.0
COLBERT_MAX_TOKENS_PER_DOC 511
RETRIEVE_CANDIDATES_MULTIPLIER 10                         # k × multiplier candidates before rerank
SQL_DB_USER / SQL_DB_PASSWORD / SQL_DB_HOST / SQL_DB_PORT / SQL_DB_DATABASE / SQL_DRIVER
S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY / S3_SECURE
PAGE_BUCKET                parent-pages                   # parent-page doc store
DEFAULT_PIPELINE_TYPE      simple                         # simple | semi_structured
DEFAULT_CHUNK_SIZE         1024
DEFAULT_CHUNK_OVERLAP      100
```

### Controller worker
```
RABBITMQ_HOST / RABBITMQ_PORT / RABBITMQ_USER / RABBITMQ_PASSWORD / RABBITMQ_VHOST
SQL_DB_HOST / SQL_DB_PORT / SQL_DB_USER / SQL_DB_PASSWORD / SQL_DB_DATABASE / SQL_DRIVER
S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY / S3_SECURE
EXCHANGE_NAME              ingestion_test0   # must match the RabbitMQ exchange name
INGESTION_SERVICE_URL      http://backend-indexing-ingestion:10000
PARSING_SERVICE_URL        http://backend-parsing:10001
PARSED_CHUNKS_BUCKET       parsed-chunks
HTTPX_TIMEOUT              86400             # seconds; aligned to RabbitMQ consumer_timeout
```

### Enterprise Data Sources (port 9500)
```
SQL_DB_URL             postgresql+asyncpg://...
KAFKA_CONNECT_URL      http://kafka-connect:8083
STORAGE_SERVICE_URL    http://backend-storage:7000
```

### Enterprise Intake (port 9000)
```
STORAGE_SERVICE_URL    http://backend-storage:7000
```

### MCP Server (port 11000)
```
STORAGE_SERVICE_URL    http://backend-storage:7000
INDEXING_SERVICE_URL   http://backend-indexing-ingestion:10000
ENABLE_UPLOAD          false   # set true only once X-Owner-Id is trustably forwarded
STUB_OWNER_ID          <uuid>  # temporary until gateway auth is wired
```

---

## First-Run Checklist

1. **MinIO bucket initialisation** — handled automatically by `artemis-minio-init` (one-shot
   container that runs `mc` to create buckets and configure Kafka notifications). The bucket
   `artemis` with event type `ARTEMIS` is required for the ingestion pipeline.

2. **Alembic migrations** — `artemis-db-migrations` runs `alembic upgrade head` before any
   service that needs the schema starts. This is automatic via `depends_on`.

3. **ksqlDB streams** — `artemis-ksqldb-init` runs the SQL init scripts after ksqlDB is
   healthy. Enterprise streams are applied by `ksqldb-enterprise-init` (enterprise profile).

4. **Kafka Connect connectors** — `artemis-kafka-connect-init` registers the Debezium source
   and JDBC sink connectors after Kafka Connect is healthy. Requires the `enterprise` profile.

5. **APISIX routes** — `artemis-apisix-init` registers routes via the Admin API. Requires
   the `gateway` profile.

---

## Release Compose

The release compose (`docker-compose.release.yaml`) differs from dev in these ways:

- All Artemis service images are pinned to `${ARTEMIS_VERSION}` (must be set in the environment)
- Dev tools and development-only service exposures are removed
- Only the gateway port is published externally (services are not directly accessible)

Set the version before deploying:
```bash
export ARTEMIS_VERSION=v1.0.0
docker compose -f deployment/docker/docker-compose.release.yaml \
  --profile infra --profile backend --profile ai --profile ai-tei --profile gateway up -d
```
