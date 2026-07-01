# Artemis Documentation

Artemis is a RAG (Retrieval-Augmented Generation) document ingestion system built on an
event-driven microservices architecture. It accepts files through a REST API or enterprise
connectors, parses them with GPU-accelerated OCR, embeds them into a vector store, and
exposes semantic search and an MCP server for AI client integration.

---

## Architecture

- [Overview](architecture/overview.md) — system diagram, data flows, service ports, deployment modes
- [Multi-Tenancy & Isolation](architecture/tenancy.md) — namespace_id, owner_id, group_id, access control
- [Data Contracts](architecture/data-contracts.md) — message schemas between services
- [Event Topology](architecture/event-topology.md) — Kafka topics, ksqlDB streams, CDC pipeline
- [Dispatch Subsystem](architecture/dispatch-subsystem.md) — storage → MinIO → Kafka → ksqlDB → RabbitMQ
- [Ingestion Subsystem](architecture/ingestion-subsystem.md) — controller worker, parsing service, indexing service
- [Enterprise Subsystem](architecture/enterprise-subsystem.md) — data sources, intake, connector lifecycle
- [Retrieval Subsystem](architecture/retrieval-subsystem.md) — dense / hybrid / multi-stage / reranker / parent-page

## Services

Custom services implemented in this repository:

- [Storage Service](services/storage.md) — namespace management, file upload, task dispatch (port 7000)
- [Parsing Service](services/parsing.md) — document parsing via Docling (port 10001)
- [Indexing Service](services/indexing.md) — embedding, vector storage, retrieval (port 10000)
- [Controller Worker](services/controller-worker.md) — Celery orchestration of parse→index chain
- [Enterprise Intake](services/enterprise-intake.md) — Kafka HTTP sink bridge (port 9000)
- [Enterprise Data Sources](services/enterprise-data-sources.md) — connector control plane (port 9500)
- [MCP Server](services/mcp.md) — Model Context Protocol server (port 11000)
- [CLI / TUI](services/cli-tui.md) — terminal interface for operators

## Infrastructure

How Artemis configures and uses each OSS component:

- [PostgreSQL](infrastructure/postgresql.md) — relational DB, schema, Alembic, CDC
- [MinIO](infrastructure/minio.md) — object storage, bucket layout, S3 notifications
- [Kafka](infrastructure/kafka.md) — event streaming, Kafka Connect, connector plugins
- [ksqlDB](infrastructure/ksqldb.md) — stream processing, CSAS transformations
- [RabbitMQ](infrastructure/rabbitmq.md) — Celery broker, queue layout
- [Qdrant](infrastructure/qdrant.md) — vector database, collection schema, multi-tenancy
- [TEI](infrastructure/tei.md) — text embedding inference
- [Docling](infrastructure/docling.md) — document parsing service
- [vLLM](infrastructure/vllm.md) — ColBERT reranking and late-interaction embeddings
- [APISIX](infrastructure/apisix.md) — API gateway, routing

## Guides

- [Deployment](guides/deployment.md) — Docker Compose profiles, startup order, env vars
- [Development](guides/development.md) — Bazel commands, running services locally, testing
- [Ingestion Walkthrough](guides/ingestion-walkthrough.md) — end-to-end trace of a single upload
- [Retrieval Modes](guides/retrieval-modes.md) — dense / hybrid / multi-stage / parent-page
- [Observability](guides/observability.md) — OTel tracing, Jaeger, Prometheus, custom span attributes

---

## Design artifacts

- [Epic 1: Namespace Design](epic-1-namespace-design.md) — original namespace design doc (historical)
