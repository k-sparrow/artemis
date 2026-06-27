# Multi-Tenancy & Isolation

Artemis uses `namespace_id` (a UUID) as the **single isolation axis** across every layer of the
stack — the database, the vector store, the object store, and every service-to-service contract.
No other field determines data separation; `owner_id` and `group_id` are ownership and grouping
concepts layered on top of it.

---

## Namespace Types

| Type | ID generation | Who owns it | Use case |
|------|---------------|-------------|----------|
| `PRIVATE` | UUID4 (random) | A single user (chat session, personal workspace) | Direct REST API uploads |
| `SHARED` | UUID5(ARTEMIS_NS, name) — deterministic from natural key | An enterprise org | Enterprise connector ingestion; multiple users read the same namespace |

`ARTEMIS_NS` is the RFC 4122 DNS namespace UUID (`6ba7b810-9dad-11d1-80b4-00c04fd430c8`),
stable across all deployments. The name used as the UUID5 seed is the human-readable project
name (e.g. `"acme-docs"`), making the `namespace_id` reproducible without storing it first.

---

## Ownership: `owner_id`

`owner_id` is a UUID identifying the owner of a namespace. It is an external IdP subject or
organisation UUID — not stored in any identity table, just used as an FK anchor.

**How it enters the system:**
- All requests must include an `X-Owner-Id` header (enforced by `caller_owner_id_dependency`
  in the storage service — returns 401 if the header is absent or not a valid UUID)
- In production, the API gateway will set this header from the validated JWT `sub` claim
  (deferred to the Hydra auth epic — see [APISIX](../infrastructure/apisix.md))
- For enterprise connectors (present implementation): `owner_id = uuid5(ARTEMIS_NS, org_name)`,
  baked into the Kafka Connect connector config at creation time and forwarded through the
  ksqlDB CSAS into the `IntakeRequest` body

**Storage:**
- `owner` table: thin FK anchor — just `{id: UUID, deleted_at: timestamp}`
- Upserted by the storage service on namespace creation (simple mode)
- `namespace.owner_id` has `ON DELETE RESTRICT` — you cannot delete an owner with active namespaces

---

## Grouping: `group_id`

`group_id` is an optional UUID that groups objects within a namespace at a finer granularity
than the namespace itself.

| Context | Value | Set by |
|---------|-------|--------|
| Enterprise connector | `connector_id` (= `DataSource.id`) | Kafka Connect SMT at connector creation |
| Private upload | NULL (no grouping below namespace level) | — |

`group_id` flows through the entire pipeline:
- Kafka header `artemis.group_id` → ksqlDB kwarg → `IngestionInfo.group_id` → Celery kwargs
- `IngestionResult.object.scope.group_id` → `ingested_objects.group_id` (via JDBC sink)
- `Document.metadata["group_id"]` → Qdrant chunk payload

**Uses:**
- `GET /namespaces/{id}/objects?group_id={id}` — list objects belonging to one connector
- `DELETE /namespaces/{id}/objects?group_id={id}` — delete all objects from a connector
- Future: retrieval filter scoped to a single connector's documents

---

## Access Control Rules

The storage service enforces access on every request through `_fetch_namespace()`:

```
PRIVATE namespace:
  read  → caller_owner_id must equal namespace.owner_id  (403 otherwise)
  write → caller_owner_id must equal namespace.owner_id  (403 otherwise)

SHARED namespace:
  read  → any valid owner_id is accepted
  write → caller_owner_id must equal namespace.owner_id  (403 otherwise)
```

"Write" means any mutation: PATCH (rename), DELETE namespace, POST objects, DELETE objects.
`GET /namespaces`, `GET /namespaces/{id}`, `GET /namespaces/{id}/objects` are "read".

---

## Propagation Chain

`namespace_id` (and `owner_id`, `group_id`) travel with every message and are never
re-derived by downstream services from a lookup:

```
POST /namespaces/{id}/objects  (X-Owner-Id header)
  │
  ▼
Storage service
  • validates X-Owner-Id, calls _fetch_namespace()
  • generates obj_id = uuid5(namespace_id, source)
  • stamps IngestionTaskDetails JSON into MinIO metadata
  │   {info: {namespace_id, group_id}, source: {obj_id, ...}}
  ▼
MinIO PUT event ──► Kafka (artemis.ingestion.storage.s3)
  │
  ▼
ksqlDB CSAS
  • extracts namespace_id, group_id, obj_id, task_id from contract JSON
  • builds Celery v2 kwargs; keys stream by namespace_id (ordering guarantee)
  │
  ▼
RabbitMQ ──► Celery worker
  • receives namespace_id, group_id in IngestionInfo
  • passes to parsing service (as context) and indexing service (as query param)
  │
  ▼
Indexing service
  • stamps namespace_id + group_id on every LangChain Document.metadata
  • Qdrant payload: {"namespace_id": "...", "group_id": "...", "obj_id": "..."}
  • DELETE /ingest?namespace=<uuid>&obj_id=<uuid> scopes deletion to namespace
  │
  ▼
IngestionResult (stored in apollo_celery_taskmeta.result)
  • object.scope.{namespace_id, group_id}
  │
  ▼
Debezium CDC ──► ksqlDB ──► JDBC sink
  • ingested_objects.namespace_id (NOT NULL, indexed FK)
  • ingested_objects.group_id (nullable, indexed)
  • ingestion_tasks.namespace_id (NOT NULL, denormalised for per-namespace queries)
```

---

## Vector Store Isolation (Qdrant)

All namespaces share a **single Qdrant collection**. Isolation is enforced at query time
through a mandatory `namespace_id` payload filter:

- Every chunk upsert stamps `namespace_id` in `Document.metadata`
- LangChain's `QdrantVectorStore` translates this to a Qdrant payload index filter on every search
- The `namespace_id` payload index is configured as `keyword` type for efficient filtering
- A query without a `namespace_id` filter is not possible through the indexing service API

The `HNSW` index is configured with `m=0` (global graph disabled) and `payload_m=16`
(per-payload-value graph) — the standard multi-tenant configuration for Qdrant that builds
an independent graph per namespace rather than mixing vectors across namespaces.

---

## Ownership Cascade (Deletion)

When an owner is deleted:

```
DELETE owner row
  │
  ▼ (DB trigger: soft_cascade_namespace_on_owner_delete)
namespace.deleted_at = now()  (for all owned namespaces)
  │
  ▼ (Debezium watches namespace.deleted_at)
CDC event ──► Celery delete_namespace task dispatched per namespace
  │
  ▼
tasks.delete_namespace: DELETE /ingest?namespace=<id>
  (clears all vectors and record-manager entries from Qdrant + Postgres)
```

`ingested_objects` FK to `namespace` was dropped in migration 0004 (`passive_deletes=True`
on the ORM relationship). Orphaned rows are cleaned up asynchronously by the Celery pipeline
after vector deletion — they are not visible through the API once the namespace is soft-deleted.

---

## What Is NOT Yet Enforced

| Gap | Status |
|-----|--------|
| `TRUSTED_PROXIES` — `X-Owner-Id` accepted from any caller, not just the gateway | Deferred to Hydra auth epic |
| Gateway JWT claim extraction (`sub` → `X-Owner-Id`) | Deferred to Hydra auth epic |
| Per-object ACL (OpenFGA) | Design deferred post-v1 |

Until the gateway auth epic is complete, any client that can reach the storage service
can pass an arbitrary `X-Owner-Id`. In development this is intentional; in production the
gateway must enforce authentication before requests reach the backend.
