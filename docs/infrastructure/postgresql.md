# PostgreSQL

**Version:** PostgreSQL 16  
**Role:** Relational database for namespace management, object registry, task status,
Celery result backend, and enterprise connector metadata. `ingestion_status` is also the CDC
source for `ingested_objects`.

---

## Schema

All tables are in the `public` schema of the `documents` database.

### `owner`

Thin ownership anchor. Exists to provide an FK target for `namespace.owner_id` and as a
Debezium CDC trigger point for the soft-delete cascade.

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | Primary key. External IdP subject or org UUID — no FK to any identity table |
| `deleted_at` | timestamptz | Soft-delete. DB trigger fires on DELETE to cascade to namespaces |

### `namespace`

Core data-separation unit. Every object is scoped to exactly one namespace.

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | PK. UUID4 for PRIVATE; UUID5(ARTEMIS_NS, name) for SHARED |
| `name` | text | Display label. UUID5 seed for SHARED |
| `type` | enum | `private` or `shared` |
| `owner_id` | UUID | FK → owner.id ON DELETE RESTRICT |
| `deleted_at` | timestamptz | Soft-delete. Debezium watches this column |
| `created_at` | timestamptz | |
| `updated_at` | timestamptz | |

### `ingested_objects`

Object registry. One row per successfully ingested object. Written exclusively by the
Debezium JDBC sink (SUCCESS events only, upsert on `id`). The storage service reads from
this table; it never writes to it.

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | PK. obj_id = uuid5(namespace_id, source) |
| `namespace_id` | UUID | FK → namespace.id (indexed) |
| `source` | text | Display label: filename for private; full fs path for enterprise |
| `object_type` | text | "file" for private; open-ended for enterprise |
| `content_type` | text | MIME type |
| `size_bytes` | bigint | Nullable |
| `group_id` | UUID | Nullable (indexed). connector_id for enterprise; null for private |
| `ingested_at` | timestamptz | Server default: now() |

Note: the FK from `ingested_objects.namespace_id` to `namespace.id` was dropped in migration
0004. `passive_deletes=True` on the ORM relationship prevents SQLAlchemy from issuing a
`SET NULL` UPDATE before namespace deletion. Orphaned rows are cleaned up asynchronously
by the Celery tombstone pipeline.

### `ingestion_status`

Transactional outbox for task state. One row per ingestion run, created by `tasks.ingest` and
updated in place by every later task. Rows are never deleted. Written exclusively by the
controller worker. The storage service reads it directly to serve
`GET /namespaces/{id}/tasks/{task_id}`, and Debezium captures it for CDC.

| Column | Type | Notes |
|--------|------|-------|
| `task_id` | UUID | PK. The contract task_id (the id the storage service returned to the caller) |
| `namespace_id` | UUID | NOT NULL, indexed. No FK to `namespace` |
| `obj_id` | UUID | Nullable |
| `source` | text | Nullable. Display label |
| `object_type` | text | Nullable |
| `content_type` | text | Nullable |
| `size_bytes` | bigint | Nullable |
| `group_id` | UUID | Nullable |
| `operation` | text | `CREATE`, `MODIFY`, `DELETE`, or `AUTO_DELETE` |
| `stage` | text | Last task to start, e.g. `tasks.index` |
| `status` | text | `running`, `success`, or `failure` |
| `failure_reason` | text | Nullable. `{ExcType}: {message}`, truncated to 2000 characters |
| `created_at` | timestamp | Deliberately timezone-less: Debezium encodes it as μs BIGINT |
| `updated_at` | timestamp | Same encoding. Doubles as the completion time once `status` is terminal |

### `parse_stage_state`

Claim table for the race between `poll_parse` and `advance_from_callback`. One row per document
stage waiting on a conversion. `tasks.submit_parse` creates it, whichever task claims it first
dispatches the rest of the pipeline, and that task then deletes the row.

| Column | Type | Notes |
|--------|------|-------|
| `obj_id` | text | PK (with `stage`) |
| `stage` | text | PK (with `obj_id`). Only `convert` today |
| `docling_task_id` | text | Nullable. Backfilled after submit |
| `resume_context` | JSONB | Everything the rest of the pipeline needs to dispatch |
| `created_at` | timestamptz | |
| `claimed_at` | timestamptz | Nullable |
| `claimed_by` | text | Nullable |

### `intake_dedup_ledger`

Content-addressed dedup for the enterprise intake service, which stops connector redeliveries
from ingesting the same file twice.

| Column | Type | Notes |
|--------|------|-------|
| `namespace_id` | UUID | PK (with `path`, `sha256`) |
| `path` | text | PK. Canonical filesystem path, with symlinks followed |
| `sha256` | text | PK. File content hash |
| `task_id` | UUID | Nullable. Filled in after the upload succeeds |
| `created_at` | timestamptz | |

### `data_source`

Enterprise connector registry. Owned by the enterprise data sources service.

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | PK |
| `name` | text | Display label |
| `source_type` | text | `filesystem`, `github-pr`, etc. |
| `connector_name` | text | Unique. Kafka Connect connector name |
| `namespace_id` | UUID | Logical reference; no FK to storage DB |
| `namespace_name` | text | Nullable. UUID5 seed for the shared namespace |
| `org_name` | text | Used to derive owner_id via uuid5(ARTEMIS_NS, org_name) |
| `config` | JSON | Source-type-specific connector params |
| `created_at` | timestamptz | |
| `updated_at` | timestamptz | |
| `deleted_at` | timestamptz | Soft-delete |

### Celery result tables

`celery_taskmeta` and `celery_tasksetmeta` belong to Celery's stock result backend. Celery
creates them itself (`database_create_tables_at_setup`), not a migration. They are
informational only: nothing reads them for task state or CDC.

---

## Alembic Migrations

Migrations live in `src/backend/alembic/versions/`. They are run by the
`artemis-db-migrations` Docker Compose sidecar:

```yaml
artemis-db-migrations:
  image: artemis/db-migrations:dev
  command: alembic upgrade head
  depends_on:
    postgres:
      condition: service_healthy
```

Downstream services that need the schema declare:
```yaml
depends_on:
  artemis-db-migrations:
    condition: service_completed_successfully
```

Current migrations:
- `0001` — Celery result tables (`apollo_celery_taskmeta`, `apollo_celery_tasksetmeta`);
  WAL publication `celery_results_publication` + replication slot
- `0002` — Initial application schema (namespace, owner, ingested_objects, ingestion_tasks)
- `0003` — (subsequent schema changes)
- `0004` — Drop FK from `ingested_objects.namespace_id` to `namespace.id` (passive deletes)
- `0005` — Add `ingested_at` column to `ingested_objects`
- `0006` — Add `parse_stage_state`
- `0007` — Add `ingestion_status` and publication `ingestion_status_publication`
- `0008` — Drop `apollo_celery_taskmeta` and its CDC infrastructure
- `0009` — Add `intake_dedup_ledger`
- `0010` — Drop `ingestion_tasks`; index `ingestion_status.namespace_id`

---

## WAL / CDC Configuration

PostgreSQL is configured for logical replication via the `command:` arg in compose:

```yaml
command: |
  postgres
  -c wal_level=logical
  -c hot_standby=on
  -c max_wal_senders=10
  -c max_replication_slots=10
  -c hot_standby_feedback=on
```

Migration 0007 creates publication `ingestion_status_publication` covering `ingestion_status`.
Debezium does not auto-create it (`publication.autocreate.mode=disabled`), and it reads the
publication through replication slot `ingestion_status_rep_slot`.

`ingestion_status` uses the default replica identity, which is enough because `task_id` is its
real primary key.

---

## Soft-Delete Trigger

A DB trigger handles the cascade when an owner is deleted:

```sql
CREATE OR REPLACE FUNCTION soft_cascade_namespace_on_owner_delete()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    UPDATE namespace
    SET deleted_at = now()
    WHERE owner_id = OLD.id
      AND deleted_at IS NULL;
    RETURN OLD;
END;
$$;

CREATE TRIGGER owner_soft_cascade
AFTER DELETE ON owner
FOR EACH ROW EXECUTE FUNCTION soft_cascade_namespace_on_owner_delete();
```

This is defined in `src/backend/storage/api/models.py` and emitted by Alembic migrations
for production, and via SQLAlchemy `DDL` events for local `create_all` usage.

---

## Connection Configuration

All services connect using the `SQL_DB_*` env var family:

```
SQL_DB_HOST      postgres
SQL_DB_PORT      5432
SQL_DB_USER      postgres
SQL_DB_PASSWORD  testpass
SQL_DB_DATABASE  documents
SQL_DRIVER       postgresql+asyncpg     # async services
                 postgresql+psycopg     # sync services (alembic, worker backend)
```

The storage service and indexing service use async SQLAlchemy (`asyncpg`). The controller
worker's custom `DatabaseBackend` uses synchronous psycopg. Alembic uses `postgresql+psycopg`.
