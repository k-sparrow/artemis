# PostgreSQL

**Version:** PostgreSQL 16  
**Role:** Relational database for namespace management, object registry, task history,
Celery result backend, and enterprise connector metadata. Also the CDC source for the
`ingested_objects` and `ingestion_tasks` pipeline.

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

### `ingestion_tasks`

Task history. One row per completed ingestion task (SUCCESS or FAILURE). Written exclusively
by the Debezium JDBC sink. The storage service reads from this table to serve `GET /tasks/{task_id}`.

| Column | Type | Notes |
|--------|------|-------|
| `task_id` | UUID | PK. The contract task_id (the id the storage service returned to the caller) |
| `obj_id` | UUID | Nullable (indexed). NULL on FAILURE tasks |
| `namespace_id` | UUID | FK → namespace.id (NOT NULL, indexed; denormalised for per-namespace queries) |
| `status` | text | `SUCCESS` or `FAILURE` |
| `failure_reason` | text | Nullable. Traceback excerpt; NULL on SUCCESS |
| `completed_at` | timestamptz | date_done from Celery result |
| `operation` | text | `CREATE`, `MODIFY`, or `DELETE` |

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

### `apollo_celery_taskmeta`

Celery result backend table. Written by the controller worker. Read by Debezium for CDC.
Schema defined by SQLAlchemy-Celery (`celery[sqlalchemy]`).

| Column | Type | Notes |
|--------|------|-------|
| `task_id` | varchar(155) | PK. Per-subtask Celery UUID |
| `status` | varchar(50) | PENDING / STARTED / SUCCESS / FAILURE |
| `result` | text | JSON: IngestionResult (SUCCESS) or FailureRecord (FAILURE) |
| `date_done` | datetime | |
| `traceback` | text | Nullable. Nulled by FailureRecordingTask.on_failure |
| `name` | text | Task name: "tasks.index", "tasks.delete_document", etc. |
| `args` | text | |
| `kwargs` | text | |
| `worker` | text | |
| `retries` | int | |
| `queue` | text | |

`result_extended=True` in the DatabaseBackend preserves `name`, `args`, `kwargs`, `worker` — required for the ksqlDB failure fan-out's `name IN (...)` filter.

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

Migration 0001 creates:
- Publication `celery_results_publication` covering `apollo_celery_taskmeta`
- Replication slot for Debezium

Debezium is configured with `REPLICA IDENTITY FULL` on `apollo_celery_taskmeta` so that CDC
events contain all column values (required for `ExtractNewRecordState` to produce a flat row
with all fields, not just the changed columns).

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
