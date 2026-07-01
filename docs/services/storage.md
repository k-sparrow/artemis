# Storage Service

**Port:** 7000  
**Framework:** FastAPI + async SQLAlchemy + aiobotocore  
**Source:** `src/backend/storage/`

The storage service is the entry point for all document ingestion. It owns the namespace
data model, controls access, uploads files to MinIO, and dispatches ingestion tasks into
the Kafka pipeline.

---

## Endpoints

### Namespaces

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `POST` | `/namespaces` | Create namespace + upsert owner | `201 NamespaceResponse` |
| `GET` | `/namespaces` | List accessible namespaces (PRIVATE: own only; SHARED: all) | `200 [NamespaceResponse]` |
| `GET` | `/namespaces/{id}` | Get namespace by ID | `200 NamespaceResponse` |
| `PATCH` | `/namespaces/{id}` | Rename namespace (PRIVATE only) | `200 NamespaceResponse` |
| `DELETE` | `/namespaces/{id}` | Soft-delete namespace; dispatch delete_namespace task | `204` |

### Objects (files)

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `POST` | `/namespaces/{id}/objects` | Upload file to MinIO; dispatch ingest task | `202 UploadResponse` |
| `PUT` | `/namespaces/{id}/objects/{obj_id}` | Re-upload / replace a specific object; dispatches ingest with the same `obj_id` | `202 UploadResponse` |
| `GET` | `/namespaces/{id}/objects` | Paginated list from `ingested_objects` | `200 [ObjectResponse]` |
| `DELETE` | `/namespaces/{id}/objects/{obj_id}` | Dispatch delete task (404 if not in `ingested_objects` yet) | `202` |
| `DELETE` | `/namespaces/{id}/objects` | Batch delete by `?group_id={id}` | `202` |

### Tasks

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `GET` | `/namespaces/{id}/tasks` | List ingestion tasks for namespace | `200 [TaskResponse]` |
| `GET` | `/tasks/{task_id}` | Single task status | `200 TaskResponse` |

---

## Access Control

All endpoints require an `X-Owner-Id` header (UUID). The `caller_owner_id_dependency`
FastAPI dependency parses and validates this header.

`_fetch_namespace(id, caller_owner_id, require_write=False)` enforces:

| Namespace type | Read rule | Write rule |
|---------------|-----------|-----------|
| PRIVATE | `namespace.owner_id == caller_owner_id` | same |
| SHARED | any authenticated caller accepted | `namespace.owner_id == caller_owner_id` |

Violations raise `HTTP_403_FORBIDDEN`. A non-existent or soft-deleted namespace raises
`HTTP_404_NOT_FOUND`.

---

## Upload Flow

`POST /namespaces/{id}/objects`:

1. Fetch and validate namespace (access check)
2. Compute `obj_id = uuid5(namespace_id, source)` where `source` is the filename
3. Check `ingested_objects` for an existing row with this `obj_id`:
   - First upload → `upload_action = CREATE`
   - Re-upload (same filename) → `upload_action = MODIFY`
4. Generate `task_id` (UUID4)
5. Construct `IngestionTaskDetails` with all upload metadata
6. PUT file to MinIO at key `{namespace_id}/{obj_id}` with:
   - `Metadata["task_id"] = task_id`
   - `Metadata["contract"] = IngestionTaskDetails.model_dump_json()`
7. MinIO fires an S3 event notification to Kafka; ksqlDB Topology A picks it up
8. Return `{task_id, s3_key}` with `HTTP_202_ACCEPTED`

The storage service does NOT dispatch Celery tasks directly. The entire pipeline is event-driven from the MinIO PUT.

---

## DB Models

The storage service owns these tables (defined in `src/backend/storage/api/models.py`):

- `owner` — thin ownership anchor; soft-delete trigger cascades to namespaces
- `namespace` — core isolation unit (PRIVATE / SHARED)
- `ingested_objects` — read-only from storage service perspective; written by JDBC sink
- `ingestion_tasks` — read-only from storage service perspective; written by JDBC sink

The `ingested_objects` and `ingestion_tasks` tables have no FK to `namespace` at the DB
level (FK dropped in migration 0004); storage service queries them with explicit WHERE
filters.

---

## Namespace Deletion

`DELETE /namespaces/{id}`:
1. Set `namespace.deleted_at = now()` (soft-delete)
2. Dispatch `tasks.delete_namespace(namespace_id=id)` to the Celery worker

The DB trigger fires on owner DELETE and cascades `deleted_at` to all owned namespaces,
which in turn triggers Debezium → Celery delete tasks for each namespace.

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `SQL_DB_HOST` | `postgres` | |
| `SQL_DB_PORT` | `5432` | |
| `SQL_DB_USER` | `postgres` | |
| `SQL_DB_PASSWORD` | `testpass` | |
| `SQL_DB_DATABASE` | `documents` | |
| `SQL_DRIVER` | `postgresql+asyncpg` | |
| `S3_ENDPOINT_URL` | `http://minio:9000` | |
| `S3_ACCESS_KEY` | `minioadmin` | |
| `S3_SECRET_KEY` | `minioadmin` | |
| `S3_ARTEMIS_BUCKET` | `artemis` | Raw upload target |
| `S3_ARTEMIS_BUCKET_KAFKA_EVENT` | `artemis` | Bucket that fires Kafka events (usually the same) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Optional; disables OTel if unset |
| `OTEL_SERVICE_NAME` | `backend-storage` | |
