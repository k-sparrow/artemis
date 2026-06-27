# Ingestion Walkthrough

This document traces a single private file upload end-to-end through every hop in the
system, with concrete field values at each stage.

**Scenario:** A user uploads `report.pdf` (1.2 MB) to their private namespace.

---

## Step 1 — Upload Request

```
POST /namespaces/550e8400-e29b-41d4-a716-446655440000/objects
X-Owner-Id: a1b2c3d4-0000-0000-0000-000000000001
Content-Type: multipart/form-data

file=@report.pdf
```

The storage service:
1. Validates `X-Owner-Id` via `caller_owner_id_dependency` (must be a valid UUID)
2. Calls `_fetch_namespace()` — confirms the namespace is PRIVATE and owned by
   `a1b2c3d4-0000-0000-0000-000000000001`
3. Generates a `task_id` UUID: `f47ac10b-58cc-4372-a567-0e02b2c3d479`
4. Computes `obj_id = uuid5(namespace_id, "report.pdf")` →
   `3e4a5b6c-7d8e-9f0a-b1c2-d3e4f5a6b7c8`
5. Constructs `IngestionTaskDetails`:
   ```json
   {
     "upload_action": "create",
     "s3":    { "bucket": "artemis", "object": "550e8400.../3e4a5b6c...", "size": 1258291 },
     "source": { "source": "report.pdf", "content_type": "application/pdf",
                 "obj_id": "3e4a5b6c...", "object_type": "file" },
     "info":  { "namespace_id": "550e8400...", "group_id": null }
   }
   ```
6. Uploads `report.pdf` to MinIO at key `550e8400.../3e4a5b6c...`
   with metadata headers:
   - `X-Amz-Meta-Task_id: f47ac10b-...` (top-level for SMT)
   - `X-Amz-Meta-Contract: <JSON string above>`
7. Returns `202 Accepted` with `{ "task_id": "f47ac10b-...", "s3_key": "550e8400.../3e4a5b6c..." }`

---

## Step 2 — MinIO Bucket Notification → Kafka

MinIO emits an S3 event to `artemis.ingestion.storage.s3`:

```json
{
  "EventName": "s3:ObjectCreated:Put",
  "Records": [{
    "s3": {
      "bucket": { "name": "artemis" },
      "object": {
        "key": "550e8400.../3e4a5b6c...",
        "userMetadata": {
          "X-Amz-Meta-Task_id": "f47ac10b-...",
          "X-Amz-Meta-Contract": "{\"upload_action\":\"create\",\"s3\":{...},\"source\":{...},\"info\":{...}}"
        }
      }
    }
  }]
}
```

---

## Step 3 — ksqlDB Topology A: Reshape → Celery v2

**CSAS `artemis-ingestion-celery-tasks`** extracts and reshapes:

```json
{
  "task_id": "f47ac10b-...",
  "args": [],
  "kwargs": {
    "s3":            { "bucket": "artemis", "object": "550e8400.../3e4a5b6c...", "size": 1258291 },
    "source":        { "source": "report.pdf", "content_type": "application/pdf",
                       "obj_id": "3e4a5b6c...", "object_type": "file" },
    "upload_action": "CREATE",
    "info":          { "namespace_id": "550e8400...", "group_id": null }
  }
}
```

Kafka record key: `"550e8400..."` (namespace_id — for per-namespace ordering)

**CSAS `artemis-ingestion-celery-tasks-routed`** re-partitions:

Kafka record key: `{"task_id": "f47ac10b-..."}` (struct, for HeaderFrom$Key SMT)

---

## Step 4 — Camel RabbitMQ Sink → RabbitMQ

The sink connector applies three SMTs:
- `HeaderFrom$Key` — `CamelHeader.id = "f47ac10b-..."` (Celery task UUID)
- `InsertHeader` — `CamelHeader.task = "tasks.ingest"`
- `InsertHeader` — `CamelSpringRabbitmqContentType = "application/json"`

The message arrives in the `gpu_bound` queue of RabbitMQ exchange `artemis.ingestion`.

---

## Step 5 — `tasks.ingest` Entry Task

The Celery worker picks up the message. `tasks.ingest` runs:

1. Records `task_id = "f47ac10b-..."` (read from `self.request.id`) as OTel span attribute
2. Matches `upload_action = UploadAction.CREATE`
3. Dispatches the chain:
   ```python
   chain(
     fetch_and_parse.s(s3, source=source, namespace_id=namespace_id,
                       task_id="f47ac10b-...", operation="create"),
     index.s(namespace_id=namespace_id, upload_action="create",
             source=source, s3=s3, task_id="f47ac10b-..."),
   ).apply_async()
   ```

---

## Step 6 — `tasks.fetch_and_parse` (gpu_bound queue)

1. Builds `BlobRef(bucket="artemis", key="550e8400.../3e4a5b6c...")`
2. Calls `POST http://backend-parsing:10001/v1/parse` with body:
   ```json
   { "source_ref": { "bucket": "artemis", "key": "550e8400.../3e4a5b6c..." } }
   ```
3. The parsing service:
   - Reads `report.pdf` from MinIO by BlobRef
   - Calls Docling Serve: `POST http://docling-serve:5001/v1/convert/file`
   - Receives `DoclingDocument` with pages and structured chunks
   - Builds `ParseArtifact` with `pages[]` + `chunks[]`
   - Writes `ParseArtifact` JSON to MinIO at `parsed-chunks/<uuid>.json`
   - Returns `{ "out_key": "abc123.json" }`
4. `fetch_and_parse` returns `{ "bucket": "parsed-chunks", "key": "abc123.json" }`

---

## Step 7 — `tasks.index` (io_bound queue)

Receives `{ "bucket": "parsed-chunks", "key": "abc123.json" }` from previous task.

1. Calls `POST http://backend-indexing-ingestion:10000/ingest?namespace=550e8400...`
   with body:
   ```json
   { "artifact_ref": { "bucket": "parsed-chunks", "key": "abc123.json" } }
   ```
2. The indexing service:
   - Reads `ParseArtifact` from MinIO
   - Stamps each chunk's `Document.metadata`:
     `{"namespace_id": "550e8400...", "obj_id": "3e4a5b6c...", "group_id": null,
      "source": "report.pdf", "parent_id": "550e8400.../3e4a5b6c.../p1"}`
   - Calls TEI to embed each chunk
   - Upserts vectors into Qdrant collection `artemis`
   - Writes page markdown to `parent-pages` bucket (S3 doc store)
   - Returns `{ "num_added": 12, "num_skipped": 0, "ids": [...] }`
3. `tasks.index` deletes `parsed-chunks/abc123.json` from MinIO (cleanup)
4. Returns `IngestionResult`:
   ```json
   {
     "task_id":   "f47ac10b-...",
     "operation": "create",
     "object":    { "id": "3e4a5b6c...", "source": "report.pdf",
                    "scope": { "namespace_id": "550e8400...", "group_id": null },
                    "properties": { "object_type": "file", "content_type": "application/pdf",
                                    "size_bytes": 1258291 } },
     "indexing":  { "num_added": 12, "num_skipped": 0, "ids": [...] }
   }
   ```

This result is stored as JSON in `apollo_celery_taskmeta.result`.

---

## Step 8 — Debezium CDC → ksqlDB Topology B

Debezium detects the new row in `apollo_celery_taskmeta` (WAL change) and emits a CDC event
to `apollo.ingestion.celery.results.public.apollo_celery_taskmeta`.

ksqlDB Fan-out A (`artemis-celery-ingested-objects`) fires:
- Filter: `name = 'tasks.index' AND status = 'SUCCESS'`
- Extracts object fields from `result` JSON
- Produces to `artemis.celery.ingested_objects` (key = `{id: "3e4a5b6c..."}`)
- JDBC sink upserts into `ingested_objects` table

ksqlDB Fan-out B (`artemis-celery-ingestion-tasks`) fires:
- Same filter
- Extracts `task_id = "f47ac10b-..."` from `result.task_id` (the CONTRACT id)
- Produces to `artemis.celery.ingestion_tasks` (key = `{task_id: "f47ac10b-..."}`)
- JDBC sink upserts into `ingestion_tasks` table

---

## Step 9 — Caller Polls Task Status

```
GET /tasks/f47ac10b-58cc-4372-a567-0e02b2c3d479
X-Owner-Id: a1b2c3d4-0000-0000-0000-000000000001
```

Storage service queries `ingestion_tasks` by `task_id = "f47ac10b-..."`.

Response:
```json
{
  "task_id":      "f47ac10b-...",
  "status":       "SUCCESS",
  "operation":    "create",
  "completed_at": "2026-06-25T12:34:56Z",
  "obj_id":       "3e4a5b6c...",
  "namespace_id": "550e8400..."
}
```

---

## Step 10 — Retrieval

```
POST /retrieve/invoke
Content-Type: application/json

{
  "input": "quarterly revenue trends",
  "config": { "configurable": { "namespace_id": "550e8400..." } }
}
```

The indexing service embeds the query, searches Qdrant with a `namespace_id` filter, and
returns the top-k chunks. With `return_parents=true`, chunks are replaced by their parent
page markdown from the MinIO doc store.

---

## Timeline Summary

```
t+0s    POST /namespaces/{id}/objects  →  202 Accepted, task_id returned
t+1s    MinIO → Kafka → ksqlDB → RabbitMQ
t+2s    tasks.ingest dispatches chain
t+5s    tasks.fetch_and_parse calls parsing service
t+30s   Docling parses PDF (GPU) — time varies with document size
t+35s   ParseArtifact written to MinIO
t+40s   tasks.index calls indexing service
t+45s   TEI embeds 12 chunks, Qdrant upserted
t+50s   artifact deleted, IngestionResult stored in Postgres
t+52s   Debezium reads WAL, ksqlDB processes, JDBC sinks write DB rows
t+55s   GET /tasks/{task_id} returns SUCCESS
```
