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

The message arrives in the `artemis.ingestion.parse` queue (routing key `parse`) of RabbitMQ
exchange `artemis.ingestion`.

---

## Step 5 — `tasks.ingest` Entry Task

The parse worker picks up the message. `tasks.ingest` runs:

1. Records `task_id = "f47ac10b-..."` (read from `self.request.id`) as OTel span attribute
2. Inserts the run's `ingestion_status` row with `status = "running"` and
   `stage = "tasks.ingest"`. Every later task updates this same row.
3. Matches `upload_action = UploadAction.CREATE`
4. Dispatches `tasks.parse` on its own. `tasks.index` is not chained here:
   ```python
   parse.apply_async(
     args=(s3,),
     kwargs={"source": source, "namespace_id": "550e8400...", "group_id": None,
             "task_id": "f47ac10b-...", "operation": "CREATE"},
   )
   ```

---

## Step 6 — Parse Sub-chain (artemis.ingestion.parse and artemis.ingestion.parse.long queues)

`tasks.parse` replaces itself with `submit_parse → poll_parse`. The rest of the sub-chain is
dispatched once conversion finishes.

**`tasks.submit_parse`**

1. Writes a `parse_stage_state` row (`obj_id`, `stage = "convert"`, plus everything the later
   tasks need) before contacting Docling Serve, so a very fast callback can find it
2. Calls `POST http://backend-parsing:10001/v1/parse/submit` with a form field holding a
   `BlobRef`, not the file's bytes:
   ```json
   { "bucket": "artemis", "key": "550e8400.../3e4a5b6c..." }
   ```
3. The parsing service:
   - Checks that the source object exists
   - Calls Docling Serve: `POST /v1/convert/source/batch`, with the S3 source and an S3 target
     under `docling-replay/scratch/convert/3e4a5b6c.../`, and registers an HTTP callback
   - Returns `SubmitResult { "parsing_task_id": "...", "obj_id": "3e4a5b6c...", "mode": "source" }`

**`tasks.poll_parse` or `tasks.advance_from_callback`**

Two paths can see the conversion finish:

- `tasks.poll_parse` checks `GET /v1/parse/status/{parsing_task_id}` every 60–75 s
- Docling Serve calls `POST /v1/parse/callback/3e4a5b6c...`, and the parsing service publishes
  `tasks.advance_from_callback`

Both try to claim the `parse_stage_state` row. Only the winner dispatches the rest of the
pipeline as a Celery chain: `resolve_parse → submit_chunk → poll_chunk → index`. The loser does
nothing. Polling stays on because callback delivery is not guaranteed.

**`tasks.resolve_parse`**

Calls `POST /v1/parse/resolve`. The parsing service copies the conversion JSON to
`docling-replay/replay/3e4a5b6c....json` with an S3 server-side copy, splits the Markdown into
pages, and writes them to `docling-replay/pages/3e4a5b6c....json`. Returns `{ "obj_id": "3e4a5b6c..." }`.

**`tasks.submit_chunk`** (artemis.ingestion.parse.long queue)

Calls `POST /v1/chunk/submit`. The parsing service reads the replay JSON and uploads it to
Docling Serve inline. Returns `{ "chunking_task_id": "...", "obj_id": "3e4a5b6c..." }`.

**`tasks.poll_chunk`**

Polls `GET /v1/chunk/status/{chunking_task_id}`. On success it calls `POST /v1/chunk/finalize`,
and the parsing service:

- Fetches the chunk result from Docling Serve
- Merges it with the cached pages into a `ParseArtifact` with `pages[]` + `chunks[]`
- Writes the artifact to MinIO at `parsed-chunks/parse/3e4a5b6c....json`
- Returns `BlobRef { "bucket": "parsed-chunks", "key": "parse/3e4a5b6c....json" }`

---

## Step 7 — `tasks.index` (artemis.ingestion.index queue)

Receives `{ "bucket": "parsed-chunks", "key": "parse/3e4a5b6c....json" }` from `tasks.poll_chunk`.

1. Calls `POST http://backend-indexing-ingestion:10000/ingest?namespace=550e8400...`
   with body:
   ```json
   { "artifact_ref": { "bucket": "parsed-chunks", "key": "parse/3e4a5b6c....json" } }
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
3. `tasks.index` deletes `parsed-chunks/parse/3e4a5b6c....json` from MinIO (cleanup)
4. Returns `IngestionResult`, which Celery's result backend keeps for information only:
   ```json
   {
     "task_id":   "f47ac10b-...",
     "operation": "CREATE",
     "object":    { "id": "3e4a5b6c...", "source": "report.pdf",
                    "scope": { "namespace_id": "550e8400...", "group_id": null },
                    "properties": { "object_type": "file", "content_type": "application/pdf",
                                    "size_bytes": 1258291 } },
     "indexing":  { "num_added": 12, "num_skipped": 0 }
   }
   ```
5. `TerminalOutboxTask.on_success` sets `status = "success"` on the `ingestion_status` row

---

## Step 8 — Debezium CDC → ksqlDB Topology B

Debezium reads the row update from the WAL and emits a CDC event to
`apollo.ingestion.celery.results.public.ingestion_status`.

ksqlDB Fan-out A (`artemis-celery-ingested-objects`) fires:
- Filter: `stage = 'tasks.index' AND status = 'success'`
- Projects the object fields, which are plain columns on `ingestion_status`
- Produces to `artemis.celery.ingested_objects` (key = `{id: "3e4a5b6c..."}`)
- JDBC sink upserts into `ingested_objects` table

---

## Step 9 — Caller Polls Task Status

```
GET /namespaces/550e8400-e29b-41d4-a716-446655440000/tasks/f47ac10b-58cc-4372-a567-0e02b2c3d479
X-Owner-Id: a1b2c3d4-0000-0000-0000-000000000001
```

Storage service reads `ingestion_status` directly by `task_id = "f47ac10b-..."`, with no CDC in
between. The row exists from Step 5 onwards, so a caller polling earlier sees
`status = "running"` and the current `stage`.

Response:
```json
{
  "task_id":        "f47ac10b-...",
  "obj_id":         "3e4a5b6c...",
  "namespace_id":   "550e8400...",
  "source":         "report.pdf",
  "status":         "success",
  "stage":          "tasks.index",
  "operation":      "CREATE",
  "failure_reason": null,
  "created_at":     "2026-06-25T12:34:02Z",
  "completed_at":   "2026-06-25T12:34:56Z"
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
t+2s    tasks.ingest inserts ingestion_status, dispatches tasks.parse
t+3s    tasks.submit_parse submits the conversion
t+30s   Docling parses PDF (GPU) — time varies with document size
t+31s   Callback or poll_parse wins the claim, dispatches the tail chain
t+35s   Pages cached, chunk job submitted and finished
t+36s   ParseArtifact written to MinIO
t+40s   tasks.index calls indexing service
t+45s   TEI embeds 12 chunks, Qdrant upserted
t+50s   artifact deleted, ingestion_status set to success
t+50s   GET /namespaces/{id}/tasks/{task_id} returns success
t+52s   Debezium reads WAL, ksqlDB processes, JDBC sink writes ingested_objects
```
