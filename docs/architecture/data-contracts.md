# Data Contracts

This document describes every message schema and inter-service contract in Artemis. For each
contract: what it contains, where it originates, and where it terminates.

All contracts are defined in `src/lib/core/ingestion/contract.py` and used by both the
storage service and the controller worker.

---

## `IngestionTaskDetails`

The main ingestion event. Constructed by the storage service, serialised as JSON into
MinIO object metadata, and consumed by the Celery worker after travelling through Kafka and
ksqlDB.

**Origin:** storage service at upload time  
**Destination:** `tasks.ingest` Celery kwargs

```
{
  "upload_action": "create" | "modify" | "delete",
  "s3": {
    "bucket": str,      // MinIO bucket name
    "object": str,      // object key = "{namespace_id}/{obj_id}{ext}" (ext = source filename's suffix, e.g. ".pdf"; empty if none)
    "size":   int       // bytes
  },
  "source": {
    "source":       str,  // display name (filename for private; fs path for enterprise)
    "content_type": str,  // MIME type
    "obj_id":       UUID, // uuid5(namespace_id, source)
    "object_type":  str   // "file" or open-ended for enterprise
  },
  "info": {
    "namespace_id": UUID,
    "group_id":     UUID | null
  }
}
```

> **`pipeline_type` not in contract:** The indexing service accepts `pipeline_type`
> as a per-request query param on `POST /ingest`, but it is not carried in
> `IngestionTaskDetails`. The controller worker always omits it, so
> `DEFAULT_PIPELINE_TYPE` applies to every task. Wiring `pipeline_type` end-to-end
> (upload endpoint → contract → worker dispatch) is a planned but not yet implemented
> enhancement.

**Storage:** as a single JSON string in the MinIO PUT metadata header
`X-Amz-Meta-Contract`. The `task_id` travels separately as `X-Amz-Meta-Task_id` (a plain
string, not inside the contract), because the Kafka Connect `HeaderFrom$Key` SMT that
promotes it to the `CamelHeader.id` AMQP header can only reach top-level Kafka record keys.

**ksqlDB reshape:** ksqlDB extracts each field via `EXTRACTJSONFIELD`, normalises
`upload_action` to uppercase via `UCASE()`, and rebuilds the Celery v2 message format:

```
{
  "args": [],
  "kwargs": { ...IngestionTaskDetails fields... }
}
```

The task_id becomes the Kafka record key (not a kwarg), which `HeaderFrom$Key` promotes to
`CamelHeader.id`. Celery v2 reads this as the task UUID.

---

## `BlobRef`

A self-describing claim-check reference to bytes in object storage. Used at every hop where
a service would otherwise have to send large binary payloads over HTTP.

**Defined in:** `src/lib/core/ingestion/contract.py`

```
{
  "bucket": str,  // MinIO bucket name
  "key":    str   // object key within the bucket
}
```

**Used at two boundaries:**
1. Worker → Parsing service: the input file reference (`{bucket: artemis, key: namespace_id/obj_id}`)
2. Parsing service → Worker → Indexing service: the artifact reference
   (`{bucket: parsed-chunks, key: <uuid>.json}`)

By carrying the bucket explicitly, the reference is self-describing — no shared configuration
needed to know where the producer wrote and the consumer should read.

---

## `ParseArtifact`

The structured output of the parsing service. Written to MinIO as JSON; referenced by `BlobRef`.

**Origin:** parsing service (writes to `parsed-chunks` bucket)  
**Destination:** indexing service (reads via `BlobRef`)

```
{
  "pages": [
    {
      "page_no":  int,
      "markdown": str   // full page text in Markdown
    }
  ],
  "chunks": [
    {
      "page_content": str,             // chunk text
      "source":       str,             // display name (same as SourceDetails.source)
      "type":         str,             // DocItemLabel value: TEXT | TABLE | SECTION_HEADER | ...
      "page_no":      int              // links chunk to parent page
    }
  ]
}
```

**Intentionally absent:** `dl_meta` (Docling internal metadata is stripped at this boundary).

**Replay cache:** the raw `DoclingDocument` JSON is also written to the `docling-replay`
bucket by the parsing service, but this is private to that service and not part of the
published contract.

---

## `IngestionResult`

The return value of `tasks.index` and `tasks.delete_document`. Celery's stock result backend
stores it for information only (`AsyncResult`, Flower). Nothing downstream reads it:
`ingested_objects` and task status both come from `ingestion_status`.

**Origin:** controller worker (`tasks.index` / `tasks.delete_document`)  
**Destination:** `celery_taskmeta.result` (informational)

```
{
  "task_id": str | null,         // contract task_id (the id the caller holds)
  "operation": str,              // "CREATE" | "MODIFY" | "DELETE" | "AUTO_DELETE"
  "object": {
    "id":     UUID,              // obj_id
    "source": str,
    "scope": {
      "namespace_id": UUID,
      "group_id":     UUID | null
    },
    "properties": {
      "object_type":  str,
      "content_type": str,
      "size_bytes":   int | null
    }
  },
  "indexing": {
    "num_added":   int,
    "num_skipped": int
  }
}
```

---

## `ingestion_status`

One row per contract `task_id`, the id the storage service returned to the caller. `tasks.ingest`
inserts it, and the worker updates it in place at each stage (see
[Controller Worker](../services/controller-worker.md#task-state)).

**Origin:** controller worker (`src/backend/controller/worker/backend/outbox.py`)  
**Destination:** direct read by the storage service; Debezium CDC → ksqlDB → `ingested_objects`

```
{
  "task_id":        UUID,         // contract task_id
  "namespace_id":   UUID,
  "obj_id":         UUID | null,
  "source":         str | null,
  "object_type":    str | null,
  "content_type":   str | null,
  "size_bytes":     int | null,
  "group_id":       UUID | null,
  "operation":      str,          // "CREATE" | "MODIFY" | "DELETE" | "AUTO_DELETE"
  "stage":          str,          // last task to start, e.g. "tasks.index"
  "status":         str,          // "running" | "success" | "failure"
  "failure_reason": str | null    // "{ExcType}: {message}" truncated to 2000 chars
}
```

---

## `UpsertResult`

Response from the indexing service's `POST /ingest` endpoint.

**Origin:** indexing service  
**Destination:** controller worker (`tasks.index` reads this, wraps it in `IngestionResult`)

```
{
  "num_added":   int,
  "num_skipped": int,
  "ids":         [str]   // Qdrant point IDs of added chunks
}
```

---

## `IntakeRequest`

Input to the enterprise intake service's `POST /intake` endpoint. Produced by the ksqlDB
`artemis-enterprise-datasource-filesystem-intake` CSAS from Camel FileSource events.

**Origin:** ksqlDB CSAS (enterprise topology C)  
**Destination:** enterprise intake service

```
{
  "source": {
    "type": "filesystem",       // only implemented source type
    "path": str                 // absolute filesystem path (from CamelFileAbsolutePath header)
  },
  "display_name": str,          // filename
  "namespace_id": UUID,
  "group_id":     UUID | null,  // connector_id for enterprise sources
  "owner_id":     UUID          // uuid5(ARTEMIS_NS, org_name), derived at connector creation
}
```

**InlineSource** (skeleton only):
```
{
  "type":     "inline",
  "content":  str,
  "encoding": str   // default "utf-8"
}
```

**UrlSource** (skeleton only):
```
{
  "type": "url",
  "url":  str
}
```

---

## Celery v2 Message Format (on the wire)

What actually travels through RabbitMQ:

```
AMQP headers:
  CamelHeader.id:                        <task_id UUID>
  CamelHeader.task:                      "tasks.ingest"
  CamelSpringRabbitmqContentType:        "application/json"

Body (JSON):
{
  "args":   [],
  "kwargs": {
    "s3":           { "bucket": ..., "object": ..., "size": ... },
    "source":       { "source": ..., "content_type": ..., "obj_id": ..., "object_type": ... },
    "upload_action": "CREATE" | "MODIFY" | "DELETE",
    "info":         { "namespace_id": ..., "group_id": ... }
  }
}
```

The Camel Spring RabbitMQ Sink Connector applies three SMTs to produce this format:
- `HeaderFrom$Key` — promotes the Kafka record key `{task_id: "<uuid>"}` to `CamelHeader.id`
- `InsertHeader` — adds `CamelHeader.task = "tasks.ingest"`
- `InsertHeader` — adds `CamelSpringRabbitmqContentType = "application/json"`
