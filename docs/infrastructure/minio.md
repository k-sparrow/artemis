# MinIO

**Version:** `quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z`  
**Role:** S3-compatible object store for raw uploaded files, parse artifacts, parent-page
markdown, and Docling replay cache. Also the trigger for the ingestion pipeline via S3
bucket event notifications to Kafka.

---

## Bucket Layout

| Bucket | Written by | Read by | Contents |
|--------|-----------|---------|----------|
| `artemis` | Storage service | Controller worker | Raw uploaded files. Key pattern: `{namespace_id}/{obj_id}` |
| `parsed-chunks` | Parsing service | Indexing service | ParseArtifact JSON. Key pattern: `{uuid4}.json` |
| `docling-replay` | Parsing service | Parsing service (replay) | Raw `DoclingDocument` JSON. Not in any inter-service contract |
| `parent-pages` | Indexing service | Indexing service (retrieval) | Page markdown. Key pattern: `{namespace_id}/{obj_id}/p{page_no}` |

Buckets are created automatically on startup by `artemis-minio-init` (an `mc`-based
one-shot container). The parsing and indexing services also ensure their buckets exist in
their lifespan handlers.

---

## Claim-Check Pattern

Artemis never sends large payloads over HTTP. Services exchange a `BlobRef {bucket, key}`
and read/write directly to MinIO:

```
Storage service                     Parsing service
   │                                       │
   │  POST /v1/parse                       │
   │  { source_ref: {bucket, key} } ──────►│
   │                                       │  reads file from MinIO
   │                                       │  writes ParseArtifact to parsed-chunks/
   │                                       │  returns { out_key: "abc.json" }
   │◄──────────────────────────────────────│
   │
   │  (worker passes BlobRef {bucket: "parsed-chunks", key: "abc.json"} to indexing)
   │
                                    Indexing service
                                           │
                                           │  reads ParseArtifact from parsed-chunks/abc.json
                                           │  embeds chunks, writes to Qdrant
                                           │  deletes parsed-chunks/abc.json (cleanup)
```

The bucket is carried in `BlobRef` to make each reference self-describing — the consumer
reads from the exact bucket the producer wrote to, with no shared configuration required.

---

## S3 Event Notifications → Kafka

MinIO is configured to emit notifications on all object operations to a Kafka topic:

```yaml
MINIO_NOTIFY_KAFKA_ENABLE_ARTEMIS: "on"
MINIO_NOTIFY_KAFKA_BROKERS_ARTEMIS: broker:9092
MINIO_NOTIFY_KAFKA_TOPIC_ARTEMIS: artemis.ingestion.storage.s3
```

The event type filter `ARTEMIS` is set in the bucket notification configuration (by
`artemis-minio-init`). This means only PUT/DELETE operations on the `artemis` bucket
trigger notifications — operations on `parsed-chunks`, `docling-replay`, and `parent-pages`
do not.

The notification payload is a standard S3 event JSON structure. ksqlDB extracts the
contract JSON from the `userMetadata` field:

```json
{
  "EventName": "s3:ObjectCreated:Put",
  "Records": [{
    "s3": {
      "bucket": { "name": "artemis" },
      "object": {
        "key": "...",
        "userMetadata": {
          "X-Amz-Meta-Task_id":   "<uuid>",
          "X-Amz-Meta-Contract":  "<IngestionTaskDetails JSON>"
        }
      }
    }
  }]
}
```

---

## Object Key Design

**Raw uploads** (`artemis` bucket):
```
{namespace_id}/{obj_id}
```

`obj_id = uuid5(namespace_id, source)` — stable across re-uploads of the same file. A
second upload of the same filename to the same namespace overwrites this key, triggering an
`s3:ObjectCreated:Put` event with `upload_action=modify`.

**Parse artifacts** (`parsed-chunks` bucket):
```
{uuid4}.json
```

Random UUID per parse run. The worker receives this key from the parsing service and passes
it to the indexing service as a `BlobRef`. The indexing service deletes the artifact after
successful indexing.

**Parent pages** (`parent-pages` bucket):
```
{namespace_id}/{obj_id}/p{page_no}
```

Deterministic key — allows prefix-based deletion when an object is removed:
`DELETE /ingest?namespace=<ns>&obj_id=<obj>` triggers a prefix delete of
`{namespace_id}/{obj_id}/`.

---

## Authentication

Development credentials (from compose):
```
MINIO_ROOT_USER:     minioadmin
MINIO_ROOT_PASSWORD: minioadmin
```

Services connect using `S3_ACCESS_KEY` / `S3_SECRET_KEY` env vars. In production these
should be replaced with per-service IAM credentials or MinIO access keys with scoped
bucket permissions.

---

## MinIO Console

Available at `http://localhost:9090` (dev) when the `infra` profile is running. Use the
root credentials to inspect buckets, browse objects, and review event notification config.
