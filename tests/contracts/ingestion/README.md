# Ingestion Contract Tests

Contract tests for the `artemis.ingestion.storage.s3` → `artemis.ingestion.celery.tasks`
ksqlDB stream topology.

## What is tested

These tests validate the **ksqlDB stream logic** defined in
`tools/oci/images/ksqldb/artemis_init.ksql`. They sit at the
[contract/schema layer](../../../CLAUDE.md#contract--schema----tags--integration):
one boundary, no application service images, infrastructure limited to
`KafkaContainer + KSQLDbContainer`.

The `streams` fixture runs the `artemis/ksqldb-init:latest` sidecar — the
same image that production deploys. `artemis_init.ksql` is baked into that
image, so there is no SQL duplication and no file mounting. Any change to the
`.ksql` file requires rebuilding the image (`bazel run //tools/oci/images/ksqldb:artemis_init_tarball`)
before these tests will see it.

---

## Topology

```
MinIO bucket notification
    → artemis.ingestion.storage.s3   (JSON, MinIO S3 event envelope)
    → [ksqlDB: artemis-ingestion-storage-s3 → artemis-ingestion-celery-tasks]
    → artemis.ingestion.celery.tasks (JSON, Celery v2 task message)
    → Camel Spring RabbitMQ Sink Connector
        SMT HeaderFrom:  task_id value  → Kafka header CamelHeader.id
        SMT InsertHeader: CamelHeader.task = "tasks.ingest"
    → RabbitMQ → Celery worker tasks.ingest
```

---

## Input contract (`artemis.ingestion.storage.s3`)

MinIO S3 event envelope (JSON).  Only the fields consumed by the stream are
listed; MinIO emits additional fields that are ignored.

```json
{
  "EventName": "s3:ObjectCreated:Put",
  "Records": [
    {
      "s3": {
        "bucket": { "name": "<bucket>" },
        "object": {
          "key": "<object-key>",
          "userMetadata": {
            "X-Amz-Meta-Task_id":   "<uuid>",
            "X-Amz-Meta-Contract":  "<JSON string — IngestionTaskDetails>"
          }
        }
      }
    }
  ]
}
```

`X-Amz-Meta-Contract` is a JSON-serialised `IngestionTaskDetails`:

```json
{
  "upload_action": "create" | "modify" | "delete",
  "s3":     { "bucket": "<str>", "object": "<str>" },
  "source": { "source": "<str>", "content_type": "<str>",
              "obj_id": "<str>", "object_type": "<str>" },
  "info":   { "namespace_id": "<uuid>" }
}
```

---

## Output contract (`artemis.ingestion.celery.tasks`)

Celery v2 task message (JSON value, JSON key).

### Kafka record key

`namespace_id` — JSON-encoded string (`KEY_FORMAT = JSON`).  All events for the
same namespace land on the same partition, preserving CREATE / MODIFY / DELETE
ordering per namespace.

### Kafka record value

```json
{
  "task_id": "<uuid>",
  "args":    [],
  "kwargs": {
    "upload_action": "CREATE" | "MODIFY" | "DELETE",
    "s3":     { "bucket": "<str>", "object": "<str>" },
    "source": { "source": "<str>", "content_type": "<str>",
                "obj_id": "<str>",  "object_type": "<str>" },
    "info":   { "namespace_id": "<uuid>" }
  }
}
```

Key points:

| Field | Notes |
|---|---|
| `task_id` | Top-level, **not** inside `kwargs`. The `HeaderFrom` SMT on the Camel sink connector reads it here and promotes it to the `CamelHeader.id` Kafka header (Celery v2 protocol). |
| `args` | Always `[]`. Celery v2 envelope requirement. |
| `upload_action` | Uppercased by `UCASE()`. Storage service writes lowercase; the Celery worker expects uppercase `UploadAction` enum values. |
| `namespace_id` (key) | ksqlDB requires the `PARTITION BY` expression to appear in the `SELECT` projection; it then writes it as the Kafka record key and strips it from the value. |

---

## Test coverage

| Test class | What it asserts |
|---|---|
| `TestS3InputContract` | `create` and `delete` events both produce output records (basic parse coverage) |
| `TestCeleryOutputContract` | Top-level keys are exactly `{task_id, args, kwargs}`; `args == []`; kwargs keys are exactly `{s3, source, upload_action, info}`; `task_id` is top-level (not in kwargs); `upload_action` is uppercased for both `create` and `delete`; Kafka key is JSON-encoded `namespace_id`; `s3.bucket` and `s3.object` are extracted correctly; `info.namespace_id` passes through |

---

## Running

```bash
bazel test //tests/contracts/ingestion:test_s3_to_celery --test_tag_filters=integration
```

No Docker images need to be pre-built. The test stack (Kafka + ksqlDB) is
started by testcontainers and torn down automatically.