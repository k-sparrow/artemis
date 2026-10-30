# Event Topology

This document describes the complete Kafka / ksqlDB / RabbitMQ event topology: every topic,
every stream, and the transformation applied at each hop.

Two ksqlDB init scripts define the topology:
- `tools/oci/images/ksqldb/artemis_init.ksql` — Topologies A + B (private ingestion + CDC)
- `tools/oci/images/ksqldb/artemis_enterprise_init.ksql` — Topology C (enterprise connectors)

---

## Kafka Topics

| Topic | Producer | Consumer | Key | Format |
|-------|----------|----------|-----|--------|
| `artemis.ingestion.storage.s3` | MinIO (bucket notification) | ksqlDB source stream | null | JSON |
| `artemis.ingestion.celery.tasks` | ksqlDB CSAS | ksqlDB re-partition step | namespace_id (JSON) | JSON |
| `artemis.ingestion.celery.tasks.routed` | ksqlDB CSAS | Camel RabbitMQ Sink | `{task_id}` (JSON struct) | JSON |
| `apollo.ingestion.celery.results.public.ingestion_status` | Debezium PostgreSQL source | ksqlDB source stream | row PK (`task_id`) | JSON (ExtractNewRecordState) |
| `artemis.celery.ingested_objects` | ksqlDB CSAS + tombstone CSAS | Debezium JDBC Sink | `{id}` (JSON_SR struct) | JSON_SR / KAFKA null |
| `artemis.datasource.filesystem` | Camel FileWatch Source connector | ksqlDB source stream | header-promoted values | JSON |
| `artemis.datasource.filesystem.intake` | ksqlDB CSAS | Aiven HTTP Sink | — | JSON |

---

## Topology A: MinIO S3 → Celery Task Dispatch

A file upload to MinIO triggers an S3 bucket notification to Kafka. ksqlDB reshapes the
raw event into a Celery v2 task message and routes it to RabbitMQ.

```
MinIO PUT event
  │
  ▼
artemis.ingestion.storage.s3   (raw S3 event, JSON)
  │  Fields used:
  │    Records[1].s3.bucket.name
  │    Records[1].s3.object.userMetadata.X-Amz-Meta-Task_id
  │    Records[1].s3.object.userMetadata.X-Amz-Meta-Contract  (IngestionTaskDetails JSON string)
  │
  ▼ CSAS: artemis-ingestion-celery-tasks
  │  Extracts all IngestionTaskDetails fields via EXTRACTJSONFIELD
  │  Normalises upload_action to uppercase via UCASE()
  │  Partitions by namespace_id → ordering guarantee per namespace
  │
  ▼
artemis.ingestion.celery.tasks   (Celery v2 message, key=namespace_id)
  │  Value: { args: [], kwargs: {s3, source, upload_action, info} }
  │  Key:   namespace_id (JSON)
  │
  ▼ CSAS: artemis-ingestion-celery-tasks-routed
  │  Re-partitions by task_id → HeaderFrom$Key can promote it to CamelHeader.id
  │
  ▼
artemis.ingestion.celery.tasks.routed   (key={task_id: "<uuid>"} JSON struct)
  │
  ▼ Camel Spring RabbitMQ Sink Connector
  │  SMT: HeaderFrom$Key  → CamelHeader.id = task_id
  │  SMT: InsertHeader    → CamelHeader.task = "tasks.ingest"
  │  SMT: InsertHeader    → CamelSpringRabbitmqContentType = "application/json"
  │
  ▼
RabbitMQ exchange: artemis.ingestion
  │
  ├──► queue: artemis.ingestion.parse        (ingest + parse sub-chain tasks)
  ├──► queue: artemis.ingestion.parse.long   (submit_chunk)
  └──► queue: artemis.ingestion.index        (index tasks)
```

**Why two CSAS hops?**

The first CSAS partitions by `namespace_id` so all events for the same namespace land on
the same partition, preserving relative ordering of CREATE / MODIFY / DELETE. The second
CSAS re-partitions by `task_id` to make it the Kafka record key, which is required because
the Camel RabbitMQ sink's `HeaderFrom$Key` SMT reads from the Kafka record key — not from
the value — to produce the `CamelHeader.id` AMQP header that Celery v2 needs as its task UUID.

**Why task_id is in the KEY, not the VALUE:**

`PARTITION BY STRUCT(\`task_id\` := ...)` produces the named key `{"task_id": "<uuid>"}` and
strips it from the value body, leaving the value as pure `{args, kwargs}` — the exact Celery v2
format. The sink connector uses `ByteArrayConverter` on the value (avoids JSON→HashMap→bytes
conversion error) and `JsonConverter` on the key (to read the task_id struct).

---

## Topology B: Task State CDC → `ingested_objects`

The controller worker keeps one `ingestion_status` row per contract `task_id` and updates it in
place as the document moves through the pipeline. Debezium reads those writes from the
PostgreSQL WAL, and ksqlDB turns the two terminal successes into upserts and deletes on
`ingested_objects`.

Task state does not go through this pipeline. The storage service reads `ingestion_status`
directly, so `GET /namespaces/{id}/tasks/{task_id}` shows the live stage.

```
ingestion_status (PostgreSQL, wal_level=logical, default replica identity)
  │
  ▼ Debezium PostgreSQL source connector
  │  Captures: public.ingestion_status
  │  SMT: ExtractNewRecordState (unwraps Debezium envelope to flat row)
  │  Plugin: pgoutput
  │
  ▼
apollo.ingestion.celery.results.public.ingestion_status
  │  Fields: task_id, namespace_id, obj_id, source, object_type, content_type, size_bytes,
  │          group_id, operation, stage, status, failure_reason, updated_at (μs BIGINT)
  │
  ▼ Source stream: artemis-ingestion-status
  │
  ├──► Fan-out A (index succeeded)
  │    CSAS: artemis-celery-ingested-objects
  │    • Filters: stage='tasks.index' AND status='success'
  │    • Projects: namespace_id, source, object_type, content_type, size_bytes, group_id
  │    • Key: STRUCT(id := obj_id)
  │    ▼
  │    artemis.celery.ingested_objects   (JSON_SR value + JSON_SR key)
  │    ▼
  │    Debezium JDBC Sink → ingested_objects (upsert, pk.mode=record_key, pk.fields=id)
  │
  └──► Tombstone fan-out (delete succeeded)
       CSAS: artemis-celery-ingested-objects-deletes
       • Filters: stage='tasks.delete_document' AND status='success'
       • Same output topic as Fan-out A: artemis.celery.ingested_objects
       • VALUE_FORMAT='KAFKA' + CAST(NULL AS VARCHAR) → null-value tombstone record
       • Key: STRUCT(id := obj_id)
       ▼
       Debezium JDBC Sink (delete.enabled=true) → DELETEs ingested_objects row by key
```

**Failure recording:**

`OutboxTask.on_failure()` sets `status='failure'` and `failure_reason` on the same row. No
fan-out matches a failure, so nothing reaches `ingested_objects`. Callers see the failure through
the direct read.

**Why the row is keyed by the contract `task_id`:**

Each subtask has its own Celery UUID, but the caller only holds the UUID of the entry task
(`tasks.ingest`). That id is passed down the chain as a kwarg, and `OutboxTask` uses it for
every write, which is what makes `GET /namespaces/{id}/tasks/{task_id}` resolvable at any stage.

---

## Topology C: Enterprise Connectors → Intake Service

Enterprise source connectors (Camel FileWatch) emit events to Kafka with file path and
ownership context as headers. ksqlDB reshapes the event into an `IntakeRequest` JSON body
and the Aiven HTTP sink delivers it to the enterprise intake service.

```
Camel FileWatch source connector
  │  Source topic:  artemis.datasource.filesystem
  │  Kafka headers promoted from connector config (SMT):
  │    artemis.namespace    (namespace name)
  │    artemis.namespace_id (UUID)
  │    artemis.org_name
  │    artemis.group_id     (= connector_id / DataSource.id)
  │    artemis.owner_id     (uuid5(ARTEMIS_NS, org_name))
  │    CamelHeader.CamelFileAbsolutePath
  │    CamelHeader.CamelFileName
  │
  ▼ Source stream: artemis-datasource-filesystem (or similar)
  │
  ▼ CSAS: artemis-enterprise-datasource-filesystem-intake
  │  Builds IntakeRequest JSON:
  │    source.type = "filesystem"
  │    source.path = CamelHeader.CamelFileAbsolutePath
  │    display_name = CamelHeader.CamelFileName
  │    namespace_id = artemis.namespace_id header
  │    group_id     = artemis.group_id header
  │    owner_id     = artemis.owner_id header
  │
  ▼
artemis.datasource.filesystem.intake
  │
  ▼ Aiven HTTP Sink connector
  │  http.headers.content.type = "application/json"  (required: Starlette 1.x is strict)
  │
  ▼
POST /intake  (enterprise intake service, port 9000)
```

---

## ksqlDB Quirks and Decisions

These behaviours tripped us up and are worth knowing before editing the init scripts.

**STRUCT field access requires backticks:**
```sql
-- Wrong:
Records[1]->s3->object->userMetadata->X-Amz-Meta-Contract

-- Right:
Records[1]->s3->object->userMetadata->`X-Amz-Meta-Contract`
```

**PARTITION BY must appear in SELECT:**
```sql
-- Wrong: alias in SELECT, use alias in PARTITION BY
SELECT foo AS bar ... PARTITION BY bar

-- Right: repeat the expression
SELECT foo AS bar, foo AS partition_key ... PARTITION BY foo
```

**Debezium encodes PostgreSQL TIMESTAMP as microseconds since epoch (BIGINT):**
```sql
FROM_UNIXTIME(updated_at / 1000)  -- divide by 1000 to convert μs → ms → datetime
```

**JSON_SR is required for JDBC sink:** the JDBC sink's Debezium NPE guard requires a schema
(JSON Schema Registry format), not plain JSON. Source streams can use JSON; JDBC sink
output streams must use `VALUE_FORMAT = 'JSON_SR'`.

**`CREATE STREAM IF NOT EXISTS` for source streams, not `CREATE OR REPLACE`:**
`OR REPLACE` requires the topic to pre-exist. Source streams create the topic on first run;
`IF NOT EXISTS` is idempotent and handles re-runs correctly.

**`ARRAY_REMOVE(ARRAY[0], 0)` is the only way to produce an empty array:**
Required for the Celery v2 `"args": []` field in the task message envelope.

**ksqlDB licensing:** Confluent Community License (not Apache 2.0). Cannot be used as a
component in a hosted/SaaS offering. Migration to Apache Flink is planned post-v1 (Epic 8)
when a stateful use case justifies the operational overhead.
