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
| `apollo.ingestion.celery.results.public.apollo_celery_taskmeta` | Debezium PostgreSQL source | ksqlDB source stream | row PK (int) | JSON (ExtractNewRecordState) |
| `artemis.celery.ingested_objects` | ksqlDB CSAS + tombstone CSAS | Debezium JDBC Sink | `{id}` (JSON_SR struct) | JSON_SR / KAFKA null |
| `artemis.celery.ingestion_tasks` | ksqlDB CSAS (3× INSERT INTO) | Debezium JDBC Sink | `{task_id}` (JSON_SR struct) | JSON_SR |
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
  ├──► queue: artemis.ingestion.fetch-and-parse   (fetch_and_parse tasks)
  └──► queue: artemis.ingestion.index             (index tasks)
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

## Topology B: Celery Result CDC → DB Tables

When `tasks.index` or `tasks.delete_document` completes, the result is stored in
`apollo_celery_taskmeta`. Debezium reads the PostgreSQL WAL and produces a CDC event. ksqlDB
filters and reshapes the event into two output streams that feed JDBC sinks.

```
apollo_celery_taskmeta (PostgreSQL, wal_level=logical, REPLICA IDENTITY FULL)
  │
  ▼ Debezium PostgreSQL source connector
  │  Captures: public.apollo_celery_taskmeta
  │  SMT: ExtractNewRecordState (unwraps Debezium envelope to flat row)
  │  Plugin: pgoutput
  │
  ▼
apollo.ingestion.celery.results.public.apollo_celery_taskmeta
  │  Fields: task_id, name, status, result (JSON string), date_done (μs BIGINT), traceback
  │
  ▼ Source stream: artemis-celery-taskmeta
  │
  ├──► Fan-out A (SUCCESS, tasks.index)
  │    CSAS: artemis-celery-ingested-objects
  │    • Filters: name='tasks.index' AND status='SUCCESS'
  │    • Extracts from result JSON: id, namespace_id, source, object_type, content_type,
  │      size_bytes (CAST to BIGINT), group_id (null guard for JSON "null" string)
  │    • Key: STRUCT(id := obj_id)
  │    ▼
  │    artemis.celery.ingested_objects   (JSON_SR value + JSON_SR key)
  │    ▼
  │    Debezium JDBC Sink → ingested_objects (upsert, pk.mode=record_value, pk.fields=id)
  │
  ├──► Fan-out B (SUCCESS, tasks.index)
  │    INSERT INTO: artemis-celery-ingestion-tasks
  │    • Filters: name='tasks.index' AND status='SUCCESS'
  │    • Extracts: task_id (from result.task_id — the CONTRACT id, not the row's task_id),
  │      obj_id, namespace_id, status, FROM_UNIXTIME(date_done/1000) as completed_at,
  │      operation, failure_reason=NULL
  │    • Key: STRUCT(task_id := result.task_id)
  │    ▼
  │    artemis.celery.ingestion_tasks   (JSON_SR value + JSON_SR key)
  │    ▼
  │    Debezium JDBC Sink → ingestion_tasks (upsert, pk.mode=record_key, pk.fields=task_id)
  │
  ├──► Fan-out C (SUCCESS, tasks.delete_document)
  │    INSERT INTO: artemis-celery-ingestion-tasks
  │    • Same fields as Fan-out B; operation='DELETE'
  │
  ├──► Fan-out D (FAILURE, tasks.fetch_and_parse / tasks.index / tasks.delete_document)
  │    INSERT INTO: artemis-celery-ingestion-tasks
  │    • Filters: status='FAILURE' AND task_id IS NOT NULL (guard — see below)
  │    • Extracts: failure_reason from result.failure_reason (FailureRecord field)
  │    • Guard: EXTRACTJSONFIELD(result, '$.task_id') IS NOT NULL
  │      Drops the pre-overwrite exception-encoded row; passes only the FailureRecord overwrite
  │
  └──► Tombstone fan-out (SUCCESS, tasks.delete_document)
       CSAS: artemis-celery-ingested-objects-deletes
       • Same output topic as Fan-out A: artemis.celery.ingested_objects
       • VALUE_FORMAT='KAFKA' + CAST(NULL AS VARCHAR) → null-value tombstone record
       • Key: STRUCT(id := obj_id)
       ▼
       Debezium JDBC Sink (delete.enabled=true) → DELETEs ingested_objects row by key
```

**Failure recording mechanism:**

Celery's `mark_as_failure` writes `{exc_type, exc_message, exc_module}` to `result` first.
`FailureRecordingTask.on_failure()` then overwrites the same row with a `FailureRecord` JSON
(contract `task_id`, namespace_id, failure_reason) and nulls the traceback column. Debezium
emits two CDC events for the same row (the two writes). The guard
`EXTRACTJSONFIELD(result, '$.task_id') IS NOT NULL` ensures only the second (FailureRecord)
event reaches the JDBC sink.

**Why `result.task_id` ≠ `taskmeta.task_id`:**

`apollo_celery_taskmeta.task_id` is the Celery UUID of the specific subtask (`tasks.index`).
The caller was given the UUID of the entry task (`tasks.ingest`), which is propagated down
the chain as a kwarg and embedded in `IngestionResult.task_id`. Keying the `ingestion_tasks`
row by `result.task_id` is what makes `GET /tasks/{task_id}` resolvable.

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
FROM_UNIXTIME(date_done / 1000)  -- divide by 1000 to convert μs → ms → datetime
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
