# ksqlDB

**Version:** Confluent ksqlDB 7.9.1 (port 8088)  
**Role:** Stream processing layer between Kafka topics. Reshapes MinIO S3 events into
Celery task messages (Topology A), processes Debezium CDC events into the object registry
(Topology B), and reshapes enterprise file events into intake requests (Topology C).

---

## Init Scripts

Both scripts run once on startup via the `artemis-ksqldb-init` / `ksqldb-enterprise-init`
one-shot containers after ksqlDB passes its health check.

| Script | Profile | Topologies |
|--------|---------|------------|
| `tools/oci/images/ksqldb/artemis_init.ksql` | `backend` | A + B (private ingestion + CDC) |
| `tools/oci/images/ksqldb/artemis_enterprise_init.ksql` | `enterprise` | C (connectors → intake) |

---

## Streams Defined

**Topology A (S3 → Celery):**

| Stream | Source topic | Output topic | Purpose |
|--------|-------------|--------------|---------|
| `artemis-ingestion-storage-s3` | `artemis.ingestion.storage.s3` | — | Source: raw MinIO S3 events |
| `artemis-ingestion-celery-tasks` | ← above | `artemis.ingestion.celery.tasks` | Flatten contract, build Celery v2 message, key=namespace_id |
| `artemis-ingestion-celery-tasks-routed` | ← above | `artemis.ingestion.celery.tasks.routed` | Re-key by task_id for Camel RabbitMQ SMT |

**Topology B (CDC → DB):**

| Stream / Statement | Source | Output topic | Purpose |
|-------------------|--------|--------------|---------|
| `artemis-celery-taskmeta` | `apollo.ingestion.celery.results.*` | — | Source: Debezium CDC rows |
| `artemis-celery-ingested-objects` | ← above | `artemis.celery.ingested_objects` | SUCCESS events; key=obj_id; JDBC sink → ingested_objects |
| `artemis-celery-ingestion-tasks` | ← above | `artemis.celery.ingestion_tasks` | SUCCESS tasks.index; key=contract task_id |
| INSERT INTO ingestion_tasks | ← above | same | SUCCESS tasks.delete_document |
| INSERT INTO ingestion_tasks | ← above | same | FAILURE tasks.* (with task_id IS NOT NULL guard) |
| `artemis-celery-ingested-objects-deletes` | ← above | `artemis.celery.ingested_objects` | Tombstones for delete_document SUCCESS; KAFKA null value |

**Topology C (enterprise connectors → intake):**

| Stream | Source | Output topic | Purpose |
|--------|--------|--------------|---------|
| `artemis-datasource-filesystem` | `artemis.datasource.filesystem` | — | Source: Camel FileSource events |
| `artemis-enterprise-datasource-filesystem-intake` | ← above | `artemis.datasource.filesystem.intake` | Reshape to IntakeRequest JSON |

---

## Key Implementation Notes

### STRUCT Field Access Requires Backticks

Fields with special characters (hyphens, dots) in STRUCT paths require backtick quoting:

```sql
-- MinIO userMetadata field with a hyphen:
Records[1]->s3->object->userMetadata->`X-Amz-Meta-Contract`
```

### PARTITION BY Expression Must Appear in SELECT

ksqlDB requires the `PARTITION BY` expression to be projected in the `SELECT` clause. The
projected alias becomes the Kafka record key and is stripped from the value:

```sql
SELECT
  STRUCT(`task_id` := `task_id`) AS `task_key`,  -- used as the partition key
  `args`,
  `kwargs`
FROM ...
PARTITION BY STRUCT(`task_id` := `task_id`)       -- same expression
```

### Debezium Encodes TIMESTAMP as Microseconds

PostgreSQL `TIMESTAMP` columns are encoded by Debezium as `BIGINT` (microseconds since Unix
epoch). Convert when projecting:

```sql
FROM_UNIXTIME(date_done / 1000)   -- divide by 1000: μs → ms → DATETIME
```

### JSON_SR Required for JDBC Sink

The Debezium JDBC sink NPEs on plain JSON. Output streams that feed JDBC sinks must use
`JSON_SR` (JSON Schema Registry) format:

```sql
CREATE OR REPLACE STREAM my-stream
WITH (VALUE_FORMAT = 'JSON_SR', KEY_FORMAT = 'JSON_SR', ...)
```

This requires the Schema Registry (`schemaregistry` service) to be running.

### `CREATE STREAM IF NOT EXISTS` for Source Streams

`CREATE OR REPLACE STREAM` requires the output topic to pre-exist. Source streams
(which read from pre-existing topics) should use `IF NOT EXISTS` to be idempotent
across restarts:

```sql
CREATE OR REPLACE STREAM `artemis-ingestion-storage-s3` ...   -- OK: reads from pre-existing topic
CREATE STREAM IF NOT EXISTS `artemis-celery-taskmeta` ...      -- OK: source stream, topic pre-exists
```

### The Only Way to Produce an Empty Array

Required for the Celery v2 `"args": []` field:

```sql
ARRAY_REMOVE(ARRAY[0], 0)
```

ksqlDB has no `ARRAY[]` literal that produces an empty array.

### JSON Null vs String "null"

`EXTRACTJSONFIELD` returns the string `"null"` when the JSON value is `null`. Pydantic's
`UUID | None` type coercion rejects the string `"null"`. Normalise explicitly:

```sql
CASE
  WHEN EXTRACTJSONFIELD(result, '$.object.scope.group_id') = 'null'
    THEN CAST(NULL AS VARCHAR)
  ELSE EXTRACTJSONFIELD(result, '$.object.scope.group_id')
END AS `group_id`
```

### One-Shot Container Health Check

The `artemis-ksqldb-init` container must wait for ksqlDB to accept connections before
running the SQL. Use exit-code wait, not `LogMessageWaitStrategy` (which races with startup
and can miss the ready message):

```python
# In testcontainers-based tests:
ksqldb_container.wait_for_logs("Server up and running")  # unreliable
# Better: poll the /info endpoint until HTTP 200
```

---

## Licensing

ksqlDB is distributed under the **Confluent Community License** (not Apache 2.0). This
license permits use for internal tooling and on-premises deployment but prohibits offering
ksqlDB itself as a hosted/SaaS service.

Artemis uses ksqlDB for stateless stream projections only. A migration to Apache Flink
(fully Apache 2.0) is planned post-v1 (Epic 8) when a stateful use case (windowed
aggregations, stream joins) justifies the operational overhead of a Flink cluster.

---

## Accessing ksqlDB

```bash
# With dev-tools profile — ksqlDB CLI
docker exec -it artemis-ksqldb-cli ksql http://ksqldb:8088

# Show streams
ksql> SHOW STREAMS;

# Query a stream (limited)
ksql> SELECT * FROM `artemis-ingestion-storage-s3` EMIT CHANGES LIMIT 5;
```
