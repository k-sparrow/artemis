# Kafka

**Version:** Apache Kafka 4.0.0 (KRaft mode — no ZooKeeper)  
**Role:** Event streaming backbone for MinIO S3 upload notifications, enterprise file
source events, and Debezium CDC from PostgreSQL.

---

## Configuration

Kafka runs in KRaft mode (combined broker + controller in a single node):

```
KAFKA_PROCESS_ROLES:  broker,controller
KAFKA_NODE_ID:        1
```

Listeners:
- `INTERNAL://broker:9092` — service-to-service (within Docker network)
- `EXTERNAL://localhost:19092` — host access (mapped to port 9092 on the host)
- `CONTROLLER://broker:29093` — KRaft controller

No ZooKeeper is required. The cluster ID is fixed in `docker-compose.dev.yaml`:
`MkU3OEVBNTcwNTJENDM2Qk`.

---

## Schema Registry

**Version:** Confluent Schema Registry 6.0.0 (port 8081)

Required by:
- Debezium JDBC sink connectors — `VALUE_FORMAT='JSON_SR'` in ksqlDB CSAS requires
  the schema to be registered
- The JDBC sink uses `JsonSchemaConverter` and would NPE without schema metadata

Schema Registry URL (within Docker network): `http://schemaregistry:8081`

---

## Kafka Connect

**Version:** Confluent Kafka Connect (included in the enterprise stack)  
**Port:** 8083 (REST API)

Connector plugins installed in the Kafka Connect image:

| Plugin | Purpose |
|--------|---------|
| `debezium-connector-postgresql` | CDC source from PostgreSQL WAL |
| Debezium JDBC sink | Writes to PostgreSQL from Kafka topics |
| PostgreSQL JDBC driver | Required by the JDBC sink |
| Camel FileWatch source | inotify-based filesystem monitoring |
| Aiven HTTP sink | Forwards Kafka messages to HTTP endpoints |

---

## Connector Configurations

All connectors are registered by `artemis-kafka-connect-init` (a one-shot container) at
startup via the Kafka Connect REST API.

### Debezium PostgreSQL Source

Captures `apollo_celery_taskmeta` rows from the PostgreSQL WAL:

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "postgres",
  "database.dbname":   "documents",
  "table.include.list": "public.apollo_celery_taskmeta",
  "plugin.name":        "pgoutput",
  "publication.name":   "celery_results_publication",
  "transforms":         "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
}
```

`ExtractNewRecordState` unwraps the Debezium envelope and emits a flat row matching the
table's column names. The topic name is:
`apollo.ingestion.celery.results.public.apollo_celery_taskmeta`

### Debezium JDBC Sink (ingested_objects)

```json
{
  "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
  "topics": "artemis.celery.ingested_objects",
  "connection.url": "jdbc:postgresql://postgres:5432/documents",
  "insert.mode": "upsert",
  "primary.key.mode": "record_value",
  "primary.key.fields": "id",
  "delete.enabled": "true",
  "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
  "key.converter":   "io.confluent.connect.json.JsonSchemaConverter"
}
```

`delete.enabled=true` allows tombstone records (null value) to DELETE rows. The tombstone
fan-out in ksqlDB produces null-value records to this topic when `tasks.delete_document`
succeeds.

### Debezium JDBC Sink (ingestion_tasks)

Same as above but targets `artemis.celery.ingestion_tasks` and uses:
```json
"primary.key.mode": "record_key",
"primary.key.fields": "task_id"
```

### Camel FileWatch Source

Deployed by the enterprise data sources service when a filesystem data source is registered:

```json
{
  "connector.class": "org.apache.camel.kafkaconnector.filewatch.CamelFilewatchSourceConnector",
  "camel.source.path.path": "<watch_path>",
  "transforms": "setNamespace,setGroupId,setOwner,...",
  "transforms.setNamespace.value": "<namespace_id>",
  "transforms.setGroupId.value":   "<connector_id>",
  "transforms.setOwner.value":     "<owner_id>"
}
```

SMTs inject `artemis.namespace_id`, `artemis.group_id`, and `artemis.owner_id` as Kafka
headers at the connector level — these are baked in at connector creation time and never
require per-message computation.

### Aiven HTTP Sink

Routes enterprise file events to the intake service:

```json
{
  "connector.class": "io.aiven.kafka.connect.http.HttpSinkConnector",
  "http.url": "http://backend-enterprise-intake:9000/intake",
  "http.headers.content.type": "application/json",
  "topics": "artemis.datasource.filesystem.intake"
}
```

`http.headers.content.type=application/json` is required — Starlette 1.x rejects requests
without an explicit Content-Type header (added when fastmcp pulled in a newer Starlette).

---

## Topics

See [Event Topology](../architecture/event-topology.md) for the full topic list with
producers, consumers, and key formats.

---

## Accessing Kafka

From a host machine (dev):
```bash
# List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume from a topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic artemis.ingestion.storage.s3 --from-beginning
```

With the `dev-tools` profile, Kafka UI is available at `http://localhost:18080`.
