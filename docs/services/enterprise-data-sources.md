# Enterprise Data Sources Service

**Port:** 9500  
**Framework:** FastAPI + async SQLAlchemy + httpx  
**Source:** `src/backend/enterprise/data_sources/`

The enterprise data sources service is the control plane for Kafka Connect source
connectors. Operators use it (via the TUI or REST) to register data sources, and the
service deploys and manages the corresponding Kafka Connect connectors.

---

## Endpoints

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `POST` | `/data-sources` | Create data source + deploy Kafka connector | `201 DataSourceResponse` |
| `GET` | `/data-sources` | List all data sources with live connector status | `200 [DataSourceResponse]` |
| `GET` | `/data-sources/{id}` | Get single data source | `200 DataSourceResponse` |
| `POST` | `/data-sources/{id}/pause` | Pause Kafka connector | `200` |
| `POST` | `/data-sources/{id}/resume` | Resume Kafka connector | `200` |
| `POST` | `/data-sources/{id}/restart` | Restart Kafka connector | `200` |
| `DELETE` | `/data-sources/{id}` | Delete connector + soft-delete row + dispatch group delete | `204` |

---

## Create Data Source Flow

`POST /data-sources`:

1. Validate request body (source type, namespace reference, config params)
2. Derive `owner_id = uuid5(ARTEMIS_NS, org_name)` (not persisted — re-derived at call time)
3. Derive `connector_name` (unique; used as FK to Kafka Connect)
4. Render connector config from template for the given `source_type`
5. `PUT http://kafka-connect:8083/connectors/{connector_name}/config` (upsert; idempotent)
6. Persist `data_source` row to PostgreSQL
7. Return `DataSourceResponse` with current connector status

---

## Delete Data Source Flow

`DELETE /data-sources/{id}`:

1. `DELETE http://kafka-connect:8083/connectors/{connector_name}` (stops the connector)
2. Set `data_source.deleted_at = now()` (soft-delete)
3. Enumerate `ingested_objects` for `group_id = data_source.id` via storage service
4. For each object: dispatch `tasks.ingest(upload_action=DELETE)` via Celery (triggered
   through the storage service's batch delete endpoint)

Step 4 triggers the same Kafka → RabbitMQ → Celery pipeline as a normal delete, so all
Qdrant vectors and `ingested_objects` rows are cleaned up asynchronously.

---

## Connector Templates

Source connector configurations are defined in
`src/backend/enterprise/data_sources/api/sources/templates.py`.

### Filesystem (Camel FileWatch)

Watches a directory with inotify. Parameters:

| Param | Description |
|-------|-------------|
| `watch_path` | Absolute path to the directory (must be mounted into the connector container) |
| `recursive` | Whether to recurse into subdirectories |

Generated connector config excerpt:
```json
{
  "connector.class": "org.apache.camel.kafkaconnector.filewatch.CamelFilewatchSourceConnector",
  "camel.source.path.path": "<watch_path>",
  "camel.source.endpoint.recursive": "true",
  "transforms": "setNamespace,setGroupId,setOwner,setTask",
  "transforms.setNamespace.type": "org.apache.kafka.connect.transforms.InsertHeader$Value",
  "transforms.setNamespace.header": "artemis.namespace_id",
  "transforms.setNamespace.value.literal": "<namespace_id>",
  "transforms.setGroupId.header": "artemis.group_id",
  "transforms.setGroupId.value.literal": "<connector_id>",
  "transforms.setOwner.header": "artemis.owner_id",
  "transforms.setOwner.value.literal": "<owner_id>"
}
```

The SMTs inject `artemis.namespace_id`, `artemis.group_id`, and `artemis.owner_id` as Kafka
headers at connector creation time — these are baked in for the lifetime of the connector.

---

## DB Model

`data_source` table (in `documents` database, same PostgreSQL as storage service):

| Column | Notes |
|--------|-------|
| `id` | UUID PK — becomes `group_id` on all objects from this connector |
| `name` | Display label |
| `source_type` | `filesystem`, `github-pr`, etc. |
| `connector_name` | Unique. Used as Kafka Connect connector name |
| `namespace_id` | Logical reference (no FK to storage DB) |
| `namespace_name` | UUID5 seed for SHARED namespace derivation |
| `org_name` | Source of `owner_id` derivation |
| `config` | JSON: source-type-specific params (watch_path, recursive, etc.) |
| `created_at` | |
| `updated_at` | |
| `deleted_at` | Soft-delete |

---

## Live Connector Status

`GET /data-sources` enriches each row with the current Kafka Connect connector status by
calling `GET http://kafka-connect:8083/connectors/{connector_name}/status`. Status is not
persisted — it is fetched live on every list request.

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `SQL_DB_URL` | `postgresql+asyncpg://...` | Full DSN string |
| `KAFKA_CONNECT_URL` | `http://kafka-connect:8083` | |
| `STORAGE_SERVICE_URL` | `http://backend-storage:7000` | Used for group delete enumeration |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-enterprise-data-sources` | |
