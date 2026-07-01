# Enterprise Intake Service

**Port:** 9000  
**Framework:** FastAPI + httpx  
**Source:** `src/backend/enterprise/intake/`

The enterprise intake service is a stateless HTTP bridge between the Kafka HTTP sink and
the storage service. It receives `IntakeRequest` messages (delivered by the Aiven HTTP sink
connector from Kafka topic `artemis.datasource.filesystem.intake`) and materialises them
into actual uploads to the storage service.

---

## Endpoints

| Method | Path | Description | Response |
|--------|------|-------------|----------|
| `POST` | `/intake` | Accept and upload a file from a data source | `202 IntakeResponse` |
| `GET` | `/health` | Readiness probe | `200` |

---

## IntakeRequest

Delivered by the Aiven HTTP sink as JSON POST body:

```json
{
  "source": {
    "type": "filesystem",
    "path": "/watched/reports/quarterly.pdf",
    "display_name": "quarterly.pdf"
  },
  "namespace_id": "<uuid>",
  "group_id": "<uuid>",
  "owner_id": "<uuid>"
}
```

| Field | Origin |
|-------|--------|
| `source.type` | Set by ksqlDB CSAS from Camel FileWatch headers |
| `source.path` | `CamelFileAbsolutePath` header from Camel FileSource connector |
| `source.display_name` | Filename extracted from path |
| `namespace_id` | `artemis.namespace_id` Kafka header (set at connector creation) |
| `group_id` | `artemis.group_id` Kafka header (= DataSource.id) |
| `owner_id` | `artemis.owner_id` Kafka header (= uuid5(ARTEMIS_NS, org_name)) |

---

## Intake Flow

For `source.type = filesystem`:

1. Verify namespace exists: `GET {STORAGE_SERVICE_URL}/namespaces/{namespace_id}` with
   `X-Owner-Id: {owner_id}` — returns 404 if namespace not found or soft-deleted
2. Read file bytes from `source.path` (mounted filesystem path)
3. Infer MIME type from file extension
4. `POST {STORAGE_SERVICE_URL}/namespaces/{namespace_id}/objects` with:
   - Multipart file body
   - `X-Owner-Id: {owner_id}` header
   - `group_id` as query parameter
5. Return `{task_id, namespace_id}` to the Aiven HTTP sink

The HTTP sink does not use the response body — it only checks for 2xx status.

---

## Source Types

| Type | Status | Implementation |
|------|--------|----------------|
| `filesystem` | Implemented end-to-end | Reads from mounted FS path; MIME from extension; Camel FileWatch connector produces these messages |
| `inline` | Intake implemented; no connector | Intake service encodes the text body as bytes and uploads to storage; no Kafka Connect source connector exists yet to produce `InlineSource` messages (e.g. GitHub PR body) |
| `url` | Intake implemented; no connector | Intake service fetches bytes over HTTP from `source.url`; no Kafka Connect source connector exists yet to produce `UrlSource` messages |

The intake service's `_resolve_bytes` handles all three source types. The gap for `inline`
and `url` is on the producer side — there are no Kafka Connect connectors (e.g. GitHub PR,
GitHub PR Comment) deployed yet to route content through these paths.

---

## Owner Resolution

`owner_id` arrives pre-derived in the `IntakeRequest` body. The ksqlDB Topology C CSAS
extracts it from the `artemis.owner_id` Kafka header, which is injected by the Camel
FileSource connector at connector creation time (from the `org_name` → `uuid5` derivation
in the enterprise data sources service).

The intake service forwards `owner_id` as `X-Owner-Id` to the storage service with no
further derivation.

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `STORAGE_SERVICE_URL` | `http://localhost:7000` | |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-enterprise-intake` | |
