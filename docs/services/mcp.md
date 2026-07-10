# MCP Server

**Port:** 11000  
**Transport:** MCP Streamable HTTP (MCP 1.0), mounted at `/`  
**Framework:** FastMCP + FastAPI lifespan  
**Source:** `src/backend/mcp/`

The MCP server exposes Artemis capabilities as Model Context Protocol tools for AI
clients: Claude Desktop, Claude Code, custom agents, or any MCP-compatible client.

---

## MCP Tools

| Tool | Upstream call | Description |
|------|---------------|-------------|
| `list_namespaces` | `GET /namespaces` (storage) | List all accessible namespaces |
| `get_namespace` | `GET /namespaces/{id}` (storage) | Get namespace by ID |
| `list_objects` | `GET /namespaces/{id}/objects` (storage) | List indexed objects in a namespace, optionally filtered by `group_id` |
| `get_object` | `GET /namespaces/{id}/objects/{obj_id}` (storage) | Get metadata for a single document |
| `list_groups` | `GET /namespaces/{id}/groups` (storage) | List distinct groups with object counts |
| `upload_file` | `POST /namespaces/{id}/objects` (storage) | Upload a base64-encoded file for indexing *(only when `ENABLE_UPLOAD=true`)* |
| `retrieve` | `POST /retrieve/invoke` (indexing) | Semantic search within a namespace |

## MCP Resources

| Resource | Description |
|----------|-------------|
| `artemis://about` | Static markdown overview of Artemis — concepts (namespace, group, object), the async ingestion model, and the retrieve/return_parents behaviour. Intended to orient clients unfamiliar with Artemis. |

`upload_file` is only included in the tool list when `ENABLE_UPLOAD=true`. This gate
prevents AI clients from uploading arbitrary files when the MCP server is accessible.

---

## Tool Signatures

### `retrieve`

```
retrieve(
  namespace_id: str,
  query: str,
  top_k: int = 5,
  group_id: str | None = None,
  doc_id: str | None = None,
  return_parents: bool = False
) -> list[{content, source, score}]
```

Calls the indexing service's LangServe endpoint. `group_id` restricts search to a
specific connector's documents; `doc_id` restricts to chunks from a single file.
`return_parents` swaps each matched chunk for its full parent page (dedup'd —
multiple chunks from the same page collapse to one result); `score` is typically
null in that mode since it is a chunk-level ranking signal.

### `get_object`

```
get_object(
  namespace_id: str,
  obj_id: str
) -> dict  # {obj_id, source, content_type, size_bytes, group_id, ingested_at}
```

Returns metadata for a single indexed object. Does not return file content — use
`retrieve()` to search the indexed text.

### `upload_file`

```
upload_file(
  namespace_id: str,
  filename: str,
  content_base64: str,
  content_type: str = "application/octet-stream"
) -> dict  # {task_id}
```

Content must be base64-encoded before passing to this tool. The storage service decodes
and stores the bytes, then dispatches an async ingestion task. Only available when
`ENABLE_UPLOAD=true`.

### `list_objects`

```
list_objects(
  namespace_id: str,
  group_id: str | None = None
) -> list[dict]  # [{obj_id, source, content_type, size_bytes, group_id, ingested_at}]
```

The storage service's response also carries `namespace_id` and `object_type`;
both are stripped before returning to the client — `namespace_id` because the
caller already supplied it, `object_type` because it is currently constant
(`"file"`) for every ingestion path and carries no signal.

---

## MCP Session

The MCP server uses FastMCP's `streamable_http_app()` transport:

```python
session_manager = FastMCP(...)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with session_manager.run():
        yield

app = FastAPI(lifespan=lifespan)
app.mount("/", session_manager.streamable_http_app())
```

The session manager is started in the FastAPI lifespan — this is the correct mount order;
`streamable_http_app()` must be called at module level, not inside the lifespan.

---

## Owner Identity (Current Stub)

All requests to upstream services use a stub owner identity:

```
STUB_OWNER_ID=<uuid>
```

This UUID is sent as `X-Owner-Id` on every storage service call. In the current system,
this means all MCP tool calls operate as the same owner.

**Deferred:** The gateway (APISIX) will eventually extract the user's identity from the
auth token and forward `X-User-Id` / `X-Org-Id` headers to the MCP server. The MCP server
will then derive `X-Owner-Id` from these headers. The remaining blockers are APISIX
sub-claim → header extraction and `TRUSTED_PROXIES` enforcement (Hydra auth epic).

---

## Connecting to the MCP Server

### Claude Code

Add to your Claude Code MCP config (`~/.claude/settings.json` or workspace settings):

```json
{
  "mcpServers": {
    "artemis": {
      "type": "http",
      "url": "http://localhost:11000/"
    }
  }
}
```

Or via the CLI:
```bash
claude mcp add artemis --transport http http://localhost:11000/
```

### Claude Desktop

Via the gateway (when gateway profile is running):
```json
{
  "mcpServers": {
    "artemis": {
      "type": "http",
      "url": "http://localhost:9080/mcp/"
    }
  }
}
```

---

## Configuration

| Env var | Default | Notes |
|---------|---------|-------|
| `STORAGE_SERVICE_URL` | `http://backend-storage:7000` | |
| `INDEXING_SERVICE_URL` | `http://backend-indexing:10000` | |
| `ENABLE_UPLOAD` | `false` | Set `true` to expose upload_file tool |
| `STUB_OWNER_ID` | `00000000-0000-0000-0000-000000000000` | Zero UUID stub; set to a real owner UUID in production |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-mcp` | |
