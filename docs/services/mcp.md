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
| `list_objects` | `GET /namespaces/{id}/objects` (storage) | List indexed objects in a namespace |
| `list_groups` | `GET /namespaces/{id}/objects?group_id=...` (storage) | List objects for a connector group |
| `upload_file` | `POST /namespaces/{id}/objects` (storage) | Upload a file for indexing |
| `retrieve` | `POST /retrieve/invoke` (indexing) | Semantic search within a namespace |

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
  return_parents: bool = false
) -> list[Document]
```

Calls the indexing service's LangServe endpoint. `return_parents=true` returns full page
markdown instead of individual chunks.

### `upload_file`

```
upload_file(
  namespace_id: str,
  file_path: str,
  display_name: str | None = None
) -> UploadResponse
```

Reads the file from the local filesystem (relative to the MCP server's working directory)
and POSTs it to the storage service.

### `list_objects`

```
list_objects(
  namespace_id: str,
  page: int = 1,
  page_size: int = 20
) -> list[ObjectSummary]
```

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
| `STUB_OWNER_ID` | — | Required until gateway auth is wired |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | |
| `OTEL_SERVICE_NAME` | `backend-mcp` | |
