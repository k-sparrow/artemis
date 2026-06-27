# APISIX

**Version:** apache/apisix:3.16.0-debian  
**Role:** API gateway — ingress routing, (future) auth token validation, rate limiting.
Currently routes external requests to the MCP server and enterprise data-sources service.

**Compose profile:** `gateway`  
**Ports:** 9080 (proxy), 9180 (admin API)

---

## Backend Store

APISIX stores configuration in **etcd** (v3.5.18, port 2379). All routes, upstreams,
plugins, and certificates are persisted in etcd and loaded at startup.

The etcd service (`artemis-etcd`) is declared as a dependency of `artemis-apisix`; APISIX
waits for etcd to be healthy before starting.

---

## Init Container

Routes are registered by `artemis-apisix-init` — a one-shot container that runs after
APISIX is healthy and calls the Admin REST API:

```bash
curl -X PUT http://apisix:9180/apisix/admin/routes/mcp \
  -H 'X-API-KEY: <admin-key>' \
  -d '{"uri": "/mcp/*", "upstream": {"nodes": {"backend-mcp:11000": 1}}}'
```

All routes are created idempotently via `PUT` (overwrite-safe on restart).

---

## Current Routes

| Route ID | URI pattern | Upstream service | Notes |
|----------|-------------|-----------------|-------|
| `mcp` | `/mcp/*` | `backend-mcp:11000` | MCP Streamable HTTP; strip-path prefix not set — upstream receives `/mcp/...` |
| `data-sources` | `/data-sources/*` | `backend-enterprise-data-sources:9500` | Enterprise only |

All other paths return 404 by default. There is no "pass-through to storage" route — the
storage service (port 7000) is currently accessed directly by the MCP server via
`STORAGE_SERVICE_URL`, not through the gateway.

---

## Deferred Features

The following are designed but not yet implemented (blocked on the Hydra auth epic):

| Feature | Notes |
|---------|-------|
| Token validation | Hydra opaque token introspection plugin |
| `X-User-Id` / `X-Org-Id` injection | APISIX extracts claims from Hydra introspection response; forwards to upstream as headers |
| `X-Owner-Id` forwarding | Storage service currently reads this from request header; gateway will set it from JWT |
| Rate limiting | Per-user request throttling via `limit-req` plugin |
| `X-Request-Id` injection | Trace correlation; will be included in OTel spans |
| Remove direct port exposure | Services (7000, 10000, 11000, 9500) exposed on host only for dev; production routes all external traffic through 9080 |

---

## Admin API

The Admin API is available at `http://localhost:9180` (dev). All management operations
use the `X-API-KEY` header:

```bash
# List all routes
curl -H 'X-API-KEY: <key>' http://localhost:9180/apisix/admin/routes

# Check APISIX status
curl http://localhost:9080/apisix/status
```

The admin key is set via `APISIX_ADMIN_KEY` in compose.

---

## APISIX Dashboard (Optional)

APISIX Dashboard is not included in the default dev-tools profile. It can be added as an
optional service for visual route management if needed.

---

## TLS

TLS termination is not configured in dev. In production, APISIX handles TLS termination
for the gateway, with certificates stored in etcd. Upstream communication (APISIX →
backend services) remains plaintext within the cluster network.
