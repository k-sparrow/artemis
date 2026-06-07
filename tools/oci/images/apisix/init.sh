#!/usr/bin/env bash
# One-shot init container: waits for the APISIX Admin API, registers all
# routes, then exits so Docker Compose can track completion via
# `condition: service_completed_successfully`.
set -euo pipefail

ADMIN_URL="${APISIX_ADMIN_URL:-http://apisix:9180}"
ADMIN_KEY="${APISIX_ADMIN_KEY:-artemis-apisix-admin-key}"

echo "⏳ Waiting for APISIX Admin API at ${ADMIN_URL}..."
until curl -sf -o /dev/null \
    -H "X-API-KEY: ${ADMIN_KEY}" \
    "${ADMIN_URL}/apisix/admin/routes"; do
  echo "  APISIX not ready (retrying in 3s)..."
  sleep 3
done

echo "→ Registering routes..."

curl -sf -X PUT "${ADMIN_URL}/apisix/admin/routes/mcp" \
  -H "X-API-KEY: ${ADMIN_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "uri": "/mcp/*",
    "upstream": {
      "type": "roundrobin",
      "nodes": {"backend-mcp:11000": 1}
    }
  }'

curl -sf -X PUT "${ADMIN_URL}/apisix/admin/routes/data-sources" \
  -H "X-API-KEY: ${ADMIN_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "uris": ["/data-sources", "/data-sources/*"],
    "upstream": {
      "type": "roundrobin",
      "nodes": {"backend-enterprise-data-sources:9500": 1}
    }
  }'

echo "✓ APISIX routes registered."
