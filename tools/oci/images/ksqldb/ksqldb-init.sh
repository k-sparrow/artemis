#!/usr/bin/env bash
# One-shot init container: waits for ksqlDB, runs the ingestion stream
# definitions, then exits cleanly so Docker Compose can track completion
# via `condition: service_completed_successfully`.
set -euo pipefail

echo "⏳ Waiting for ksqlDB at ${KSQLDB_SERVER_URL}..."
until [ "$(curl -s -o /dev/null -w '%{http_code}' "${KSQLDB_SERVER_URL}/healthcheck")" = "200" ]; do
  echo "  ksqlDB not ready (retrying in 5s)..."
  sleep 5
done

echo "→ Creating ingestion streams..."
/bin/ksql --file /home/appuser/artemis_init.ksql -- "${KSQLDB_SERVER_URL}"
echo "✓ Ingestion ksqlDB streams created."
