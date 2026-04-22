#!/usr/bin/env bash
# One-shot init container: waits for ksqlDB, runs the enterprise stream
# definitions, then exits cleanly so Docker Compose can track completion
# via `condition: service_completed_successfully`.
set -euo pipefail

echo "⏳ Waiting for ksqlDB at ${KSQLDB_SERVER_URL}..."
until [ "$(curl -s -o /dev/null -w '%{http_code}' "${KSQLDB_SERVER_URL}/healthcheck")" = "200" ]; do
  echo "  ksqlDB not ready (retrying in 5s)..."
  sleep 5
done

echo "→ Creating enterprise filesystem streams..."
/bin/ksql --file /home/appuser/artemis_enterprise_init.ksql -- "${KSQLDB_SERVER_URL}"
echo "✓ Enterprise ksqlDB streams created."