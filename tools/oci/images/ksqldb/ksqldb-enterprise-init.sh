#!/usr/bin/env bash
# One-shot init container: waits for ksqlDB, runs the enterprise stream
# definitions, then exits cleanly so Docker Compose can track completion
# via `condition: service_completed_successfully`.
#
# The ksql CLI exits 0 even when statements fail, so we inspect the output
# for error markers and retry. This handles the race between the ksqlDB REST
# API becoming healthy and its internal Kafka admin client being ready to
# create topics and write to the command topic.
set -euo pipefail

MAX_RETRIES="${KSQLDB_INIT_MAX_RETRIES:-18}"
RETRY_INTERVAL="${KSQLDB_INIT_RETRY_INTERVAL:-10}"

echo "⏳ Waiting for ksqlDB at ${KSQLDB_SERVER_URL}..."
until [ "$(curl -s -o /dev/null -w '%{http_code}' "${KSQLDB_SERVER_URL}/healthcheck")" = "200" ]; do
  echo "  ksqlDB not ready (retrying in 5s)..."
  sleep 5
done

echo "→ Creating enterprise filesystem streams..."

attempt=0
while true; do
  attempt=$((attempt + 1))
  output=$(/bin/ksql --file /home/appuser/artemis_enterprise_init.ksql -- "${KSQLDB_SERVER_URL}" 2>&1 || true)
  if echo "$output" | grep -qiE "Could not write|topic does not exist|Error:|FAILED"; then
    if [ "$attempt" -ge "$MAX_RETRIES" ]; then
      echo "✗ Failed to create enterprise ksqlDB streams after ${MAX_RETRIES} attempts."
      echo "$output"
      exit 1
    fi
    echo "  Attempt ${attempt}/${MAX_RETRIES} failed, retrying in ${RETRY_INTERVAL}s..."
    sleep "$RETRY_INTERVAL"
  else
    echo "✓ Enterprise ksqlDB streams created."
    exit 0
  fi
done