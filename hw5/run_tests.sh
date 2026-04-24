#!/bin/bash
# Run the integration test suite.
# Brings up all services, waits for them to be healthy, runs tests, then tears down.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> Building and starting services..."
docker compose up --build -d kafka schema-registry clickhouse producer

echo "==> Waiting for producer to be healthy..."
until docker compose exec -T producer curl -sf http://localhost:8000/health > /dev/null 2>&1; do
  echo "    Producer not ready yet..."
  sleep 3
done

echo "==> Running integration tests..."
docker compose run --rm --build tests

EXIT_CODE=$?

echo "==> Tearing down services..."
docker compose down

exit $EXIT_CODE
