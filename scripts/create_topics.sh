#!/usr/bin/env bash
# Creates the pipeline topics with explicit partition counts and retention.
# Idempotent: safe to re-run (uses --if-not-exists).
#
# Runs INSIDE the kafka container:
#   docker compose exec kafka bash /scripts/create_topics.sh
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

# Dev profile: single broker, so replication factor must be 1.
# Production profile (M6) overrides this to 3 with min.insync.replicas=2.
RF="${RF:-1}"

# topic:partitions:retention_ms
TOPICS=(
  "raw-logs:6:86400000"            # 24h  — unprocessed ingested events
  "parsed-logs:6:604800000"        # 7d   — structured, enriched events
  "error-logs:3:1209600000"        # 14d  — status >= 500 or level=ERROR
  "dead-letter-queue:1:2592000000" # 30d  — failed parsing/validation
  "service-metrics-1m:1:259200000" # 3d   — ksqlDB windowed aggregates
)

echo "Creating topics on ${BOOTSTRAP} (replication factor ${RF})"
echo

for entry in "${TOPICS[@]}"; do
  IFS=':' read -r name partitions retention <<< "$entry"
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$RF" \
    --config "retention.ms=$retention" \
    --config "cleanup.policy=delete"
  echo "  ok  ${name}  partitions=${partitions}  retention=${retention}ms"
done

echo
echo "Current topics:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --list | sed 's/^/  /'
