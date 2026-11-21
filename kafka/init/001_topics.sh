#!/usr/bin/env bash
# Idempotent Kafka topic bootstrap for kafka-worker-lab.
# Creates the two lab topics when absent, then verifies their exact settings.
# Existing topics are never altered: any mismatch is reported and fails the run.
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9093}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

TOPICS=(
  "payments.events|3|1|604800000"
  "payments.dlq|3|1|2592000000"
)

failures=0

for spec in "${TOPICS[@]}"; do
  IFS='|' read -r topic partitions replication_factor retention_ms <<< "$spec"

  # --if-not-exists creates only absent topics. It never updates an existing
  # topic, so an incorrect existing configuration is left untouched and is
  # detected by the verification below.
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$replication_factor" \
    --config "min.insync.replicas=1" \
    --config "retention.ms=$retention_ms"

  header=$("$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --describe --topic "$topic" | sed -n '1p')

  if [ -z "$header" ]; then
    echo "ERROR: no metadata returned for topic $topic" >&2
    failures=$((failures + 1))
    continue
  fi

  actual_partitions=$(printf '%s\n' "$header" | sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p')
  actual_rf=$(printf '%s\n' "$header" | sed -n 's/.*ReplicationFactor: \([0-9][0-9]*\).*/\1/p')
  actual_configs=$(printf '%s\n' "$header" | sed -n 's/.*Configs:[[:space:]]*//p' | tr -d '[:space:]')
  normalized_configs=",${actual_configs},"

  mismatch=""
  if [ "$actual_partitions" != "$partitions" ]; then
    mismatch="${mismatch} partitions=${actual_partitions:-missing} (expected ${partitions});"
  fi
  if [ "$actual_rf" != "$replication_factor" ]; then
    mismatch="${mismatch} replication-factor=${actual_rf:-missing} (expected ${replication_factor});"
  fi
  case "$normalized_configs" in
    *",min.insync.replicas=1,"*) ;;
    *) mismatch="${mismatch} min.insync.replicas=1 missing (configs=${actual_configs:-none});" ;;
  esac
  case "$normalized_configs" in
    *",retention.ms=${retention_ms},"*) ;;
    *) mismatch="${mismatch} retention.ms expected ${retention_ms} (configs=${actual_configs:-none});" ;;
  esac

  if [ -n "$mismatch" ]; then
    echo "ERROR: topic $topic has unexpected configuration:${mismatch}" >&2
    echo "  actual: $header" >&2
    failures=$((failures + 1))
  else
    echo "OK: $topic verified (partitions=${partitions}, replication-factor=${replication_factor}, min.insync.replicas=1, retention.ms=${retention_ms})"
  fi
done

if [ "$failures" -gt 0 ]; then
  echo "ERROR: ${failures} topic(s) failed verification; existing topics were not modified" >&2
  exit 1
fi

echo "Kafka topic initialization and verification completed successfully"
