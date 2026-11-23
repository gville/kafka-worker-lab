#!/usr/bin/env bash
# Docker-based integration check: wait for broker metadata, then connect through
# the internal listener and assert the lab topics exist.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

REQUIRED_TOPICS=(payments.events payments.dlq)
SERVICE="kafka-init"
CHECK_MOUNT="$REPO_ROOT/kafka/check:/kafka-check:ro"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "Docker CLI not found in PATH."
docker compose version >/dev/null 2>&1 || fail "docker compose is not available."

# --all also lists a stopped broker, so a stopped Kafka produces a readiness
# timeout instead of a generic "stack unavailable" error.
if [ -z "$(docker compose ps -q --all 2>/dev/null || true)" ]; then
  fail "Compose stack is not available in $REPO_ROOT; run docker compose up -d first."
fi

set +e
OUTPUT="$(docker compose run --rm --no-deps \
  -v "$CHECK_MOUNT" \
  -e "KAFKA_READINESS_TIMEOUT_SECONDS=${KAFKA_READINESS_TIMEOUT_SECONDS:-60}" \
  --entrypoint bash "$SERVICE" \
  -c '/kafka-check/wait-for-kafka.sh && /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS:-kafka:9093}" --list' 2>&1)"
RC=$?
set -e

if [ "$RC" -ne 0 ]; then
  printf '%s\n' "$OUTPUT" >&2
  fail "Docker-based Kafka integration check failed."
fi

for topic in "${REQUIRED_TOPICS[@]}"; do
  if ! printf '%s\n' "$OUTPUT" | grep -Fxq -- "$topic"; then
    printf '%s\n' "$OUTPUT" >&2
    fail "kafka:9093 did not list required topic $topic"
  fi
done

printf '%s\n' "$OUTPUT" | grep -E '^(OK: Kafka metadata|attempt )' >&2 || true
printf 'OK: Compose service (kafka:9093) lists %s\n' "${REQUIRED_TOPICS[*]}"
printf 'Kafka integration check passed\n'
