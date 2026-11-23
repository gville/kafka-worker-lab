#!/usr/bin/env bash
# Reproducible connectivity check for both Kafka listeners in kafka-worker-lab.
# - Compose service -> kafka:9093
# - Host Kafka CLI  -> localhost:9092
# - Published host port must be exactly 127.0.0.1:9092
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

INTERNAL_BOOTSTRAP="kafka:9093"
HOST_BOOTSTRAP="localhost:9092"
EXPECTED_KAFKA_BINDING="127.0.0.1:9092"
REQUIRED_TOPICS=(payments.events payments.dlq)
PORT_FORMAT='{{range $p, $conf := .NetworkSettings.Ports}}{{range $conf}}{{.HostIp}}:{{.HostPort}}{{"\n"}}{{end}}{{end}}'

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

# KAFKA_TOPICS_BIN overrides the host CLI. No host-specific path is stored here.
if [ -n "${KAFKA_TOPICS_BIN:-}" ]; then
  HOST_TOPICS_BIN="$KAFKA_TOPICS_BIN"
elif command -v kafka-topics.sh >/dev/null 2>&1; then
  HOST_TOPICS_BIN="$(command -v kafka-topics.sh)"
else
  fail "local Kafka CLI not found. Set KAFKA_TOPICS_BIN to kafka-topics.sh from Kafka 4.0.0 on this host, then rerun:
  KAFKA_TOPICS_BIN=/path/to/kafka-topics.sh ./kafka/check/connectivity.sh"
fi
command -v "$HOST_TOPICS_BIN" >/dev/null 2>&1 || fail "KAFKA_TOPICS_BIN is not executable: $HOST_TOPICS_BIN. Point it to kafka-topics.sh from Kafka 4.0.0."

command -v docker >/dev/null 2>&1 || fail "Docker CLI not found in PATH; start the local stack first."
docker compose version >/dev/null 2>&1 || fail "docker compose is not available; a recent Docker Compose plugin is required."

# Use the Compose service name, not a hardcoded container name.
KAFKA_CID="$(docker compose ps -q kafka 2>/dev/null || true)"
[ -n "$KAFKA_CID" ] || fail "Kafka Compose service is not running. Start the stack with: docker compose up -d"

set +e
INTERNAL_OUTPUT="$("$REPO_ROOT/kafka/check/integration.sh" 2>&1)"
INTERNAL_RC=$?
set -e
if [ "$INTERNAL_RC" -ne 0 ]; then
  printf '%s\n' "$INTERNAL_OUTPUT" >&2
  fail "Docker-based integration check failed; verify Kafka readiness through $INTERNAL_BOOTSTRAP."
fi
printf '%s\n' "$INTERNAL_OUTPUT"

set +e
HOST_OUTPUT="$("$HOST_TOPICS_BIN" --bootstrap-server "$HOST_BOOTSTRAP" --list 2>&1)"
HOST_RC=$?
set -e
if [ "$HOST_RC" -ne 0 ]; then
  printf '%s\n' "$HOST_OUTPUT" >&2
  fail "host connectivity check through $HOST_BOOTSTRAP failed; verify the broker is healthy, KAFKA_TOPICS_BIN is Kafka 4.0.0 and Java 17+ is active."
fi

assert_topics() {
  local label="$1" output="$2" topic
  for topic in "${REQUIRED_TOPICS[@]}"; do
    if ! printf '%s\n' "$output" | grep -Fxq -- "$topic"; then
      printf '%s\n' "$output" >&2
      fail "$label did not list required topic $topic"
    fi
  done
  printf 'OK: %s lists %s\n' "$label" "${REQUIRED_TOPICS[*]}"
}

assert_topics "host tooling ($HOST_BOOTSTRAP)" "$HOST_OUTPUT"

KAFKA_BINDINGS="$(docker inspect --format "$PORT_FORMAT" "$KAFKA_CID" | sed '/^[[:space:]]*$/d')"
if [ "$KAFKA_BINDINGS" != "$EXPECTED_KAFKA_BINDING" ]; then
  printf 'expected binding: %s\n' "$EXPECTED_KAFKA_BINDING" >&2
  printf 'actual bindings:\n%s\n' "$KAFKA_BINDINGS" >&2
  fail "Kafka host port is not published exclusively as $EXPECTED_KAFKA_BINDING"
fi
printf 'OK: Kafka host port binding is exactly %s\n' "$EXPECTED_KAFKA_BINDING"

while IFS= read -r cid; do
  [ -n "$cid" ] || continue
  bindings="$(docker inspect --format "$PORT_FORMAT" "$cid" | sed '/^[[:space:]]*$/d')"
  if printf '%s\n' "$bindings" | grep -Eq '^(0\.0\.0\.0|\[::\]|:::|\[0:0:0:0:0:0:0:0\]|0:0:0:0:0:0:0:0):'; then
    printf '%s\n' "$bindings" >&2
    fail "container $cid publishes a host port on all interfaces"
  fi
done < <(docker compose ps -q --all)
printf 'OK: no Compose container publishes a wildcard host port\n'

printf 'Kafka listener connectivity and port publication checks passed\n'
