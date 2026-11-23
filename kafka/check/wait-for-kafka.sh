#!/usr/bin/env bash
# Runs inside the Compose network. Waits for broker metadata before connecting.
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9093}"
METADATA_BIN="/opt/kafka/bin/kafka-metadata-quorum.sh"
ATTEMPT_TIMEOUT_SECONDS=5
RETRY_INTERVAL_SECONDS=1
READINESS_TIMEOUT_SECONDS="${KAFKA_READINESS_TIMEOUT_SECONDS:-60}"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

command -v timeout >/dev/null 2>&1 || fail "timeout is required in the Kafka image but was not found."
if ! [[ "$READINESS_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [ "$READINESS_TIMEOUT_SECONDS" -lt 1 ]; then
  fail "KAFKA_READINESS_TIMEOUT_SECONDS must be a positive integer."
fi

now_ms() {
  date +%s%3N
}

start_ms="$(now_ms)"
total_ms=$((READINESS_TIMEOUT_SECONDS * 1000))
attempt=0
last_error="(no attempt completed)"

while :; do
  elapsed_ms=$(( $(now_ms) - start_ms ))
  remaining_ms=$((total_ms - elapsed_ms))
  if [ "$remaining_ms" -le 0 ]; then
    break
  fi

  attempt=$((attempt + 1))
  attempt_timeout_ms=$((ATTEMPT_TIMEOUT_SECONDS * 1000))
  if [ "$remaining_ms" -lt "$attempt_timeout_ms" ]; then
    attempt_timeout_ms="$remaining_ms"
  fi
  attempt_timeout="$(printf '%d.%03d' $((attempt_timeout_ms / 1000)) $((attempt_timeout_ms % 1000)))"

  set +e
  output="$(timeout "$attempt_timeout" "$METADATA_BIN" \
    --bootstrap-server "$BOOTSTRAP_SERVERS" describe --status 2>&1)"
  rc=$?
  set -e

  if [ "$rc" -eq 0 ] && [ -n "$output" ]; then
    printf 'OK: Kafka metadata available through %s after %d attempt(s) in %ds\n' \
      "$BOOTSTRAP_SERVERS" "$attempt" "$((elapsed_ms / 1000))" >&2
    exit 0
  fi

  if [ "$rc" -ne 0 ]; then
    last_error="$(printf '%s\n' "$output" | grep -E -m1 '(Exception|ERROR|Error|Timed out|timed out|Failed|Unable)' || true)"
    if [ -z "$last_error" ]; then
      last_error="$(printf '%s\n' "$output" | head -n 1)"
    fi
  else
    last_error="empty metadata response"
  fi

  # Do not sleep past the total deadline.
  remaining_ms=$((total_ms - ($(now_ms) - start_ms)))
  if [ "$remaining_ms" -le 0 ]; then
    break
  fi
  sleep_ms=$((RETRY_INTERVAL_SECONDS * 1000))
  if [ "$remaining_ms" -lt "$sleep_ms" ]; then
    sleep_ms="$remaining_ms"
  fi
  sleep_seconds="$(printf '%d.%03d' $((sleep_ms / 1000)) $((sleep_ms % 1000)))"
  if [ $((sleep_ms % 1000)) -eq 0 ]; then
    sleep_label="$((sleep_ms / 1000))"
  else
    sleep_label="$sleep_seconds"
  fi
  printf 'attempt %d: broker metadata unavailable; retrying in %ss\n' \
    "$attempt" "$sleep_label" >&2
  sleep "$sleep_seconds"
done

printf 'ERROR: Kafka did not become ready through %s within %ds (%d attempts); last error: %s\n' \
  "$BOOTSTRAP_SERVERS" "$READINESS_TIMEOUT_SECONDS" "$attempt" "$last_error" >&2
exit 1
