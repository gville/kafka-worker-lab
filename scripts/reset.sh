#!/usr/bin/env bash
# Reset the local lab to a clean baseline. Destructive: removes the Compose
# containers, the project network and the volumes declared in docker-compose.yml.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

USAGE="Usage: ./scripts/reset.sh [--help | --dry-run | --yes]"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

DRY_RUN=0
ASSUME_YES=0

for arg in "$@"; do
  case "$arg" in
    --help)
      cat <<EOF
$USAGE

Removes the Compose containers, the project network and the local Kafka and
PostgreSQL volumes declared in docker-compose.yml, including all lab data.
External volumes are not managed by this project and are not removed.
This operation is irreversible.

  --help     Show this help and exit.
  --dry-run  Preview the affected resources without removing anything.
  --yes      Skip the interactive confirmation (type 'yes' is otherwise required).

After a reset, recreate the baseline with: docker compose up -d
EOF
      exit 0
      ;;
    --dry-run) DRY_RUN=1 ;;
    --yes) ASSUME_YES=1 ;;
    *) fail "Unknown argument: $arg. $USAGE" ;;
  esac
done

command -v docker >/dev/null 2>&1 || fail "Docker CLI not found in PATH."
docker compose version >/dev/null 2>&1 || fail "docker compose is not available; the Compose plugin is required."
docker info >/dev/null 2>&1 || fail "Docker daemon is not reachable."
docker compose config --quiet || fail "Compose configuration in $REPO_ROOT is invalid; run 'docker compose config' to inspect it."

# config resolves COMPOSE_PROJECT_NAME, .env and any other project-name source.
PROJECT_NAME="$(docker compose config --format json 2>/dev/null | sed -n 's/^  "name": "\([^"]*\)".*/\1/p' | head -n 1 || true)"
[ -n "$PROJECT_NAME" ] || fail "Could not determine the effective Compose project name."

preview() {
  printf 'Compose project: %s\n' "$PROJECT_NAME"
  printf 'Containers (running and stopped):\n'
  docker compose ps -a --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}' || fail "Could not list Compose containers."
  printf 'Compose-managed volumes for project %s:\n' "$PROJECT_NAME"
  docker volume ls --filter "label=com.docker.compose.project=$PROJECT_NAME" --format 'table {{.Name}}' || fail "Could not list Compose-managed volumes."
  printf 'Compose-managed networks for project %s:\n' "$PROJECT_NAME"
  docker network ls --filter "label=com.docker.compose.project=$PROJECT_NAME" --format 'table {{.Name}}' || fail "Could not list Compose-managed networks."
  if docker compose down --help 2>/dev/null | grep -q -- '--dry-run'; then
    printf 'Exact Compose plan (dry run, no changes):\n'
    docker compose down --dry-run --volumes || fail "Compose could not produce a dry-run plan."
  else
    printf 'This Compose version cannot show an exact dry-run plan; use the listings above as the preview.\n'
  fi
  printf 'External volumes are not part of this Compose project and are not removed.\n'
}

preview

if [ "$DRY_RUN" -eq 1 ]; then
  printf 'Dry run only: no containers, volumes or data were removed.\n'
  exit 0
fi

if [ "$ASSUME_YES" -eq 0 ]; then
  [ -t 0 ] || fail "No interactive terminal is available. Pass --yes to confirm the reset explicitly."
  printf 'WARNING: this irreversibly removes the lab containers, network and local Kafka and PostgreSQL data.\n' >&2
  printf 'Type "yes" to continue: ' >&2
  if ! IFS= read -r answer; then
    fail "No confirmation received; nothing was removed."
  fi
  if [ "$answer" != "yes" ]; then
    printf 'Aborted: nothing was removed.\n' >&2
    exit 1
  fi
fi

set +e
OUTPUT="$(docker compose down --volumes 2>&1)"
RC=$?
set -e
if [ "$RC" -ne 0 ]; then
  printf '%s\n' "$OUTPUT" >&2
  printf 'ERROR: docker compose down --volumes failed with exit code %d; the reset is incomplete. Inspect the stack with "docker compose ps -a" before retrying.\n' "$RC" >&2
  exit "$RC"
fi
printf '%s\n' "$OUTPUT"

remaining_containers="$(docker compose ps -q --all 2>/dev/null || true)"
remaining_volumes="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT_NAME" 2>/dev/null || true)"
if [ -n "$remaining_containers" ] || [ -n "$remaining_volumes" ]; then
  [ -z "$remaining_containers" ] || printf 'Remaining containers: %s\n' "$remaining_containers" >&2
  [ -z "$remaining_volumes" ] || printf 'Remaining Compose-managed volumes: %s\n' "$remaining_volumes" >&2
  fail "Reset is incomplete: containers or Compose-managed volumes remain for project $PROJECT_NAME."
fi

printf 'OK: no containers remain for Compose project %s\n' "$PROJECT_NAME"
printf 'OK: no Compose-managed volumes remain for project %s\n' "$PROJECT_NAME"
printf 'Kafka lab reset completed. Recreate the baseline with: docker compose up -d\n'
printf 'External volumes are not managed by Compose and were not removed.\n'
