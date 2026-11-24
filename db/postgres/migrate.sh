#!/usr/bin/env bash
# Reusable SQL migration runner for the lab's PostgreSQL databases.
# Applies pending *.sql files in lexicographic order, each in its own
# transaction, and records applied versions in <MIGRATIONS_SCHEMA>.schema_migrations.
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required (example: postgres://user:pass@host:5432/db?sslmode=disable)}"
: "${MIGRATIONS_DIR:?MIGRATIONS_DIR is required}"

MIGRATIONS_SCHEMA="${MIGRATIONS_SCHEMA:-payment_api}"

if [[ ! -d "$MIGRATIONS_DIR" ]]; then
    printf 'MIGRATIONS_DIR does not exist: %s\n' "$MIGRATIONS_DIR" >&2
    exit 1
fi

tmp_sql="$(mktemp)"
trap 'rm -f "$tmp_sql"' EXIT

# Infrastructure bootstrap, not a migration: the registry must be queryable
# before any version has been applied. It is never recorded as a version.
# psql interpolates -v variables in stdin and -f input, but not in -c strings.
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -v schema="$MIGRATIONS_SCHEMA" <<'SQL'
CREATE SCHEMA IF NOT EXISTS :"schema" AUTHORIZATION CURRENT_USER;

CREATE TABLE IF NOT EXISTS :"schema".schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);
SQL

shopt -s nullglob
migration_files=( "$MIGRATIONS_DIR"/*.sql )
if (( ${#migration_files[@]} == 0 )); then
    printf 'No .sql migrations found in %s\n' "$MIGRATIONS_DIR"
    exit 0
fi

insert_sql="INSERT INTO :\"schema\".schema_migrations (version) VALUES (:'version');"

while IFS= read -r migration; do
    version="$(basename "$migration" .sql)"

    applied="$(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
        -v schema="$MIGRATIONS_SCHEMA" -v version="$version" -At <<'SQL'
SELECT 1 FROM :"schema".schema_migrations WHERE version = :'version';
SQL
)"

    if [[ "$applied" == "1" ]]; then
        printf 'skip %s (already applied)\n' "$version"
        continue
    fi

    printf 'apply %s\n' "$version"
    # One psql input file: BEGIN, migration body, registry INSERT and COMMIT.
    # With ON_ERROR_STOP, a failure aborts before COMMIT and the connection
    # closes, so neither the schema change nor the version row is persisted.
    {
        printf 'BEGIN;\n'
        cat "$migration"
        printf '\n%s\n' "$insert_sql"
        printf 'COMMIT;\n'
    } > "$tmp_sql"

    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
        -v schema="$MIGRATIONS_SCHEMA" -v version="$version" \
        -f "$tmp_sql"
done < <(printf '%s\n' "${migration_files[@]}" | LC_ALL=C sort)

printf 'migrations up to date\n'
