# kafka-worker-lab
A hands-on laboratory for exploring Apache Kafka, event-driven architecture, distributed workers, and fault-tolerant message processing.

## Prerequisites

Run all commands from the repository root.

- Docker Engine with the Compose plugin (`docker compose version`).
- `./kafka/check/connectivity.sh` also needs Kafka 4.0.0 CLI (`kafka-topics.sh`) on the host `PATH` and Java 17 or newer. If they are installed elsewhere, see the template in [Verify the stack](#verify-the-stack).
- `./kafka/check/integration.sh` runs in a short-lived Compose container and needs neither Kafka CLI nor Java on the host.

## Startup

```bash
docker compose up -d
docker compose ps -a
```

Expected state:

| Service | Expected status |
| --- | --- |
| `kafka` | `Up (healthy)` |
| `postgres` | `Up (healthy)` |
| `kafka-init` | `Exited (0)` |
| `payments-migrations` | `Exited (0)` |

`kafka-init` and `payments-migrations` are one-shot initialization jobs: `Exited (0)` means they finished successfully. Plain `docker compose ps` does not list them because they are no longer running. To inspect their work:

```bash
docker compose logs kafka-init
docker compose logs payments-migrations
```

## Addresses and connections

| Service | From the host | From Compose |
| --- | --- | --- |
| Kafka | `localhost:9092` | `kafka:9093` |
| PostgreSQL | `localhost:5432` | `postgres:5432` |

- Both Kafka listeners use `PLAINTEXT`. This local version has no authentication and no TLS.
- The Compose network is `kafka_worker_lab`. `kafka:9093` resolves only inside that network, not from the host.
- Host ports are published exclusively on loopback (`127.0.0.1:9092` and `127.0.0.1:5432`), never on all interfaces. A listener may bind `0.0.0.0` inside its container; that does not change the host publication.
- PostgreSQL uses user `lab_admin`, password `lab_password` and initial database `postgres`. This password is a fixed credential of the local lab only: do not reuse it in other environments or consider it suitable for production.
- Initialization creates seven lab databases (`payments_db`, `payment_worker_db`, `provider_db`, `observability_db`, `analytics_db`, `notification_db`, `workers_api_db`) in addition to the `postgres` maintenance database.

## Verify the stack

### Internal check (recommended when the host has no CLI)

```bash
./kafka/check/integration.sh
```

It waits for broker metadata on `kafka:9093`, then lists the lab topics from a short-lived Compose container. It needs no host networking, Kafka CLI or Java installation. Set `KAFKA_READINESS_TIMEOUT_SECONDS` to change the 60-second bound, for example `KAFKA_READINESS_TIMEOUT_SECONDS=10 ./kafka/check/integration.sh`.

### Host check

With the stack running:

```bash
./kafka/check/connectivity.sh
```

This requires the Kafka 4.0.0 CLI on the host `PATH` and Java 17 or newer; see [Prerequisites](#prerequisites). It checks, in order:

1. a Compose service connects through `kafka:9093`;
2. host tooling connects through `localhost:9092`;
3. the Kafka host port is published exactly as `127.0.0.1:9092`, with no additional or wildcard binding.

The CLI inside the container cannot replace this check: it would only exercise the internal listener, not the `localhost:9092` listener published to the host.

If `kafka-topics.sh` is not on `PATH` or the default Java is older than 17, prefix the variables. This is a **PLANTILLA**: replace the three example paths with your local installation before running it.

```bash
# PLANTILLA: reemplazar las rutas por las de la instalación local.
JAVA_HOME=/path/to/jdk-17 \
PATH=/path/to/jdk-17/bin:$PATH \
KAFKA_TOPICS_BIN=/path/to/kafka-topics.sh \
  ./kafka/check/connectivity.sh
```

If a compatible local CLI is already available, there is no need to install Kafka again on the host.

### PostgreSQL checks

Optional checks to confirm that PostgreSQL is accepting connections and that the lab databases were created:

```bash
docker compose exec postgres pg_isready -U lab_admin -d postgres
docker compose exec postgres psql -U lab_admin -d postgres -Atc "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY 1"
```

Both commands run inside the `postgres` container, so they do not require `psql` installed on the host. The query lists the seven lab databases plus the `postgres` maintenance database.

### Database migrations

`docker compose up -d` runs the `payments-migrations` one-shot service after PostgreSQL reports healthy. It applies the versioned SQL files in `db/postgres/migrations/payments_db/` in lexicographic order. To rerun the same pending-migration check on demand (safe to repeat):

```bash
docker compose run --rm payments-migrations
```

The runner is `db/postgres/migrate.sh`; it uses `psql` and adds no Go dependency. Its inputs:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | required | Full PostgreSQL connection URL for the target database and owner role. Never printed by the runner. |
| `MIGRATIONS_DIR` | required | Directory containing the ordered `*.sql` files. |
| `MIGRATIONS_SCHEMA` | `payment_api` | Schema holding `schema_migrations` and the migrated objects. |

Host-side example (requires `psql` on the host and the published localhost port):

```bash
DATABASE_URL='postgres://payment_api:lab_password@localhost:5432/payments_db?sslmode=disable' \
MIGRATIONS_DIR=db/postgres/migrations/payments_db \
  ./db/postgres/migrate.sh
```

Behavior: the runner creates `payment_api.schema_migrations` with `CREATE TABLE IF NOT EXISTS` before reading it, so a fresh database needs no bootstrap migration and the initialization itself is never recorded as a version. Each pending file runs as `BEGIN; <file>; INSERT INTO schema_migrations; COMMIT;` in one `psql` session; with `ON_ERROR_STOP`, a failure rolls back both the schema change and the version row and stops the run. Applied versions are skipped. There is no concurrent-run lock in v1, so run a single migration job at a time.

Outbox ordering: the single sequential publisher reads pending rows ordered by `id`. That `id` reflects the identity sequence's assignment order, not the global commit order of concurrent transactions; a strict global order remains to be evaluated in 3.3. These migrations introduce no advisory lock and do not change publication logic.

## Stop, restart and reset

`docker compose stop`, `docker compose start` and `docker compose restart` preserve the named volumes and all data; they are not a reset.

The reset is destructive and irreversible. It removes the Compose containers and the project network, and deletes the entire local `postgres_data` volume with all its contents: the seven lab databases, any other database in the cluster, and all roles, schemas and data. It also deletes the entire `kafka_data` volume, including Kafka state such as topics, offsets and consumer groups. Docker images and resources outside this Compose project are kept; Compose does not remove external volumes.

Preview what would be removed without changing anything:

```bash
./scripts/reset.sh --dry-run
```

Run with interactive confirmation (the literal `yes` is required):

```bash
./scripts/reset.sh
```

For non-interactive use:

```bash
./scripts/reset.sh --yes
```

After a reset, recreate the clean baseline with `docker compose up -d`; initialization recreates the databases and topics from empty volumes.

## Troubleshooting

| Symptom | Likely cause | Diagnostic command | Recommended action |
| --- | --- | --- | --- |
| `docker compose ps` does not show `kafka-init` | One-shot job already finished | `docker compose ps -a` | `Exited (0)` is expected. For another exit code: `docker compose logs kafka-init`. |
| `kafka` is `starting` or `unhealthy` | Broker metadata not available yet or startup failed | `docker compose logs --tail=50` | Wait for the health check (30 s start period, 10 s interval, 12 retries); inspect logs; check that `127.0.0.1:9092` is free. |
| `./kafka/check/integration.sh` times out | Broker stopped or not ready | `docker compose ps -a`, `docker compose logs kafka` | Run `docker compose up -d`, inspect logs, or raise `KAFKA_READINESS_TIMEOUT_SECONDS` for slow starts. |
| `connectivity.sh` reports `local Kafka CLI not found` | `kafka-topics.sh` is not on `PATH` | `command -v kafka-topics.sh` | Set `KAFKA_TOPICS_BIN` to the Kafka 4.0.0 executable (template above) or use `integration.sh` only. |
| `UnsupportedClassVersionError` from the host CLI | Host Java older than 17 | `java -version` | Prefix `JAVA_HOME` and `PATH` with a JDK 17+ using the template above. |
| Host CLI cannot reach `localhost:9092` | Broker unhealthy or port published incorrectly | `docker compose ps`, `./kafka/check/connectivity.sh` | The port must read `127.0.0.1:9092->9092/tcp`; the check fails on wildcard or extra bindings. Do not publish on `0.0.0.0`. |
| `postgres` is `unhealthy` or unreachable from the host | Docker/Compose down, port busy or init failed | `docker compose ps`, `docker compose logs postgres`, `docker compose exec postgres pg_isready -U lab_admin -d postgres` | Confirm `127.0.0.1:5432->5432/tcp`; wait or restart; inspect the PostgreSQL service logs with `docker compose logs postgres`, not the `kafka-init` logs. |
| `payments-migrations` exited non-zero | A migration failed; that version was rolled back | `docker compose logs payments-migrations`, `docker compose exec postgres psql -U lab_admin -d payments_db -c "SELECT version, applied_at FROM payment_api.schema_migrations ORDER BY version"` | Fix the SQL and rerun `docker compose run --rm payments-migrations`. Do not edit an already applied version; add a new ordered file instead. |
| Lab databases are missing | Connected to another instance or an already initialized volume | `docker compose exec postgres psql -U lab_admin -d postgres -Atc "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY 1"` | Verify the `kafka-worker-lab-postgres` container; `postgres` is the maintenance database, not an eighth lab database. PostgreSQL initialization scripts run only when the data volume is initialized for the first time with an empty data directory. If the volume already existed before those scripts were added or changed, PostgreSQL does not run them again automatically. To rebuild from scratch, use the documented [reset procedure](#stop-restart-and-reset); it is destructive and deletes all persisted data. |
| Old data or topics are still present | `stop`/`start`/`restart` keep volumes; no reset was run | `./scripts/reset.sh --dry-run` | Do not confuse this with a restart: the reset is destructive and irreversible and deletes the Kafka and PostgreSQL volumes. Preview first with `--dry-run`; only if losing all data is intended, run `./scripts/reset.sh` and then `docker compose up -d`. |
| `docker` or `docker compose` command not found | Docker or the Compose plugin is missing | `command -v docker`, `docker compose version` | Install or enable Docker Engine and the Compose plugin first. |
