# kafka-worker-lab
A hands-on laboratory for exploring Apache Kafka, event-driven architecture, distributed workers, and fault-tolerant message processing.

## Kafka connection settings

The broker exposes two listeners. Use the address that matches where the client runs:

| Client location | Bootstrap server | Notes |
| --- | --- | --- |
| Inside Docker Compose | `kafka:9093` | Internal listener. Resolves only on the `kafka_worker_lab` Compose network; used by initialization jobs and application services. |
| On the host (macOS) | `localhost:9092` | External listener. Used by local Kafka 4.0.0 CLI tooling; the broker advertises `EXTERNAL://localhost:9092`. |

Both listeners use `PLAINTEXT`. This local version has no authentication and no TLS.

`kafka:9093` is not resolvable from the host. The external port is published only on loopback (`127.0.0.1:9092:9092`), never on all interfaces. A listener may bind `0.0.0.0` inside its container; that does not change the host publication.

### Verify connectivity

With the stack running (`docker compose up -d`), run:

```bash
./kafka/check/connectivity.sh
```

`connectivity.sh` checks the internal listener from a Compose service and then runs `kafka-topics.sh` **from the host** to verify the published `localhost:9092` listener. If `kafka-topics.sh` from Kafka 4.0.0 is already on `PATH`, no configuration is needed. If it is installed elsewhere, set `KAFKA_TOPICS_BIN` to the path of the executable, not its directory. The script does not add any host-specific path to the repository.

The Kafka 4.0.0 CLI requires Java 17 or newer. If the host's default Java is older, select a compatible JDK for the check by prefixing the variables to the command; no `export` or shell profile change is needed:

```bash
JAVA_HOME=/path/to/jdk-17 \
PATH=/path/to/jdk-17/bin:$PATH \
KAFKA_TOPICS_BIN=/path/to/kafka-topics.sh \
  ./kafka/check/connectivity.sh
```

If a compatible local CLI is already available, there is no need to install Kafka again on the host. The CLI inside the container cannot replace this check: it would only exercise the internal listener, not the `localhost:9092` listener published to the host.

It checks, in order:

1. a Compose service connects through `kafka:9093`;
2. host tooling connects through `localhost:9092`;
3. the Kafka host port is published exactly as `127.0.0.1:9092`, with no additional or wildcard binding.

### Wait for Kafka readiness

The Docker-based integration check waits for broker metadata on `kafka:9093` before listing the lab topics. It runs in a short-lived Compose container and does not need host networking:

```bash
./kafka/check/integration.sh
```

Set `KAFKA_READINESS_TIMEOUT_SECONDS` to change the 60-second bound, for example `KAFKA_READINESS_TIMEOUT_SECONDS=10 ./kafka/check/integration.sh`.

### Reset the lab

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

After a reset, recreate the clean baseline with `docker compose up -d`; initialization recreates the databases and topics from empty volumes. Stopping or restarting services (`docker compose stop`, `docker compose start`, `docker compose restart`) preserves all data and is not a reset.
