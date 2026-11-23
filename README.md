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
KAFKA_TOPICS_BIN=/path/to/kafka-topics.sh ./kafka/check/connectivity.sh
```

`KAFKA_TOPICS_BIN` must point to `kafka-topics.sh` from Kafka 4.0.0 installed on the host. If that command is already on `PATH`, the variable may be omitted. The script does not add any host-specific path to the repository. Kafka 4.0.0 requires Java 17 or newer; set `JAVA_HOME` and `PATH` accordingly if the CLI reports `UnsupportedClassVersionError`.

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
