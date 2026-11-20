# Spec Delta

## Purpose

Provides the reproducible local runtime, Kafka and PostgreSQL, that every other capability depends on, so the laboratory can be started, inspected, and reset on a single machine.

## ADDED Requirements

### Requirement: Local stack startup
The system SHALL provide a Docker Compose definition that starts a single-node Apache Kafka 4.x broker in KRaft mode and a PostgreSQL instance with no external managed services.

#### Scenario: Fresh local startup
- **WHEN** a learner runs the documented startup command on a machine with Docker
- **THEN** the Kafka broker and PostgreSQL instance start and report healthy

#### Scenario: Kafka operates in KRaft mode
- **WHEN** the Kafka broker starts
- **THEN** it runs Apache Kafka 4.x without ZooKeeper and accepts producer and consumer connections

### Requirement: Service connectivity
Kafka SHALL expose an external listener on `localhost:9092` for host-side tooling and an internal listener on `kafka:9093` for Compose services. PostgreSQL SHALL be reachable by Compose services through a stable network address and by host-side tooling through a documented local address.

#### Scenario: Container connectivity
- **WHEN** an application service starts inside the Compose network
- **THEN** it resolves and connects to Kafka through `kafka:9093` and to PostgreSQL using documented service names

#### Scenario: Host tooling connectivity
- **WHEN** a learner uses host-side Kafka or PostgreSQL tooling
- **THEN** Kafka tooling connects through `localhost:9092` and PostgreSQL tooling connects using the documented local address

### Requirement: Worker container restart policy
Each worker SHALL run as an independent process in its own container defined in `docker-compose.yml` with `restart: on-failure`. A simulated crash SHALL terminate the worker process with an error so Docker restarts the container. After restart, the worker SHALL reconnect to Kafka, and messages whose offsets were not committed SHALL be redelivered. The Workers API SHALL NOT implement a worker restart mechanism.

#### Scenario: Worker restarts after a simulated crash
- **WHEN** a simulated crash terminates a worker container with a non-zero exit
- **THEN** Docker restarts that container according to `restart: on-failure`

#### Scenario: Worker reconnects after restart
- **WHEN** the restarted worker process starts
- **THEN** it reconnects to Kafka and uncommitted messages are delivered again

#### Scenario: Restart is not managed by the control plane
- **WHEN** a crash fault stops a worker
- **THEN** restart is handled by Docker Compose and not by the Workers API

### Requirement: Data store initialization
PostgreSQL SHALL initialize exactly seven logical databases before dependent services start: `payments_db`, `payment_worker_db`, `provider_db`, `observability_db`, `analytics_db`, `notification_db`, and `workers_api_db`. Each SHALL have the required schema before its owning service starts. Repeated initialization SHALL NOT destroy existing data.

#### Scenario: First initialization
- **WHEN** the stack starts against an empty volume
- **THEN** exactly the seven agreed logical databases and their schema exist before their services begin processing

#### Scenario: No additional databases
- **WHEN** initialization completes
- **THEN** no logical database other than the seven agreed ones is created

#### Scenario: Restart preserves data
- **WHEN** the stack is restarted without removing volumes
- **THEN** existing data is retained and initialization completes without error

### Requirement: Database ownership
Each logical database SHALL have a single logical owning service: `payments_db` belongs to the Payment API, `payment_worker_db` to the Payment Worker, `provider_db` to the Simulated Payment Provider, `notification_db` to the Notification Worker, `analytics_db` to the Analytics Worker, `observability_db` to the Observability API, and `workers_api_db` to the Workers API. The Payment API owns the payments, idempotency, and outbox data in `payments_db`.

#### Scenario: Every database has its approved owner
- **WHEN** initialization completes
- **THEN** `payments_db` has the Payment API as its sole logical owner for payments, idempotency, and outbox data, and `payment_worker_db`, `provider_db`, `notification_db`, `analytics_db`, `observability_db`, and `workers_api_db` each exist with the owner listed in this requirement

#### Scenario: Outbox Publisher has restricted technical access
- **WHEN** the Outbox Publisher works with `payments_db`
- **THEN** it queries pending publications only in the outbox table and marks records published only after Kafka acknowledgement, without accessing the payments or idempotency tables

#### Scenario: Pending outbox count uses only the outbox table
- **WHEN** the Outbox Publisher counts records pending publication
- **THEN** it reads only the outbox table in `payments_db` and does not access payments or idempotency data

#### Scenario: Payments database has no other service access
- **WHEN** a service other than the Payment API or the Outbox Publisher needs payment data
- **THEN** it has no access to `payments_db` and uses the owning service's documented API or Kafka events

#### Scenario: Worker writes only its own database
- **WHEN** a worker persists a `once` fault record
- **THEN** it writes only to its own logical database and does not use a shared fault database

#### Scenario: Worker once records stay outside the Workers API database
- **WHEN** a worker registers a `once` fault record
- **THEN** the record is stored only in the worker's own logical database, not in `workers_api_db`, and is not retrieved from the Workers API

#### Scenario: Workers API stores only fault identity associations
- **WHEN** the Workers API receives a fault configuration with a `clientConfigKey`
- **THEN** it writes only the association between that `clientConfigKey` and the generated UUID v4 `faultId`, including its deletion status, to `workers_api_db` and keeps active configuration in memory

#### Scenario: Fault association uniqueness is atomic in PostgreSQL
- **WHEN** two concurrent Workers API requests create an association for the same `clientConfigKey` with no non-deleted association
- **THEN** PostgreSQL permits only one creation, and Workers API returns HTTP 409 Conflict for the loser without generating another `faultId` or modifying the winner's association

### Requirement: Database access isolation
Services SHALL NOT read or write another service's database except for the explicitly authorized `payments_db` outbox-table access. No other service SHALL have access to `payments_db`. `workers_api_db` SHALL be accessible only to the Workers API. `notification_db` SHALL store only `once` fault records and SHALL NOT be used for notification deduplication.

#### Scenario: No cross-service database access
- **WHEN** a service needs data owned by another service and no explicit table-level access is authorized
- **THEN** it uses that service's documented API or Kafka events and never queries the other database directly

#### Scenario: Workers API database is exclusive
- **WHEN** a service other than the Workers API needs `clientConfigKey`-to-`faultId` data
- **THEN** it has no access to `workers_api_db` and uses the Workers API contract instead

#### Scenario: Notification deduplication remains in memory
- **WHEN** the Notification Worker detects a duplicate outcome event
- **THEN** it uses in-memory deduplication and does not read or write `notification_db` for that purpose

### Requirement: Kafka topic initialization
The system SHALL disable automatic topic creation and SHALL create `payments.events` and `payments.dlq` explicitly before dependent consumers start. Topic creation SHALL be idempotent. Both topics SHALL use three partitions and replication factor 1. `payments.events` SHALL retain events for seven days, and `payments.dlq` SHALL retain events for 30 days.

#### Scenario: Event topic exists on fresh start
- **WHEN** the stack starts with an empty Kafka volume
- **THEN** `payments.events` exists with three partitions, replication factor 1, and seven-day retention

#### Scenario: Dead letter topic exists on fresh start
- **WHEN** the stack starts with an empty Kafka volume
- **THEN** `payments.dlq` exists with three partitions, replication factor 1, and 30-day retention

#### Scenario: Repeated initialization
- **WHEN** topic initialization runs against existing topics
- **THEN** it completes without deleting, duplicating, or changing the configured topics

#### Scenario: Automatic topic creation is disabled
- **WHEN** a producer references a topic that was not explicitly initialized
- **THEN** the broker does not create the topic automatically

### Requirement: Kafka producer durability settings
Kafka producers used by services SHALL set `acks=all` and `enable.idempotence=true`. The Kafka topics SHALL use `min.insync.replicas=1`.

#### Scenario: Producer waits for all in-sync replicas
- **WHEN** a service publishes a record to Kafka
- **THEN** the producer waits for acknowledgement from all in-sync replicas before treating the publish as successful

#### Scenario: Idempotent producer enabled
- **WHEN** a service configures its Kafka producer
- **THEN** `enable.idempotence=true` is enabled

#### Scenario: Minimum in-sync replicas configured
- **WHEN** the topics are initialized
- **THEN** `min.insync.replicas=1` is configured

### Requirement: Health and readiness
Every service SHALL expose `/health` and `/ready` endpoints, and readiness SHALL respect the availability of its dependencies. The Kafka health check SHALL be based on a broker metadata query. Dependent services SHALL avoid processing before the infrastructure is available.

#### Scenario: Kafka health uses broker metadata
- **WHEN** the Kafka health check runs
- **THEN** it queries broker metadata and reports the result

#### Scenario: Service health and readiness endpoints
- **WHEN** a service is running
- **THEN** `/health` reports liveness and `/ready` reports readiness according to its dependencies

#### Scenario: Dependency waits for readiness
- **WHEN** an application service starts before Kafka or PostgreSQL is ready
- **THEN** the service waits or retries until its dependencies report ready

#### Scenario: Unhealthy dependency is visible
- **WHEN** Kafka or PostgreSQL becomes unhealthy
- **THEN** the affected readiness endpoint reports not ready through its documented health surface

### Requirement: Structured JSON logs
Services SHALL emit structured JSON log records to stdout or stderr. This stage SHALL NOT require or include Loki, Grafana, or Alloy.

#### Scenario: Logs are structured JSON
- **WHEN** a service writes a log record
- **THEN** the record is emitted as JSON to stdout or stderr

#### Scenario: Outbox pending count uses the existing log surface
- **WHEN** the Outbox Publisher polls the outbox or completes a publish mark
- **THEN** it emits point-in-time pending-count snapshots in its existing structured JSON logs on every cycle, including retry cycles while Kafka is unavailable and immediately after each successful mark, without a new endpoint, metric, service, database, or configuration

#### Scenario: No external log stack is required
- **WHEN** the local stack starts and runs
- **THEN** it does not require or include Loki, Grafana, or Alloy

### Requirement: Local-only service access
The first local version SHALL be intended exclusively for local development, SHALL NOT implement or require service authentication or TLS, and SHALL NOT expose services to external networks or include production security mechanisms.

#### Scenario: No authentication or TLS
- **WHEN** the local services communicate with each other
- **THEN** they do not require service authentication or TLS

#### Scenario: No external exposure
- **WHEN** the local stack runs
- **THEN** no service is exposed to an external network

#### Scenario: No production security mechanisms
- **WHEN** the first local version runs
- **THEN** it does not require or include production security mechanisms

### Requirement: Local connectivity and port publication
Kafka and PostgreSQL SHALL be reachable from containers through the Compose network and, when needed, from the host through ports published exclusively on localhost. Kafka listeners MAY bind `0.0.0.0` inside the container, but host port publication SHALL remain limited to localhost.

#### Scenario: Host ports are localhost-only
- **WHEN** the local stack publishes Kafka or PostgreSQL ports to the host
- **THEN** those ports bind exclusively to localhost

#### Scenario: Containers use the internal network
- **WHEN** application services connect to Kafka or PostgreSQL
- **THEN** they use the Compose network addresses without authentication or TLS

#### Scenario: Container listener binding is not host exposure
- **WHEN** a Kafka listener binds `0.0.0.0` inside its container
- **THEN** host-published ports remain localhost-only and no service is exposed to an external network

### Requirement: Clean reset
The system SHALL provide a documented way to stop the stack and remove all local state so a learner can repeat experiments from a clean baseline.

#### Scenario: Reset to clean baseline
- **WHEN** a learner runs the documented reset command
- **THEN** containers, local data, and Kafka state are removed

#### Scenario: Restart after reset
- **WHEN** the stack is started after a reset
- **THEN** initialization recreates the clean baseline
