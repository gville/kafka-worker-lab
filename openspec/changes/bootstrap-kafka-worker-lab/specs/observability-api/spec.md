# Spec Delta

## Purpose

Owns the payment read projection and event history in `observability_db`, including processing-failure statuses and operational dead-letter inspection, exposes read-only HTTP payment, consumer-group, and operational query views, and assigns projection, history, operational dead-letter record, and `once` record writes to internal consumers so learners can understand and debug the laboratory.

## ADDED Requirements

### Requirement: Payment read projection
The Observability internal consumers SHALL maintain the payment read projection and ordered event history in `observability_db` by consuming `PaymentRequested`, `PaymentApproved`, and `PaymentRejected` events from `payments.events` and dead-letter records from `payments.dlq`. Once the projection reaches a terminal status, no later or delayed lifecycle event SHALL overwrite it.

#### Scenario: Requested event creates a pending projection
- **WHEN** a `PaymentRequested` event is consumed
- **THEN** the projection contains the payment with status `PENDING`

#### Scenario: Approved event updates the projection
- **WHEN** a `PaymentApproved` event is consumed for an existing payment
- **THEN** the projection status becomes `APPROVED`

#### Scenario: Rejected event updates the projection
- **WHEN** a `PaymentRejected` event is consumed for an existing payment
- **THEN** the projection status becomes `REJECTED`

#### Scenario: Processing dead letter updates the projection
- **WHEN** a dead-letter record with `failureCategory=processing` is consumed for a payment
- **THEN** the projection status becomes terminal `FAILED`

#### Scenario: Validation dead letter does not update the projection
- **WHEN** a dead-letter record with `failureCategory=validation` is consumed for a payment
- **THEN** the projection status is unchanged

#### Scenario: Duplicate event does not duplicate history
- **WHEN** the same lifecycle event is consumed more than once
- **THEN** the projection reflects it once and the history contains a single entry for it

#### Scenario: Terminal status is not regressed
- **WHEN** a terminal lifecycle event or a `processing` dead-letter record is applied and a delayed `PaymentRequested` event arrives later
- **THEN** the projection retains `APPROVED`, `REJECTED`, or `FAILED` and does not return to `PENDING`

#### Scenario: Delayed PaymentApproved does not overwrite FAILED
- **WHEN** the projection reached terminal `FAILED` and a `PaymentApproved` event arrives later
- **THEN** the projection retains `FAILED` and does not change to `APPROVED`

#### Scenario: Delayed PaymentRejected does not overwrite FAILED
- **WHEN** the projection reached terminal `FAILED` and a `PaymentRejected` event arrives later
- **THEN** the projection retains `FAILED` and does not change to `REJECTED`

### Requirement: Failed terminal status
`FAILED` SHALL mean processing could not complete after retries were exhausted and the request was sent to `payments.dlq`. It SHALL be set only when a dead-letter record with `failureCategory=processing` is consumed. It SHALL remain distinct from `REJECTED`, which means the provider explicitly rejected the payment for business reasons.

#### Scenario: Failed is reported as failed
- **WHEN** a payment has a `processing` dead-letter record after exhausted retries
- **THEN** the API reports its status as `FAILED` and not `REJECTED`

#### Scenario: Validation record does not set failed
- **WHEN** a payment has only a `validation` dead-letter record
- **THEN** the API does not set its status to `FAILED`

#### Scenario: Rejected is a business outcome
- **WHEN** a payment has a `PaymentRejected` event from the provider
- **THEN** the API reports its status as `REJECTED` and not `FAILED`

### Requirement: Dead letter identity
The API SHALL compute a dead-letter identity from `eventId` and `failureCategory` when `eventId` is present. When `eventId` is absent, it SHALL use reliable original Kafka coordinates (`sourceTopic`, `sourcePartition`, `sourceOffset`) plus `failureCategory`. A missing `failureCategory` SHALL be treated as `unknown`, including when `eventId` is also absent.

#### Scenario: Missing event identifier uses Kafka coordinates
- **WHEN** the original event lacks `eventId` and the same dead-letter record is consumed more than once with the same `sourceTopic`, `sourcePartition`, `sourceOffset`, and `failureCategory`
- **THEN** the API treats both records as the same failure and suppresses duplicate history, status, and operational entries

#### Scenario: Missing failure category uses unknown
- **WHEN** a dead-letter record lacks `failureCategory` and is consumed more than once with the same `eventId` or the same reliable original Kafka coordinates
- **THEN** the API uses `unknown` for the missing category and suppresses the duplicate

### Requirement: Idempotent dead letter processing
The API SHALL suppress duplicate dead-letter publications that share an identity. A duplicate SHALL NOT create duplicate history entries, reapply a status transition, or create duplicate operational dead-letter entries.

#### Scenario: Duplicate processing record
- **WHEN** the same `processing` dead-letter record is consumed more than once with the same `eventId` and `failureCategory`
- **THEN** the projection remains `FAILED`, the history contains one failure entry, and the operational view contains one record

#### Scenario: Duplicate validation record
- **WHEN** the same `validation` dead-letter record is consumed more than once with the same `eventId` and `failureCategory`
- **THEN** the projection status remains unchanged, the history does not gain duplicate entries, and the operational view contains one record

### Requirement: Dead letter recovery limit
When neither `eventId` nor reliable original Kafka coordinates are available, the API SHALL NOT claim identification or deduplication and SHALL NOT introduce additional recovery mechanisms or artificial identifiers. The design does not guarantee automatic recovery from every imaginable failure.

#### Scenario: Identity cannot be recovered
- **WHEN** a dead-letter record lacks `eventId` and reliable original Kafka coordinates cannot be recovered
- **THEN** the API does not claim identification or deduplication and creates no artificial identifier

### Requirement: Unidentifiable dead letter handling
For a dead-letter record whose identity cannot be recovered, the API SHALL emit a critical error log, SHALL NOT incorporate the record into the projection, and SHALL commit the offset so processing continues with later messages. The original record SHALL remain in Kafka subject to the configured retention.

#### Scenario: Record is skipped and processing continues
- **WHEN** a dead-letter record has no recoverable identity
- **THEN** the API emits a critical error log, omits the record from the projection, commits the offset, continues with later messages, and leaves the original record in Kafka subject to retention

### Requirement: Payment lookup
The API SHALL return a payment's projected status and core details for a requested payment identifier.

#### Scenario: Existing payment
- **WHEN** a client requests an existing payment identifier
- **THEN** the API returns its projected status, amount, currency, and relevant timestamps

#### Scenario: Unknown payment
- **WHEN** a client requests a payment identifier that does not exist
- **THEN** the API returns a not-found response

### Requirement: Payment listing
The API SHALL list projected payments with documented filtering and pagination.

#### Scenario: Filtered listing
- **WHEN** a client requests payments filtered by status
- **THEN** the API returns only matching payments with pagination information

### Requirement: Payment event history
The API SHALL return the ordered lifecycle event history for a requested payment.

#### Scenario: Payment timeline
- **WHEN** a client requests the history of a processed payment
- **THEN** the API returns its lifecycle events in chronological order with event type and outcome information

#### Scenario: Failed payment timeline
- **WHEN** a client requests the history of a payment with a `processing` dead-letter record
- **THEN** the API returns its lifecycle events including a `FAILED` failure entry

#### Scenario: Validation failure timeline
- **WHEN** a client requests the history of a payment with only a `validation` dead-letter record
- **THEN** the API does not add a `FAILED` entry and the projected status is unchanged

### Requirement: Consumer group information
The API SHALL report Kafka consumer groups with their members, committed offsets, and per-partition lag.

#### Scenario: Consumer progress
- **WHEN** a client requests consumer-group information
- **THEN** the API returns the groups, members, and lag for `payments.events` and `payments.dlq`

### Requirement: Operational information
The API SHALL report operational state including topic availability, dead letter counts, worker heartbeat status, and active fault scenarios.

#### Scenario: Operational summary
- **WHEN** a client requests operational information
- **THEN** the API returns the current infrastructure, dead letter, worker, and fault-injection state

#### Scenario: Pending outbox count stays outside operational queries
- **WHEN** a client requests operational information
- **THEN** the API reports its existing Kafka, dead-letter, worker, and fault sources, does not read `payments_db`, and does not report the pending outbox count, which remains in the Outbox Publisher's structured JSON logs

### Requirement: Dead letter category inspection
The API SHALL expose consumed `payments.dlq` records for read-only operational inspection and SHALL identify each by `failureCategory`, using `unknown` when it is absent. Each inspected record SHALL expose at least `failureCategory`, `attempts`, `lastError`, `failedAt`, and the original event, including `eventId`, `paymentId`, `sourceTopic`, `sourcePartition`, and `sourceOffset` when available. A `validation` record SHALL be inspectable without changing the payment projection status.

#### Scenario: Inspect validation record
- **WHEN** a client inspects a dead-letter record with `failureCategory=validation`
- **THEN** the API returns its category, attempts, last error, failure time, and original event identifiers, and the payment projection status is unchanged

#### Scenario: Inspect processing record
- **WHEN** a client inspects a dead-letter record with `failureCategory=processing`
- **THEN** the API returns its category, attempts, last error, failure time, and original event identifiers

#### Scenario: Missing failure category
- **WHEN** a dead-letter record has no `failureCategory`
- **THEN** the API reports its category as `unknown`

#### Scenario: Filter by failure category
- **WHEN** a client filters dead-letter records by `failureCategory`
- **THEN** the API returns only records matching the requested category

### Requirement: Consumer write ownership
Observability's internal Kafka consumers SHALL own all writes to `observability_db`, limited to projection state, ordered history, required `once` fault records, and operational dead-letter records. HTTP query endpoints SHALL NOT perform these writes.

#### Scenario: Internal consumer updates Observability data
- **WHEN** an Observability internal consumer processes a `payments.events` or `payments.dlq` record
- **THEN** it updates only the required projection state, history, `once` record, or operational dead-letter record in `observability_db`

#### Scenario: HTTP queries do not write
- **WHEN** a client uses an Observability HTTP query endpoint
- **THEN** the endpoint performs no projection, history, `once`, or operational dead-letter write

### Requirement: Projection-only reads
The API's HTTP query endpoints SHALL answer payment and history queries from `observability_db` and MUST NOT read `payments_db`, `payment_worker_db`, `provider_db`, `analytics_db`, `notification_db`, or `workers_api_db`.

#### Scenario: Payment query uses the projection
- **WHEN** a payment or history query is served
- **THEN** its payment data comes from `observability_db`

#### Scenario: No cross-service SQL
- **WHEN** a payment or history query is served
- **THEN** it does not query another service's database

### Requirement: Read-only HTTP query behavior
The API's HTTP query endpoints SHALL NOT change payment, event, worker, projection, history, operational dead-letter, `once`, or infrastructure state. Internal consumer writes to `observability_db` are outside this constraint.

#### Scenario: State remains unchanged by queries
- **WHEN** a client uses any observability HTTP query endpoint
- **THEN** no payment, event, worker, projection, history, operational dead-letter, `once`, or infrastructure state is modified

#### Scenario: Consumer writes are internal
- **WHEN** an internal Observability consumer updates projection, history, operational dead-letter, or `once` state in `observability_db`
- **THEN** the update is part of event consumption and is not performed by an HTTP query endpoint

### Requirement: Partial degradation
If one data source is unavailable, the API SHALL return the information it can obtain and identify the unavailable source instead of failing the entire response.

#### Scenario: One source unavailable
- **WHEN** one underlying data source cannot be reached
- **THEN** the API returns available information and marks the affected section as unavailable

#### Scenario: Projection unavailable
- **WHEN** `observability_db` cannot be reached
- **THEN** payment and history sections are marked unavailable and the API does not fall back to another service's database

### Requirement: Slow processing fault for projection consumption
`slow-processing` SHALL be available to the Observability consumers of `payments.events` and `payments.dlq`. When enabled, the projection consumer SHALL apply a configurable delay with a default of 5 seconds before executing its projection or dead-letter processing logic. The delay SHALL NOT by itself fail processing or change offset-commit behavior, and processing SHALL continue normally after the delay.

#### Scenario: Delay precedes event projection
- **WHEN** `slow-processing` is enabled and a lifecycle event is consumed
- **THEN** the delay is applied before the projection update and the event is applied normally after the delay

#### Scenario: Delay precedes dead-letter processing
- **WHEN** `slow-processing` is enabled and a dead-letter record is consumed
- **THEN** the delay is applied before dead-letter handling and the projection outcome is applied normally after the delay

#### Scenario: Default delay is five seconds
- **WHEN** `slow-processing` is enabled for an Observability consumer without an explicit duration
- **THEN** the injected delay is 5 seconds

#### Scenario: Delay causes no error or retry
- **WHEN** the injected delay for an Observability consumer completes
- **THEN** no fault-induced error or retry occurs and offset-commit behavior is unchanged

### Requirement: Once fault records
The Observability projection consumer SHALL persist `once` fault records in `observability_db`, identified by worker type, `faultId`, Kafka topic, partition, and offset. It SHALL register a record atomically immediately before that fault's injection point, and only the execution that succeeds in registering SHALL apply it. Records SHALL survive restarts and redeliveries and SHALL be retained for the life of the data with no automatic cleanup in v1.

#### Scenario: Register before applying
- **WHEN** the projection consumer is about to apply a `once` fault
- **THEN** it registers the record in `observability_db` before executing the fault behavior

#### Scenario: Reapplication is skipped
- **WHEN** the same fault identity is redelivered after a restart or crash
- **THEN** the existing record causes the fault to be skipped

#### Scenario: Always faults are not recorded
- **WHEN** a fault runs in `always` mode
- **THEN** no `once` record is written

#### Scenario: Fault records are separate from projection deduplication
- **WHEN** a `once` fault record exists for a message
- **THEN** it only prevents reapplying that fault and does not replace or change `eventId` or dead-letter deduplication in the projection
