# Spec Delta

## Purpose

Simulates downstream notification delivery by consuming `PaymentApproved` and `PaymentRejected` events and emitting log-only notifications without affecting payment processing.

## ADDED Requirements

### Requirement: Consume payment outcomes
The system SHALL consume `PaymentApproved` and `PaymentRejected` events from `payments.events` using a consumer group dedicated to notifications.

#### Scenario: Outcome event consumed
- **WHEN** a `PaymentApproved` or `PaymentRejected` event becomes available
- **THEN** a Notification Worker receives it without competing with payment processing consumers

#### Scenario: Failed payment does not notify
- **WHEN** a payment is dead-lettered after exhausted retries and no approval or rejection event exists
- **THEN** no notification is emitted for it

### Requirement: Simulated notifications
The worker SHALL emit a simulated notification record appropriate to the payment outcome to its log and SHALL NOT depend on a real external notification provider or persisted notification records. `notification_db` SHALL be used exclusively for `once` fault records; no notification persistence and no persistent business-event deduplication SHALL be introduced.

#### Scenario: Approval notification
- **WHEN** a `PaymentApproved` event is consumed
- **THEN** a simulated approval notification is emitted for that payment

#### Scenario: Rejection notification
- **WHEN** a `PaymentRejected` event is consumed
- **THEN** a simulated rejection notification is emitted for that payment

### Requirement: In-process notification deduplication
Repeated delivery of the same outcome event within the lifetime of a running worker SHALL NOT emit duplicate notification records. No persistent deduplication SHALL be added.

#### Scenario: Duplicate outcome event
- **WHEN** the same outcome event is delivered more than once to a running worker
- **THEN** the worker emits a single notification record for that event

#### Scenario: Restart behavior is at-least-once
- **WHEN** a Notification Worker restarts and receives a previously processed event
- **THEN** it may emit the notification again because notifications are log-only and at-least-once

#### Scenario: Crash redelivery may re-emit
- **WHEN** a `crash-after-processing` fault causes Kafka to redeliver an outcome event after the worker restarts
- **THEN** the worker may emit the simulated notification again because the in-memory deduplication record was lost, and this is accepted in v1

### Requirement: Consumer isolation
A backlog or failure in notification processing SHALL NOT block payment processing or other consumer groups.

#### Scenario: Notifications fall behind
- **WHEN** notification processing is slow or stopped
- **THEN** payment processing and other consumers continue and notifications catch up after recovery

### Requirement: Slow processing fault
`slow-processing` SHALL be available to Notification Workers. When enabled, the worker SHALL apply a configurable delay with a default of 5 seconds before executing its notification processing logic. The delay SHALL NOT by itself fail processing, introduce an error or retry, or change offset-commit behavior, and processing SHALL continue normally after the delay.

#### Scenario: Delay precedes notification processing
- **WHEN** `slow-processing` is enabled and a Notification Worker processes an outcome event
- **THEN** the delay is applied before notification processing and the notification is emitted normally after the delay

#### Scenario: Default delay is five seconds
- **WHEN** `slow-processing` is enabled for a Notification Worker without an explicit duration
- **THEN** the injected delay is 5 seconds

#### Scenario: Delay causes no error or retry
- **WHEN** the injected delay for a Notification Worker completes
- **THEN** no fault-induced error or retry occurs and offset-commit behavior is unchanged

### Requirement: Once fault records
The Notification Worker SHALL persist `once` fault records in `notification_db`, identified by worker type, `faultId`, Kafka topic, partition, and offset. It SHALL register a record atomically immediately before that fault's injection point, and only the execution that succeeds in registering SHALL apply it. Records SHALL survive restarts and redeliveries and SHALL be retained for the life of the data with no automatic cleanup in v1.

#### Scenario: Register before applying
- **WHEN** the Notification Worker is about to apply a `once` fault
- **THEN** it registers the record in `notification_db` before executing the fault behavior

#### Scenario: Reapplication is skipped
- **WHEN** the same fault identity is redelivered after a restart or crash
- **THEN** the existing record causes the fault to be skipped

#### Scenario: Always faults are not recorded
- **WHEN** a fault runs in `always` mode
- **THEN** no `once` record is written

### Requirement: Fault records are separate from notification deduplication
`notification_db` SHALL be used only for `once` fault records. Notification deduplication SHALL remain in memory and SHALL NOT use `notification_db`.

#### Scenario: Duplicate detection remains in memory
- **WHEN** the same outcome event is delivered more than once to a running worker
- **THEN** deduplication is performed from in-memory state and `notification_db` is not used

#### Scenario: Fault records do not suppress notifications
- **WHEN** a `once` fault record exists for a message
- **THEN** it only prevents reapplying that fault and does not act as a business event deduplication record
