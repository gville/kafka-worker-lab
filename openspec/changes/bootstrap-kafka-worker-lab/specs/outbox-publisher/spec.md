# Spec Delta

## Purpose

Reliably transfers committed outbox records to Kafka so downstream workers receive payment events even when publishing fails or the publisher process crashes.

## ADDED Requirements

### Requirement: Publish pending outbox records
The system SHALL continuously read unpublished outbox records and publish them as `PaymentRequested` events to the `payments.events` topic.

#### Scenario: Pending record is published
- **WHEN** an unpublished outbox record exists
- **THEN** the publisher sends the corresponding `PaymentRequested` event to `payments.events` and the event becomes available to consumers

### Requirement: Mark after acknowledgement
The system SHALL mark an outbox record as published only after Kafka acknowledges the publish.

#### Scenario: Acknowledgement before marking
- **WHEN** the Kafka broker acknowledges the event
- **THEN** the outbox record is marked as published and is not selected again in normal operation

#### Scenario: No acknowledgement
- **WHEN** the Kafka broker does not acknowledge the event
- **THEN** the outbox record remains unpublished and eligible for retry

### Requirement: Crash safety
If the publisher stops after Kafka accepts an event but before the outbox record is marked, the event SHALL be republished after restart, and the system SHALL NOT lose the event.

#### Scenario: Crash between publish and mark
- **WHEN** the publisher crashes after a successful publish but before marking the record
- **THEN** after restart it publishes the record again and the original event is not lost

### Requirement: Per-payment ordering
Events belonging to the same payment SHALL be published in the order they were recorded, using `paymentId` as the consistent partition key.

#### Scenario: Order preserved across generations
- **WHEN** multiple outbox records for one payment become eligible
- **THEN** consumers observe them in recording order

### Requirement: Single publisher instance
v1 SHALL run exactly one Outbox Publisher instance. The publisher SHALL publish pending records sequentially in insertion order. Coordination and concurrency across multiple publisher instances SHALL NOT be implemented in v1 and are deferred.

#### Scenario: Sequential publication
- **WHEN** multiple unpublished outbox records exist
- **THEN** the single publisher instance publishes them one at a time in insertion order and marks each after acknowledgement

#### Scenario: No multi-instance concurrency
- **WHEN** v1 runs the Outbox Publisher
- **THEN** only one instance is deployed and no cross-instance coordination or concurrent publication is attempted

### Requirement: Broker unavailability handling
While Kafka is unavailable, the publisher SHALL retain pending records, retry with backoff, and report pending work through its existing structured JSON logging surface.

#### Scenario: Kafka unavailable
- **WHEN** the Kafka broker is unreachable
- **THEN** no outbox record is marked published, pending records are retried after the broker returns, and structured JSON logs report a snapshot of the pending outbox records on each retry cycle

### Requirement: Pending outbox visibility
The publisher SHALL report the number of unpublished outbox records through the existing structured JSON logging surface. The count SHALL be derived only from the `payments_db` outbox table using the publisher's authorized access. No new database, service, HTTP endpoint, metric, or configuration SHALL be introduced for this indicator.

#### Scenario: Pending count is read from the outbox table
- **WHEN** the publisher reports outstanding work
- **THEN** the count is obtained by querying only the outbox table in `payments_db`, not the payments or idempotency tables

#### Scenario: Pending count while Kafka is unavailable
- **WHEN** Kafka is unavailable and outbox records remain unpublished
- **THEN** the reported snapshots reflect those pending records and the records stay eligible for retry

#### Scenario: Pending count decreases after Kafka recovers
- **WHEN** Kafka becomes available and the pending records are published and successfully marked in `payments_db`
- **THEN** the snapshot emitted after the successful mark reflects the reduced number of unpublished records

#### Scenario: Existing logging surface is reused
- **WHEN** a learner consults the pending outbox indicator
- **THEN** the snapshots are available in the publisher's structured JSON logs without an additional HTTP endpoint, metric, or configuration

### Requirement: Pending count emission cadence
The publisher SHALL emit a point-in-time snapshot of the unpublished count on every polling cycle, including retry cycles while Kafka is unavailable, and immediately after a record is successfully marked published in `payments_db`. Emission SHALL occur per cycle even when the count is unchanged, and no special emission SHALL be added to detect a Kafka outage.

#### Scenario: Snapshot emitted on every polling cycle
- **WHEN** the publisher completes a polling cycle over the outbox table
- **THEN** it emits a snapshot of the unpublished count in its structured JSON logs

#### Scenario: Snapshot emitted during retry cycles
- **WHEN** Kafka is unavailable and the publisher performs a retry cycle
- **THEN** it emits a snapshot of the pending count on that retry cycle without a separate outage-detection emission

#### Scenario: Snapshot emitted after the publish mark
- **WHEN** a record is successfully marked published in `payments_db`
- **THEN** the publisher emits a snapshot immediately after that mark, not merely after the Kafka acknowledgement

#### Scenario: Unchanged count is still emitted
- **WHEN** a polling cycle observes the same unpublished count as the previous cycle
- **THEN** the publisher still emits a snapshot and keeps no state for remembering the previous count

### Requirement: Pending count snapshot semantics
Each emitted entry SHALL represent a point-in-time snapshot of the unpublished rows in the outbox table. The most recent log entry SHALL reflect the state observed through the last completed polling cycle or successful publish mark. No real-time guarantee or maximum freshness in seconds SHALL be claimed; the polling interval remains configurable and no new value SHALL be fixed.

#### Scenario: Latest entry reflects last completed operation
- **WHEN** a learner reads the most recent pending-count entry
- **THEN** it reflects the state observed at the last completed polling cycle or successful publish mark

#### Scenario: No real-time or seconds-based guarantee
- **WHEN** a learner relies on the pending-count indicator
- **THEN** no real-time update or maximum freshness in seconds is guaranteed and the configurable polling interval has no newly fixed value

### Requirement: Duplicate tolerance
The publisher MAY publish the same event more than once, and downstream consumers SHALL tolerate duplicate delivery without incorrect outcomes.

#### Scenario: Duplicate publication
- **WHEN** the same outbox event is published more than once
- **THEN** downstream processing does not produce incorrect duplicate outcomes
