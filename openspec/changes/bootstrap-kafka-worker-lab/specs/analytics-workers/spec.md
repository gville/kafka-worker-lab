# Spec Delta

## Purpose

Consumes `PaymentRequested`, `PaymentApproved`, and `PaymentRejected` events to compute and persist business metrics in `analytics_db` that demonstrate stream processing and aggregation in the laboratory.

## ADDED Requirements

### Requirement: Consume payment lifecycle events
The system SHALL consume `PaymentRequested`, `PaymentApproved`, and `PaymentRejected` events from `payments.events` using a consumer group dedicated to analytics.

#### Scenario: Lifecycle events consumed
- **WHEN** any payment lifecycle event becomes available
- **THEN** Analytics Workers consume it independently of other consumer groups

### Requirement: Requested payments
Every `PaymentRequested` event SHALL increment `payments_requested_total`. Pending, failed, approved, and rejected payments SHALL all count as requested.

#### Scenario: Requested payment counted
- **WHEN** a `PaymentRequested` event is consumed
- **THEN** `payments_requested_total` increases by one

#### Scenario: Pending and failed remain requested only
- **WHEN** a requested payment has no approved or rejected outcome
- **THEN** it remains counted only in `payments_requested_total`

### Requirement: Outcome metrics
A `PaymentApproved` event SHALL increment `payments_approved_total` and add its amount to `payments_approved_amount_total` for that currency. A `PaymentRejected` event SHALL increment `payments_rejected_total`. Pending and failed payments SHALL NOT increment either counter or the approved amount. No separate failed counter SHALL be added.

#### Scenario: Approved payment
- **WHEN** a `PaymentApproved` event is consumed
- **THEN** `payments_approved_total` increases by one and its amount is added to the matching currency total

#### Scenario: Rejected payment
- **WHEN** a `PaymentRejected` event is consumed
- **THEN** `payments_rejected_total` increases by one and the approved amount remains unchanged

#### Scenario: No failed counter
- **WHEN** a payment failed processing and no approved or rejected event exists
- **THEN** only `payments_requested_total` reflects it and no failed counter is added

### Requirement: Cumulative metrics and currency separation
The approved counters SHALL be cumulative and SHALL NOT use time windows in v1. `payments_approved_amount_total` SHALL be maintained separately by currency with no conversion between currencies.

#### Scenario: Currencies remain separate
- **WHEN** approved payments use different currencies
- **THEN** each currency has its own approved amount total and no conversion is applied

### Requirement: Derived approval rate
The worker SHALL expose `approval_rate` calculated as `payments_approved_total / (payments_approved_total + payments_rejected_total) * 100`. Pending and failed payments SHALL be excluded. When `payments_approved_total + payments_rejected_total == 0`, the value SHALL be `null`, meaning insufficient information rather than 0%. The rate SHALL be derived from existing counters and SHALL NOT be persisted as an additional metric.

#### Scenario: Approval rate uses definitive outcomes
- **WHEN** approved and rejected counts are available
- **THEN** the approval rate uses only those two counters

#### Scenario: Pending and failed are excluded
- **WHEN** payments are pending or failed
- **THEN** they do not contribute to either the numerator or denominator of the approval rate

#### Scenario: No definitive outcomes
- **WHEN** `payments_approved_total` and `payments_rejected_total` are both zero
- **THEN** `approval_rate` is `null` and is not reported as 0%

### Requirement: Idempotent aggregation
The worker SHALL deduplicate consumed events by `eventId`. Recording that an event was processed and updating the affected metrics SHALL occur in a single PostgreSQL transaction in `analytics_db`. The Kafka offset SHALL be committed only after that transaction commits. Repeated delivery of the same event SHALL NOT double-count it in any metric.

#### Scenario: Duplicate event does not double count
- **WHEN** the same payment lifecycle event is delivered more than once
- **THEN** the affected metrics reflect the event exactly once

#### Scenario: Processed record and metrics commit together
- **WHEN** an event is processed successfully
- **THEN** its processed-event record and the corresponding metric updates commit in the same transaction

#### Scenario: Transaction failure leaves the offset uncommitted
- **WHEN** the deduplication record or metric update fails before commit
- **THEN** neither change is persisted and the Kafka offset is not committed

#### Scenario: Offset commits after the transaction
- **WHEN** the deduplication record and metric updates commit successfully
- **THEN** the worker commits the Kafka offset, and a later redelivery of the same `eventId` does not increment the metrics again

### Requirement: Metric persistence
The worker SHALL persist the approved counters in `analytics_db`, and they SHALL survive worker restarts. The derived approval rate SHALL NOT be persisted as a separate metric.

#### Scenario: Restart preserves counters
- **WHEN** an Analytics Worker restarts
- **THEN** previously computed counters remain available, the approval rate is derived again, and processing resumes from committed offsets

### Requirement: Metric exposure
The Analytics Worker's documented read endpoint SHALL return the persisted counters and the derived approval rate, which SHALL be `null` when no definitive outcomes exist.

#### Scenario: Query current metrics
- **WHEN** a client requests analytics metrics through the Analytics Worker's read endpoint
- **THEN** the endpoint returns the current counters and the derived approval rate, or `null` when there are no approved or rejected payments

### Requirement: Processing isolation
A backlog or failure in analytics processing SHALL NOT block payment processing or other consumer groups.

#### Scenario: Analytics falls behind
- **WHEN** analytics processing is slow or stopped
- **THEN** payment processing and other consumers continue and analytics catches up after recovery

### Requirement: Slow processing fault
`slow-processing` SHALL be available to Analytics Workers. When enabled, the worker SHALL apply a configurable delay with a default of 5 seconds before executing its analytics processing logic. The delay SHALL NOT by itself fail processing, alter the transactional metric update, or change offset-commit behavior, and processing SHALL continue normally after the delay.

#### Scenario: Delay precedes analytics processing
- **WHEN** `slow-processing` is enabled and an Analytics Worker consumes a lifecycle event
- **THEN** the delay is applied before the metric update and the event is processed normally after the delay

#### Scenario: Default delay is five seconds
- **WHEN** `slow-processing` is enabled for an Analytics Worker without an explicit duration
- **THEN** the injected delay is 5 seconds

#### Scenario: Delay causes no error or retry
- **WHEN** the injected delay for an Analytics Worker completes
- **THEN** no fault-induced error or retry occurs, the metric update semantics are unchanged, and the offset is still committed only after the transaction commits

### Requirement: Once fault records
The Analytics Worker SHALL persist `once` fault records in `analytics_db`, identified by worker type, `faultId`, Kafka topic, partition, and offset. It SHALL register a record atomically immediately before that fault's injection point, and only the execution that succeeds in registering SHALL apply it. Records SHALL survive restarts and redeliveries and SHALL be retained for the life of the data with no automatic cleanup in v1.

#### Scenario: Register before applying
- **WHEN** the Analytics Worker is about to apply a `once` fault
- **THEN** it registers the record in `analytics_db` before executing the fault behavior

#### Scenario: Reapplication is skipped
- **WHEN** the same fault identity is redelivered after a restart or crash
- **THEN** the existing record causes the fault to be skipped

#### Scenario: No additional Analytics database
- **WHEN** the Analytics Worker persists `once` fault records
- **THEN** it uses `analytics_db` and no additional database is created

#### Scenario: Always faults are not recorded
- **WHEN** a fault runs in `always` mode
- **THEN** no `once` record is written

#### Scenario: Fault records are separate from event deduplication
- **WHEN** a `once` fault record exists for a message
- **THEN** it only prevents reapplying that fault and does not replace or change `eventId` deduplication
