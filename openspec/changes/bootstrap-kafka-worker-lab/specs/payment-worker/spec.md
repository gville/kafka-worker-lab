# Spec Delta

## Purpose

Consumes `PaymentRequested` events and drives each payment through the simulated provider, applying idempotency, the fixed three-attempt retry policy, and outcome publication. It also performs dead-letter routing: processing failures exhausted after retries become terminal `FAILED` projections, while validation failures are dead-lettered directly for operational inspection without changing payment status.

## ADDED Requirements

### Requirement: Payment event consumption
The system SHALL consume `PaymentRequested` events from `payments.events` as a member of the `payment-worker` consumer group.

#### Scenario: Payment event is processed
- **WHEN** a `PaymentRequested` event becomes available
- **THEN** a Payment Worker in the consumer group receives and begins processing it

### Requirement: Provider charge invocation
The Payment Worker SHALL call the Simulated Payment Provider using `paymentId` as the idempotency key and SHALL record the charge outcome.

#### Scenario: Approved payment
- **WHEN** the provider approves the charge
- **THEN** the worker records an approved outcome for the payment

#### Scenario: Rejected payment
- **WHEN** the provider permanently rejects the charge
- **THEN** the worker records a rejected outcome with the provider reason

### Requirement: Fixed retry policy
The worker SHALL make at most three total attempts, waiting 1 second before attempt 2 and 2 seconds before attempt 3, without jitter. It SHALL retry timeout, HTTP 503, and uncertain-result outcomes, and SHALL NOT retry permanent rejection.

#### Scenario: Transient failure then success
- **WHEN** a charge fails with HTTP 503 and a later attempt succeeds
- **THEN** the payment completes with the successful outcome after the fixed backoff

#### Scenario: Timeout then success
- **WHEN** a charge attempt times out and a later attempt succeeds
- **THEN** the payment completes with the successful outcome

#### Scenario: Uncertain result then success
- **WHEN** a charge returns an uncertain result and a retry with the same `paymentId` returns the stored outcome
- **THEN** the payment completes with the stored outcome

#### Scenario: Permanent rejection is not retried
- **WHEN** the provider returns a permanent rejection
- **THEN** the worker records the rejection without further charge attempts

### Requirement: Idempotent processing
Repeated delivery of the same `PaymentRequested` event SHALL NOT create a second charge or a conflicting terminal outcome.

#### Scenario: Redelivered event
- **WHEN** the same payment event is delivered more than once
- **THEN** the worker calls the provider with the same `paymentId`, receives the original outcome, and does not create a second charge

#### Scenario: Redelivery after restart
- **WHEN** the worker restarts and receives a previously processed event
- **THEN** the provider's `paymentId` idempotency returns the original outcome and the payment is not charged twice

### Requirement: Forced processing error
When a `processing-error` fault is injected, the worker SHALL inject the error before calling the Simulated Payment Provider. In `always` mode it SHALL inject the error on every attempt; in `once` mode it SHALL inject it once per message so later attempts process normally. The approved three-attempt retry policy and `processing` DLQ path SHALL remain unchanged.

#### Scenario: Always mode fails every attempt
- **WHEN** `processing-error` is enabled in `always` mode and the worker processes an event
- **THEN** the error is injected on each attempt and the worker makes three total attempts with 1-second and 2-second backoff

#### Scenario: Forced error exhausts retries in always mode
- **WHEN** `processing-error` is enabled in `always` mode and all three attempts fail
- **THEN** the worker publishes `failureCategory=processing` with `attempts=3` to `payments.dlq`, publishes no outcome event, and commits the original offset only after broker acknowledgement

#### Scenario: Once mode fails one attempt
- **WHEN** `processing-error` is enabled in `once` mode and the first attempt fails because of the injected error
- **THEN** the next attempt proceeds with normal processing, the fault is not reapplied, and no fault-induced DLQ occurs

#### Scenario: Provider is not called before the forced error completes
- **WHEN** `processing-error` is injected for an event
- **THEN** the provider is not called before the forced error behavior completes

### Requirement: Forced processing delay
When a `slow-processing` fault is injected, the worker SHALL apply a configurable delay with a default of 5 seconds before calling the Simulated Payment Provider, and SHALL NOT call the provider until the delay completes. The worker SHALL NOT treat the delay as an error or trigger a retry. The delay SHALL NOT alter the fixed retry policy or offset-commit behavior. After the delay, processing SHALL continue normally.

#### Scenario: Delay does not trigger a retry
- **WHEN** `slow-processing` is injected while processing an event
- **THEN** processing is delayed, completes normally, and makes no additional attempt because of the delay

#### Scenario: Default delay is five seconds
- **WHEN** `slow-processing` is injected without an explicit duration
- **THEN** the delay is 5 seconds

#### Scenario: Delay duration is configurable
- **WHEN** `slow-processing` is injected with an explicit duration
- **THEN** the delay uses that configured duration

#### Scenario: Provider is not called before the delay completes
- **WHEN** `slow-processing` is injected while processing an event
- **THEN** the provider is not called until the delay completes

### Requirement: Once fault records
The Payment Worker SHALL persist `once` fault records in `payment_worker_db`, identified by worker type, `faultId`, Kafka topic, partition, and offset. It SHALL register a record atomically immediately before that fault's injection point, and only the execution that succeeds in registering SHALL apply the fault. Records SHALL survive restarts and redeliveries and SHALL be retained for the life of the data with no automatic cleanup in v1.

#### Scenario: Register before applying
- **WHEN** the Payment Worker is about to apply a `once` fault
- **THEN** it registers the record in `payment_worker_db` before executing the fault behavior

#### Scenario: Reapplication is skipped
- **WHEN** the same fault identity is redelivered after a restart or crash
- **THEN** the existing record causes the fault to be skipped

#### Scenario: Always faults are not recorded
- **WHEN** a fault runs in `always` mode
- **THEN** no `once` record is written

### Requirement: Crash fault redelivery
When `crash-before-processing` or `crash-after-processing` stops the worker before the offset is committed, Kafka SHALL redeliver the message after restart. Because the worker keeps no deduplication store, it SHALL avoid repeated persistent effects through provider idempotency by `paymentId` and the stable result `eventId`. No deduplication store SHALL be added to the Payment Worker.

#### Scenario: Once fault records are not business deduplication
- **WHEN** a `once` fault record exists in `payment_worker_db`
- **THEN** it only prevents reapplying that fault and is not used as a business event deduplication store

#### Scenario: Crash before processing
- **WHEN** `crash-before-processing` stops the worker before processing the event
- **THEN** the event is not processed, the offset is not committed, and redelivery begins processing normally

#### Scenario: Crash after processing
- **WHEN** `crash-after-processing` stops the worker after the provider call and result publication are complete but before the offset is committed
- **THEN** the persistent effects are complete, the offset is not committed, and redelivery reuses the same `paymentId` and result `eventId` without a second charge or conflicting outcome

#### Scenario: Worker restart is handled by the container runtime
- **WHEN** a simulated crash terminates the worker process with an error
- **THEN** Docker Compose restarts the container according to `restart: on-failure`, and the worker reconnects to Kafka

### Requirement: Accepted duplicate result publication
The Payment Worker SHALL be allowed to publish the same result event again with the same stable `eventId` in v1. Such duplicate publication SHALL be deduplicated downstream by Analytics and Observability.

#### Scenario: Duplicate result publication is accepted
- **WHEN** the worker crashes after publishing a result and before committing the offset, and redelivery publishes the same result again
- **THEN** the duplicate result carries the same stable `eventId`, Analytics and Observability deduplicate it, and no worker-side deduplication store is added

### Requirement: Result publication
After the provider returns an approve or reject decision, the worker SHALL publish a `PaymentApproved` or `PaymentRejected` event to `payments.events` with a stable `eventId` and the request's `correlationId`.

#### Scenario: Approved result published
- **WHEN** a payment is approved
- **THEN** a `PaymentApproved` event is published for downstream consumers

#### Scenario: Rejected result published
- **WHEN** a payment is permanently rejected
- **THEN** a `PaymentRejected` event is published for downstream consumers

#### Scenario: Result publication is at-least-once
- **WHEN** publishing a result event fails after processing
- **THEN** the worker retries publication with the same stable `eventId` and does not silently drop the outcome

### Requirement: Dead letter record contents
Every record published to `payments.dlq` SHALL include at least `attempts`, `lastError`, `failedAt`, and the original event. It SHALL include the original event's `sourceTopic`, `sourcePartition`, and `sourceOffset` when those coordinates are available. The original event SHALL include `eventId` and `paymentId` when those values are available.

#### Scenario: Processing failure record
- **WHEN** a payment request is dead-lettered after exhausted processing attempts
- **THEN** the record contains `attempts=3`, `lastError`, `failedAt`, the original event with `eventId` and `paymentId` when available, and its source coordinates when available

#### Scenario: Validation failure record
- **WHEN** an invalid or version-incompatible event is dead-lettered
- **THEN** the record contains `attempts=0`, `lastError`, `failedAt`, the original event with `eventId` and `paymentId` when available, and its source coordinates when available

#### Scenario: Source coordinates captured for fallback identity
- **WHEN** a dead-letter record is published and the original event's Kafka coordinates are available
- **THEN** the record includes `sourceTopic`, `sourcePartition`, and `sourceOffset` so the consumer can apply the approved fallback identity

### Requirement: Attempts definition
`attempts` SHALL represent the number of processing attempts performed by the Payment Worker, regardless of whether an individual attempt reaches or invokes the Simulated Payment Provider.

#### Scenario: Injected error before the provider call counts
- **WHEN** a processing attempt fails because of an injected `processing-error` before the provider is called
- **THEN** that attempt counts toward `attempts`

#### Scenario: Provider invocation counts
- **WHEN** a processing attempt reaches and invokes the provider
- **THEN** it counts as one processing attempt and no separate provider-call counter is recorded

### Requirement: Attempts counting
Every processing attempt SHALL count, including one where `processing-error` stops processing before the provider call. A processing failure that exhausts the three total attempts SHALL record `attempts=3` even if the provider was never invoked. A validation event dead-lettered directly SHALL record `attempts=0`. No separate provider-call counter and no additional event field SHALL be introduced.

#### Scenario: Exhausted injected errors record three attempts
- **WHEN** all three attempts fail because of the injected `processing-error` and the provider is never invoked
- **THEN** the `failureCategory=processing` record contains `attempts=3`

#### Scenario: Validation records zero attempts
- **WHEN** an invalid or version-incompatible event is dead-lettered directly
- **THEN** the record contains `attempts=0`

### Requirement: Optional failure category
`failureCategory` MAY be present in a dead-letter record. When present, it SHALL be either `processing` or `validation`.

#### Scenario: Processing record includes category
- **WHEN** a payment request is dead-lettered after exhausted processing attempts
- **THEN** the record contains `failureCategory=processing`

#### Scenario: Validation record includes category
- **WHEN** an invalid or version-incompatible event is dead-lettered
- **THEN** the record contains `failureCategory=validation`

### Requirement: Processing failures after exhausted retries
When processing still fails after three total attempts, the worker SHALL publish a dead-letter record with `failureCategory=processing`, SHALL NOT publish a `PaymentApproved` or `PaymentRejected` event, and SHALL stop retrying. This category SHALL be the only dead-letter category that signals terminal status `FAILED`.

#### Scenario: Exhausted retries
- **WHEN** a payment event fails all three processing attempts
- **THEN** the worker publishes a `failureCategory=processing` record to `payments.dlq`, publishes no approval or rejection event, and does not process it again

#### Scenario: Failed is not a provider rejection
- **WHEN** a payment event is dead-lettered with `failureCategory=processing`
- **THEN** no `PaymentRejected` event is produced and the payment is distinct from a provider business rejection

### Requirement: Validation failures
When a consumed event is invalid or incompatible with the expected schema version, the worker SHALL publish a dead-letter record with `failureCategory=validation` directly, without retries and without calling the provider. A validation record SHALL remain available for operational inspection but SHALL NOT signal a `FAILED` payment status transition and SHALL NOT produce an approval or rejection event.

#### Scenario: Invalid event
- **WHEN** a consumed event is invalid
- **THEN** the worker publishes it to `payments.dlq` with `failureCategory=validation` without retrying or calling the provider

#### Scenario: Incompatible schema version
- **WHEN** a consumed event is incompatible with the expected schema version
- **THEN** the worker publishes it to `payments.dlq` with `failureCategory=validation` without retrying or calling the provider

#### Scenario: Validation failure does not mark payment failed
- **WHEN** a validation dead-letter record is produced
- **THEN** no `FAILED` status transition is signaled and no approval or rejection event is published

### Requirement: Dead letter isolation
A dead-lettered event SHALL NOT prevent subsequent payment events from being processed.

#### Scenario: Poison message is isolated
- **WHEN** an event cannot be processed and reaches `payments.dlq`
- **THEN** subsequent payment events continue to be processed

### Requirement: Offset commitment safety
The worker SHALL commit the original event's consumer offset only after the outcome is handled or, for a dead-lettered event, only after receiving broker acknowledgement that the DLQ record was published.

#### Scenario: DLQ publication is not acknowledged
- **WHEN** DLQ publication is not acknowledged by the broker
- **THEN** the worker does not commit the original offset and the event remains eligible for reprocessing

#### Scenario: Duplicate DLQ record after crash
- **WHEN** the worker publishes a DLQ record, crashes before committing the original offset, and restarts
- **THEN** the event may be dead-lettered again, producing a duplicate DLQ record under the at-least-once model

#### Scenario: Crash before completion
- **WHEN** the worker crashes before finishing an event
- **THEN** the event is delivered again after restart and processing resumes
