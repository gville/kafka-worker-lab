# Spec Delta

## Purpose

Accepts payment requests, enforces client idempotency, and atomically records payment state together with an outbox event for reliable downstream publication.

## ADDED Requirements

### Requirement: Payment creation
The system SHALL accept a payment creation request containing an amount, a currency, and an `Idempotency-Key`, persist a new payment, and return its identifier and initial status. A missing `Idempotency-Key` SHALL return HTTP 400 Bad Request without creating a payment. Invalid request content with an `Idempotency-Key` present SHALL return HTTP 422 Unprocessable Content without creating a payment.

#### Scenario: Valid payment request
- **WHEN** a client submits a valid amount, currency, and `Idempotency-Key`
- **THEN** the system persists a payment in its initial pending state and returns a stable payment identifier

#### Scenario: Missing idempotency key
- **WHEN** a client submits a payment request without an `Idempotency-Key`
- **THEN** the system returns HTTP 400 Bad Request for the missing required header and does not create a payment

#### Scenario: Invalid payment input
- **WHEN** a client submits invalid request content, such as an invalid amount or unsupported currency, with an `Idempotency-Key` present
- **THEN** the system returns HTTP 422 Unprocessable Content and does not create a payment

### Requirement: Global idempotency key scope
The system SHALL use one global idempotency-key namespace and SHALL NOT expire keys in v1.

#### Scenario: Same key across requests
- **WHEN** the same `Idempotency-Key` is used on requests from different clients
- **THEN** the system treats it as the same global key

#### Scenario: Key does not expire
- **WHEN** time passes after an idempotency key is recorded
- **THEN** the key remains valid for the lifetime of the stored data

### Requirement: Idempotent replay
For a repeated `Idempotency-Key` with the same request content, the system SHALL return the original payment and MUST NOT create another payment or another outbox event.

#### Scenario: Repeated identical request
- **WHEN** a client submits the same request with the same `Idempotency-Key` more than once
- **THEN** every response identifies the same payment and the system holds exactly one payment and one initial outbox event

### Requirement: Idempotency conflict
If the same `Idempotency-Key` is reused with different valid request content, the system SHALL return HTTP 409 Conflict and MUST NOT modify or create a payment.

#### Scenario: Same key, different payload
- **WHEN** a client reuses an `Idempotency-Key` with a different amount, currency, or other request content
- **THEN** the system returns HTTP 409 Conflict and the original payment remains unchanged

### Requirement: Concurrent idempotent requests
Concurrent requests that share an `Idempotency-Key` SHALL result in exactly one persisted payment and one consistent set of responses.

#### Scenario: Parallel duplicate submissions
- **WHEN** multiple requests with the same `Idempotency-Key` arrive concurrently
- **THEN** exactly one payment is created and all successful responses refer to that payment

### Requirement: Atomic transactional outbox
Persisting a payment, its idempotency record, and its corresponding outbox event SHALL occur in a single atomic PostgreSQL transaction.

#### Scenario: All records commit together
- **WHEN** a payment creation transaction commits
- **THEN** the payment record, its idempotency record, and its outbox event are durably stored

#### Scenario: Partial failure is rolled back
- **WHEN** writing the payment, the idempotency record, or the outbox event fails before commit
- **THEN** none of those records remains stored

### Requirement: Creation acknowledgement
A successful payment creation SHALL return HTTP 202 Accepted only after the atomic transaction has committed. The API SHALL NOT wait for Kafka acknowledgement or for outbox publication before responding. If Kafka is unavailable, the outbox event SHALL remain pending for the Outbox Publisher to publish later.

#### Scenario: Accepted after commit
- **WHEN** the payment creation transaction commits successfully
- **THEN** the API returns HTTP 202 Accepted with the payment identifier and initial pending status

#### Scenario: No Kafka confirmation required
- **WHEN** Kafka has not acknowledged the `PaymentRequested` event
- **THEN** the API still returns HTTP 202 Accepted after the transaction commits

#### Scenario: Kafka unavailable
- **WHEN** Kafka is unavailable during payment creation
- **THEN** the transaction still commits, the API returns HTTP 202 Accepted, and the outbox event remains pending for later publication

### Requirement: PaymentRequested outbox event
Each payment creation SHALL produce exactly one `PaymentRequested` outbox event using the approved envelope and containing the data required by the Payment Worker to perform a provider charge.

#### Scenario: Outbox event matches payment
- **WHEN** a payment is created successfully
- **THEN** its `PaymentRequested` event contains `eventId`, `eventType`, `schemaVersion`, `paymentId`, `correlationId`, `occurredAt`, `producer`, and the amount and currency in `data`

### Requirement: Initial payment status
The payment creation response SHALL report the new payment's initial pending status.

#### Scenario: Pending creation response
- **WHEN** a client creates a payment successfully
- **THEN** the response reports a pending status and the stable payment identifier

#### Scenario: Stored status remains the creation result
- **WHEN** a payment is created and later processed
- **THEN** the Payment API does not consume outcome events and does not update its stored payment status
