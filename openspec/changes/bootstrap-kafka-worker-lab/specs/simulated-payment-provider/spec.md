# Spec Delta

## Purpose

Stands in for an external payment processor so learners can deterministically reproduce the agreed normal, slow, timeout, HTTP 503, permanent-rejection, and uncertain-result behaviors with globally selected scenarios and idempotent charge processing keyed by `paymentId`.

## ADDED Requirements

### Requirement: Charge operation
The provider SHALL accept a charge request containing a `paymentId`, amount, and currency, and return a charge outcome or a typed failure. The charge operation SHALL be exposed exclusively at `POST /charge`.

#### Scenario: Charge request path
- **WHEN** a client submits a valid charge request to `POST /charge`
- **THEN** the provider processes it and returns the charge outcome

#### Scenario: Alternative charge paths are absent
- **WHEN** a client calls `POST /charges` or another charge path
- **THEN** the provider does not treat it as a charge operation

#### Scenario: Normal charge
- **WHEN** the `normal` scenario is active and a valid charge request is received
- **THEN** the provider returns an approved outcome with a charge reference

#### Scenario: Permanently rejected charge
- **WHEN** the permanent-rejection scenario is active and a valid charge request is received
- **THEN** the provider returns a final rejected outcome with a reason

### Requirement: Idempotent charges by paymentId
For the same `paymentId`, the provider SHALL return the original charge outcome and MUST NOT apply a second charge.

#### Scenario: Repeated charge request
- **WHEN** the provider receives the same `paymentId` more than once
- **THEN** it returns the stored original outcome and records only one charge

#### Scenario: Concurrent duplicate charges
- **WHEN** concurrent charge requests share a `paymentId`
- **THEN** exactly one charge is recorded and all responses agree with it

### Requirement: Global scenario scope
Provider scenario selection SHALL be global. The selected scenario SHALL apply to all incoming charge requests until it is changed or reset. The provider SHALL NOT support per-payment scenario selection in v1.

#### Scenario: Scenario applies to all charges
- **WHEN** a global scenario is selected and multiple charge requests arrive
- **THEN** every charge request uses the selected scenario

#### Scenario: Scenario change affects subsequent charges
- **WHEN** a learner changes the global scenario
- **THEN** subsequent charge requests use the new scenario while previously recorded charge outcomes remain unchanged

#### Scenario: Per-payment targeting is unavailable
- **WHEN** a learner attempts to select a scenario for a single payment
- **THEN** the provider does not expose or honor per-payment selection and uses the global scenario

### Requirement: Configurable response scenarios
The provider SHALL support the agreed `normal`, `slow`, `timeout`, HTTP 503, permanent rejection, and uncertain result scenarios, selectable through a documented global control mechanism.

#### Scenario: Normal scenario
- **WHEN** a learner selects `normal` and a charge request is received
- **THEN** the provider approves the charge

#### Scenario: Slow scenario
- **WHEN** a learner selects `slow` and a charge request is received
- **THEN** the provider approves after a delay that remains within the caller timeout

#### Scenario: Timeout scenario
- **WHEN** a learner selects `timeout` and a charge request is received
- **THEN** the caller observes a timeout

#### Scenario: HTTP 503 scenario
- **WHEN** a learner selects HTTP 503 and a charge request is received
- **THEN** the provider returns a transient service-unavailable response

#### Scenario: Permanent rejection scenario
- **WHEN** a learner selects permanent rejection and a charge request is received
- **THEN** the provider returns a final rejected outcome with a reason

#### Scenario: Uncertain result scenario
- **WHEN** a learner selects uncertain result and a charge request is received
- **THEN** the provider processes the charge but returns an ambiguous response, and a retry with the same `paymentId` returns the stored outcome

#### Scenario: Scenario reset
- **WHEN** a learner clears the selected scenario
- **THEN** subsequent charges use the default behavior

### Requirement: Configurable latency
The provider SHALL support configurable response latency, including delays that cause the caller to time out.

#### Scenario: Delay exceeds caller timeout
- **WHEN** the configured delay is longer than the caller's timeout
- **THEN** the caller observes a timeout while the provider records the delayed charge behavior

### Requirement: Charge record
The provider SHALL record charge attempts and their outcomes so learners can inspect what the provider received and returned.

#### Scenario: Charge history inspection
- **WHEN** a learner requests the provider's charge records
- **THEN** the records show each charge's `paymentId`, amount, currency, scenario, and outcome

### Requirement: Experiment reset
The provider SHALL provide a documented way to clear charge records and scenario configuration for repeatable experiments.

#### Scenario: Reset provider state
- **WHEN** a learner resets the provider
- **THEN** prior charges and scenario settings are cleared and later charges start from a clean baseline
