# Spec Delta

## Purpose

Provides a local control plane for worker instances, registration, liveness, commands, and per-instance fault injection, with active control state in memory and the association between client-provided `clientConfigKey` values and internal `faultId` values persisted in `workers_api_db`, so learners can observe and manipulate distributed behavior without callbacks or worker control addresses.

## ADDED Requirements

### Requirement: Worker registration
The API SHALL allow a worker instance to register with a stable identity, worker type, and reported capabilities or version, and SHALL NOT require a callback or control address.

#### Scenario: Worker registers
- **WHEN** a worker instance starts and registers
- **THEN** the API records the instance identity, type, and capabilities without requiring a worker control address

### Requirement: Heartbeat polling
Registered workers SHALL poll the API with a heartbeat every 5 seconds, and the API SHALL mark a worker `offline` when more than 15 seconds have elapsed since its last received heartbeat.

#### Scenario: Heartbeat interval
- **WHEN** a registered worker is running
- **THEN** it sends a heartbeat every 5 seconds

#### Scenario: Heartbeat keeps worker available
- **WHEN** a registered worker sends a heartbeat within 15 seconds of its last heartbeat
- **THEN** the API does not report it as `offline`

#### Scenario: Missing heartbeats mark worker offline
- **WHEN** more than 15 seconds have elapsed since a registered worker's last heartbeat
- **THEN** the API reports it as `offline` while retaining its last known information

### Requirement: Heartbeat response delivery
The heartbeat response SHALL carry the worker's pending commands and current per-instance fault configuration.

#### Scenario: Pending command delivered
- **WHEN** a worker has a pending command and sends a heartbeat
- **THEN** the response includes that command

#### Scenario: Fault configuration delivered
- **WHEN** a worker has an active fault configuration and sends a heartbeat
- **THEN** the response includes the current fault configuration

### Requirement: Worker listing
The API SHALL list registered workers with their identity, type, liveness, capabilities, and last heartbeat.

#### Scenario: Inspect registered workers
- **WHEN** a client requests the worker list
- **THEN** the API returns each registered worker with its identity, type, liveness, capabilities, and last heartbeat

### Requirement: Worker commands
The API SHALL accept commands targeting a specific worker instance, queue them, and deliver each command in the target's next heartbeat response.

#### Scenario: Command delivered to target
- **WHEN** a client issues a supported command to a registered worker instance
- **THEN** the command is queued and returned in that instance's next heartbeat response and applied

#### Scenario: Untargeted command
- **WHEN** a client issues a command that matches no registered worker
- **THEN** the API reports that no target was found without affecting other workers

### Requirement: Fault injection controls
The API SHALL allow a learner to enable, inspect, and clear per-worker-instance faults with `once` and `always` modes. Each configuration SHALL be identified by a client-provided `clientConfigKey`, which names the logical rule. Fault injection SHALL affect only worker-side processing and SHALL be separate from the provider's global scenario selection, which it SHALL NOT change.

#### Scenario: Once fault
- **WHEN** a learner enables a supported fault in `once` mode for a target instance
- **THEN** each matching message can exhibit the fault once, other matching messages remain eligible, and a message that already exhibited it does not exhibit it again

#### Scenario: Always fault
- **WHEN** a learner enables a supported fault in `always` mode for a target instance
- **THEN** every affected operation in that instance exhibits the fault until it is cleared

#### Scenario: Fault cleared
- **WHEN** a learner clears a fault for a target instance
- **THEN** subsequent operations in that instance return to normal behavior and the active in-memory configuration is removed

#### Scenario: Faults are per instance
- **WHEN** a fault is enabled for one worker instance
- **THEN** other instances of the same worker type are unaffected

#### Scenario: Provider scenario is unchanged
- **WHEN** a learner enables a per-instance fault
- **THEN** the provider's global scenario selection is unchanged

### Requirement: Fault identity persistence
The Workers API SHALL require a client-provided `clientConfigKey` and generate a UUID v4 `faultId` for each new association. Associations SHALL be global within Workers API: no two non-deleted associations SHALL share a key. Only associations and their deletion status SHALL be persisted in `workers_api_db`, and they SHALL survive restarts. Reconfiguring an existing non-deleted rule SHALL recover its `faultId`; active configuration SHALL remain in memory.

#### Scenario: Create a new configuration
- **WHEN** the Workers API accepts a fault configuration with a valid `clientConfigKey` that has no non-deleted association
- **THEN** it generates a new UUID v4 `faultId`, persists the association in `workers_api_db`, and keeps the active configuration in memory

#### Scenario: Reconfigure after a Workers API restart
- **WHEN** the `clientConfigKey` of a non-deleted rule is submitted again after a Workers API restart
- **THEN** the Workers API recovers the original `faultId` from the persisted association while the active configuration must be configured again

#### Scenario: Always configuration remains volatile
- **WHEN** the Workers API restarts
- **THEN** active `always` configuration is lost and must be configured again, even though associations and their deletion status remain persisted

### Requirement: Atomic fault association creation
PostgreSQL SHALL atomically enforce uniqueness of non-deleted `clientConfigKey` associations. When concurrent creation requests target the same absent key, exactly one SHALL succeed with the created association and `faultId`; each loser SHALL receive HTTP 409 Conflict without generating another `faultId` or modifying the winner's association. Workers API pre-validation SHALL NOT replace this database guarantee.

#### Scenario: Concurrent creations for the same key
- **WHEN** two creation requests for the same `clientConfigKey` with no non-deleted association arrive concurrently
- **THEN** exactly one creates the association and receives its `faultId`, and the other receives HTTP 409 Conflict

#### Scenario: Losing request has no side effects
- **WHEN** a concurrent creation request loses the uniqueness race
- **THEN** it generates no additional `faultId` and does not modify the association created by the winner

#### Scenario: Pre-validation is not the guarantee
- **WHEN** Workers API pre-validation observes no association for a key
- **THEN** PostgreSQL still atomically enforces uniqueness when the association is created

#### Scenario: Creation race conflict differs from identity conflict
- **WHEN** HTTP 409 Conflict is returned for a creation race
- **THEN** it means another request created the key association, not that an existing association had a different `workerType` or `faultType`

#### Scenario: Existing association follows the approved identity rules
- **WHEN** a request finds a non-deleted association after another request created it
- **THEN** matching `workerType` and `faultType` update the active configuration under the approved update rules, while a difference returns HTTP 409 Conflict without changing the association or generating a `faultId`

### Requirement: Clear fault semantics
Clearing a fault SHALL deactivate only the active in-memory configuration for that target rule. It SHALL NOT mark the `clientConfigKey`-to-`faultId` association as deleted, and reconfiguring with the same key SHALL recover the original `faultId`. Historical `once` records SHALL remain unchanged.

#### Scenario: Clearing keeps the non-deleted association
- **WHEN** a learner clears a fault configured with a `clientConfigKey`
- **THEN** its active configuration is removed, the cleared fault no longer applies, and the association remains active and non-deleted in `workers_api_db` with the same `faultId`

#### Scenario: Reconfiguring after a clear reuses the faultId
- **WHEN** a fault is configured again with the `clientConfigKey` of a cleared fault
- **THEN** Workers API recovers the original `faultId` instead of creating a new association

#### Scenario: Clearing preserves historical once records
- **WHEN** a cleared fault had already registered `once` records
- **THEN** those records are not deleted or modified and keep their original `faultId`

### Requirement: clientConfigKey format
A `clientConfigKey` SHALL contain 1 to 128 ASCII characters limited to letters, digits, hyphen (`-`), underscore (`_`), and dot (`.`), with no spaces. Comparison SHALL be case-sensitive. The Workers API SHALL return HTTP 400 Bad Request when the key is missing or does not satisfy this format, without modifying an association or generating a `faultId`.

#### Scenario: Missing key is rejected
- **WHEN** a fault configuration omits `clientConfigKey`
- **THEN** Workers API returns HTTP 400 Bad Request, leaves existing associations unchanged, and generates no `faultId`

#### Scenario: Invalid key format is rejected
- **WHEN** a fault configuration supplies a key longer than 128 characters, shorter than 1 character, containing a space, or containing any other character
- **THEN** Workers API returns HTTP 400 Bad Request, creates no association, and generates no `faultId`

#### Scenario: Keys differing only in case are distinct
- **WHEN** configurations are submitted with `Test-01` and `test-01`
- **THEN** Workers API treats them as different logical rules and creates distinct associations with distinct UUID v4 `faultId` values

### Requirement: Fault configuration identity conflicts
The Workers API SHALL reject with HTTP 409 Conflict when a `clientConfigKey` matches a non-deleted association but `workerType` or `faultType` differs. It SHALL NOT modify the association or generate a `faultId`.

#### Scenario: Different workerType is rejected
- **WHEN** a configuration uses the active key of an existing rule with a different `workerType`
- **THEN** Workers API returns HTTP 409 Conflict, leaves the association unchanged, and creates no new `faultId`

#### Scenario: Different faultType is rejected
- **WHEN** a configuration uses the active key of an existing rule with a different `faultType`
- **THEN** Workers API returns HTTP 409 Conflict, leaves the association unchanged, and creates no new `faultId`

### Requirement: Fault configuration updates
For a request whose `clientConfigKey` matches a non-deleted association and whose `workerType` and `faultType` are unchanged, the Workers API SHALL apply the request to that rule's active in-memory configuration. It SHALL preserve the association and original `faultId`, create no new association or UUID, and leave historical `once` records unchanged.

#### Scenario: Mode is updated
- **WHEN** the same key is resubmitted with the same `workerType` and `faultType` but a different `mode`
- **THEN** the active configuration uses the new mode, the association and original `faultId` are unchanged, and no new association or UUID is created

#### Scenario: Instance target is updated
- **WHEN** the same key is resubmitted with the same identity but a different worker instance target
- **THEN** the active configuration targets the new instance and the association and original `faultId` are unchanged

#### Scenario: Slow-processing duration is updated
- **WHEN** the same key is resubmitted for `slow-processing` with the same identity but a different supported duration
- **THEN** the active configuration uses the new duration and the association and original `faultId` are unchanged

#### Scenario: Other defined fault parameters are updated
- **WHEN** the same key is resubmitted with the same identity and a change to another parameter already defined for that `faultType`
- **THEN** the active configuration applies that change while preserving the association and original `faultId`

#### Scenario: Mode changes preserve once records
- **WHEN** the same rule changes from `once` to `always` and later from `always` to `once`
- **THEN** historical `once` records are not deleted, modified, or reset and keep the original `faultId`

#### Scenario: Updated configuration remains volatile
- **WHEN** Workers API restarts after an update
- **THEN** the updated active configuration is lost and must be resubmitted with the same key to recover the original `faultId`

### Requirement: Fault configuration deletion and key reuse
Deleting a fault configuration SHALL deactivate any active configuration for that rule and mark its association as deleted in `workers_api_db` while retaining it as historical data with the same `faultId`. The key SHALL then be available for a new configuration. Deletion SHALL NOT delete or modify worker `once` records, which SHALL keep their original `faultId`.

#### Scenario: Deleting deactivates and preserves the historical association
- **WHEN** a fault configuration with an active fault is deleted
- **THEN** its active configuration is removed, the deleted fault no longer applies, its association is marked as deleted, and its historical `faultId` is unchanged

#### Scenario: A deleted key can create a new rule
- **WHEN** a new fault configuration is created with a `clientConfigKey` whose only association is marked as deleted
- **THEN** Workers API creates a new association with a new UUID v4 `faultId` and retains the historical association unchanged

#### Scenario: Deleting preserves historical once records
- **WHEN** a deleted configuration had already registered `once` records
- **THEN** those records are not deleted or modified and keep the original `faultId` of the deleted association

**Error outcomes**: a missing or invalid `clientConfigKey` returns HTTP 400 Bad Request; an identity mismatch or a lost creation race returns HTTP 409 Conflict. These outcomes do not modify an association or generate a `faultId`. A same-identity request with changed other parameters is an update, not an error.

### Requirement: Supported fault types
The only injectable fault types SHALL be `processing-error` (force an error during processing), `slow-processing` (add artificial delay during processing), `crash-before-processing` (simulate a crash before processing the message), and `crash-after-processing` (simulate a crash after processing but before committing the offset).

#### Scenario: Processing error fault
- **WHEN** `processing-error` is enabled for a Payment Worker instance and it processes a message
- **THEN** processing fails because of the injected error

#### Scenario: Slow processing fault
- **WHEN** `slow-processing` is enabled for an instance and it processes a message
- **THEN** processing is artificially delayed

#### Scenario: Crash before processing
- **WHEN** `crash-before-processing` is enabled and the instance receives a message
- **THEN** the instance stops before processing it and the offset is not committed

#### Scenario: Crash after processing
- **WHEN** `crash-after-processing` is enabled and the instance finishes processing a message and its persistent effects complete
- **THEN** the instance stops before committing the offset

### Requirement: Processing error semantics
`processing-error` SHALL cause a normal processing error in the Payment Worker without changing the approved retry policy: three total attempts with 1-second and 2-second backoff. In `always` mode it is injected on every attempt and exhausted attempts follow the `processing` DLQ path with `attempts=3`. In `once` mode it is injected once per matching message; later attempts process normally.

#### Scenario: Always mode fails every attempt
- **WHEN** `processing-error` is enabled in `always` mode for a Payment Worker instance and it processes a message
- **THEN** the error is injected on each attempt and the worker makes three total attempts with 1-second and 2-second backoff

#### Scenario: Processing error exhausts retries in always mode
- **WHEN** `processing-error` is enabled in `always` mode and all three attempts fail
- **THEN** the worker dead-letters with `failureCategory=processing` and `attempts=3` and commits the original offset only after DLQ acknowledgement

#### Scenario: Once mode injects the error on one attempt only
- **WHEN** `processing-error` is enabled in `once` mode and the first attempt for a message fails because of the injected error
- **THEN** the fault is not reapplied on later attempts, which continue with normal processing, and the three-attempt and DLQ policies are unchanged

### Requirement: Processing error scope and injection point
`processing-error` SHALL be available only for the Payment Worker in v1. Notification, Analytics, and Observability SHALL NOT support it, and no additional retry policy SHALL be introduced for those workers. In the Payment Worker it SHALL be injected before calling the Simulated Payment Provider, and the provider SHALL NOT be called before the forced error behavior completes.

#### Scenario: Provider is not called before the forced error completes
- **WHEN** `processing-error` is injected in the Payment Worker
- **THEN** the provider is not called before the forced error behavior completes

#### Scenario: Other workers do not support processing error
- **WHEN** `processing-error` is targeted at a Notification, Analytics, or Observability worker
- **THEN** the fault is not applied and no additional retry policy is introduced for that worker

### Requirement: Slow processing semantics
`slow-processing` SHALL add an artificial delay during processing with a configurable duration and a default of 5 seconds. The delay SHALL NOT by itself fail processing or cause retries, and processing SHALL continue normally after the delay. It SHALL NOT alter existing retry policies or offset-commit behavior.

#### Scenario: Delay does not cause an error or retry
- **WHEN** `slow-processing` is enabled and a worker processes a message
- **THEN** processing is delayed and then continues normally without a fault-induced error or retry

#### Scenario: Default delay is five seconds
- **WHEN** `slow-processing` is enabled without an explicit duration
- **THEN** the injected delay is 5 seconds

#### Scenario: Delay duration is configurable
- **WHEN** `slow-processing` is enabled with an explicit duration
- **THEN** the injected delay uses that configured duration

### Requirement: Slow processing scope
`slow-processing` SHALL be available to all four worker types in v1: Payment Worker, Notification Workers, Analytics Workers, and the Observability consumers of `payments.events` and `payments.dlq`. Each worker SHALL apply the delay before executing its processing logic. In the Payment Worker the delay SHALL occur before calling the Simulated Payment Provider, and the provider SHALL NOT be called until the delay completes.

#### Scenario: Applies to every worker type
- **WHEN** `slow-processing` is enabled for a Payment, Notification, Analytics, or Observability consumer instance
- **THEN** the fault is available and the delay is applied before that instance's processing logic

#### Scenario: Delay precedes processing logic
- **WHEN** `slow-processing` is enabled for a Notification, Analytics, or Observability consumer
- **THEN** the delay is applied before the worker executes its processing logic

#### Scenario: Provider is not called before the delay completes
- **WHEN** `slow-processing` is injected in the Payment Worker
- **THEN** the provider is not called until the delay completes

### Requirement: Crash fault offset handling
After `crash-before-processing` or `crash-after-processing`, the worker SHALL NOT have committed the message offset, and Kafka SHALL redeliver the message after the worker restarts.

#### Scenario: Message is redelivered after restart
- **WHEN** the worker restarts after either crash fault
- **THEN** Kafka redelivers the message because the offset was not committed

#### Scenario: Redelivery uses deduplication or idempotency
- **WHEN** the message is redelivered after `crash-after-processing`
- **THEN** the worker uses its documented deduplication or idempotency mechanism and does not repeat persistent effects that the mechanism covers

### Requirement: Accepted post-crash duplicates
After `crash-after-processing`, redelivery SHALL rely only on each consumer's documented mechanism. The Payment Worker MAY republish the same stable `eventId`, which Analytics and Observability deduplicate. The Notification Worker MAY re-emit after restart because in-memory deduplication is lost. Both are accepted in v1; no additional deduplication store or notification persistence beyond `notification_db`'s `once` records SHALL be introduced.

#### Scenario: Payment Worker duplicate result is accepted
- **WHEN** the Payment Worker crashes after publishing a result and before committing the offset, and the result is republished after restart
- **THEN** the duplicate result carries the same stable `eventId`, Analytics and Observability deduplicate it, and no Payment Worker deduplication store is added

#### Scenario: Notification may re-emit after restart
- **WHEN** a `crash-after-processing` fault causes redelivery of an outcome event after a Notification Worker restart
- **THEN** the simulated notification may be emitted again and this is accepted in v1

### Requirement: Worker restart
Each worker SHALL run as an independent process in its own container with Docker Compose `restart: on-failure`. A simulated crash SHALL terminate the worker process with an error so Docker restarts the container. After restart, the worker SHALL reconnect to Kafka, and messages whose offsets were not committed SHALL be redelivered. The Workers API SHALL NOT implement its own worker restart mechanism.

#### Scenario: Docker restarts a crashed worker
- **WHEN** a simulated crash terminates a worker process with an error
- **THEN** Docker restarts the worker container according to `restart: on-failure`

#### Scenario: Uncommitted messages are redelivered after restart
- **WHEN** a restarted worker reconnects to Kafka
- **THEN** messages whose offsets were not committed are delivered again

#### Scenario: No restart mechanism in the control plane
- **WHEN** a worker crashes
- **THEN** the Workers API does not restart it or provide a restart command; restart is handled by Docker Compose

### Requirement: Once mode semantics
A `once` fault SHALL mean at most one execution of the fault per matching message. Each matching message remains eligible, but once a record exists the fault SHALL NOT be applied again to that same message, including after a crash, restart, or redelivery. If the worker stops after the registration commits but before the fault behavior executes, the record SHALL remain and redelivery SHALL skip the fault, so `once` does not guarantee that the fault executes.

#### Scenario: Once applies per message
- **WHEN** a `once` fault is enabled and multiple messages match the configuration
- **THEN** each matching message may exhibit the fault once and other matching messages remain eligible

#### Scenario: Once fault after restart
- **WHEN** a `once` fault is applied to a message and the worker crashes and restarts
- **THEN** the fault is not applied again to that same message and processing continues normally

#### Scenario: Registration without execution still skips on redelivery
- **WHEN** the worker stops after the `once` record is registered but before the fault behavior executes
- **THEN** redelivery recognizes the record, skips the fault, and processing continues normally

### Requirement: Once record identity
Each worker SHALL persist `once` fault records in its own logical database, identified by worker type, `faultId`, Kafka topic, partition, and offset. The `faultId` SHALL be the internal identifier that the Workers API generated for the configuration's client-provided `clientConfigKey` and persisted in `workers_api_db`, so resubmitting the `clientConfigKey` of a non-deleted rule after a Workers API restart recovers the same `faultId` and cannot allow reapplication to a message already faulted.

#### Scenario: Record contains the approved identity
- **WHEN** a worker registers a `once` fault for a message
- **THEN** the record contains the worker type, `faultId`, Kafka topic, Kafka partition, and Kafka offset

#### Scenario: Worker-owned storage
- **WHEN** a worker persists a `once` record
- **THEN** it writes only to its own logical database and does not use a shared fault database or the Workers API

#### Scenario: Reconfigured logical fault keeps its identity
- **WHEN** the same logical `once` fault is configured again with its `clientConfigKey` after a Workers API restart
- **THEN** its `faultId` is unchanged and the existing record prevents reapplication to the same message

### Requirement: Atomic once registration
Before applying a `once` fault, the worker SHALL atomically register its identity immediately before that fault's injection point. Only the execution that succeeds in registering SHALL apply the fault. Concurrent or later executions for the same identity SHALL recognize the existing record and skip the fault.

#### Scenario: Registration precedes application
- **WHEN** a worker is about to apply a `once` fault
- **THEN** it registers the fault identity immediately before the fault injection point

#### Scenario: Only one execution applies
- **WHEN** concurrent or later executions target the same fault identity
- **THEN** only the execution that registered the record applies the fault and the others skip it

#### Scenario: Record survives restart and redelivery
- **WHEN** the worker restarts and Kafka redelivers the message
- **THEN** the existing record prevents reapplication and the fault is skipped

### Requirement: Once registration timing by fault type
For a `once` fault, registration SHALL occur atomically immediately before the injection point of that fault type: `crash-before-processing` registers immediately before inducing the crash and before starting processing; `crash-after-processing` registers after processing and its effects complete and immediately before inducing the crash; `processing-error` and `slow-processing` register immediately before applying the corresponding behavior.

#### Scenario: Crash before processing registration
- **WHEN** `crash-before-processing` runs in `once` mode
- **THEN** the record is registered immediately before the crash is induced and before processing starts

#### Scenario: Crash after processing registration
- **WHEN** `crash-after-processing` runs in `once` mode
- **THEN** processing and its effects complete, the record is registered, and the crash is induced immediately afterwards

#### Scenario: Processing error registration
- **WHEN** `processing-error` runs in `once` mode
- **THEN** the record is registered immediately before the forced error behavior is applied

#### Scenario: Slow processing registration
- **WHEN** `slow-processing` runs in `once` mode
- **THEN** the record is registered immediately before the delay is applied

### Requirement: Once record retention
`once` records SHALL survive worker restarts and Kafka redeliveries and SHALL be retained for the life of the worker's data. No automatic cleanup SHALL be added in v1. `always` faults SHALL NOT create these records.

#### Scenario: Records are retained
- **WHEN** a `once` record has been written
- **THEN** it remains available for future deduplication and no automatic cleanup is applied in v1

#### Scenario: Always faults are not recorded
- **WHEN** a fault runs in `always` mode
- **THEN** no `once` record is written

### Requirement: Always mode semantics
An `always` fault SHALL keep applying to affected operations until it is cleared or until the Workers API restarts. Active configuration remains in memory, so after a restart it is lost and the fault must be configured again, even when it is resubmitted with the same `clientConfigKey`; only `clientConfigKey`-to-`faultId` associations and their deletion status are persisted in `workers_api_db`.

#### Scenario: Always fault persists until cleared
- **WHEN** an `always` fault is enabled
- **THEN** it keeps applying to affected operations until it is cleared

#### Scenario: Always fault ends when Workers API restarts
- **WHEN** the Workers API restarts while an `always` fault is active
- **THEN** the in-memory configuration is lost and the fault no longer applies until it is configured again

### Requirement: Control commands and provider scenarios
`pause` and `resume` SHALL be control commands, not injectable fault types. Provider results such as `approved`, `rejected`, timeout, and HTTP 503 SHALL be configured only through the provider's global scenario selection and SHALL NOT be modeled as worker faults.

#### Scenario: Pause and resume are commands
- **WHEN** a learner pauses and later resumes a worker instance
- **THEN** both actions are delivered as commands and are not represented as fault types

#### Scenario: Provider results stay with the provider
- **WHEN** a learner wants `approved`, `rejected`, timeout, or HTTP 503 behavior
- **THEN** it is selected through the provider's global scenario control and not through worker fault injection

### Requirement: Command acknowledgement and idempotency
The API SHALL assign each command an identifier and SHALL treat an acknowledged command as applied so repeated delivery does not apply its effect more than once.

#### Scenario: Duplicate command delivery
- **WHEN** the same command is delivered more than once to a worker
- **THEN** the worker applies the command effect only once and reports the acknowledgement

#### Scenario: Command acknowledgement recorded
- **WHEN** a worker acknowledges a delivered command
- **THEN** the API records it as applied and stops returning it in heartbeat responses
