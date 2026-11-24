-- Payloads are part of the committed transaction: an idempotency record must
-- carry its response snapshot and an outbox row must carry its event data.
-- Follow-up migration because 001-004 are already applied and registered.
ALTER TABLE payment_api.idempotency ALTER COLUMN response_snapshot SET NOT NULL;
ALTER TABLE payment_api.outbox ALTER COLUMN data SET NOT NULL;
