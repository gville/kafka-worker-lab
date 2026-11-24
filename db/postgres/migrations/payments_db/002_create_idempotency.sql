-- Global, non-expiring idempotency namespace. request_hash is a 32-byte
-- SHA-256 digest; one payment may have at most one idempotency record.
CREATE TABLE IF NOT EXISTS payment_api.idempotency (
    idempotency_key text PRIMARY KEY,
    request_hash bytea NOT NULL,
    payment_id uuid NOT NULL,
    response_snapshot jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT idempotency_request_hash_sha256 CHECK (octet_length(request_hash) = 32),
    CONSTRAINT idempotency_payment_id_fkey FOREIGN KEY (payment_id)
        REFERENCES payment_api.payments (payment_id),
    CONSTRAINT idempotency_payment_id_uq UNIQUE (payment_id)
);

ALTER TABLE payment_api.idempotency OWNER TO payment_api;
