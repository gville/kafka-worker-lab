-- Transactional outbox. Rows are published in numeric id order by the single
-- sequential Outbox Publisher; only published_at is updated after Kafka acks.
CREATE TABLE IF NOT EXISTS payment_api.outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id uuid NOT NULL DEFAULT gen_random_uuid(),
    event_type text NOT NULL,
    schema_version integer NOT NULL DEFAULT 1,
    payment_id uuid NOT NULL,
    correlation_id uuid NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    producer text NOT NULL DEFAULT 'payment-api',
    data jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    CONSTRAINT outbox_event_type_allowed CHECK (event_type = 'PaymentRequested'),
    CONSTRAINT outbox_event_id_uq UNIQUE (event_id),
    CONSTRAINT outbox_payment_id_uq UNIQUE (payment_id),
    CONSTRAINT outbox_payment_id_fkey FOREIGN KEY (payment_id)
        REFERENCES payment_api.payments (payment_id)
);

ALTER TABLE payment_api.outbox OWNER TO payment_api;

-- Supports insertion-order polling of unpublished rows.
CREATE INDEX IF NOT EXISTS outbox_pending_id_idx
    ON payment_api.outbox (id)
    WHERE published_at IS NULL;
