-- Payment records owned by the Payment API. Amounts are stored as integer
-- minor units so no currency conversion or floating-point rounding occurs.
CREATE TABLE IF NOT EXISTS payment_api.payments (
    payment_id uuid PRIMARY KEY,
    correlation_id uuid NOT NULL DEFAULT gen_random_uuid(),
    amount_minor_units bigint NOT NULL,
    currency text NOT NULL,
    status text NOT NULL DEFAULT 'PENDING',
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT payments_amount_positive CHECK (amount_minor_units > 0),
    CONSTRAINT payments_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT payments_status_allowed CHECK (
        status IN ('PENDING', 'APPROVED', 'REJECTED', 'FAILED')
    )
);

ALTER TABLE payment_api.payments OWNER TO payment_api;
