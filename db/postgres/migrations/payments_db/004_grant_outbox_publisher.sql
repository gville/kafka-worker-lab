-- Minimum technical access for the Outbox Publisher: connect to the database,
-- use the service schema, and read/mark rows in the outbox table only.
-- No privileges on payments, idempotency, sequences, or the public schema.
GRANT CONNECT ON DATABASE payments_db TO outbox_publisher;
GRANT USAGE ON SCHEMA payment_api TO outbox_publisher;
GRANT SELECT, UPDATE ON TABLE payment_api.outbox TO outbox_publisher;
