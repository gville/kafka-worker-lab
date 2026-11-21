-- Local PostgreSQL bootstrap for kafka-worker-lab.
-- Runs once from /docker-entrypoint-initdb.d on an empty data directory and is
-- safe to re-run against an already initialized directory: every object is
-- created only when absent and ownership is reasserted idempotently.
\set ON_ERROR_STOP on

-- Service roles. The Outbox Publisher deliberately has no database of its own:
-- its restricted access to the payments_db outbox table is granted later by
-- the payments migrations, not here.
DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'payment_api',
        'outbox_publisher',
        'payment_worker',
        'payment_provider',
        'observability_api',
        'analytics_worker',
        'notification_worker',
        'workers_api'
    ]
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format(
                'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE',
                role_name,
                'lab_password'
            );
        END IF;
    END LOOP;
END
$$;

-- Exactly seven logical databases, each owned by its single logical service.
-- CREATE DATABASE is not allowed inside a function, so psql \gexec emits the
-- statements outside the DO block and only for databases that do not exist.
SELECT format('CREATE DATABASE %I OWNER %I', d.datname, d.owner_name)
FROM (VALUES
    ('payments_db', 'payment_api'),
    ('payment_worker_db', 'payment_worker'),
    ('provider_db', 'payment_provider'),
    ('observability_db', 'observability_api'),
    ('analytics_db', 'analytics_worker'),
    ('notification_db', 'notification_worker'),
    ('workers_api_db', 'workers_api')
) AS d(datname, owner_name)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = d.datname
)
\gexec

ALTER DATABASE payments_db OWNER TO payment_api;
ALTER DATABASE payment_worker_db OWNER TO payment_worker;
ALTER DATABASE provider_db OWNER TO payment_provider;
ALTER DATABASE observability_db OWNER TO observability_api;
ALTER DATABASE analytics_db OWNER TO analytics_worker;
ALTER DATABASE notification_db OWNER TO notification_worker;
ALTER DATABASE workers_api_db OWNER TO workers_api;

-- PostgreSQL grants CONNECT and TEMP to PUBLIC by default. Remove those
-- implicit privileges so each logical database is reachable only by its owning
-- service role (plus the local lab_admin superuser). Restricted publisher
-- access is intentionally not granted here: it belongs to task 3.2 and is
-- verified by task 2.11.
REVOKE ALL PRIVILEGES ON DATABASE payments_db FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE payment_worker_db FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE provider_db FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE observability_db FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE analytics_db FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE notification_db FROM PUBLIC;
REVOKE ALL PRIVILEGES ON DATABASE workers_api_db FROM PUBLIC;

-- One base schema per owning service. No business tables are created here;
-- task 3.2 owns the payments, idempotency and outbox tables.
\connect payments_db
CREATE SCHEMA IF NOT EXISTS payment_api AUTHORIZATION payment_api;

\connect payment_worker_db
CREATE SCHEMA IF NOT EXISTS payment_worker AUTHORIZATION payment_worker;

\connect provider_db
CREATE SCHEMA IF NOT EXISTS payment_provider AUTHORIZATION payment_provider;

\connect observability_db
CREATE SCHEMA IF NOT EXISTS observability_api AUTHORIZATION observability_api;

\connect analytics_db
CREATE SCHEMA IF NOT EXISTS analytics_worker AUTHORIZATION analytics_worker;

\connect notification_db
CREATE SCHEMA IF NOT EXISTS notification_worker AUTHORIZATION notification_worker;

\connect workers_api_db
CREATE SCHEMA IF NOT EXISTS workers_api AUTHORIZATION workers_api;

-- The only persisted Workers API state: clientConfigKey -> faultId with a
-- deletion status. Active configuration stays in memory and is not stored.
CREATE TABLE IF NOT EXISTS workers_api.fault_associations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_config_key text NOT NULL,
    fault_id uuid NOT NULL DEFAULT gen_random_uuid(),
    is_deleted boolean NOT NULL DEFAULT false,
    deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT fault_associations_key_format CHECK (
        char_length(client_config_key) BETWEEN 1 AND 128
        AND client_config_key ~ '^[A-Za-z0-9._-]+$'
    ),
    CONSTRAINT fault_associations_deletion_status CHECK (
        (is_deleted AND deleted_at IS NOT NULL)
        OR (NOT is_deleted AND deleted_at IS NULL)
    )
);

ALTER TABLE workers_api.fault_associations OWNER TO workers_api;

-- Atomic, case-sensitive uniqueness for non-deleted keys only. A deleted key
-- remains as history and may be reused by a new association with a new UUID.
CREATE UNIQUE INDEX IF NOT EXISTS fault_associations_active_key_uq
    ON workers_api.fault_associations (client_config_key COLLATE "C")
    WHERE NOT is_deleted;
