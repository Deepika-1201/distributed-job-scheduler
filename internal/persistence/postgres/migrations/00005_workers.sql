-- +goose Up
-- Worker sessions and tenant running caps (LLD §12.2).
CREATE TABLE worker_sessions (
    id               uuid PRIMARY KEY,
    pool             text NOT NULL,
    worker_id        text NOT NULL,
    job_types        text[] NOT NULL DEFAULT '{}',
    slots            integer NOT NULL CHECK (slots BETWEEN 1 AND 10000),
    labels           jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(labels) = 'object'),
    runtime_version  text NOT NULL DEFAULT '',
    state            text NOT NULL CHECK (state IN ('ACTIVE', 'CLOSED', 'EXPIRED')),
    created_at       timestamptz NOT NULL DEFAULT now(),
    heartbeat_at     timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz NOT NULL,
    closed_at        timestamptz,
    CHECK ((state = 'ACTIVE') = (closed_at IS NULL))
);

CREATE INDEX worker_sessions_pool_idx ON worker_sessions (pool) WHERE state = 'ACTIVE';
CREATE INDEX worker_sessions_expiry_idx ON worker_sessions (lease_expires_at) WHERE state = 'ACTIVE';

ALTER TABLE tenants ADD COLUMN max_running integer CHECK (max_running >= 1);
CREATE INDEX jobs_running_tenant_idx ON jobs (tenant_id) WHERE state = 'RUNNING';

-- +goose Down
DROP INDEX jobs_running_tenant_idx;
ALTER TABLE tenants DROP COLUMN max_running;
DROP TABLE worker_sessions;
