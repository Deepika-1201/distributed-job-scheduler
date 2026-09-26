-- +goose Up
CREATE TABLE tenants (
    id         uuid PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE job_types (
    tenant_id          uuid NOT NULL REFERENCES tenants (id),
    name               text NOT NULL,
    version            integer NOT NULL DEFAULT 1 CHECK (version >= 1),
    pool               text NOT NULL,
    default_priority   smallint NOT NULL DEFAULT 2 CHECK (default_priority BETWEEN 1 AND 4),
    attempt_timeout_ms bigint NOT NULL CHECK (attempt_timeout_ms > 0),
    retry_policy       jsonb NOT NULL,
    at_most_once       boolean NOT NULL DEFAULT false,
    payload_schema     jsonb,
    enabled            boolean NOT NULL DEFAULT true,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name)
);

-- Active jobs only; terminal jobs move to job_history (LLD §8.1).
CREATE TABLE jobs (
    id                  uuid PRIMARY KEY,
    tenant_id           uuid NOT NULL,
    job_type            text NOT NULL,
    job_type_version    integer NOT NULL,
    pool                text NOT NULL,
    schedule_id         uuid,
    fire_time           timestamptz,
    state               text NOT NULL CHECK (state IN ('SCHEDULED', 'READY', 'RUNNING', 'RETRY_PENDING', 'PAUSED')),
    priority            smallint NOT NULL CHECK (priority BETWEEN 1 AND 4),
    payload             json NOT NULL,
    labels              jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(labels) = 'object'),
    dedupe_key          text,
    correlation_id      text,
    created_by          text NOT NULL,
    request_id          text,
    run_at              timestamptz NOT NULL,
    start_deadline      timestamptz,
    deadline            timestamptz,
    attempt_timeout_ms  bigint NOT NULL CHECK (attempt_timeout_ms > 0),
    retry_policy        jsonb NOT NULL,
    at_most_once        boolean NOT NULL,
    attempt_count       integer NOT NULL DEFAULT 0,
    budget_attempts     integer NOT NULL DEFAULT 0,
    budget_lost         integer NOT NULL DEFAULT 0,
    budget_started_at   timestamptz,
    cancel_requested_at timestamptz,
    last_error          text,
    created_at          timestamptz NOT NULL,
    ready_at            timestamptz,
    updated_at          timestamptz NOT NULL,
    current_attempt_id  uuid,
    current_session_id  uuid,
    attempt_started_at  timestamptz,
    attempt_deadline    timestamptz,
    FOREIGN KEY (tenant_id, job_type) REFERENCES job_types (tenant_id, name),
    CHECK ((schedule_id IS NULL) = (fire_time IS NULL)),
    CHECK ((state = 'RUNNING') = (current_attempt_id IS NOT NULL)),
    CHECK (current_attempt_id IS NULL
           OR (current_session_id IS NOT NULL AND attempt_started_at IS NOT NULL AND attempt_deadline IS NOT NULL))
);

CREATE INDEX jobs_ready_idx ON jobs (pool, priority, run_at, id) WHERE state = 'READY';
CREATE INDEX jobs_due_idx ON jobs (run_at) WHERE state IN ('SCHEDULED', 'RETRY_PENDING');
CREATE INDEX jobs_running_session_idx ON jobs (current_session_id) WHERE state = 'RUNNING';
CREATE INDEX jobs_running_deadline_idx ON jobs (attempt_deadline) WHERE state = 'RUNNING';
CREATE UNIQUE INDEX jobs_dedupe_uq ON jobs (tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX jobs_tenant_created_idx ON jobs (tenant_id, created_at DESC, id DESC);

CREATE TABLE job_history (
    id                  uuid NOT NULL,
    tenant_id           uuid NOT NULL,
    job_type            text NOT NULL,
    job_type_version    integer NOT NULL,
    pool                text NOT NULL,
    schedule_id         uuid,
    fire_time           timestamptz,
    state               text NOT NULL CHECK (state IN ('SUCCEEDED', 'FAILED', 'DEAD_LETTERED', 'CANCELLED', 'EXPIRED', 'SKIPPED')),
    priority            smallint NOT NULL,
    payload             json NOT NULL,
    labels              jsonb NOT NULL,
    dedupe_key          text,
    correlation_id      text,
    created_by          text NOT NULL,
    request_id          text,
    run_at              timestamptz NOT NULL,
    start_deadline      timestamptz,
    deadline            timestamptz,
    attempt_timeout_ms  bigint NOT NULL,
    retry_policy        jsonb NOT NULL,
    at_most_once        boolean NOT NULL,
    attempt_count       integer NOT NULL,
    budget_attempts     integer NOT NULL,
    budget_lost         integer NOT NULL,
    budget_started_at   timestamptz,
    cancel_requested_at timestamptz,
    last_error          text,
    created_at          timestamptz NOT NULL,
    ready_at            timestamptz,
    updated_at          timestamptz NOT NULL,
    finished_at         timestamptz NOT NULL,
    reason              text,
    result              json,
    PRIMARY KEY (id, finished_at)
) PARTITION BY RANGE (finished_at);

-- Catches rows if maintenance ever falls behind, so terminal transitions never fail.
CREATE TABLE job_history_default PARTITION OF job_history DEFAULT;
CREATE INDEX job_history_tenant_finished_idx ON job_history (tenant_id, finished_at DESC, id DESC);

-- Append-only: one row per attempt, written when it ends.
CREATE TABLE attempts (
    id          uuid NOT NULL,
    job_id      uuid NOT NULL,
    tenant_id   uuid NOT NULL,
    number      integer NOT NULL,
    session_id  uuid NOT NULL,
    state       text NOT NULL CHECK (state IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT', 'LOST')),
    retryable   boolean NOT NULL,
    error       text,
    started_at  timestamptz NOT NULL,
    deadline    timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    actor       text NOT NULL,
    PRIMARY KEY (job_id, number, finished_at)
) PARTITION BY RANGE (finished_at);

CREATE TABLE attempts_default PARTITION OF attempts DEFAULT;

CREATE TABLE idempotency_keys (
    tenant_id    uuid NOT NULL,
    operation    text NOT NULL,
    key          text NOT NULL CHECK (length(key) BETWEEN 1 AND 255),
    request_hash bytea NOT NULL,
    resource_id  uuid NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, operation, key)
);

CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);

-- +goose Down
DROP TABLE idempotency_keys, attempts, job_history, jobs, job_types, tenants;
