-- +goose Up
CREATE TABLE schedules (
    id             uuid PRIMARY KEY,
    tenant_id      uuid NOT NULL REFERENCES tenants (id),
    name           text NOT NULL,
    job_type       text NOT NULL,
    payload        json NOT NULL,
    labels         jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(labels) = 'object'),
    priority       smallint CHECK (priority BETWEEN 1 AND 4), -- NULL: the job type's default
    trigger_kind   text NOT NULL CHECK (trigger_kind IN ('cron', 'fixed_rate', 'fixed_delay')),
    cron_expr      text,
    time_zone      text,
    interval_ms    bigint CHECK (interval_ms > 0),
    start_at       timestamptz,
    end_at         timestamptz,
    max_runs       integer CHECK (max_runs >= 1),
    jitter_ms      bigint NOT NULL DEFAULT 0 CHECK (jitter_ms >= 0),
    misfire_policy text NOT NULL CHECK (misfire_policy IN ('fire_once', 'skip', 'fire_all')),
    overlap_policy text NOT NULL CHECK (overlap_policy IN ('skip', 'buffer_one', 'allow', 'cancel_previous')),
    state          text NOT NULL CHECK (state IN ('ACTIVE', 'PAUSED', 'COMPLETED', 'DELETED')),
    next_fire_at   timestamptz,
    last_fire_at   timestamptz,
    fire_count     integer NOT NULL DEFAULT 0 CHECK (fire_count >= 0),
    created_by     text NOT NULL,
    created_at     timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL,
    FOREIGN KEY (tenant_id, job_type) REFERENCES job_types (tenant_id, name),
    CHECK ((trigger_kind = 'cron') = (cron_expr IS NOT NULL AND time_zone IS NOT NULL)),
    CHECK ((trigger_kind = 'cron') = (interval_ms IS NULL)),
    CHECK (end_at IS NULL OR start_at IS NULL OR end_at > start_at)
);

CREATE UNIQUE INDEX schedules_name_uq ON schedules (tenant_id, name) WHERE state <> 'DELETED';
CREATE INDEX schedules_due_idx ON schedules (next_fire_at) WHERE state = 'ACTIVE';
CREATE INDEX schedules_tenant_created_idx ON schedules (tenant_id, created_at DESC, id DESC);

-- One row per fire time ever materialized: invariant I2 holds after jobs move to history.
CREATE TABLE schedule_fires (
    schedule_id uuid NOT NULL,
    fire_time   timestamptz NOT NULL,
    job_id      uuid NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schedule_id, fire_time)
);

CREATE INDEX jobs_schedule_idx ON jobs (schedule_id, fire_time) WHERE schedule_id IS NOT NULL;
CREATE INDEX jobs_start_deadline_idx ON jobs (start_deadline)
    WHERE state IN ('SCHEDULED', 'READY') AND attempt_count = 0 AND start_deadline IS NOT NULL;

-- +goose Down
DROP INDEX jobs_start_deadline_idx, jobs_schedule_idx;
DROP TABLE schedule_fires, schedules;
