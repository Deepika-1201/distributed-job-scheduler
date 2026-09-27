-- +goose Up
-- Tenant quotas (LLD §15.1); NULL means the platform default. max_running now applies per pool.
ALTER TABLE tenants
    ADD COLUMN rate_limit double precision CHECK (rate_limit > 0 AND rate_limit <= 1000000),
    ADD COLUMN max_pending integer CHECK (max_pending BETWEEN 1 AND 1000000),
    ADD COLUMN max_schedules integer CHECK (max_schedules >= 1),
    ADD COLUMN min_schedule_interval_ms bigint CHECK (min_schedule_interval_ms BETWEEN 1000 AND 86400000),
    ADD COLUMN max_payload_bytes integer CHECK (max_payload_bytes BETWEEN 1 AND 65536);

-- +goose Down
ALTER TABLE tenants DROP COLUMN rate_limit, DROP COLUMN max_pending, DROP COLUMN max_schedules,
    DROP COLUMN min_schedule_interval_ms, DROP COLUMN max_payload_bytes;
