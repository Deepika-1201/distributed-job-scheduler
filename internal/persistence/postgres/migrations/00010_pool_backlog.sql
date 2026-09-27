-- +goose Up
-- Pool backlog targets, and the owner's latest backlog sample (ADR-021).
ALTER TABLE pools
    ADD COLUMN backlog_target_ms bigint CHECK (backlog_target_ms BETWEEN 10000 AND 86400000),
    ADD COLUMN oldest_due_at timestamptz,
    ADD COLUMN sampled_at timestamptz;

-- +goose Down
ALTER TABLE pools DROP COLUMN backlog_target_ms, DROP COLUMN oldest_due_at, DROP COLUMN sampled_at;
