-- +goose Up
-- Per-pool worker tokens (ADR-025) and API key last use (LLD §20.2).
CREATE TABLE worker_tokens (
    id           uuid PRIMARY KEY,
    pool         text NOT NULL,
    name         text NOT NULL,
    prefix       text NOT NULL UNIQUE,
    secret_hash  bytea NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz,
    revoked_at   timestamptz,
    last_used_at timestamptz
);
CREATE INDEX worker_tokens_pool_idx ON worker_tokens (pool, created_at);

ALTER TABLE api_keys ADD COLUMN last_used_at timestamptz;

-- +goose Down
ALTER TABLE api_keys DROP COLUMN last_used_at;
DROP TABLE worker_tokens;
