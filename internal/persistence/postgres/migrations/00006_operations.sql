-- +goose Up
-- Bulk cancel and re-drive, processed by the engine in batches (LLD §13.3).
CREATE TABLE operations (
    id                 uuid PRIMARY KEY,
    tenant_id          uuid NOT NULL REFERENCES tenants (id),
    kind               text NOT NULL CHECK (kind IN ('cancel', 'redrive')),
    filter             jsonb NOT NULL,
    state              text NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    cursor_created_at  timestamptz NOT NULL,
    cursor_id          uuid NOT NULL,
    succeeded          integer NOT NULL DEFAULT 0,
    skipped            integer NOT NULL DEFAULT 0,
    failed             integer NOT NULL DEFAULT 0,
    consecutive_errors integer NOT NULL DEFAULT 0,
    last_error         text,
    created_by         text NOT NULL,
    request_id         text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    finished_at        timestamptz,
    CHECK ((state IN ('SUCCEEDED', 'FAILED')) = (finished_at IS NOT NULL))
);

CREATE INDEX operations_open_idx ON operations (created_at) WHERE state IN ('PENDING', 'RUNNING');

-- +goose Down
DROP TABLE operations;
