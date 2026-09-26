-- +goose Up
-- Long-lived ownership: pool dispatchers and singleton duties (LLD §11).
CREATE TABLE leases (
    name        text PRIMARY KEY,
    holder      text NOT NULL,
    address     text NOT NULL DEFAULT '',
    epoch       bigint NOT NULL CHECK (epoch >= 1),
    acquired_at timestamptz NOT NULL,
    renewed_at  timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL
);

-- +goose Down
DROP TABLE leases;
