-- +goose Up
-- Engine liveness and the engine that last renewed each session: the evidence session expiry
-- needs while workers ride out database outages (ADR-029).
CREATE TABLE engine_nodes (
    node_id    text PRIMARY KEY,
    beat_at    timestamptz NOT NULL,
    stopped_at timestamptz
);

ALTER TABLE worker_sessions ADD COLUMN renewed_by text;

-- +goose Down
ALTER TABLE worker_sessions DROP COLUMN renewed_by;
DROP TABLE engine_nodes;
