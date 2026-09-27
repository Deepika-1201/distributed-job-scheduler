-- +goose Up
-- Platform administration (LLD §14.1).
ALTER TABLE api_keys DROP CONSTRAINT api_keys_role_check;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_role_check
    CHECK (role IN ('viewer', 'submitter', 'operator', 'admin', 'platform-admin'));

ALTER TABLE job_types ADD COLUMN paused boolean NOT NULL DEFAULT false;
ALTER TABLE worker_sessions ADD COLUMN draining boolean NOT NULL DEFAULT false;

CREATE TABLE pools (
    name       text PRIMARY KEY,
    paused     boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE pools;
ALTER TABLE worker_sessions DROP COLUMN draining;
ALTER TABLE job_types DROP COLUMN paused;
ALTER TABLE api_keys DROP CONSTRAINT api_keys_role_check;
ALTER TABLE api_keys ADD CONSTRAINT api_keys_role_check CHECK (role IN ('viewer', 'submitter', 'operator', 'admin'));
