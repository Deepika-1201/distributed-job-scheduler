-- +goose Up
CREATE TABLE api_keys (
    id          uuid PRIMARY KEY,
    tenant_id   uuid NOT NULL REFERENCES tenants (id),
    name        text NOT NULL,
    prefix      text NOT NULL UNIQUE,
    secret_hash bytea NOT NULL,
    role        text NOT NULL CHECK (role IN ('viewer', 'submitter', 'operator', 'admin')),
    created_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz,
    revoked_at  timestamptz
);

-- Control-plane actions only; submissions are attributed on the job (LLD §9.5).
CREATE TABLE audit_log (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id  uuid NOT NULL,
    actor      text NOT NULL,
    action     text NOT NULL,
    target     text NOT NULL,
    request_id text,
    details    jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX audit_log_tenant_created_idx ON audit_log (tenant_id, created_at DESC);
CREATE INDEX job_history_tenant_created_idx ON job_history (tenant_id, created_at DESC, id DESC);

-- +goose Down
DROP INDEX job_history_tenant_created_idx;
DROP TABLE audit_log, api_keys;
