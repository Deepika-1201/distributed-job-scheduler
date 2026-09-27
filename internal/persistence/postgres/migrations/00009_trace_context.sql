-- +goose Up
-- The submitting request's W3C traceparent, so an execution can link to it (ADR-020).
ALTER TABLE jobs ADD COLUMN trace_parent text;
ALTER TABLE job_history ADD COLUMN trace_parent text;

-- +goose Down
ALTER TABLE job_history DROP COLUMN trace_parent;
ALTER TABLE jobs DROP COLUMN trace_parent;
