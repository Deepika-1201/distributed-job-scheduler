-- +goose Up
-- Least-privilege runtime role and partition maintenance functions (ADR-027, LLD §20.4).
-- Roles belong to the server, not the database: create it only if missing, and tolerate a
-- concurrent migration of another database creating it first.
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'jobscheduler_runtime') THEN
        BEGIN
            CREATE ROLE jobscheduler_runtime NOLOGIN;
        EXCEPTION WHEN duplicate_object OR unique_violation THEN
            NULL;
        END;
    END IF;
END
$$;
-- +goose StatementEnd

GRANT USAGE ON SCHEMA public TO jobscheduler_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO jobscheduler_runtime;
REVOKE UPDATE, DELETE ON audit_log FROM jobscheduler_runtime;
REVOKE INSERT, UPDATE, DELETE ON goose_db_version FROM jobscheduler_runtime;
-- Tables created later by this role, in migrations or by jobscheduler_create_partition.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO jobscheduler_runtime;

-- +goose StatementBegin
CREATE FUNCTION jobscheduler_create_partition(parent text, day date) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE
    part text := parent || '_p' || to_char(day, 'YYYYMMDD');
    lo   timestamptz := day::timestamp AT TIME ZONE 'UTC';
BEGIN
    IF parent NOT IN ('job_history', 'attempts') THEN
        RAISE EXCEPTION 'jobscheduler_create_partition: % is not a history table', parent
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF to_regclass(part) IS NOT NULL THEN
        RETURN false;
    END IF;
    EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                   part, parent, lo, lo + interval '24 hours');
    RETURN true;
EXCEPTION WHEN duplicate_table THEN
    RETURN false; -- created concurrently
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION jobscheduler_drop_partition(parent text, day date) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp SET lock_timeout = '1s' AS $$
DECLARE
    part text := parent || '_p' || to_char(day, 'YYYYMMDD');
BEGIN
    IF parent NOT IN ('job_history', 'attempts') THEN
        RAISE EXCEPTION 'jobscheduler_drop_partition: % is not a history table', parent
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                   WHERE i.inhparent = parent::regclass AND c.relname = part) THEN
        RETURN false;
    END IF;
    -- Dropping locks the parent; the 1 s lock timeout keeps history inserts from queueing behind it.
    EXECUTE format('DROP TABLE %I', part);
    RETURN true;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION jobscheduler_create_partition(text, date) FROM PUBLIC;
REVOKE ALL ON FUNCTION jobscheduler_drop_partition(text, date) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobscheduler_create_partition(text, date) TO jobscheduler_runtime;
GRANT EXECUTE ON FUNCTION jobscheduler_drop_partition(text, date) TO jobscheduler_runtime;

-- +goose Down
-- The role is left in place: other databases on the server may use it.
DROP FUNCTION jobscheduler_drop_partition(text, date);
DROP FUNCTION jobscheduler_create_partition(text, date);
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM jobscheduler_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM jobscheduler_runtime;
REVOKE USAGE ON SCHEMA public FROM jobscheduler_runtime;
