-- Local development only: the login role the scheduler connects as (ADR-027). Migrations run
-- as the owner "jobs" and grant jobscheduler_runtime its privileges.
CREATE ROLE jobscheduler_runtime NOLOGIN;
CREATE ROLE jobscheduler_app LOGIN PASSWORD 'app' IN ROLE jobscheduler_runtime;
