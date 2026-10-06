# NoPoolOwner

**Meaning:** a pool with workers or `READY` jobs has had no engine holding its lease for 30 seconds. Normally an engine takes a pool over within the lease TTL (10 s) plus one acquire round (S1, S11).

**Impact:** the pool's jobs are neither dispatched nor measured. Running jobs continue.

## Diagnose

1. **Engines:** is the engine service at its desired count? `aws ecs describe-services --cluster jobscheduler-$ENV --services jobscheduler-$ENV-engine`.
2. **Database connectivity:** engines can't take or renew leases without the database. Look for `lease renewal failed` and `lease acquisition failed` in the engine logs. If the database is down, see [database-failover-drill.md](database-failover-drill.md): workers ride the outage out (ADR-029).
3. **Leases:** `SELECT name, holder, epoch, expires_at FROM leases WHERE name LIKE 'pool:%';`. Expired rows with no new holder mean no engine is trying.

## Fix

- Restore the engine service: `aws ecs update-service --cluster jobscheduler-$ENV --service jobscheduler-$ENV-engine --force-new-deployment`.
- Engines want only pools that have active sessions or work. A pool with `READY` jobs and no workers needs workers before an owner matters.
