# DispatchStalled

**Meaning:** a pool has `READY` jobs and free worker slots, but hands out nothing for 2 minutes. Paused and capped work is excluded.

**Impact:** the pool's jobs don't start.

## Diagnose

1. **Job types:** do the free workers run the waiting job types? `GET /v1/workers?pool=<pool>` lists each session's job types. `GET /v1/jobs?state=READY` shows what is waiting.
2. **The owner:** `GET /v1/pools` shows the owning engine. Search its logs for the pool: `aws logs tail /jobscheduler/jobscheduler-$ENV-engine --since 15m | grep '"pool":"<pool>"'`. Look for `lease lost`, claim errors or `DATABASE_UNAVAILABLE`.
3. **Workers polling elsewhere:** workers whose polls fail keep retrying with backoff. Their logs show `poll failed`.

## Fix

- **Missing job types:** deploy workers that handle them, or move the job type to a pool that has them (`PATCH /v1/job-types/<name>`).
- **A stuck owner:** stop its task. Its leases go to another engine within about 13 s: `aws ecs stop-task --cluster jobscheduler-$ENV --task <id>`.
- **A database problem:** see [api-errors.md](api-errors.md) and [database-connections-saturated.md](database-connections-saturated.md).
