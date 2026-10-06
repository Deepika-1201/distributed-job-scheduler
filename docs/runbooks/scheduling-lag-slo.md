# SchedulingLagSLO

**Meaning:** in a pool, due jobs take more than 1 s at p99 to become `READY` (NFR-3), for 10 minutes. The promoter, which turns due `SCHEDULED` and `RETRY_PENDING` jobs into `READY` ones, is falling behind.

**Impact:** jobs start late. Dispatch is unaffected once they are `READY`.

## Diagnose

1. **Is the database slow?** Compare `db_transaction_duration_seconds` by `operation`, especially the promoter's, with the previous day, and check the database's CPU in RDS Performance Insights. Phase 5 found database CPU to be the binding constraint (LLD §19.4).
2. **Is there a burst of due work?** A cron boundary or a bulk re-drive can make thousands of jobs due at once. Check `jobs_submitted_total` and the `jobs_ready` growth across pools.
3. **Are the engines healthy?** Every engine runs a promoter. Check that the engine service has its desired tasks, and the logs: `aws logs tail /jobscheduler/jobscheduler-$ENV-engine --since 30m | grep -i promot`.

## Fix

- **Database CPU:** scale the instance class up (`db_instance_class`) or reduce competing load, such as bulk operations; they process one batch a second per engine.
- **One-off bursts** drain on their own. Spread cron schedules across the minute with `jitter` if they recur.
- **Engines missing:** see [no-pool-owner.md](no-pool-owner.md).
