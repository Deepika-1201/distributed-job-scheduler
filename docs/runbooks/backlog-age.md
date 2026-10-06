# BacklogAge

**Meaning:** a pool's oldest dispatchable job has waited longer than the pool's backlog target, for 10 minutes ([ADR-021](../decisions/ADR-021-pool-backlog.md)). Held work, such as paused pools or job types, or tenants at their cap, doesn't count.

**Impact:** jobs in the pool start late. Past the target, admission sheds new `LOW` submissions with `503 overloaded`, and `NORMAL` ones past three times the target.

## Diagnose

1. `GET /v1/pools`: the pool's owner, workers, free slots, backlog and target.
2. **No workers, or none free:** the pool's capacity is short. Check its ECS service: running count against desired, and whether autoscaling is at its maximum.
3. **Free slots but no dispatch:** the free workers can't run the waiting job types, or dispatch is stuck. See [dispatch-stalled.md](dispatch-stalled.md).
4. **A target that is too tight** for the workload: compare it with the pool's job durations.

## Fix

- Raise the pool's maximum task count (`worker_pools.<pool>.max`) or start workers with the missing job types.
- Adjust the target if it is wrong: `PUT /v1/pools/<pool>/settings` with `{"backlog_target": "15m"}`.
- If one tenant floods the pool, cap it: `PUT /v1/tenants/<id>/quotas` with `max_running`.
