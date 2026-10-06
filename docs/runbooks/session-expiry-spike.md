# SessionExpirySpike

**Meaning:** more than 10 worker sessions in a pool expired within 5 minutes. A session expires when its worker stopped renewing it while its engine still had the database (ADR-029).

**Impact:** each expired session's running attempts are recorded as `LOST` and retried, so their work runs again. A crash counts as an attempt.

## Diagnose

1. **Are workers crashing?** Check the pool's ECS service for stopped tasks and their reasons: `aws ecs list-tasks --cluster jobscheduler-$ENV --service-name jobscheduler-$ENV-worker-<pool> --desired-status STOPPED`. Out-of-memory kills show `OutOfMemoryError`.
2. **Can workers reach the engines?** Worker logs show `heartbeat failed` with the error. Look for security group or load balancer changes, and the engine targets' health.
3. **Long pauses:** a worker that stalls longer than its 25 s fence, for example from CPU starvation, loses its session. Compare CPU against the task's limits.

## Fix

- Raise the task memory or CPU for crashing workers (`worker_pools.<pool>.cpu` and `memory`).
- Fix the network path: engines accept port 7070 from the worker security group.
- Check the retried jobs: `GET /v1/jobs/<id>/attempts` shows attempts that are `LOST` with "worker session expired".
