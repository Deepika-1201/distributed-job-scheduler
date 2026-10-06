# ADR-029: Workers ride out database outages; session expiry needs evidence

- **Status:** Accepted. Amends [ADR-014](ADR-014-worker-protocol.md) (protocol fields and errors) and the self-fencing of HLD S4.
- **Date:** 2026-10-06
- **Related:** [ADR-006](ADR-006-leader-election.md), [ADR-023](ADR-023-session-calls-fall-back-to-the-owner.md), [HLD §14.5](../architecture.md#145-failure-scenarios) S4 and S6, [LLD §21](../low-level-design.md#21-failure-testing)

## Context

- **Two scenarios pull in opposite directions.**
  - S4: a worker cut off from the engines must stop before its session can expire. Otherwise its job is retried elsewhere while it still runs. The SDK self-fences after `TTL − 5 s` (25 s) without a successful heartbeat.
  - S6: during a database outage "workers keep executing in-flight jobs and buffer `Complete` reports", and the reaper's warm-up gives them a full session TTL to renew once the database returns.
- **The fault tests showed S4 winning.** Self-fencing ignores the cause. A managed failover takes one to a few minutes (NFR-8 allows five), so every running handler was cancelled and every running job retried. Jobs that run for hours (HLD A4) would restart at every failover.
- **The danger in simply tolerating outages is a partial one.** One engine loses the database while another keeps it. The healthy node's reaper expires sessions that can't renew through the cut-off node, and their jobs run again while the first workers continue.
- **The worker can't tell the two apart.** It sees failed heartbeats either way. Only the database can tell whether the worker's engine had it.

## Problem

How can workers ride out a database outage without running jobs twice when only part of the system loses the database?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Keep self-fencing at 25 s | Simple; bounded overlap | Every failover cancels all running work; contradicts S6 and NFR-8 |
| Longer session TTL (> 5 min) | Simple | Dead workers are noticed after minutes instead of 30 s (S3), the most common failure |
| Workers tolerate any `UNAVAILABLE` | Simple | Partial outages run jobs twice, for as long as the outage lasts |
| **Engines explain database failures; workers tolerate only those; expiry waits for evidence** | Failovers keep running work; partial outages stay safe; S3 unchanged | A table, a column, two protocol fields and a stricter expiry rule |

## Decision

1. **Engines explain database failures.**
   - A failure to reach the database is answered with `UNAVAILABLE` carrying `google.rpc.ErrorInfo` with domain `jobscheduler`, reason `DATABASE_UNAVAILABLE` and the engine's `node_id` in its metadata.
   - Failures include connection errors, failover SQLSTATEs, and a database too slow to answer 1 s before the worker's deadline.
2. **Engines record their liveness.**
   - Each engine writes `engine_nodes.beat_at = now()` every heartbeat interval (5 s) while it has the database.
   - After explaining a failure, an engine writes no liveness for one session TTL.
   - A liveness write guards itself against late execution. It applies only if the database's `statement_timestamp()` is within the write's timeout (≤ 1 s) of when it was sent, measured from the database time of the previous write. A write delayed by a healing network does nothing.
   - On a graceful stop it marks itself stopped.
3. **Sessions record which engine last renewed them,** in `worker_sessions.renewed_by`.
4. **Expiry needs evidence.** The reaper expires a session whose lease has passed only if one of these holds:
   - its renewing engine is stopped;
   - its renewing engine wrote liveness after the lease passed;
   - the lease passed more than the outage tolerance ago (5 min).
5. **Workers ride out explained failures.**
   - `RegisterResponse` and `HeartbeatResponse` carry the engine's `node_id`, and `RegisterResponse` the `outage_tolerance`.
   - While every heartbeat failure since the last renewal is a `DATABASE_UNAVAILABLE` from the node that last renewed the session, the worker keeps its handlers running, until `outage_tolerance − 5 s`.
   - Explanations must be contiguous: each must arrive within `TTL − 5 s` of the previous one, or of the last renewal. A worker that was frozen, for example by a long pause, falls back to the TTL rule.
   - Any other failure keeps the `TTL − 5 s` self-fence.

## Why it is safe

- **Whole outage:** the renewing engine can't write liveness, and then pauses for one TTL, so no reaper expires its sessions while workers ride it out. When the database returns, the workers renew within one heartbeat.
- **Races with the pause:** a worker's first explanation arrives at least 5 s before its lease ends. A liveness write sent before the pause began lands within 1 s of being sent, so before that lease ends, and it is no evidence against the worker.
- **One engine cut off:** that engine stops writing liveness, so its sessions wait for the tolerance. Its workers stop at `tolerance − 5 s`, before that.
- **A worker that switched engines:** a failure explained by another node doesn't count, so the worker self-fences at `TTL − 5 s`, as S4 requires.
- **A dead worker** behind a live engine expires at the TTL, as before (S3).

## Trade-offs

- If an engine crashes, workers that died with it are noticed after the tolerance instead of the TTL. Workers that survive reconnect to another engine and renew, which moves `renewed_by`.
- After a failure is explained, expiry of that engine's sessions waits one extra TTL.
- A worker connected to a cut-off engine can't take new work until the outage ends. Its running jobs continue.

## Consequences

- Migration 00013 adds `engine_nodes` and `worker_sessions.renewed_by`. Maintenance deletes nodes silent for over a day.
- The protocol gains three fields and one error reason. Older SDKs ignore them and keep self-fencing at 25 s.
- The fault tests in LLD §21 cover the whole outage (cut and stall), one cut-off engine, and an engine crash.

## Revisit when

- Workers get individual identities (mTLS): the worker could renew through any engine, and expiry could rely on that instead of the renewing engine.
