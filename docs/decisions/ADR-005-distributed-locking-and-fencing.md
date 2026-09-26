# ADR-005: Distributed locking and fencing

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [HLD §14](../architecture.md#14-failure-handling), [ADR-004](ADR-004-database.md), [ADR-006](ADR-006-leader-election.md), [ADR-007](ADR-007-execution-semantics.md)

## Context

Many actors modify the same rows concurrently: API requests, materializers, promoters, dispatchers, reapers and worker reports. Processes pause (garbage collection, CPU starvation), crash and get partitioned. A worker presumed dead may keep running and report late (a zombie). A pool owner that lost its lease may not know it yet.

## Problem

How is exclusive work distribution achieved, and how are stale actors prevented from corrupting state?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| A. Pessimistic global lock (Quartz-style lock row) | Simple | Serializes the whole cluster |
| **B. Row claims** (`SELECT … FOR UPDATE SKIP LOCKED`) | Parallel, non-blocking work distribution | Only protects rows while the transaction is open |
| **C. Optimistic concurrency** (state or version guards on `UPDATE`) | No lock waits; losers learn they lost (zero rows updated) | Callers must handle losing the race |
| D. PostgreSQL advisory locks | Cheap named locks | Tied to a session (break with transaction-pooling proxies, lost on reconnect); invisible in table data; easy to leak |
| E. External lock service (Redis/Redlock, etcd, ZooKeeper, Kubernetes Lease) | Purpose-built | A lock service alone can't stop a paused holder from writing late; fencing must still be checked where the data lives, which is PostgreSQL; an extra dependency |
| **F. Lease rows in PostgreSQL with epochs** | Long-lived ownership that expires on the database clock; the epoch is checked in the same transaction as the owner's writes | Coordination depends on the database (already the source of truth) |

## Decision

Combine **B, C and F**, and add **fencing tokens** for attempts.

| Need | Mechanism |
|---|---|
| Distributing short units of work (schedules, due jobs, expired sessions) | B: `FOR UPDATE SKIP LOCKED`, in short batched transactions |
| Every state transition | C: `UPDATE … WHERE id = ? AND state = ? [AND …]`. Zero rows means another actor won; re-read and respond. |
| Long-lived ownership (pools, singleton duties) | F: lease row `(name, holder, epoch, expires_at)`. Acquired only when expired (epoch + 1); renewed at TTL/3; the holder's writes check the epoch in the same transaction. |
| Stale worker reports | Attempt fencing token: a per-job counter incremented for every attempt. `Complete` succeeds only if the attempt is current, its token matches and it is still running. |

- **No advisory locks** and **no external lock service**.
- **Isolation level is `READ COMMITTED`.** `SERIALIZABLE` would add retries under queue contention without adding safety, since every write already carries an explicit guard.

## How each race is resolved

| Race | Resolution |
|---|---|
| Two schedulers fire the same schedule | `SKIP LOCKED` on the schedule row, plus the unique `(schedule_id, fire_time)` constraint |
| Two promoters pick the same due job | `SKIP LOCKED`, plus the guard `WHERE state = 'SCHEDULED'` |
| Two dispatchers assign the same job | Only the pool owner claims; the epoch fences a stale owner; the `READY → RUNNING` guard succeeds once |
| Two workers report on the same job | Only the current attempt with the matching token can complete it |
| Two API requests (e.g., cancel vs. retry) | State guards: the first commit wins; the second gets `409` with the current state |
| Cancel vs. dispatch | Row lock ordering plus guards: either cancelled before assignment, or cancellation is delivered to the running attempt |
| Two reapers find the same expired session | `SKIP LOCKED` on the session, plus guarded attempt and job updates |

## Trade-offs

- Correctness depends on *every* write path using guarded updates. This is enforced by a repository API that exposes only guarded transitions, and by concurrency tests.
- Coordination depends on database availability. That adds nothing new, since nothing works without the database anyway.

## Consequences

- Split brain can waste work but cannot corrupt state.
- Transaction hygiene is mandatory: short transactions, `lock_timeout`, and keepalives so orphaned locks clear quickly.
- The concurrency test suite must cover every race in the table above.

## Revisit when

- Data is sharded or goes multi-region. Leases must stay co-located with the data they fence.
