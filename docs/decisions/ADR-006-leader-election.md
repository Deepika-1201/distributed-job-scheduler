# ADR-006: Leader election

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [HLD §14.5 S11](../architecture.md#s11--a-leader-crashes), [ADR-005](ADR-005-distributed-locking-and-fencing.md)

## Context

Several engine replicas run the same loops. Some duties benefit from a single active actor:

- **pool dispatch**, where one owner holds in-memory buffers, priority state and tenant counts;
- **maintenance**, such as creating and dropping partitions, retention, and later the outbox relay.

## Problem

Is leader election needed? If so, at what scope, and with what mechanism?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| A. One global leader runs all engine duties | Simple mental model | Throughput bound to one node; everything pauses on failover; correctness hinges on fencing the leader |
| B. No leaders at all | No failover | Dispatch policy (weights, caps) becomes approximate or contended; singleton maintenance jobs duplicate work |
| **C. Scoped leases in PostgreSQL** (per pool, per singleton duty) | Failover affects only the scope involved; epochs are fenced atomically with writes; no new infrastructure | A lease table and renewal traffic (small) |
| D. Kubernetes Lease API | Built in on Kubernetes | Ties correctness to the orchestrator (no use on ECS or in docker compose); fencing must still be checked in PostgreSQL |
| E. etcd or ZooKeeper | Proven consensus-based coordination | A new stateful cluster to run; fencing must still be checked in PostgreSQL |
| F. Embedded consensus (Raft) | Instructive | Large correctness and operational burden for no V1 benefit |

## Decision

- **No global leader, and correctness never depends on leadership.** Scheduling loops are leaderless ([ADR-001](ADR-001-scheduler-architecture.md)), and every write is guarded ([ADR-005](ADR-005-distributed-locking-and-fencing.md)).
- **Scoped leases in PostgreSQL (option C)** exist only for pool dispatch and singleton duties.

**Lease protocol:**
1. **Acquire:** a conditional update that succeeds only when the lease has expired by the database clock. It sets `holder`, `epoch = epoch + 1` and `expires_at = now() + TTL`.
2. **Renew** every TTL/3 (pools: TTL 10 s, renewed every 3 s).
3. **Self-fence:** the holder stops acting once renewal is overdue by TTL minus a safety margin (8 s for pools).
4. **Fenced writes:** every write the holder makes checks that its epoch is still current, in the same transaction.
5. **Graceful release:** on `SIGTERM` the holder releases its leases so ownership hands off immediately.
6. **Rebuild on takeover:** a new holder rebuilds in-memory state from the database, since cached state is disposable.

## Trade-offs

- Failover pauses dispatch for the affected pools for about one TTL (≤ ~15 s including reload).
- Lease renewals add a small, constant write load.
- Tenant caps are briefly approximate during an ownership change.

## Consequences

- Split brain can at worst waste work (a stale holder's writes fail the epoch check); it can never corrupt state.
- Clock drift between nodes doesn't matter, because expiry uses only the database clock.
- The same mechanism works in docker compose, on ECS and on Kubernetes.
- "One pool, one lease" reduces to a single leader when there is only one pool, and generalizes to partitioned pools at tier L.

## Revisit when

- Leases can no longer be co-located with the data they fence (sharding, multi-region).
- Lease churn becomes significant (many thousands of pool partitions).
