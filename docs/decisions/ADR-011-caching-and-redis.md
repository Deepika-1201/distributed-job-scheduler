# ADR-011: Caching and Redis

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [ADR-003](ADR-003-message-broker.md), [ADR-005](ADR-005-distributed-locking-and-fencing.md), [ADR-006](ADR-006-leader-election.md)

## Context

Job platforms often add Redis for a worker registry, distributed locks and leader election, rate limiting, caching hot metadata, or scheduling metadata. Each of these needs to be checked against this design before adding a stateful component.

## Problem

Does V1 need Redis, or any shared cache? What is the source of truth for each concern?

## Options considered

| Use case | Redis | Alternative in this design | Assessment |
|---|---|---|---|
| Worker registry | Sessions in Redis with TTLs | Sessions in PostgreSQL, live state in dispatcher memory | Attempts reference sessions transactionally, so they belong with the jobs |
| Distributed locks and leader election | Redlock or `SET NX PX` | PostgreSQL leases with epochs | Redis can't fence writes to PostgreSQL ([ADR-005](ADR-005-distributed-locking-and-fencing.md)) |
| API rate limiting | Exact global token buckets | Token bucket on each `api` node (tenant limit ÷ replica count) | Approximate but adequate for internal tenants |
| Hot metadata (job types, tenants, quotas) | Shared cache | In-process cache with a short TTL (≈ 10 s) and version checks | Tiny data set that is read-mostly |
| Scheduling metadata and timers | Sorted-set timers | PostgreSQL timer store ([ADR-001](ADR-001-scheduler-architecture.md)) | Timers must be durable and transactional with job state |

## Decision

**No Redis in V1.**

| Role | Where it lives |
|---|---|
| Source of truth | PostgreSQL |
| Cache | In-process only, disposable, TTL-bounded |
| Message broker | None in V1 ([ADR-003](ADR-003-message-broker.md)) |

Critical job state is never cached anywhere it could be mistaken for the truth. Dispatcher memory is a rebuildable projection, fenced by lease epochs.

## Trade-offs

- Rate limits are approximate: uneven load balancing lets a tenant exceed its limit slightly.
- Configuration changes (quotas, job types) take effect within the cache TTL, not instantly.

## Consequences

- One stateful component fewer to provision, secure, monitor and pay for.
- Rate-limiter math must account for `api` autoscaling (replica count from service discovery or configuration).

## Revisit when

- Exact global rate limits become a requirement.
- `api` replicas grow large enough that per-node limits become too coarse.
- Metadata read load or latency demands a shared cache.
