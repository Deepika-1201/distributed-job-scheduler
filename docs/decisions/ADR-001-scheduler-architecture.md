# ADR-001: Scheduler architecture

- **Status:** Accepted. The lookahead is amended to 2.5 min by [ADR-032](ADR-032-materialize-between-cron-boundaries.md).
- **Date:** 2026-09-26
- **Related:** [HLD §11](../architecture.md#11-scheduling-architecture), [ADR-005](ADR-005-distributed-locking-and-fencing.md), [ADR-006](ADR-006-leader-election.md), [ADR-013](ADR-013-cron-evaluation.md), [ADR-016](ADR-016-withdrawing-provisional-schedule-jobs.md)

## Context

The platform supports one-off delayed jobs, cron schedules with IANA time zones, fixed-rate and fixed-delay schedules. At tier M that means about 100k active schedules, up to 10M future-dated jobs and bursts of 2–5k jobs/s at cron boundaries. Due work must become ready within 1 s (p99). Several engine replicas run at once, any of them can crash, and invariant I2 requires at most one job per `(schedule, fire time)`.

## Problem

1. How does the system detect that work is due?
2. How do several scheduler instances cooperate without creating duplicates or a single point of failure?

## Options considered

### Detecting due work

| Option | Pros | Cons |
|---|---|---|
| **A. Indexed database polling** (`run_at <= now()`, batched) | Stateless; failover is trivial; all state stays in the DB; precision equals the poll interval | A constant small query load; ~250 ms granularity |
| B. Time buckets (a row or key per minute) | Cheap scans at very large scale | Unnecessary with a B-tree on `run_at`; bucket-boundary edge cases |
| C. In-memory min-heap per owner | Millisecond precision | Needs ownership; must reload on failover; the memory window must be bounded |
| D. Hierarchical timing wheel per owner | Millisecond precision; O(1) insert for millions of near-term timers | Same ownership and reload problems as C, plus more complexity |
| E. Broker delayed delivery | No scheduler code | Delay caps (SQS ≤ 15 min); messages can't be cancelled or edited; a second source of truth |

### Coordinating several schedulers

| Option | Pros | Cons |
|---|---|---|
| 1. A single leader runs all scheduling | Simple mental model | Throughput bound to one node; failover pause; correctness depends on fencing the leader |
| 2. Global database lock (Quartz style: `SELECT … FOR UPDATE` on one lock row) | Simple | Serializes all scheduling across the cluster |
| **3. Leaderless row claims** (`FOR UPDATE SKIP LOCKED`) plus unique constraints | Scales with nodes; no failover step; correctness comes from the DB | Relies on disciplined, short transactions |
| 4. Partitioned ownership through leases | Enables in-memory timers (C/D) | Rebalancing and reload complexity that tier M doesn't need |

## Decision

1. **Materialize ahead.** Every engine node runs a materializer about once a second. It claims due schedules with `SKIP LOCKED` and inserts one job per fire time within a 2 min lookahead window, using `ON CONFLICT (schedule_id, fire_time) DO NOTHING`. It advances `next_fire_at` in the same transaction.
2. **Promote by indexed polling (option A).** Every engine node runs a promoter every ~250 ms, and continuously while there is backlog. It moves due `SCHEDULED` and `RETRY_PENDING` jobs to `READY` in batches with `SKIP LOCKED`, and applies start deadlines and overlap policies.
3. **Leaderless coordination (option 3).** No scheduling loop needs a leader. The unique `(schedule_id, fire_time)` constraint and state-guarded updates are the correctness backstop, even if row locking were bypassed.
4. **Retries are delayed jobs.** A failed attempt that will be retried becomes `RETRY_PENDING` with a future `run_at`, and the same promoter handles it.
5. **Deterministic jitter.** A schedule's optional jitter window offsets `run_at` by a hash of `(schedule_id, fire_time)`, so materializing again always gives the same result.
6. **Misfire policies** (fire once, skip, fire all up to a cap) and **overlap policies** (skip, buffer one, allow, cancel previous) are set per schedule. Defaults: fire once and skip.
7. **Time authority.** All due checks use the database clock.

## Trade-offs

- Precision is bounded by the poll interval (~250 ms). That comfortably meets the 1 s target, but it isn't millisecond scheduling.
- Polling adds a constant, small, index-only query load per engine node.
- A herd far larger than tier M's bursts (for example, 100k fires in the same second) takes seconds to promote. It mostly turns into queue wait anyway, because capacity is about 10k slots. Jitter windows are the mitigation.

## Consequences

- Any engine node can die without affecting scheduling correctness, and no failover step is needed.
- Partial indexes on `(state, run_at)` for the timer states are critical and will be designed in the LLD.
- The LLD decides whether to keep the promotion step or let dispatchers claim due jobs directly.
- A timing wheel stays available as an optional precision mode behind a `DueWorkSource` port. Building it would be learning-driven, with failure tests.

## Revisit when

- Precision tighter than ~250 ms becomes a requirement.
- Promotion and claim scans dominate database time.
- The system moves toward tier L, with partitioned ownership and in-memory timers per partition.
