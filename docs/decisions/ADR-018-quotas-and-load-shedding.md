# ADR-018: Tenant quotas and priority-aware load shedding

- **Status:** Accepted. Amended by [ADR-021](ADR-021-pool-backlog.md): shedding thresholds follow each pool's backlog target, and held work doesn't count.
- **Date:** 2026-09-27
- **Related:** [ADR-011](ADR-011-caching-and-redis.md), [ADR-017](ADR-017-platform-administration.md), [HLD §15.2](../architecture.md#152-backpressure-and-admission-control), [LLD §15](../low-level-design.md#15-quotas-and-load-shedding)

## Context

- **FR-16.** Each tenant has quotas:
  - submission rate;
  - pending jobs;
  - running jobs per pool;
  - active schedules;
  - minimum schedule interval;
  - payload size.
- **FR-17.** Under overload, `LOW` submissions are rejected first, then `NORMAL` ones. HLD §15.2 places both at admission, answering `429` and `503` + `Retry-After`.
- **Constraints.** There is no Redis in V1 ([ADR-011](ADR-011-caching-and-redis.md)), so admission counters live in PostgreSQL or in memory. At tier M a single tenant can submit thousands of jobs per second.
- **Existing mechanisms.** Rate limiting already exists (one rate for every tenant), and so does a running-jobs cap, applied across all pools.

## Problem

1. How is the pending-jobs quota enforced without a hot counter?
2. What signal triggers shedding, and at what scope?

## Options considered

**Pending-jobs quota**

| Option | Pros | Cons |
|---|---|---|
| A. An exact counter row per tenant, updated with every job insert and finish | Exact | One hot row per tenant serializes that tenant's submissions and completions, just where the load is highest |
| B. Sharded counters (N rows per tenant) | Exact, less contention | Every job write touches a counter; summing, repair and complexity |
| **C. A bounded count, cached for 5 s on each `api` node** | No extra writes. The count stops at the quota (`LIMIT quota`), so it costs O(min(quota, pending)), and only for tenants that have the quota. | Soft: a tenant can overshoot by about its submission rate × 5 s |

**Shedding signal**

| Option | Pros | Cons |
|---|---|---|
| Global backlog age | One number | A slow pool would shed submissions for every pool |
| **Per-pool age of the oldest `READY` job, plus a local connection-pool saturation check** | Sheds only work headed for an overloaded pool; each pool's age is one index probe per priority | Two signals to tune |
| CPU or latency of `api` nodes | Standard autoscaling signal | Measures the wrong tier: the bottleneck is workers or the database |

## Decision

- **Storage:** quotas are nullable columns on `tenants`, where `NULL` means the platform default.
  - `rate_limit` (per second, split across `api` replicas);
  - `max_pending` (≤ 1,000,000);
  - `max_running` (per pool);
  - `max_schedules`;
  - `min_schedule_interval`;
  - `max_payload_bytes` (≤ 64 KiB).
- **Management:**
  - platform-admins read and replace a tenant's quotas (`GET`/`PUT /v1/tenants/{id}/quotas`) and list tenants;
  - tenants read their effective quotas (`GET /v1/quotas`).
- **Enforcement:**

| Quota | Where | Response |
|---|---|---|
| Rate | Token bucket per tenant on each `api` node, at the tenant's rate | `429 rate_limited` |
| Payload size | Submission and schedule create or update | `413` |
| Pending jobs | API submissions only; bounded count, cached 5 s | `429 quota_exceeded` |
| Active schedules | Schedule creation; exact, because the tenant row is locked | `429 quota_exceeded` |
| Minimum interval | Schedule validation | `422` |
| Running jobs per pool | Dispatcher allowances (already exact) | Jobs wait `READY` |

- **Shedding:** every `api` node samples each unpaused pool's oldest `READY` age and its own connection-pool saturation, at most every 2 s. *Amended by [ADR-021](ADR-021-pool-backlog.md): the age is the pool owner's measure of dispatchable work, and the thresholds are the pool's backlog target (`LOW`) and three times it (`NORMAL`).*
  - A pool older than `JS_SHED_LOW_AFTER` (5 min) rejects `LOW` submissions.
  - Older than `JS_SHED_NORMAL_AFTER` (15 min), it rejects `NORMAL` ones too.
  - A saturated connection pool (every connection in use and callers waiting) sheds `LOW`.
  - Shed requests get `503 overloaded` with `Retry-After: 30`. `HIGH` and `CRITICAL` are never shed.
- **Exempt:** schedule-created jobs skip rate, pending-jobs and shedding checks. They were admitted when the schedule was created, and misfire and overlap policies absorb their backlog.

## Trade-offs

- The pending quota is soft, with up to about 5 s of overshoot. It protects the platform, not billing.
- A paused job type's backlog counts toward its pool's age. A paused pool is excluded. *Amended by [ADR-021](ADR-021-pool-backlog.md): paused and capped work no longer counts.*
- Each node applies its share of a tenant's rate (rate ÷ replicas), which is exact only while load spreads evenly across nodes (the known limitation of [ADR-011](ADR-011-caching-and-redis.md)).

## Consequences

- New error codes: `quota_exceeded` (`429`) and `overloaded` (`503`).
- A tenant's quota changes reach every `api` node within 30 s, the cache lifetime.

## Revisit when

- Billing needs exact usage (then option B, or a usage ledger).
- Shedding thresholds need to vary per pool.
