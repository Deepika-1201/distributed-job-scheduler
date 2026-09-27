# ADR-021: Pool backlog: dispatchable work, measured by the owner, against per-pool targets

- **Status:** Accepted. Amends [ADR-018](ADR-018-quotas-and-load-shedding.md) (shedding thresholds and signal) and [ADR-020](ADR-020-telemetry.md) (pool gauges).
- **Date:** 2026-09-27
- **Related:** [ADR-006](ADR-006-leader-election.md), [ADR-017](ADR-017-platform-administration.md), [HLD §12.6](../architecture.md#126-autoscaling-signals), [HLD §15.2](../architecture.md#152-backpressure-and-admission-control), [HLD §17.5](../architecture.md#175-dashboards-and-alerts), [LLD §18](../low-level-design.md#18-pool-backlog)

## Context

- **Three consumers of one signal.** A pool's backlog age drives load shedding (ADR-018), worker autoscaling (HLD §12.6) and the backlog alert (HLD §17.5).
- **Two definitions today:**
  - Shedding: every `api` node probes each unpaused pool's oldest `READY` job every 2 s. Paused job types count (an ADR-018 trade-off), and so do tenants at their running cap (not considered).
  - Gauges: the pool owner counts every `READY` job every 10 s (ADR-020).
- **What goes wrong:**
  - A tenant that pauses a job type, or sits at its running cap with a large backlog, sheds every other tenant's `LOW` and `NORMAL` submissions in the same pool.
  - Autoscaling on that signal would add workers that cannot take the work.
  - The alerts fire for deliberate holds: a paused pool looks stalled.
  - Only pools with live workers have an owner. A pool whose workers are all gone reports no backlog, so nothing alerts and nothing can scale it up from zero.
- **No per-pool target.** The HLD's backlog alert compares against "the pool's target", but nothing defines one, so batch and latency-sensitive pools share one threshold.

## Problem

1. What counts as a pool's backlog?
2. Where is it measured, so that every consumer agrees?
3. Where do per-pool targets live, and what do they drive?

## Options considered

**What counts**

| Option | Pros | Cons |
|---|---|---|
| Every `READY` job | One index probe per priority | Held work counts, with the effects above |
| **`READY` jobs a free slot could take now** | Every consumer sees work the platform can act on | Needs the hold rules: pool and job-type pauses, tenant caps |

**Where it is measured**

| Option | Pros | Cons |
|---|---|---|
| Each `api` node, with the hold rules | Fresh within 2 s | Held rows at the front of the queue make each probe as long as the held backlog, repeated by every node every 2 s |
| **The pool owner, in its 10 s sample, recorded on the pool row for `api` nodes to read** | One definition; one scan per pool per 10 s; `api` nodes read one small table | Up to 10 s staler; a stale sample must be ignored |

**Targets**

| Option | Pros | Cons |
|---|---|---|
| Per-pool thresholds in the Prometheus rules | No platform change | Job types and workers create pools, so the rules drift; shedding and autoscaling can't see them |
| **A backlog target on the pool, set by platform-admins and exported as a metric** | One number drives alerts, shedding and autoscaling | A new setting and endpoint |

## Decision

- **A pool's backlog is its dispatchable `READY` work.**
  - A `READY` job is *held* while its pool or job type is paused.
  - A tenant with more jobs waiting than its running cap lets it start now is held back by the cap. The jobs beyond that allowance are held, and none of its jobs set the backlog age: their wait comes from the cap, not from the pool.
  - Jobs past their start deadline are left out, as claims skip them.
  - Backlog age is measured from `run_at`: how long the oldest dispatchable job has been due, as in ADR-018.
- **Metrics (amends ADR-020):**
  - `jobs_ready{pool, priority}` and `jobs_oldest_ready_age_seconds{pool}` cover dispatchable work only.
  - New `jobs_held{pool, reason}`, with reason `pool_paused`, `job_type_paused` or `tenant_cap`.
  - New `pool_backlog_target_seconds{pool}`.
- **The owner measures it.**
  - In its 10 s sample, the owner also records the oldest dispatchable due time on the pool row. The write is fenced by its lease epoch.
  - `api` nodes read those rows, still at most every 2 s.
  - A sample older than one minute is ignored, so a pool without an owner never sheds.
- **Every pool with work has an owner.** Engines compete for pools with live workers *or* `READY` jobs, one index probe per known pool. A pool with no workers is still measured and alerted on, and autoscaling can start it from zero. `pools_unowned` counts pools of either kind.
- **Per-pool targets.**
  - `pools.backlog_target_ms`, where `NULL` means the platform default `JS_BACKLOG_TARGET` (5 min, between 10 s and 24 h).
  - Platform-admins set it with `PUT /v1/pools/{name}/settings`, and the change is audited.
  - `GET /v1/pools` shows each pool's effective target and current backlog age.
- **Shedding follows the target (amends ADR-018).**
  - A pool past its target sheds `LOW` submissions; past three times its target, it sheds `NORMAL` ones too.
  - The default target reproduces ADR-018's 5 and 15 minutes. `JS_SHED_LOW_AFTER` and `JS_SHED_NORMAL_AFTER` are removed, and setting either one fails startup.
- **Alerts and autoscaling.**
  - The backlog alert fires when `jobs_oldest_ready_age_seconds` exceeds `pool_backlog_target_seconds` for 10 minutes.
  - Autoscaling, in the deployment phase, can scale on the ratio of the two.

## Trade-offs

- A capped tenant's own backlog no longer sheds anyone. It grows until the tenant's `max_pending` quota stops it.
- A paused pool or job type sheds nothing either. Its jobs show as `jobs_held`.
- Shedding reacts up to about 10 s later than before. Its thresholds are minutes.
- The owner's sample scans the pool's `READY` jobs every 10 s. That is cheap for normal backlogs, but a backlog in the millions costs about a second of one connection per sample.
- A pool of job types that no connected worker can run still counts as dispatchable, and the dispatch-stalled alert says so.

## Consequences

- A migration adds `backlog_target_ms`, `oldest_due_at` and `sampled_at` to `pools`.
- `JS_BACKLOG_TARGET` replaces the two shedding variables on both roles.
- Engines own pools that have no workers: their leases are renewed, but their dispatch loop idles.

## Revisit when

- Backlogs in the millions make the owner's scan expensive. Then use bounded or estimated counts.
- Tenants need their own backlog alerts. Held work isn't broken down by tenant.
