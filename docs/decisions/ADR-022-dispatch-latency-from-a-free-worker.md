# ADR-022: Dispatch latency counts from a free worker

- **Status:** Accepted. Amends [ADR-020](ADR-020-telemetry.md) (the dispatch latency metric).
- **Date:** 2026-09-27
- **Related:** [ADR-021](ADR-021-pool-backlog.md), [HLD §5](../architecture.md#5-non-functional-requirements) (NFR-4), [HLD §17.3](../architecture.md#173-metrics), [LLD §17.2](../low-level-design.md#172-where-each-metric-is-recorded)

## Context

- **What NFR-4 bounds.** Dispatch latency, "`READY` → started, worker free", has a p99 of at most 1 s. It is the platform's own delay in handing work to a worker that is waiting for it.
- **What the metric measured.** `dispatch_latency_seconds` recorded `started_at − ready_at` for every dispatched job. That includes the time a job waits for a worker to become free.
- **What goes wrong.** In a pool short of workers, almost all of a job's wait is queueing.
  - The p99 then reports the queue, and saturates at the top bucket (300 s). The overview's NFR-4 tile turns red for a capacity shortage that the backlog alert ([ADR-021](ADR-021-pool-backlog.md)) already reports.
  - The SLO can't be judged in a busy pool, and a real dispatch slowdown there would be hidden.
- **How it was found.** Capturing the dashboards against a local run with an overloaded pool.

## Problem

1. How is NFR-4 measured so that queueing doesn't count?
2. Where does the time spent queueing show?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Keep the metric; judge NFR-4 only for pools with free slots, in PromQL | No code change | Free slots are sampled every 10 s, so a pool busy for part of a window counts wholesale in or out; queueing still leaks into the SLO |
| **Measure from the latest of the job's ready time, the poll's arrival and its tenant's cap freeing up; record the full wait separately** | Exact for each job: a waiting poll is a free worker, and each dispatch round already reads capped tenants' running counts. The full wait stays available for capacity questions. | One more histogram per dispatched job |
| Also track when pauses end | Excludes every hold exactly | State per job type, for a rare event whose error is at most one poll wait |

## Decision

- **`dispatch_latency_seconds{pool, priority}`** is the time from when the job could go to a free worker to its start: `started_at − max(ready_at, poll arrival, cap freed)`.
  - A poll is a worker with free slots asking for work, so its arrival is when the worker became free.
  - "Cap freed" is when a dispatch round last saw the job's tenant go from its running cap to having room. Rounds read capped tenants' running counts anyway, every 100 ms while workers wait.
  - The dispatcher computes `min(started_at − ready_at, time since the poll arrived, time since the cap freed)`. The first term uses the database clock and the others the engine's monotonic clock, so the two clocks are never compared.
- **New `queue_wait_seconds{pool, priority}`:** `started_at − ready_at`, the job's whole wait, for a free worker or behind a hold. Its buckets reach 24 h, the longest backlog target.
- **Dashboards:**
  - The overview's NFR-4 tile and the per-pool latency panels use the new definition.
  - The pools dashboard shows queue wait by priority next to the backlog age.
- **Alerts:** `DispatchStalled` still counts dispatches with `dispatch_latency_seconds_count`, which is unchanged.

## Trade-offs

- **Paused jobs:** a job held by a pause, while a worker waits idle, counts from the start of that worker's poll. A poll lasts at most 30 s, so the pause adds at most one poll's wait, once per resume.
- **Handoffs:** while a pool has no owner, workers' polls are refused or redirected, and each new poll restarts the clock. The gap shows as queue wait, owner changes and `pools_unowned`, and the no-owner alert bounds it, but dispatch latency leaves it out.
- **Cost:** one more histogram per dispatched job, with 16 buckets for each pool and priority on the pool's owner.
- **The upgrade:** samples recorded before this change used the old meaning, so a window spanning the upgrade mixes the two.

## Consequences

- HLD §17.3 and LLD §17.2 define both metrics.
- The metric name stays, so alerts and dashboards keep working; its meaning narrows to NFR-4.

## Revisit when

- Resumes are frequent enough to distort the SLO. Then record when pauses end.
- Handoff gaps must count toward NFR-4. Then workers send how long they have been free with each poll, an additive protocol field.
