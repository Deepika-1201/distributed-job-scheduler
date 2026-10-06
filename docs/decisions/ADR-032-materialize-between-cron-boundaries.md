# ADR-032: Materialize 2.5 minutes ahead, between cron boundaries

- **Status:** Accepted
- **Date:** 2026-10-06
- **Related:** [ADR-001](ADR-001-scheduler-architecture.md) (amends its 2 min lookahead), [ADR-016](ADR-016-withdrawing-provisional-schedule-jobs.md), [HLD §11.6](../architecture.md#116-precision-and-the-top-of-the-minute-spike), [LLD §10.3](../low-level-design.md#103-materializer), [LLD §23](../low-level-design.md#23-capacity)

## Context

- **Materializing ahead** ([ADR-001](ADR-001-scheduler-architecture.md)) exists so that a burst at 00:00:00 finds its job rows already there. The promoter then only changes their state ([HLD §11.6](../architecture.md#116-precision-and-the-top-of-the-minute-spike)).
- **The lookahead was 2 minutes.** The materializer picks schedules with `next_fire_at <= now() + lookahead`. So a cron fire at a whole minute F becomes due for materialization at F − 2 min, which is itself a whole minute.
- **The collision.** At every boundary, the materializer created the fires due 2 minutes later while the promoter promoted the fires due now.
- **The measurement** (run 37482248549: 5,000 schedules firing every minute, on a 4-vCPU runner):
  - Each boundary paid for 5,000 materializations (a fire-and-job insert at 0.30 ms and a cursor update at 0.24 ms, plus cron evaluation in the engines) on top of 5,000 promotions and the dispatch and completion of the promoted jobs.
  - Materialize transactions reached a p99 of 872 ms.
  - At the boundary the engines ran at 85% and 69% CPU, eleven database backends were on CPU, and others waited on WAL.
  - Scheduling lag (NFR-3) had a p99 of 2.1 s, against a target of 1 s.

## Problem

How far ahead should schedules be materialized, so that materialization never competes with the boundary it prepares for?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Keep 2 min | No change | The collision above, at every minute boundary |
| 90 s | Fires at whole minutes are materialized at the half minute. Fewer provisional rows. | Leaves 90 s of lead. A materializer that is behind, or a short engine outage, eats into it sooner. |
| **150 s** | Fires at whole minutes are materialized at the half minute, as far from both boundaries as possible. At least 2 min of lead, as before. | 25% more provisional rows. Schedule edits withdraw up to 2.5 min of fires instead of 2 ([ADR-016](ADR-016-withdrawing-provisional-schedule-jobs.md)). |
| Spread each schedule's materialization by a hash of its ID | Smooths materialization across the whole minute | The due query can no longer use the `next_fire_at` index alone. Rows aren't ready earlier anyway. |
| Pause the materializer for a few seconds around each boundary | Explicit | A special case keyed to wall-clock seconds. The work it postpones still lands right after the boundary. |

## Decision

- **The lookahead is 150 s** (`domain.DefaultPlanLimits`).
- **Why it works:**
  - Cron fires fall on whole minutes. Each is materialized at the half minute 2.5 min before it.
  - That is 30 s from the boundary before and the boundary after.
  - Hourly and daily crons follow the same rule. A fire at 03:00 is materialized at 02:57:30.
- Fixed-rate and fixed-delay schedules start at arbitrary seconds, so they were never aligned. They are unaffected.
- `TestDefaultLookaheadMaterializesBetweenBoundaries` pins the half-minute phase.

## Trade-offs

- About 25% more `SCHEDULED` rows: 2.5 rather than 2 fires per schedule that fires every minute.
- An edit or pause withdraws up to 2.5 min of provisional jobs.
- **The phase matters.** Any future lookahead must stay a whole number of minutes plus 30 s, or the collision returns.
