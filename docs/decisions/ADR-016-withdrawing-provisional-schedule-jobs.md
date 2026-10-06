# ADR-016: Schedule changes withdraw provisional jobs by deleting them

- **Status:** Accepted. Resolves HLD open question 11.
- **Date:** 2026-09-26
- **Related:** [ADR-001](ADR-001-scheduler-architecture.md), [HLD §11.7](../architecture.md#117-schedule-changes), [LLD §10.5](../low-level-design.md#105-schedule-changes)

## Context

- **Jobs exist before they are due.** The materializer creates each schedule's jobs up to 2 minutes early ([ADR-001](ADR-001-scheduler-architecture.md); 2.5 minutes since [ADR-032](ADR-032-materialize-between-cron-boundaries.md)). The `schedule_fires` ledger records every fire time it has materialized, which is how invariant I2 (one job per fire time) survives jobs moving to history.
- **The requirement.** Pausing, editing or deleting a schedule must not run the old definition afterwards ([HLD §11.7](../architecture.md#117-schedule-changes)).

## Problem

What happens to jobs that are already materialized but not yet due when their schedule changes?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| A. Keep them | Nothing to do | An edit takes effect up to 2 minutes late, and a paused schedule still runs its next fires |
| B. Cancel them into history | A full record of what was planned | The ledger still holds their fire times, so the new definition can't fire at those times: an edit would silently skip up to 2 minutes of fires. History also fills with cancellations nobody asked for. |
| **C. Delete them, with their ledger rows** | The new definition re-materializes the same fire times; history shows only work that was due | Job IDs that clients saw in listings disappear (`404`) |

## Decision

**Option C.** Withdrawing a schedule's jobs means:

1. Delete the schedule's `SCHEDULED` jobs with `run_at > now()`, along with their `schedule_fires` rows.
2. Lower `fire_count` by the number withdrawn, so `max_runs` still counts correctly.
3. Rewind the cursor to the earliest withdrawn fire time.

- **What is never withdrawn:** jobs that are already due, running or finished.
- **When it happens:** on pause, edit and delete. Audit rows record how many jobs were withdrawn.

## Trade-offs

- Jobs a schedule has materialized but that aren't due yet are provisional. Clients should track schedules, not pre-materialized job IDs.

## Consequences

- After a resume, the misfire policy handles the gap, because the cursor was rewound.
- The OpenAPI description of schedules states that not-yet-due jobs are provisional.

## Revisit when

- Clients need stable IDs for future fires. Then materialize later, or keep withdrawn ledger rows marked rather than deleted.
