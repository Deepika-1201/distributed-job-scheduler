# ADR-031: Heartbeats stay one transaction each; renewals are not batched

- **Status:** Accepted
- **Date:** 2026-10-06
- **Related:** [ADR-014](ADR-014-worker-protocol.md), [ADR-029](ADR-029-riding-out-database-outages.md), [HLD §12.2](../architecture.md#122-worker-sessions-and-leases), [LLD §12.2](../low-level-design.md#122-sessions-phase-8-migration), [LLD §23](../low-level-design.md#23-capacity)

## Context

- **The open item.** Each `Heartbeat` renews its session in a transaction of its own. [LLD §12.2](../low-level-design.md#122-sessions-phase-8-migration) left batching renewals per engine, as HLD §12.2 suggests, as a phase 14 optimization, to be decided with measurements.
- **What a heartbeat does.** Each one runs four statements:
  - renew the session lease (`UPDATE worker_sessions`);
  - find reported attempts that are no longer current (`stale[]`);
  - read the session's current attempts;
  - release attempts that the worker has left out of its reports for over 15 s (T23).
- **The measurement** ([LLD §23](../low-level-design.md#23-capacity), run 37491249332):
  - **Load:** NFR-2's worker scale. 200 workers with 60 slots each, 10 s jobs, 1,000 jobs/s, so about 10,000 jobs running.
  - **Rate:** 4,800 heartbeats over the run, 40 a second.
  - **Mean statement times:** renewal 0.51 ms; stale check 0.66 ms; current attempts 0.42 ms; release 0.36 ms.
  - **Total:** about 1.9 ms of database time per heartbeat, or 0.08 vCPU at 40 a second.
  - **Comparison:** the jobs themselves used 1.08 vCPUs of database CPU at 1,000 jobs/s.
  - **Contended upper bound:** before the dispatch fix of [LLD §23](../low-level-design.md#23-capacity), the same run saturated the runner (run 37482299386). The four statements then took 6.8 ms, or 0.27 vCPU.

## Problem

Is batching heartbeat renewals worth its complexity at tier M?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| **Keep one transaction per heartbeat** | Simple. Each reply carries the session's own `cancel[]` and `stale[]` lists, computed in the same transaction as the renewal. | Database cost grows linearly with workers |
| Batch renewals per engine: one `UPDATE … FROM unnest(…)` every few hundred ms | Fewer transactions and commits | Saves only the renewal statement. The other three are per session anyway. Replies wait for the batch, or come back before the renewal commits, which weakens the fencing evidence of [ADR-029](ADR-029-riding-out-database-outages.md). |
| Renew in memory and persist only on change | Fewest writes | Session expiry would need engine-side state. That loses the "the database is the only judge" property of [ADR-005](ADR-005-distributed-locking-and-fencing.md). |

## Decision

- **Heartbeats are not batched.** One transaction per heartbeat stays.
- **Why:**
  - At NFR-2's worker scale, heartbeats cost about 0.08 vCPU of database time, and 0.27 vCPU even on a saturated runner.
  - That is about 7% of the database time of 1,000 jobs/s, and under 2% at the 5k jobs/s design point.
  - Batching could save the renewal statement only, about a quarter of that cost.

## Trade-offs

- Database load from heartbeats grows with the worker count, and with slots per worker through the reconciliation statements.
- **When to revisit:**
  - when heartbeat transactions pass about 10% of the primary's CPU, for example around 1,000 workers;
  - or when the heartbeat interval is shortened.
- **What to do then:** batch the renewal first, and keep `cancel[]` and `stale[]` per session.
