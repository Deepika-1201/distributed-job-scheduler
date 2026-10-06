# ADR-033: Concurrent promoters defer overlap decisions that a batch boundary splits

- **Status:** Accepted
- **Date:** 2026-10-06
- **Related:** [ADR-001](ADR-001-scheduler-architecture.md), [ADR-005](ADR-005-distributed-locking-and-fencing.md), [LLD §10.4](../low-level-design.md#104-promoter-and-overlap-policies), [LLD §23](../low-level-design.md#23-capacity)

## Context

- **Promoters run in parallel.** Every engine node runs the promoter. Each batch locks up to 500 due schedule jobs with `FOR UPDATE SKIP LOCKED`, so batches on different nodes take disjoint rows.
- **Overlap policies look at earlier runs.** The policies `skip`, `buffer_one` and `cancel_previous` decide a due job from its schedule's earlier runs.
  - Inside one batch, a schedule's jobs are decided in fire-time order, and a job promoted earlier in the batch counts as active ([LLD §10.4](../low-level-design.md#104-promoter-and-overlap-policies)).
  - Across batches, nothing coordinated them.
- **The race** (found while reviewing the promoter for phase 14's cron-boundary runs):
  - Two batches split one schedule's due runs: batch A holds fire 1 and batch B holds fire 2.
  - B sees fire 1 still `SCHEDULED`, so under `skip` nothing earlier is active, and B promotes fire 2.
  - A promotes fire 1. Two runs are active, which the policy forbids.
  - `cancel_previous` fails the same way, and `buffer_one` skips a run it should have buffered.
- **When it happens.** It needs a schedule with two or more runs due at once:
  - a backlog after an outage, with misfire `fire_all`;
  - a jitter window longer than the cron interval;
  - a buffered run whose recheck falls due together with the next fire.

  At an ordinary boundary each schedule has one due run, so the race can't happen there.

## Problem

How do concurrent promoters keep each schedule's overlap decisions in fire-time order?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Lock the schedule row with its jobs (`FOR UPDATE OF s SKIP LOCKED`) | Serializes each schedule's decisions | The materializer and schedule edits lock schedule rows too. Promotion would skip, and so delay, schedules that are being materialized or edited. |
| An advisory lock per schedule | No row conflicts | Locks taken inside the batch query are hard to scope to the rows actually returned. More round trips. |
| Treat an earlier due `SCHEDULED` run as active | One extra count | Wrong when that run then expires or is skipped, and `buffer_one` would skip where it should buffer |
| **Defer: leave a schedule's jobs for a later batch while an earlier due run is outside this one** | Decisions stay in fire-time order, made with the earlier run's outcome known. Local to the promoter. | A deferred job waits one more pass |

## Decision

- **Deferral.** A batch leaves a schedule's jobs undecided when the schedule has an earlier due `SCHEDULED` run outside the batch.
  - Such a run is held by another batch, or sorts later by `run_at` because of jitter.
  - The deferred jobs stay `SCHEDULED`, are unlocked at commit, and are decided after the earlier run.
  - `allow` is exempt: it promotes regardless of earlier runs.
- **The batch query** locks the due jobs first. Then, for the locked jobs only, one pass over the schedule's earlier runs counts the active ones, the waiting ones, and the waiting ones that are due.
- **Progress.** Deferred jobs are counted apart (`PromoteStats.Deferred`). A batch that decided nothing does not repeat at once, so a deferred batch can't spin.
- **The test.** `TestPromoterDefersRunsBehindAnEarlierDueRun` holds a schedule's first run in another transaction. The second run must stay `SCHEDULED`, and is skipped once the first is promoted. The test fails without the deferral.

## Trade-offs

- In a backlog, a schedule's later runs wait until its earlier run is decided: one more promoter pass, at most 250 ms, or at once while batches come back full.
- Promoter concurrency is now safe at any degree, whether from more engine nodes or from parallel batches within a node.
