# ADR-033: A schedule's due runs are decided by whoever holds all of them

- **Status:** Accepted
- **Date:** 2026-10-06
- **Related:** [ADR-001](ADR-001-scheduler-architecture.md), [ADR-005](ADR-005-distributed-locking-and-fencing.md), [LLD §10.4](../low-level-design.md#104-promoter-and-overlap-policies), [LLD §23](../low-level-design.md#23-capacity)

## Context

- **Promoters run in parallel.** Every engine node runs the promoter. Each batch locks up to 500 due schedule jobs in `run_at` order with `FOR UPDATE SKIP LOCKED`, so batches on different nodes take disjoint rows.
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
  - a jitter window longer than the cron interval, which validation allows for cron;
  - a buffered run whose recheck falls due together with the next fire.

  At an ordinary boundary each schedule has one due run, so the race can't happen there.
- **The constraint.** The batch query is on NFR-3's critical path at every cron boundary. Phase 14 measured it on the 4-vCPU runner: 0.23 ms per call on average, and NFR-3 met at 5,000 fires per boundary (run 37491236953).

## Problem

How do concurrent promoters keep each schedule's overlap decisions in fire-time order, without ever stalling and without slowing the boundary?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Lock the schedule row with its jobs (`FOR UPDATE OF s SKIP LOCKED`) | Serializes each schedule's decisions | The materializer and schedule edits lock schedule rows too. Promotion would skip, and so delay, schedules being materialized or edited. |
| An advisory lock per schedule | No row conflicts | Locks taken inside the batch query are hard to scope to the rows actually returned |
| Treat an earlier due `SCHEDULED` run as active | One extra count | Wrong when that run then expires or is skipped, and `buffer_one` would skip where it should buffer |
| Defer a schedule's jobs while an earlier due run is outside the batch | Small change | Deferred jobs stay first in `run_at` order. A batch full of them decides nothing, again and again, behind a later-sorted earlier fire. Built first; its own test stalled. |
| Select schedules by their earliest due run (a `NOT EXISTS` per due row), then their later runs | Correct, and no batch ever stalls | Built second. On the runner the selection cost 9–27 ms per call as one statement, through a cached generic plan that scanned a whole index. As two statements it still cost 14.5 ms per call. NFR-3 failed both times (runs 37494983993, 37497395434). |
| **Keep the `run_at` batch. Complete a schedule's gaps, or leave the schedule for later.** | The boundary pays only for the query that met NFR-3. Extra work happens only when a schedule has several due runs. | Two statements in a backlog |

## Decision

- **The batch.** As before, a batch locks up to 500 due jobs in `run_at` order with `SKIP LOCKED`. Each job also gets a count of its schedule's earlier runs that are due and still waiting.
- **Gaps.** A schedule has a gap when one of its batch jobs has more earlier due runs than the batch holds for it. Batch jobs share one snapshot, so the counts agree.
  - Causes: another promoter holds an earlier run, or jitter sorted an earlier fire past the batch's limit.
- **Completing a gap.** A second statement locks the gap schedules' other due runs with `SKIP LOCKED`. In the same snapshot, it counts each gap schedule's due runs.
  - If this transaction now holds all of a schedule's due runs, the schedule is decided in fire order.
  - Otherwise none of the schedule's runs are decided. They are unlocked at commit, and decided once the holder of the missing run has finished.
- **Who decides.** A schedule's due runs are decided only by a transaction holding all of them, so concurrent batches never split one. A batch without gaps, which is every batch at an ordinary boundary, never runs the second statement.
- **Tests.**
  - `TestConcurrentPromotersKeepFireOrder` holds a schedule's first run in another transaction. The second run must stay `SCHEDULED` until the first is decided.
  - `TestPromoterProgressesWhenALaterFireSortsFirst` gives a later fire the earlier `run_at` and batches of one. Both runs must be decided in one batch: the deferral version stalled here, and the original one promoted both.

## Trade-offs

- **When a schedule waits.** It waits only while another transaction holds one of its due runs: another promoter's batch, a cancellation, or an expiry. Each of those is short, and `idle_in_transaction_session_timeout` (60 s) bounds an abandoned one.
- **Cost.** A gap batch takes two statements. The second fetches at most 500 rows, and a schedule cut off by that cap waits for the next batch.
- **Concurrency.** Promoter concurrency is now safe at any degree, whether from more engine nodes or from parallel batches within a node.
- **The lesson.** On the queue's hot paths, measure every query shape under the real statement cache. Two correct designs failed NFR-3 on cost alone.
