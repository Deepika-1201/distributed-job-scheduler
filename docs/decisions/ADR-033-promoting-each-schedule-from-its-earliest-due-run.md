# ADR-033: Promoters take each schedule by its earliest due run

- **Status:** Accepted
- **Date:** 2026-10-06
- **Related:** [ADR-001](ADR-001-scheduler-architecture.md), [ADR-005](ADR-005-distributed-locking-and-fencing.md), [LLD §10.4](../low-level-design.md#104-promoter-and-overlap-policies), [LLD §23](../low-level-design.md#23-capacity)

## Context

- **Promoters run in parallel.** Every engine node runs the promoter. Each batch locked up to 500 due schedule jobs in `run_at` order with `FOR UPDATE SKIP LOCKED`, so batches on different nodes took disjoint rows.
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

## Problem

How do concurrent promoters keep each schedule's overlap decisions in fire-time order, without ever stalling?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Lock the schedule row with its jobs (`FOR UPDATE OF s SKIP LOCKED`) | Serializes each schedule's decisions | The materializer and schedule edits lock schedule rows too. Promotion would skip, and so delay, schedules that are being materialized or edited. |
| An advisory lock per schedule | No row conflicts | Locks taken inside the batch query are hard to scope to the rows actually returned. More round trips. |
| Treat an earlier due `SCHEDULED` run as active | One extra count | Wrong when that run then expires or is skipped, and `buffer_one` would skip where it should buffer |
| Defer: keep selecting by `run_at`, and leave a schedule's jobs undecided while an earlier due run is outside the batch | Small change | Deferred jobs stay first in `run_at` order. A batch full of them decides nothing, again and again, behind a lock held elsewhere or a later fire that jitter sorted first. This was the first version; its test with a batch of one stalled. |
| **Take schedules by their earliest due run (the head), then the schedule's later due runs** | Holding the head gives one batch all of the schedule's decisions. Heads never wait behind another run, so every batch progresses. A backlog is still decided in one batch, so `cancel_previous` supersedes it at once. | A `NOT EXISTS` probe per due row to find the heads |

## Decision

- **Heads.** A batch locks up to 500 heads in `run_at` order with `SKIP LOCKED`.
  - A head is a due `SCHEDULED` job with no earlier-fire due `SCHEDULED` job of the same schedule.
  - Another batch can only decide a schedule by holding its head, so concurrent batches never split one.
- **Later runs.** The batch then locks the heads' later due runs, up to 500 more, with a plain `FOR UPDATE`.
  - No promoter holds these without the head. A cancellation that holds one makes the batch wait for at most `lock_timeout`.
  - Runs beyond the cap form the next batch, under a new head.
- **Counts.** The counts of earlier active and waiting runs are computed once per head, for the locked heads only.
  - Each schedule is then decided in fire-time order, as before.
  - Every policy, `allow` included, follows the same path.
- **Tests.**
  - `TestConcurrentPromotersKeepFireOrder` holds a schedule's first run in another transaction. The second run must stay `SCHEDULED` until the first is decided.
  - `TestPromoterProgressesWhenALaterFireSortsFirst` gives a later fire the earlier `run_at` and batches of one. Both runs must be decided in one batch: the deferral version stalled here, and the original one promoted both.

## Trade-offs

- Finding heads costs one index probe per due row on `jobs_schedule_idx`.
- A batch can decide up to 1,000 jobs: 500 heads and 500 later runs.
- Promoter concurrency is now safe at any degree, whether from more engine nodes or from parallel batches within a node.
