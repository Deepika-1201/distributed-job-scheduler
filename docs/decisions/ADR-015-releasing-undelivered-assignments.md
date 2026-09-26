# ADR-015: Releasing assignments that were never delivered

- **Status:** Accepted. Adds transition T23 to the job state machine.
- **Date:** 2026-09-26
- **Related:** [ADR-002](ADR-002-worker-pull-via-dispatcher.md), [ADR-007](ADR-007-execution-semantics.md), [ADR-008](ADR-008-retry-strategy.md), [HLD §12.3](../architecture.md#123-dispatcher), [LLD §4](../low-level-design.md#4-job-state-machine), [LLD §12.3](../low-level-design.md#123-dispatcher)

## Context

- **Commit before send.** The dispatcher commits a claim before sending the assignment ([HLD §12.3](../architecture.md#123-dispatcher)): the job moves to `RUNNING` with a new attempt and fencing token. A crash therefore never hands out work the database doesn't know about.
- **But sends can fail:**
  - the poll timed out or the client went away between the claim and the send;
  - the response was lost after the worker was sent it.
- **A gap in the design.** HLD §12.3 says such an attempt "is released immediately, or the reaper catches it". The approved state machine, however, left `RUNNING` only through an attempt outcome.

## Problem

What happens to a job whose attempt was committed but never reached a worker?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| A. Treat it as lost (the reaper, after the session lease expires) | No new transition | Waits at least the session TTL (30 s). It also uses up retry budget and counts toward poison detection (3 lost attempts dead-letter a job), so a flaky network could dead-letter healthy jobs that never ran. |
| B. Two-phase assignment: `ASSIGNED`, then `RUNNING` when the worker acknowledges | Delivery is explicit | A new state, plus an extra round trip and write per job on the hot path (from ~8 writes per job to 9 or more, [HLD §15.1](../architecture.md#151-capacity-model-tier-m)) |
| **C. Release on known non-delivery, and reconcile uncertain delivery through heartbeats** | No extra writes when delivery works; undelivered jobs return within milliseconds, or within 15 s | A new transition; depends on heartbeats listing the attempts a worker holds |

## Decision

**Option C.**

- **T23:** `RUNNING` → `READY`, performed by the dispatcher and guarded by the attempt ID.
  - It refunds the retry budget (`budget_attempts − 1`), because the attempt never ran.
  - It keeps `attempt_count`, so the next attempt gets a higher fencing token.
  - It writes no attempt row, so attempt numbers can have gaps.
- **Known non-delivery** (the poll left before its claim committed, or the caller disconnected): release at once.
- **Uncertain delivery** (the response may have been lost):
  - Each heartbeat lists the attempts the worker holds.
  - An attempt that is current for the session but missing from its heartbeats for 15 s (three intervals) is released.
- **The SDK reports an attempt in its heartbeats until the outcome is acknowledged**, not just while the handler runs. Otherwise a slow `Complete` retry would get the job released and run twice.

## Trade-offs

- A worker that fails to report an attempt it is running causes a second run. At-least-once execution ([ADR-007](ADR-007-execution-semantics.md)) allows this, and the fencing token rejects the older attempt's report.
- When a response is lost, the slot sits unused for up to 15 s.

## Consequences

- LLD §4 lists T23, and `domain` allows it for the dispatcher only.
- Releases should be counted (phase 11): a rising rate points to network or worker problems.

## Revisit when

- Load tests show high release rates. That would favor explicit acknowledgements (option B).
