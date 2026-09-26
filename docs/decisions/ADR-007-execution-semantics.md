# ADR-007: Job execution semantics

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [HLD §2 invariants](../architecture.md#correctness-invariants), [ADR-005](ADR-005-distributed-locking-and-fencing.md), [ADR-008](ADR-008-retry-strategy.md)

## Context

Workers can crash after doing work but before reporting it, be partitioned while still running, or pause and resume after their lease has expired. Most job side effects land in external systems (email, payment and HTTP APIs) that the platform doesn't control. From the platform's side, "did the job run?" is sometimes unknowable.

## Problem

What execution guarantee can the platform honestly offer?

## Options considered

| Guarantee | What it takes | Assessment |
|---|---|---|
| At-most-once | Never re-run a job once it has been handed out | Loses work whenever a worker dies mid-job. Acceptable only when a duplicate is worse than a miss. |
| **At-least-once** | Re-run whenever the outcome is unknown | Achievable. Duplicates are possible and must be handled by the handler. |
| Exactly-once | Atomic "side effect + completion record" | Impossible in general for external side effects. Achievable only when the side effect lives in the same transactional store as the completion. |
| Effectively-once | At-least-once plus idempotent handlers | The realistic target; responsibility is shared with handler authors. |

## Decision

1. The platform **guarantees at-least-once execution** for every job type by default.
2. Every job carries a **stable idempotency key, `job_id`**, identical across all its attempts. Handlers use it to deduplicate side effects, or pass it downstream (for example, as a payment provider's idempotency key).
3. Every attempt carries a **fencing token** (a per-job counter incremented for every attempt). Reports from non-current attempts are rejected. Handlers may pass the token downstream where a system supports fencing.
4. **Opt-in at-most-once** per job type. Such jobs are never re-dispatched after assignment. A lost or timed-out attempt moves the job to `FAILED` with reason `OUTCOME_UNKNOWN` for manual review.
5. The platform **never claims exactly-once**.

### How duplicates happen (documented for handler authors)

- A worker completes the side effect, then crashes before `Complete` is recorded. The job is retried.
- A worker is partitioned and its session expires. The job runs elsewhere while the original is still running (a zombie), until the worker self-fences.
- A handler ignores cancellation or its deadline, and the timeout triggers a retry while it is still running.
- An operator manually retries or re-drives a job that had in fact taken effect.
- A client retries a submission without an `Idempotency-Key`, or after its 24 h retention.

## Trade-offs

- Handler authors carry the burden of idempotency.
- At-most-once jobs may be lost on crashes, by design.

## Consequences

- The SDK exposes `job_id`, `attempt_id` and the fencing token to every handler, and its documentation states the idempotency contract.
- Tests inject duplicate executions (killed workers, partitions) and check that platform state stays correct.
- `stale_completions_rejected_total` makes zombie activity visible.

## Revisit when

- A class of jobs keeps its side effects in the platform's own database. A transactional-completion option ("exactly-once" within one store) could then be offered for those jobs.
