# ADR-008: Retry strategy

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [HLD §14.3](../architecture.md#143-retry-flow), [ADR-001](ADR-001-scheduler-architecture.md), [ADR-007](ADR-007-execution-semantics.md)

## Context

Attempts fail for different reasons:

- job errors: bad input or bugs;
- transient downstream errors: timeouts, `5xx` responses, rate limits;
- infrastructure errors: worker crashes, lost leases;
- timeouts;
- poison jobs that crash every worker that runs them.

Retrying wrongly either wastes capacity or amplifies outages into retry storms.

## Problem

How are failures classified, when are jobs retried, how long do they wait, and what happens when retries run out?

## Options considered

| Topic | Options |
|---|---|
| Where retries happen | Immediately inside the worker · **re-queued as a delayed job** |
| Delay | Fixed · exponential · **exponential with full jitter** · decorrelated jitter |
| Classification | Everything retryable · **typed outcomes from handlers** · rules in platform configuration |
| Terminal failure | Separate dead-letter table · **a job state (`DEAD_LETTERED`)** |

## Decision

- **Retries are re-queued as delayed jobs.** A retryable failure moves the job to `RETRY_PENDING` with `run_at = now + delay`, and the promoter makes it `READY` when due ([ADR-001](ADR-001-scheduler-architecture.md)). The decision is made in the transaction that records the failed attempt.
- **Default policy:**
  - exponential backoff with full jitter: `delay = random(0, min(1 h, 10 s × 2^(n−1)))`;
  - at most **10 attempts** (retries span up to about 85 minutes);
  - an overall **24 h** ceiling.

  Job types and individual jobs can override these within platform limits. Fixed and plain exponential policies are also available.
- **Handler outcomes:** success; retryable failure (optionally with a retry-after that is honoured up to the cap and the overall deadline); non-retryable failure.
- **Unclassified exceptions are retryable.** It is safer for transient faults, at the cost of some wasted attempts on bugs.
- **Timeouts are retryable.**
- **Lost attempts count as attempts.** A lost attempt is a worker crash or expired lease. Counting it stops a poison job from crashing workers forever. After repeated losses the job is dead-lettered with reason `POISON`.
- **Terminal states:**
  - `FAILED` for non-retryable errors;
  - `DEAD_LETTERED` when attempts are exhausted, the overall deadline passes, or the job is poison.

  Both are job states, not a separate table, so no rows move, references stay intact and history is preserved.
- **Re-drive:** an operator can retry one job or bulk re-drive by filter. Each re-drive adds a new attempt to the same job and starts a fresh retry budget.
- **Downstream outages:** V1 relies on jittered backoff plus manually pausing a job type or pool. Automatic circuit breaking (pause on a failure-rate spike, probe before resuming) is designed for and built later.

## Trade-offs

- Treating unclassified errors as retryable wastes attempts on deterministic bugs, until handlers classify them.
- Full jitter makes individual retry times unpredictable. That is the price of avoiding synchronized retry waves.
- Without a circuit breaker, a long downstream outage consumes each job's retry budget and dead-letters jobs that bulk re-drive must recover.

## Consequences

- Retry behaviour is fully visible as state (`RETRY_PENDING`, attempts, reasons) and metrics (`jobs_retried_total`, `jobs_dead_lettered_total`).
- The retry engine is pure domain logic (policy in, next action out) and is unit-tested exhaustively.

## Revisit when

- Downstream outages repeatedly dead-letter large numbers of jobs, which justifies the circuit breaker.
- Handlers need declarative classification rules (for example, HTTP status → retryable).
