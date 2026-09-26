# Distributed Job Scheduler — Low-Level Design

Implements the [HLD](architecture.md). This document grows phase by phase, and each section is written before the code it describes ([implementation plan](implementation-plan.md)).

| Section | Phase | Status |
|---|---|---|
| [1. Codebase structure and conventions](#1-codebase-structure-and-conventions) | 1 | Written |
| [2. Process lifecycle](#2-process-lifecycle) | 1 | Written |
| [3. Domain model](#3-domain-model) | 2 | Written (persisted fields in §8.2) |
| [4. Job state machine](#4-job-state-machine) | 2 | Written |
| [5. Attempt state machine](#5-attempt-state-machine) | 2 | Written |
| [6. Retry decision](#6-retry-decision) | 2 | Written |
| [7. Priority selection](#7-priority-selection) | 2 | Written |
| [8. Persistence](#8-persistence) | 3 | Written |
| API contracts, error model, OpenAPI | 4 | Planned |
| Scheduler internals | 6 | Planned |
| Leases and fencing | 7 | Planned |
| Worker protocol and dispatcher internals | 8 | Planned |
| Recovery: reaper, timeouts, re-drive | 9 | Planned |
| Events and outbox | Later | Planned |

The [HLD's open questions](architecture.md#appendix-c--open-questions-for-the-lld) are resolved in the phase that needs them: 1–5 in phase 3 (§8.1), 6 and 12 in phase 4, 7–9 in phase 8, 10 and 11 in phase 6, 13 in phase 10, 14 in phase 4.

---

## 1. Codebase structure and conventions

```
cmd/jobscheduler/              server binary; roles api and engine
internal/app/                  process lifecycle: components, startup, graceful shutdown
internal/config/               environment configuration
internal/domain/               pure domain logic: states, transitions, retry, priority
internal/health/               liveness and readiness
internal/httpserver/           HTTP server component
internal/observability/        logging (telemetry in phase 11)
internal/persistence/postgres/ PostgreSQL adapters
docs/                          HLD, LLD, ADRs, implementation plan
```

Later phases add `internal/api`, `internal/scheduling`, `internal/dispatch`, `internal/recovery`, `internal/coordination`, `internal/events`, plus `pkg/workerproto` and `pkg/workersdk` (in `pkg/` because external worker code imports them) and `cmd/demo-worker`.

**Conventions**

| Topic | Rule |
|---|---|
| Dependencies | `domain` imports only the standard library, enforced by a test. Application packages depend on `domain` and on interfaces (ports). Adapters implement those interfaces. |
| Errors | Wrap with `%w` and context. Domain conditions use sentinel errors (for example `ErrInvalidTransition`). No panics outside programmer errors at startup. |
| Context | Every blocking call takes `context.Context` as its first parameter. |
| Time | Domain functions receive `now` as a parameter and never call `time.Now()`. From phase 3, engine code uses the database clock for correctness decisions. |
| Randomness | Injected as `func() float64`, so jitter is deterministic in tests. |
| Logging | `log/slog`, JSON in production, snake_case keys, the IDs from [HLD §17.1](architecture.md#171-identifiers). Never payloads, results, secrets or connection strings. |
| Configuration | `JS_*` environment variables only. Validated at startup, reporting every error at once. |
| Tests | Table-driven. `testing/synctest` for time-dependent concurrency. From phase 3, integration tests run against real PostgreSQL when `JS_TEST_DATABASE_URL` is set. |

## 2. Process lifecycle

### 2.1 Components

Everything long-running in a process is a component:

```go
type Component interface {
    Name() string
    Run(ctx context.Context) error // blocks; returns nil after a clean stop once ctx is cancelled
}
```

`app.App` runs all components concurrently. If a component returns before shutdown began, whether with an error or with `nil`, that counts as a failure: every other component is stopped and the process exits non-zero.

| Role | Components (phase 1) | Added later |
|---|---|---|
| all | `ops` HTTP server (health) | Telemetry exporters (11) |
| `api` | n/a | REST server (4) |
| `engine` | n/a | Materializer, promoter (6); lease manager (7); dispatcher and worker-protocol server (8); reaper (9) |

### 2.2 Startup

1. Load and validate configuration, failing with every error listed.
2. Build the logger, tagged with service, version and roles.
3. Create the database pool. It connects lazily, so the process starts even while PostgreSQL is still starting, and readiness reports the database state.
4. Register readiness checks (`postgres`).
5. Bind every listener before starting anything, so a port conflict fails fast.
6. Run the components.

### 2.3 Shutdown

1. `SIGTERM` or `SIGINT` arrives.
2. Readiness switches to `503 draining`, so load balancers stop routing to the process.
3. The process keeps serving for `JS_SHUTDOWN_DELAY`, giving load balancers time to notice.
4. Every component's context is cancelled. HTTP servers drain in-flight requests within `JS_SHUTDOWN_TIMEOUT`.
5. The database pool closes and the process exits with 0, or with 1 if a component failed.

If a component fails, shutdown starts immediately at step 2, skipping the delay.

### 2.4 Health endpoints (ops port)

| Endpoint | Behaviour |
|---|---|
| `GET /livez` | `200 {"status":"ok"}` while the process can serve. Dependencies are not checked, to avoid restart storms during a database outage. |
| `GET /readyz` | Runs every check concurrently with a 2 s timeout. Returns `200 {"status":"ready","checks":{...}}` or `503` with `not_ready` or `draining`. Check results are `ok` or `failing`; details go only to logs. |

### 2.5 Configuration

| Variable | Default | Meaning |
|---|---|---|
| `JS_ROLES` | `api,engine` | Comma-separated roles to run |
| `JS_OPS_ADDR` | `:9090` | Health (and later metrics) listener |
| `JS_DATABASE_URL` | required | PostgreSQL connection string |
| `JS_DB_MAX_CONNS` | `10` | Pool size, 1–1000 |
| `JS_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `JS_LOG_FORMAT` | `json` | `json` or `text` |
| `JS_SHUTDOWN_DELAY` | `0s` | Time to keep serving after a signal while readiness fails; set to about 5 s behind a load balancer |
| `JS_SHUTDOWN_TIMEOUT` | `30s` | Maximum time for each component to stop |

## 3. Domain model

The entities from [HLD §9.4](architecture.md#94-conceptual-data-model). Only the fields that phase 2 logic depends on are listed here; the persisted fields are in [§8.2](#82-tables-phase-3-migration).

| Entity | Fields used by the domain logic |
|---|---|
| Job | `state`; `run_at`; `priority`; `attempt_count` (the latest attempt's number, which is also its fencing token); retry budget (`budget_attempts`, `budget_lost`, `budget_started_at`); `retry_policy` (resolved at submission); `deadline` (optional explicit overall deadline); `start_deadline`; `cancel_requested_at`; `at_most_once` (copied from the job type) |
| Attempt | `number` (= fencing token); `state`; `session_id`; `started_at`; `deadline`; `finished_at`; error summary |
| JobType | Default priority, timeouts and retry policy; `at_most_once`; pool; payload schema |
| Schedule | Trigger; misfire and overlap policies; `next_fire_at` (phase 6) |

**Retry budget.** Each job tracks attempts made, lost attempts and the start of the first attempt since the budget was last reset. The budget resets on manual retry or re-drive. `attempt_count` never resets, because fencing tokens must only ever increase.

## 4. Job state machine

This implements the lifecycle from [HLD §10.2](architecture.md#102-job-lifecycle). The table below is the complete set of legal transitions; **anything not listed is invalid**, and the code enforces the same table (`domain.ValidateJobTransition`). "(new)" is the pseudo-state a job is created from.

| # | From | To | Allowed actors | Trigger |
|---|---|---|---|---|
| T1 | (new) | `SCHEDULED` | api, materializer | Created with a future `run_at` |
| T2 | (new) | `READY` | api | Created due now (including a schedule trigger) |
| T3 | `SCHEDULED` | `READY` | promoter | `run_at` reached |
| T4 | `RETRY_PENDING` | `READY` | promoter | Backoff elapsed |
| T5 | `READY` | `RUNNING` | dispatcher | Assigned to a worker |
| T6 | `RUNNING` | `SUCCEEDED` | dispatcher | Attempt succeeded |
| T7 | `RUNNING` | `RETRY_PENDING` | dispatcher, reaper | Retry decision: retry |
| T8 | `RUNNING` | `FAILED` | dispatcher, reaper | Retry decision: non-retryable, or at-most-once |
| T9 | `RUNNING` | `DEAD_LETTERED` | dispatcher, reaper | Retry decision: exhausted, deadline or poison |
| T10 | `RUNNING` | `CANCELLED` | dispatcher, reaper | Attempt ended after a cancel request |
| T11 | `SCHEDULED` | `PAUSED` | api | Hold |
| T12 | `READY` | `PAUSED` | api | Hold |
| T13 | `PAUSED` | `SCHEDULED` | api | Resume, or run now |
| T14 | `SCHEDULED` | `CANCELLED` | api | Cancel, or withdrawn by a schedule change |
| T15 | `READY` | `CANCELLED` | api | Cancel |
| T16 | `PAUSED` | `CANCELLED` | api | Cancel |
| T17 | `RETRY_PENDING` | `CANCELLED` | api | Cancel |
| T18 | `SCHEDULED` | `EXPIRED` | promoter, dispatcher | Start deadline passed |
| T19 | `READY` | `EXPIRED` | promoter, dispatcher | Start deadline passed |
| T20 | `SCHEDULED` | `SKIPPED` | promoter | Overlap policy |
| T21 | `FAILED` | `READY` | api | Manual retry (resets the budget) |
| T22 | `DEAD_LETTERED` | `READY` | api | Re-drive (resets the budget) |

**Properties verified by tests**
- Every state is reachable from (new).
- From every non-terminal state, some terminal state is reachable. There are no traps, which supports invariant I4.
- `RUNNING` is entered only from `READY`, only by the dispatcher (supports I3).
- Terminal states have no outgoing transitions, except the api's manual retry out of `FAILED` and `DEAD_LETTERED`.

**Concurrency.** Validation is necessary but not sufficient. The persistence layer (phase 3) executes each transition as `UPDATE … WHERE id = $1 AND state = $from [AND guards]`. Zero rows affected means a concurrent actor won; the caller re-reads the row and reports the current state.

**Run now** doesn't change state for a `SCHEDULED` job; it sets `run_at = now()` and the promoter handles it. A `PAUSED` job resumes to `SCHEDULED` with `run_at = now()` (T13).

## 5. Attempt state machine

`RUNNING` is the only non-terminal attempt state.

| From | To | Allowed actors | Trigger |
|---|---|---|---|
| `RUNNING` | `SUCCEEDED` | dispatcher | `Complete(success)` |
| `RUNNING` | `FAILED` | dispatcher | `Complete(failure)`, retryable or not |
| `RUNNING` | `CANCELLED` | dispatcher | `Complete(cancelled)` |
| `RUNNING` | `TIMED_OUT` | dispatcher, reaper | Worker reported a timeout, or the engine backstop hit deadline + grace |
| `RUNNING` | `LOST` | dispatcher, reaper | Session expired, or the assignment could not be delivered |

## 6. Retry decision

`domain.DecideAfterAttempt` is a pure function run in the transaction that records an attempt's outcome ([ADR-008](decisions/ADR-008-retry-strategy.md)).

**Inputs:** the attempt outcome (terminal attempt state, a retryable flag for failures, an optional retry-after hint); the job's retry policy and budget (attempts and lost attempts including this one, start of the budget); the optional explicit deadline; whether cancellation was requested; whether the job type is at-most-once; `now` (database time); and a random source.

**Rules, applied in order (first match wins):**

| # | Condition | Next state | Reason |
|---|---|---|---|
| 1 | Attempt `SUCCEEDED` | `SUCCEEDED` | n/a |
| 2 | Cancellation requested, or attempt `CANCELLED` | `CANCELLED` | `CANCELLED` |
| 3 | Attempt `FAILED` and not retryable | `FAILED` | `NON_RETRYABLE` |
| 4 | Job type is at-most-once | `FAILED` | `OUTCOME_UNKNOWN` (lost or timed out) or `AT_MOST_ONCE` |
| 5 | Attempt `LOST` and lost attempts ≥ `max_lost_attempts` | `DEAD_LETTERED` | `POISON` |
| 6 | Attempts ≥ `max_attempts` | `DEAD_LETTERED` | `ATTEMPTS_EXHAUSTED` |
| 7 | `now + delay` is after the effective deadline | `DEAD_LETTERED` | `DEADLINE_EXCEEDED` |
| 8 | Otherwise | `RETRY_PENDING` with `run_at = now + delay` | `RETRYABLE_ERROR`, `TIMED_OUT` or `LOST` |

**Delay:**
- A handler's retry-after hint is used as given, capped at `max_delay`, with no jitter.
- Otherwise the delay comes from backoff:
  - *exponential:* `d = min(max_delay, initial_delay × multiplier^(n−1))`, where *n* counts attempts in the current budget;
  - *fixed:* `d = initial_delay`;
  - *full jitter:* the result is then uniform in `[0, d)`.

**Effective deadline:** the earlier of `budget_started_at + max_retry_duration` and the job's explicit deadline, if it has one.

**Defaults and platform limits:**

| Setting | Default policy | Platform limit |
|---|---|---|
| Strategy / jitter | exponential / full | n/a |
| `initial_delay` | 10 s | ≥ 1 s |
| `multiplier` | 2 | 1–10 (exponential only) |
| `max_delay` | 1 h | ≤ 24 h, and ≥ `initial_delay` |
| `max_attempts` | 10 | 1–50 |
| `max_retry_duration` | 24 h | ≤ 7 days |
| `max_lost_attempts` | 3 | 1 to `max_attempts` |

## 7. Priority selection

The dispatcher (phase 8) picks which priority class to serve next with **smooth weighted round-robin**. It uses weights `CRITICAL:HIGH:NORMAL:LOW = 8:4:2:1` by default, and considers **only the classes that currently have eligible work**:

1. For each class with work, add its weight to that class's running credit.
2. Pick the class with the highest credit, breaking ties by priority, highest first.
3. Subtract the total weight of the classes that took part from the chosen class's credit.

**Properties verified by tests**
- With every class busy, each cycle of 15 picks contains exactly 8, 4, 2 and 1 picks respectively, spread evenly rather than bunched. Credits return to zero at the end of each cycle, so every cycle repeats the same order.
- It is work-conserving: when a class has no work, the other classes share its turns in proportion to their weights.
- There is no starvation: while eligibility stays the same, a busy class never waits longer than one cycle (*Σweights* picks).
- Classes with no work keep their credit unchanged, the same as nginx's implementation.

## 8. Persistence

### 8.1 Storage layout

**Decision.** Active and finished jobs live in separate tables:
- `jobs` holds only non-terminal jobs (`SCHEDULED`, `READY`, `RUNNING`, `RETRY_PENDING`, `PAUSED`). It is not partitioned.
- `job_history` holds terminal jobs and is range-partitioned by day on `finished_at`.

A job moves to `job_history` in the same transaction as its terminal transition (`DELETE … RETURNING` feeding an `INSERT`), and moves back on manual retry.

**Why:**
- **Retention can't delete live work.** Retention drops whole daily partitions, with no delete churn. With one time-partitioned jobs table, dropping old partitions could delete a job that was created long ago but is still scheduled for the future.
- **The queue stays small.** Queue indexes cover only active jobs, and vacuum work stays proportional to active work.
- **Dedupe is a plain index.** "Unique among active jobs" for `dedupe_key` is a partial unique index on `jobs`.

**Costs:**
- A terminal transition is a delete plus an insert rather than an update. The WAL volume is similar.
- Fetching a job by ID checks two tables in one statement.
- No foreign keys can point at job rows, because they move.

**Attempts** are append-only: one row is written when an attempt *ends*, in `attempts`, range-partitioned by day on `finished_at`. While an attempt runs, it lives in the job row's `current_attempt_*` columns. A claim therefore touches only the job row, and each attempt costs one insert.

**Resolved open questions ([HLD Appendix C](architecture.md#appendix-c--open-questions-for-the-lld)):**

| # | Question | Decision |
|---|---|---|
| 1 | Promotion step or direct claiming? | Keep promotion. The explicit `READY` state keeps state-machine transitions T3/T5 and the scheduling-lag metric. The ready set is a partial index, so no separate ready table is needed. |
| 2 | Partitioning and indexes | As above. Daily partitions with a default partition as a safety net: a missing partition must never block a terminal transition. The indexes are listed in §8.2. |
| 3 | How attempts are stored | Current attempt as columns on the job row; finished attempts as append-only rows. |
| 4 | Separate payload table? | Not in V1. Payloads (≤ 64 KB) stay on the job row, stored as `json` so the submitted text is preserved exactly. Revisit once storage has been measured. |
| 5 | Row-level security? | Not in V1. Every tenant-facing repository method takes the tenant ID as a required argument, and tests cover cross-tenant access. Revisit in phase 12. |

### 8.2 Tables (phase 3 migration)

All timestamps are `timestamptz`. Durations are stored as `bigint` milliseconds. IDs are UUIDv7, generated by the application; attempt IDs are v4, generated at claim time.

| Table | Key columns | Notes |
|---|---|---|
| `tenants` | `id` PK, `name` unique | |
| `job_types` | PK `(tenant_id, name)` | `version`, `pool`, `default_priority`, `attempt_timeout_ms`, `retry_policy` (jsonb), `at_most_once`, `payload_schema`, `enabled` |
| `jobs` | `id` PK; FK `(tenant_id, job_type)` → `job_types` | Submission fields, lifecycle fields, retry budget, `current_attempt_*`, `cancel_requested_at`. `state` is limited to the active states. |
| `job_history` | PK `(id, finished_at)`; partitioned by day | The same fields as `jobs` minus `current_attempt_*`, plus `finished_at`, `reason`, `result`. `state` is limited to terminal states. |
| `attempts` | PK `(job_id, number, finished_at)`; partitioned by day | `id`, `session_id`, `state` (terminal), `retryable`, `error`, `started_at`, `deadline`, `actor` |
| `idempotency_keys` | PK `(tenant_id, operation, key)` | `request_hash` (SHA-256), `resource_id`, `expires_at` |

Later phases add `audit_log` and `api_keys` (4), `schedules` and the `schedule_fires` ledger (6), `leases` (7), `pools` and `worker_sessions` (8), and `operations` (9).

**Constraints on `jobs`, enforcing invariants in the database as well as in code:**
- `(state = 'RUNNING') = (current_attempt_id IS NOT NULL)`: a job is running exactly when it has a current attempt.
- A current attempt must have its session, start time and deadline set.
- `(schedule_id IS NULL) = (fire_time IS NULL)`.
- `labels` must be a JSON object, and `priority` must be between 1 and 4.

**Indexes on `jobs`:**

| Index | Used by |
|---|---|
| `(pool, priority, run_at, id) WHERE state = 'READY'` | Dispatcher claims, oldest first within a class |
| `(run_at) WHERE state IN ('SCHEDULED', 'RETRY_PENDING')` | Promoter |
| `(current_session_id) WHERE state = 'RUNNING'` | Reaper: attempts of an expired session |
| `(attempt_deadline) WHERE state = 'RUNNING'` | Reaper: engine-side timeouts |
| unique `(tenant_id, dedupe_key) WHERE dedupe_key IS NOT NULL` | Dedupe among active jobs |
| `(tenant_id, created_at DESC, id DESC)` | Listing |

`job_history` has a `(tenant_id, finished_at DESC, id DESC)` index, plus its primary key for lookups by ID. Indexes for filtering by label come with the list API in phase 4.

**Partitions.** `EnsurePartitions(from, days)` creates daily partitions ahead of time for `job_history` and `attempts`. From phase 9, a singleton maintenance duty runs it daily, drops partitions older than the retention period, and alerts if rows ever land in a default partition.

**Connection settings** (applied at connection start):
- `lock_timeout = 5s`
- `statement_timeout = 30s`
- `idle_in_transaction_session_timeout = 60s`
- `application_name = jobscheduler`

### 8.3 Concurrency control

The isolation level is `READ COMMITTED` throughout. Every transition is first checked with `domain.ValidateJobTransition`, then executed with a guard that the database enforces.

| Operation | Transaction | Guard | What a concurrent actor sees |
|---|---|---|---|
| **Submit** | Insert idempotency key → insert job (`ON CONFLICT` on dedupe) | Unique indexes | A duplicate request replays the stored result; a duplicate dedupe key returns the active job |
| **Claim** | `SELECT … FOR UPDATE SKIP LOCKED` in a CTE feeding an `UPDATE` → `RUNNING`, attempt number + 1, new attempt ID | Only `READY` rows that aren't locked | Other claimers skip locked rows, so no job is claimed twice |
| **Complete** | Lock the job row → check state, attempt number (fencing token) and attempt ID → retry decision (§6) using the database's `now()` → insert the attempt row → update to `RETRY_PENDING`, or move to history | Row lock + fencing token | A stale token gets `ErrStaleAttempt`. A repeated completion with the same outcome is a replay. |
| **Cancel** | Lock the job row → not started: move to history as `CANCELLED`; running: set `cancel_requested_at`; already cancelled: no-op; other terminal state: invalid | Row lock | If cancel holds the lock, claims skip the job. If a claim holds it, cancel waits, sees `RUNNING` and sets the flag. |

Order of checks for a completion that fails its guard:
1. If an attempt row with the same number and outcome exists, it is a duplicate `Complete` and is treated as a replay (HLD S9).
2. Otherwise, if the job doesn't exist at all, the result is `ErrNotFound`.
3. Otherwise, the result is `ErrStaleAttempt` (HLD S4).

### 8.4 Idempotency

1. Generate the job ID, then insert `(tenant, operation, key, request_hash, resource_id, expires_at = now() + 24 h)` with `ON CONFLICT DO NOTHING`.
2. If there was a conflict, lock the existing row:
   - **expired:** take it over with the new hash and resource, and continue;
   - **different hash:** `ErrIdempotencyKeyReused` (API `422`);
   - **same hash:** replay by returning the job named by `resource_id`.
3. A concurrent duplicate blocks on the first request's uncommitted unique-index entry. PostgreSQL makes it wait, and when the first commits it takes the replay path. If the wait exceeds `lock_timeout`, the result is `ErrIdempotencyInProgress` (API `409`).
4. If the dedupe key matches an existing active job, `resource_id` is pointed at that job, so retries replay it.

Expired rows are deleted in batches by maintenance (phase 9). At the top of tier M this table holds at most about one day of keys.

### 8.5 Repository API

`postgres.Store` implements the operations above for phase 3. Later phases add promotion, materialization, lease-guarded claims, reaping and manual retry.

| Method | Returns |
|---|---|
| `SubmitJob(ctx, NewJob, *Idempotency)` | The job, plus whether it was created, replayed or deduplicated |
| `GetJob(ctx, tenant, id)` | The job from `jobs` or `job_history`; `ErrNotFound` otherwise, including for another tenant's job |
| `ClaimReady(ctx, ClaimRequest)` | Up to *n* jobs from one pool and priority class, now `RUNNING` |
| `CompleteAttempt(ctx, Completion)` | The job after the transition, the retry decision, and whether it was a replay |
| `RequestCancel(ctx, tenant, id)` | The job after cancellation, or after the cancel was flagged |
| `EnsurePartitions(ctx, from, days)` | Creates partitions if they don't exist; idempotent |

Domain errors live in `domain`: `ErrNotFound`, `ErrStaleAttempt`, `ErrIdempotencyKeyReused`, `ErrIdempotencyInProgress`, and `ErrInvalidTransition` (wrapped with the current state).

### 8.6 Migrations

- `goose` applies embedded SQL files. It uses its **table-based** lock, not a session advisory lock, so concurrent migration runs serialize even through connection poolers.
- Migrations run with `jobscheduler migrate`: as a deploy step in production, and as a one-shot `migrate` service that runs before the platform in `docker compose`.

### 8.7 Integration tests

- Tests run against real PostgreSQL 17, either the server in `JS_TEST_DATABASE_URL` or an embedded one that is downloaded once and cached.
- Each test gets a fresh database cloned from a migrated template (`CREATE DATABASE … TEMPLATE`), so tests are isolated and can run in parallel.
- `go test -short` skips them.
