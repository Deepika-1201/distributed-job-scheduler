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
| [9. Job API](#9-job-api) | 4 | Written |
| [10. Scheduler](#10-scheduler) | 6 | Written |
| [11. Coordination](#11-coordination) | 7 | Written |
| [12. Worker system](#12-worker-system) | 8 | Written |
| [13. Recovery and maintenance](#13-recovery-and-maintenance) | 9 | Written |
| [14. Platform administration](#14-platform-administration) | 12 | Written |
| [15. Quotas and load shedding](#15-quotas-and-load-shedding) | 12 | Written |
| [16. Payload schemas and listing details](#16-payload-schemas-and-listing-details) | 12 | Written |
| [17. Observability](#17-observability) | 11 | Written |
| [18. Pool backlog](#18-pool-backlog) | 11 | Written |
| [19. Load-test gate](#19-load-test-gate) | 5 | Written |
| [20. Security hardening](#20-security-hardening) | 12 | Written |
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
| `JS_OPS_ADDR` | `:9090` | Health and metrics (`/metrics`) listener |
| `JS_DATABASE_URL` | required | PostgreSQL connection string |
| `JS_DB_MAX_CONNS` | `10` | Pool size, 1–1000 |
| `JS_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `JS_LOG_FORMAT` | `json` | `json` or `text` |
| `JS_OTLP_ENDPOINT` | empty | `host:port` of an OTLP/gRPC collector; empty turns trace export off (§17.1) |
| `JS_OTLP_INSECURE` | `false` | Plaintext to the collector, for local stacks |
| `JS_TRACE_SAMPLE_RATIO` | `1` | Share of root traces sampled, 0–1; child spans follow their parent |
| `JS_BACKLOG_TARGET` | `5m` | Backlog target of pools without their own, 10 s–24 h (§18); `JS_SHED_LOW_AFTER` and `JS_SHED_NORMAL_AFTER` are rejected |
| `JS_SHUTDOWN_DELAY` | `0s` | Time to keep serving after a signal while readiness fails; set to about 5 s behind a load balancer |
| `JS_SHUTDOWN_TIMEOUT` | `30s` | Maximum time for each component to stop |
| `JS_TLS_CERT_FILE` / `JS_TLS_KEY_FILE` | empty | PEM certificate and key, both or neither; turn on TLS for the API and worker servers, reloaded when the files change (§20.3) |

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
| T15 | `READY` | `CANCELLED` | api, promoter | Cancel; superseded by a newer run (`cancel_previous`) |
| T16 | `PAUSED` | `CANCELLED` | api | Cancel |
| T17 | `RETRY_PENDING` | `CANCELLED` | api, promoter | Cancel; superseded by a newer run (`cancel_previous`) |
| T18 | `SCHEDULED` | `EXPIRED` | promoter, dispatcher | Start deadline passed |
| T19 | `READY` | `EXPIRED` | promoter, dispatcher | Start deadline passed |
| T20 | `SCHEDULED` | `SKIPPED` | promoter | Overlap policy |
| T21 | `FAILED` | `READY` | api | Manual retry (resets the budget) |
| T22 | `DEAD_LETTERED` | `READY` | api | Re-drive (resets the budget) |
| T23 | `RUNNING` | `READY` | dispatcher | Release of an assignment that was never delivered or never started (§12.3); the budget is refunded |

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

## 9. Job API

The contract is written as OpenAPI in [`api/openapi.yaml`](../api/openapi.yaml). A test checks that every registered route is documented, and that every documented route is registered.

### 9.1 Conventions

- JSON over HTTP(S), base path `/v1`, authenticated with `Authorization: Bearer <api key>`.
- Timestamps are RFC 3339. Durations are Go duration strings (`"30s"`, `"1h"`). Priorities are names (`"HIGH"`).
- Request bodies are limited to 1 MiB and payloads to 64 KiB. Unknown JSON fields are rejected, so typos fail loudly.
- The request ID is taken from `X-Request-Id` if it is a safe string of up to 64 characters; otherwise one is generated. It is echoed back, logged, and written to audit records.
- **Pagination:** `?limit=` (1–200, default 50) and an opaque `?cursor=`. Responses have the form `{"items": [...], "next_cursor": "..."}`.

### 9.2 Errors

The body is always `{"error": {"code", "message", "request_id", "details"}}`. Internal error details are logged, never returned.

| HTTP | `code` | When |
|---|---|---|
| 400 | `invalid_json` | The body is not valid JSON, or has unknown fields |
| 401 | `unauthenticated` | The key is missing, malformed, unknown, expired or revoked |
| 403 | `permission_denied` | The key's role is too low (including `CRITICAL` priority below operator) |
| 404 | `not_found` | The resource doesn't exist, or belongs to another tenant |
| 409 | `conflict` | The transition is illegal for the current state (`details.state` holds it), the job type already exists, or a dedupe key is taken |
| 409 | `idempotency_in_progress` | The same idempotency key is still being processed |
| 413 | `payload_too_large` | The body or payload is over its limit |
| 422 | `invalid_argument` | Validation failed; `details.fields` maps each field to its problem |
| 422 | `idempotency_key_reused` | The same key was sent with a different body |
| 429 | `rate_limited` | The tenant's rate limit was hit; `Retry-After` is set |
| 500 | `internal` | An unexpected error, logged with the request ID |

### 9.3 Authentication and authorization

- **Key format:** `jsk_<prefix>_<secret>`, where the prefix is 8 random characters and the secret is 32 random bytes. The database stores the prefix and a SHA-256 hash of the whole key, and the key is shown once at creation.
- **Verification:** look up by prefix, then compare hashes in constant time. A successful check is cached in-process for 30 s, so a revoked key stops working within 30 s.
- **Roles** are hierarchical: `viewer` < `submitter` < `operator` < `admin` < `platform-admin`. Each key has one role and belongs to one tenant, and the tenant comes only from the key. A key can create keys up to its own role. `platform-admin` keys live in a dedicated platform tenant ([ADR-017](decisions/ADR-017-platform-administration.md)).
- **Bootstrap:** `jobscheduler bootstrap <tenant> [admin|platform-admin]` creates a tenant and its first key.

| Method | Path | Minimum role |
|---|---|---|
| `GET` | every read endpoint | viewer |
| `POST` | `/v1/jobs`, `/v1/jobs/{id}/cancel` | submitter |
| `POST` | `/v1/jobs/{id}/pause`, `/resume`, `/run`, `/retry` | operator |
| `POST` / `PATCH` | `/v1/job-types`, `/v1/job-types/{name}` | admin |
| `POST` / `DELETE` | `/v1/api-keys`, `/v1/api-keys/{id}` | admin |
| any | `/v1/workers`, `/v1/pools` and their actions | platform-admin (§14) |

### 9.4 Submitting a job

1. The job type must exist and be enabled; otherwise `422` on `type`.
2. Priority, attempt timeout and retry policy default to the job type's values. A request can override individual retry-policy fields, and the result is validated against the platform limits (§6).
3. `run_at` or `delay` may be given, but not both. Neither means "now".
4. Labels: up to 16, with keys up to 64 characters and values up to 256. `dedupe_key` is up to 255 characters. The payload defaults to `{}`.
5. With an `Idempotency-Key` header, the request hash is the SHA-256 of the body.

| Outcome | Status | `X-Submission-Outcome` |
|---|---|---|
| Created | `201` | `created` |
| Retry of an earlier request with the same key | `200` | `replayed` |
| An active job with the same `dedupe_key` exists | `200` | `deduplicated` |

### 9.5 Lifecycle operations

| Operation | Allowed from | Effect |
|---|---|---|
| Cancel | Not started / running / cancelled | Moves to `CANCELLED`, flags a running job, or is a no-op |
| Pause | `SCHEDULED`, `READY` | → `PAUSED` |
| Resume | `PAUSED` | → `SCHEDULED`; the promoter makes it `READY` when due |
| Run now | `SCHEDULED`, `PAUSED` (a no-op on `READY`) | `run_at = now()`; a paused job is resumed |
| Retry | `FAILED`, `DEAD_LETTERED` | → `READY` with a fresh retry budget. `attempt_count` is kept, because fencing tokens must keep increasing. |

Any other starting state gets `409` with the current state. Each operation writes its `audit_log` row in the same transaction (I5).

### 9.6 Admission control (phase 4 part)

- Each `api` node runs a token bucket per tenant. The rate is `JS_TENANT_RATE_LIMIT ÷ JS_API_REPLICAS` per second (default 500 ÷ 1), with a burst of twice that ([ADR-011](decisions/ADR-011-caching-and-redis.md)).
- Pending-job quotas and priority-based shedding: see §15.

### 9.7 Listing jobs

- `GET /v1/jobs` can filter by `state`, `type` and `label=key:value`.
- It reads `jobs` and `job_history` with the same filter, ordered by `(created_at, id)` descending. It fetches `limit + 1` rows from each, merges them, and encodes the cursor from the last row returned.
- An active-state filter reads only `jobs`, and a terminal-state filter reads only `job_history`.

## 10. Scheduler

Implements HLD §11 and [ADR-001](decisions/ADR-001-scheduler-architecture.md). Trigger evaluation and fire planning are pure functions in `domain`. The store runs them inside row-locked transactions, and the `scheduling` package drives the loops.

### 10.1 Tables (phase 6 migration)

| Table | Purpose | Key points |
|---|---|---|
| `schedules` | Definition and cursor | `trigger_kind` is `cron`, `fixed_rate` or `fixed_delay`. `next_fire_at` is the cursor; it is `NULL` when completed, or while a fixed-delay run is active. `state` is `ACTIVE`, `PAUSED`, `COMPLETED` or `DELETED` (soft delete). Names are unique per tenant among non-deleted schedules. |
| `schedule_fires` | Ledger: one row per `(schedule_id, fire_time)` ever materialized | Primary key `(schedule_id, fire_time)` enforces I2 even after jobs move to `job_history`. Rows are deleted only when their job is withdrawn, or by retention. |

Index `schedules (next_fire_at) WHERE state = 'ACTIVE'` serves the materializer. Two indexes serve the promoter: `jobs (schedule_id, fire_time) WHERE schedule_id IS NOT NULL`, for overlap checks, and `jobs (start_deadline) WHERE state IN ('SCHEDULED', 'READY') AND attempt_count = 0`, for expiry.

### 10.2 Triggers

Decision record for cron evaluation: [ADR-013](decisions/ADR-013-cron-evaluation.md).

- **Cron:** 5 fields (minute, hour, day of month, month, day of week) or 6 (seconds first).
  - Syntax: `*`, `?` (same as `*`), values, ranges `a-b`, steps `*/n`, `a/n` and `a-b/n`, lists, and `JAN`–`DEC` and `SUN`–`SAT` names. Day of week 7 also means Sunday.
  - Macros: `@yearly` (`@annually`), `@monthly`, `@weekly`, `@daily` (`@midnight`) and `@hourly`.
  - Day matching follows Vixie cron: if both day fields are restricted, a day matches when *either* does; otherwise both must.
  - `L`, `W` and `#` are rejected. Expressions that never fire within 10 years are rejected.
  - **DST:** a local time skipped by a DST gap fires once at the gap's end (a 02:30 job fires at 03:00 on a spring-forward day). Fire times inside the gap collapse into that one instant, so an every-15-minutes cron keeps firing every 15 real minutes.
  - **Repeated times:** a time repeated by a DST overlap fires only at its first occurrence. An every-15-minutes cron therefore pauses during the repeated hour. Fixed-rate triggers are the choice for real-time intervals.
- **Fixed-rate:** fire times are `anchor + k × interval` for k ≥ 0. The anchor is `start_at`, or the creation time, so the first run is immediate when there is no `start_at`.
- **Fixed-delay:** the first fire is at `start_at`, or now. Each run's terminal transition sets `next_fire_at = now() + interval` in the same transaction.
- **Validation:**
  - Intervals, and the smallest gap among a cron's next 100 fires, must be at least the minimum interval (`JS_MIN_SCHEDULE_INTERVAL`, 60 s).
  - `jitter` ≤ 1 h, and it must not exceed a fixed-rate interval.
  - `end_at` must be after `start_at`, and `max_runs` ≥ 1.
  - The time zone must be an IANA name; `UTC` is the default.

### 10.3 Materializer

Every engine node runs one. Each second (and again at once when a batch was full), it runs a transaction:

1. Lock up to 100 due schedules: `state = 'ACTIVE' AND next_fire_at <= now() + lookahead` (2 min), ordered by `next_fire_at`, `FOR UPDATE SKIP LOCKED`.
2. For each schedule, `domain.PlanFires` returns the fire times to create, the new cursor, and whether the schedule is now completed:
   - **Misfire:** a cursor older than `now() − 1 min` is a misfire.
     - `fire_once`: one fire at the latest missed time.
     - `skip`: no missed fire; the cursor jumps to the first fire after now.
     - `fire_all`: every missed time, at most the 100 most recent.
     - Fixed-delay schedules always fire once.
   - **Window:** then every fire time up to `now() + lookahead`, at most 1,000 per pass.
   - **Limits:** fire times after `end_at`, or beyond `max_runs` (counting `fire_count`), are not created, and the schedule becomes `COMPLETED`.
   - **Fixed-delay:** plans at most one fire, then sets the cursor to `NULL` (waiting).
3. Insert the planned fires into `schedule_fires` with `ON CONFLICT DO NOTHING`. Insert a job only for each fire actually recorded:
   - `state = SCHEDULED`, with `run_at = fire_time + jitter`;
   - `created_by = schedule:<id>`;
   - pool, timeout, retry policy and at-most-once come from the job type at materialization time;
   - priority comes from the schedule, else the job type.
4. Update `next_fire_at`, `fire_count`, `last_fire_at` and `state`.

- **Jitter** is FNV-1a of `(schedule_id, fire_time)` modulo the jitter window, so re-materializing yields the same `run_at`.
- A **disabled job type** pauses its schedules implicitly. They are filtered out of the batch query, so they never fill batches. Once the type is re-enabled, their stale cursors are handled as misfires.
- The materializer always creates `SCHEDULED` jobs (T1), even when they are already due. The promoter owns every move to `READY`.

### 10.4 Promoter and overlap policies

Every engine node runs one every 250 ms, and continuously while any step fills its batch. Each step is its own transaction over rows locked with `SKIP LOCKED`.

| Step | Rows (batch) | Effect |
|---|---|---|
| Expire | Due `SCHEDULED` jobs and `READY` jobs whose `start_deadline` has passed and that have never started (1,000) | → `EXPIRED` (T18, T19) |
| Promote | Due `RETRY_PENDING` jobs, and due `SCHEDULED` jobs without a schedule (1,000) | → `READY` (T3, T4) in one `UPDATE` |
| Schedule jobs | Due `SCHEDULED` jobs with a schedule (500), with each schedule's overlap policy | See below |

For schedule jobs, "an earlier run is active" means an earlier-fire job of the same schedule is `READY`, `RUNNING` or `RETRY_PENDING`. The batch is processed in `(schedule_id, fire_time)` order, and jobs promoted earlier in the batch count as active.

| Overlap policy | Earlier run active | Otherwise |
|---|---|---|
| `skip` (default) | → `SKIPPED` (T20) | → `READY` |
| `allow` | → `READY` | → `READY` |
| `buffer_one` | Stay `SCHEDULED` with `run_at` pushed 5 s ahead, and re-checked then. If an earlier job of the schedule is already waiting, → `SKIPPED`. | → `READY` |
| `cancel_previous` | Cancel the earlier runs, then → `READY`. A running one gets `cancel_requested_at` (ends `CANCELLED` via its worker). A `READY` or `RETRY_PENDING` one → `CANCELLED` (T15, T17 by the promoter). | → `READY` |

- **Buffer one:** re-checking after 5 s bounds the cost of waiting jobs and keeps them from filling every batch. The price is up to 5 s extra delay after the earlier run finishes.
- **Retries:** jobs that have already started are never expired by `start_deadline`; their overall `deadline` governs retries. The claim query skips the start deadline for them too.
- **Terminal transitions:** every move of a schedule's job to a terminal state also advances its fixed-delay schedule. That includes completion, cancel, expire and skip.

### 10.5 Schedule changes

- **Withdrawal** (resolves HLD open question 11; [ADR-016](decisions/ADR-016-withdrawing-provisional-schedule-jobs.md)):
  - Deletes the schedule's `SCHEDULED` jobs with `run_at > now()`, and their `schedule_fires` rows.
  - Lowers `fire_count` by the number withdrawn and sets `next_fire_at` to the earliest withdrawn fire time.
  - These jobs are provisional lookahead artifacts. Deleting them, rather than cancelling them, avoids flooding history with up to 2 min of cancellations per edit. It also lets the new definition re-materialize the same fire times. Audit rows record the count.
- **Pause:** `ACTIVE` → `PAUSED`, then withdraw. **Resume:** `PAUSED` → `ACTIVE`. The materializer then treats a stale cursor as a misfire, which applies the misfire policy.
- **Edit (`PATCH`):**
  - Withdraw, apply the changes, and recompute the cursor from `max(now, last kept fire)`.
  - A fixed-delay schedule waiting on a run keeps waiting.
  - A `COMPLETED` schedule whose new limits allow more fires becomes `ACTIVE`.
  - `job_type` cannot change.
- **Delete:** withdraw, then `state = DELETED` and cursor `NULL`. The name becomes reusable. Reads return `404`, and the jobs already run stay in history.

### 10.6 API

| Endpoint | Role | Notes |
|---|---|---|
| `POST /v1/schedules` | operator | `201`; duplicate name → `409` |
| `GET /v1/schedules`, `GET /v1/schedules/{id}` | viewer | Single reads include the next 5 fire times (`upcoming`) |
| `PATCH /v1/schedules/{id}` | operator | Fields as in create, except `job_type` |
| `DELETE /v1/schedules/{id}` | operator | `204` |
| `POST /v1/schedules/{id}/pause`, `/resume` | operator | `409` from the wrong state |

- The trigger is an object: `{"kind": "cron", "cron": "0 2 * * *", "time_zone": "Europe/Paris"}`, `{"kind": "fixed_rate", "interval": "10m"}` or `{"kind": "fixed_delay", "interval": "5m"}`.
- `GET /v1/jobs` gains a `schedule_id` filter.
- Schedule management needs operator because a schedule creates load indefinitely.
- All schedule mutations are audited.

### 10.7 Tests

- **Cron:** parsing and field semantics, plus DST gaps and overlaps in `America/New_York` and `Europe/London`, and a zone with a 30-minute shift (`Australia/Lord_Howe`).
- **`PlanFires`:** window, limits, each misfire policy, fixed-delay, jitter determinism.
- **Store:** idempotent re-materialization through the ledger; concurrent materializers never fire twice; each overlap policy; expiry; withdrawal on pause and edit; fixed-delay chaining across completion.

## 11. Coordination

Implements [ADR-005](decisions/ADR-005-distributed-locking-and-fencing.md) and [ADR-006](decisions/ADR-006-leader-election.md): long-lived ownership through lease rows, fenced by epochs.

### 11.1 Table (phase 7 migration)

`leases (name PK, holder, address, epoch, acquired_at, renewed_at, expires_at)`.

- **Names:** `pool:<pool>` names a pool's dispatcher; `singleton:<duty>` names a singleton duty such as `singleton:maintenance`.
- **Holder:** a node ID, unique per process start (`JS_NODE_ID`, default hostname plus a random suffix).
- **Address:** the holder's worker-protocol address, so non-owners can redirect workers (phase 8).

### 11.2 Operations

| Operation | SQL shape | Outcome |
|---|---|---|
| Acquire | `INSERT … ON CONFLICT (name) DO UPDATE SET holder, epoch = epoch + 1, … WHERE leases.expires_at <= now()` | A row means acquired: a new lease gets epoch 1, a taken-over lease gets epoch + 1. No row means someone else holds it. |
| Renew | `UPDATE … SET expires_at = now() + ttl WHERE name AND holder AND epoch` | No row → `ErrLeaseLost`. Renewal is allowed after expiry if nobody took the lease over, because the unchanged epoch shows nobody else acted. |
| Release | `UPDATE … SET expires_at = now() WHERE name AND holder AND epoch` | On graceful shutdown, so another node takes over at once instead of after the TTL. |
| Fence | `EXISTS (SELECT 1 FROM leases WHERE name AND holder AND epoch FOR SHARE)` inside the owner's write | The share lock serializes the write with a concurrent takeover (an `UPDATE`): a takeover waits for in-flight writes, and later writes see the new epoch. |

- **Claims are fenced:** `ClaimReady` requires the pool lease and embeds the fence in its claim statement. When nothing is claimed, a follow-up read tells an empty queue from a lost lease and returns `ErrLeaseLost` for the latter.
- **Completions are not fenced by the pool epoch.** The attempt's fencing token already guards them, whichever node relays them.

### 11.3 Lease manager

One manager per TTL class: pools at 10 s, singletons at 30 s. Each runs a loop every `ttl / 3`:

1. **Self-fence:** drop any held lease whose local validity has passed and call `OnLost`. Local validity is `renewal start + ttl − margin` (2 s), measured on the monotonic clock.
2. **Renew** held leases. `ErrLeaseLost` drops the lease and calls `OnLost`. Other errors keep it while local validity lasts, which covers a brief database blip.
3. **Acquire** wanted leases that aren't held (the `Names` callback), then call `OnAcquired`.

- `Lease(name)` returns a lease only while it is locally valid. Owners check it before every write, and the database fence catches the rest.
- On shutdown the manager releases every lease and calls `OnLost`.
- `RunWhileHeld(name, fn)` runs a singleton duty with a context that is cancelled when the lease is lost.
- **Known limitation:** pools go to whichever node asks first. Balancing them across engine nodes is phase 12 work.

### 11.4 Tests

- Store: acquire, conflict, takeover with epoch + 1 after expiry, stale renewal rejected, release enabling immediate takeover, exactly one winner among concurrent acquirers.
- Claims with a stale epoch fail with `ErrLeaseLost`.
- Manager: handoff on shutdown, and self-fencing when renewals fail (database partition) followed by takeover on another node.

## 12. Worker system

Implements HLD §12 and [ADR-002](decisions/ADR-002-worker-pull-via-dispatcher.md). Resolves HLD open questions 7 and 8 ([ADR-014](decisions/ADR-014-worker-protocol.md)).

### 12.1 Protocol

- **Service:** gRPC service `jobscheduler.worker.v1.WorkerService` ([proto](../proto/jobscheduler/worker/v1/worker.proto)). The generated code in `pkg/workerpb` is committed; `make proto` regenerates it.
- **Calls:** all unary, and `Poll` is a long-poll. Unary calls pass through any HTTP/2 load balancer and are simple to retry.
- **Versioning:** fields are only ever added. A breaking change means a `v2` package served side by side.
- **Authentication:** `authorization: Bearer <token>`, either a per-pool worker token scoped to its pool or the optional cluster token `JS_WORKER_TOKEN` ([ADR-025](decisions/ADR-025-per-pool-worker-tokens.md), §20.1).

| RPC | Served by | Behavior |
|---|---|---|
| `Register(pool, job_types, slots, labels, worker_id, runtime_version)` | any engine node | Inserts a `worker_sessions` row with `lease_expires_at = now() + 30 s`. Empty `job_types` means all types. Returns `session_id`, lease TTL and heartbeat interval (5 s). |
| `Poll(session_id, max_jobs, wait)` | the pool owner | Blocks up to `wait` (≤ 30 s) for at most `max_jobs` assignments. A non-owner answers at once with `redirect_address` from the pool lease; with no owner yet it returns `retry_after`. |
| `Heartbeat(session_id, running[])` | any engine node | Extends the session lease. Replies with `cancel[]` (running attempts whose job has `cancel_requested_at`) and `stale[]` (reported attempts that aren't current: stop them and drop their results). An expired or closed session → `NOT_FOUND`, and the worker re-registers. |
| `Complete(job_id, attempt_id, attempt_number, outcome, …)` | any engine node | `CompleteAttempt`, guarded by attempt ID and fencing token. A replay is a no-op success; a stale attempt → `FAILED_PRECONDITION`. |
| `Deregister(session_id)` | any engine node | Marks the session `CLOSED` after draining. |

**Redirects (open question 8):**

- The worker keeps one connection to its configured address for `Register`, `Heartbeat`, `Complete` and `Deregister`, and a second one for `Poll`.
- On a redirect it re-dials the poll connection to the owner.
- If the owner is unreachable, it falls back to the configured address with exponential backoff (0.5 s doubling to 10 s, full jitter).
- If the configured address is unreachable while the worker polls an owner, its other calls are retried once on the owner's connection, since every node serves them ([ADR-023](decisions/ADR-023-session-calls-fall-back-to-the-owner.md)). A worker that the owner is feeding keeps its session, and its reports land.

### 12.2 Sessions (phase 8 migration)

- **`worker_sessions`:** `id`, `pool`, `worker_id`, `job_types text[]`, `slots`, `labels`, `runtime_version`, `state` (`ACTIVE`, `CLOSED`, `EXPIRED`), `created_at`, `heartbeat_at`, `lease_expires_at`, `closed_at`.
- **`tenants.max_running`:** optional per-tenant cap on running jobs in each pool; `NULL` means unlimited. Managed through the quotas API (§15).
- **Index:** `jobs (tenant_id) WHERE state = 'RUNNING'` serves cap counting.
- **Heartbeats** update the session row directly: one write per worker every 5 s, 200 writes/s for 1,000 workers. Batching renewals per engine (HLD §12.2) is a phase 14 optimization.

### 12.3 Dispatcher

Every engine node runs the gRPC server, plus a pool lease manager (§11.3).

- **Wanted leases:** every pool with an active session, refreshed every 3 s. Whichever node acquires a pool's lease first becomes its dispatcher.
- **Waiters:** each `Poll` on the owner becomes a waiter in the pool's FIFO queue.
- **Rounds:** the pool loop runs one when a waiter arrives, and every 100 ms while waiters remain. Each round:
  1. Reads `READY` counts per priority for the pool, in one indexed `GROUP BY`.
  2. Reads the running counts of capped tenants, when any tenant has a cap. Caps are refreshed every 10 s.
  3. For each waiter, in FIFO order:
     - Allocates its free slots one at a time with the pool's smooth weighted round-robin selector over the classes with work (§7). This makes the 8:4:2:1 shares hold per job, not per batch.
     - Claims each class's share with `ClaimReady`: fenced by the pool lease, filtered by the waiter's job types, and skipping tenants at their cap.
     - Fills slots left over from classes that ran dry from any other class with work, in urgency order.
- **Exact tenant caps:** the claim over-fetches candidates. `row_number() OVER (PARTITION BY tenant_id)` then limits each capped tenant to its remaining allowance, in the same statement. Rows locked but not picked are released at commit.
- **Commit before send:** a waiter gets its jobs only after the claim commits. If the waiter has gone (timeout or client disconnect) by delivery time, its jobs are **released** (T23).
- **Losing the lease** (`ErrLeaseLost`, or `OnLost`) stops the pool loop. Its waiters return empty, and their next poll is redirected.

**T23 (`RUNNING` → `READY`, dispatcher): release of an undelivered assignment** ([ADR-015](decisions/ADR-015-releasing-undelivered-assignments.md)).

- Guarded by the attempt's ID.
- `budget_attempts` goes back down, so the attempt doesn't count against the retry budget.
- `attempt_count` stays, so fencing tokens remain monotonic. No attempt row is written, because the attempt never ran.
- **Heartbeat reconciliation:** attempts current for a session but missing from its heartbeat for over 15 s (three heartbeats) are released the same way. This catches responses lost after the handler returned.

### 12.4 Worker SDK (`pkg/workersdk`)

- `Run(ctx, Config)` registers, then runs three loops: poll, heartbeat and completion.
- **Handlers** are registered per job type, and those types are the worker's capabilities. A handler gets a `Job` with its ID (the idempotency key), attempt number (the fencing token), payload, labels and deadline.
- **Outcome mapping:**

  | Handler result | Reported as |
  |---|---|
  | Success | `SUCCEEDED` |
  | `workersdk.Permanent(err)` | Non-retryable failure |
  | `workersdk.RetryAfter(err, d)` | Retryable failure, with a hint |
  | Any other error, or a panic | Retryable failure |
  | Deadline exceeded | `TIMED_OUT` |
  | Cancelled by a cancel request | `CANCELLED` |

- **Deadlines:** the attempt timeout is enforced locally on the monotonic clock, starting when the assignment is received.
- **Completions** retry with backoff until accepted, rejected as stale, or 10 minutes pass.
- **Self-fencing:** if no heartbeat succeeds for `lease TTL − 5 s` (25 s), the worker cancels every handler, drops their results and re-registers.
- **No work on an abandoned session:** after self-fencing, or when the engine reports the session gone, the worker doesn't poll until it has registered again. It drops assignments that arrive for the old session; the engine recovers them as lost attempts.
- **One renewal at a time:** when the poll loop and the heartbeat loop both find the session gone, one re-registers. The other waits for it only until its own context ends, so shutdown never waits on re-registration against an engine that is down.
- **Drain** (context cancelled):
  1. Stop polling.
  2. Wait up to `DrainTimeout` (30 s) for running handlers.
  3. Cancel the rest; they are reported as retryable failures ("worker shutting down").
  4. `Deregister`.

### 12.5 Tests

- Store: sessions, typed claims, exact tenant caps, release.
- Dispatcher: per-slot weighted allocation.
- End to end: an in-process engine and SDK workers over real gRPC. Covers success, retry then success, permanent failure, cancellation delivered through heartbeats, timeout, and a worker redirected from a non-owner engine to the owner.
- SDK, against fake engines:
  - `Run` still returns promptly once cancelled while the heartbeat loop keeps retrying re-registration with an unreachable engine;
  - when the configured node stops, heartbeats and the outcome reach the owner that the worker polls;
  - a self-fenced worker doesn't run work assigned to the session it abandoned.

## 13. Recovery and maintenance

Implements the reaper, timeout and retention rows of HLD §14 and NFR-9.

### 13.1 Reaper

Every engine node runs one, every 5 s. Each step handles a batch of 100 and repeats at once while its batch is full.

1. **Warm-up:** do nothing until this node has had database connectivity for one session TTL (30 s). A failed ping or step restarts the warm-up (HLD S6).
2. **Expire sessions:** `ACTIVE` sessions whose `lease_expires_at` has passed → `EXPIRED`. Rows are locked with `SKIP LOCKED`, so reapers on different nodes share the work.
3. **Lose orphaned attempts:** `RUNNING` jobs whose session is no longer `ACTIVE` → attempt `LOST`.
4. **Time out overdue attempts:** `RUNNING` jobs with `attempt_deadline < now() − 30 s` → attempt `TIMED_OUT`. This is the backstop behind the worker's own deadline.

- Steps 3 and 4 go through `CompleteAttempt`, so the retry rules apply (for example, dead-lettered as poison after 3 lost attempts).
- They are fenced by attempt ID and number: a worker's late report and the reaper can't both win.
- Jobs whose session row is missing are left to step 4.

### 13.2 Maintenance

A singleton duty under lease `singleton:maintenance` (§11.3). It runs when the lease is acquired and then hourly.

| Task | Rule |
|---|---|
| Partitions | Create daily `job_history` and `attempts` partitions for today and the next 7 days. A day whose partition can't be created doesn't stop the others. |
| History retention | Drop daily partitions whose range ended more than `JS_HISTORY_RETENTION` (30 d) ago. Delete such rows from the default partitions in batches. |
| Idempotency keys | Delete keys past `expires_at` (24 h TTL). |
| Schedule ledger | Delete `schedule_fires` rows older than the history retention. Cursors only move forward, so old fire times are never materialized again. |
| Worker sessions | Delete `CLOSED` and `EXPIRED` sessions a day after they ended. |

- **Why partitions are created ahead:** so the default partitions stay empty. Rows that land there anyway, such as rows written before maintenance first ran, are removed by retention rather than moved. Moving them would lock the default partition.
- **Locking:** dropping a partition takes an `ACCESS EXCLUSIVE` lock on its parent. It runs with `lock_timeout = 1s` and retries on the next run, so history inserts queue for at most 1 s.
- **Audit log:** not touched. The runtime may only insert into it, and its retention of at least 1 year is a separate privileged job (HLD §16).
- **Deletes** run in batches of 10,000, at most 100 batches per table per run.

### 13.3 Bulk operations

| Endpoint | Role | Behavior |
|---|---|---|
| `POST /v1/operations` | operator | Body `{"kind": "cancel" \| "redrive", "filter": {...}}` → `202` with the operation |
| `GET /v1/operations/{id}` | viewer | State and counts |

- **Filter:** the same fields as `GET /v1/jobs`: `state`, `type`, `label`, `schedule_id`. `cancel` applies to active jobs (`state`, if given, must be active). `redrive` requires `state` to be `FAILED` or `DEAD_LETTERED`.
- **Scope:** only jobs created before the operation are touched. The keyset cursor starts at the operation's creation time.
- **Execution:**
  - Each engine node claims one `PENDING` or `RUNNING` operation per second with `FOR UPDATE SKIP LOCKED`. It holds that lock while it processes one batch of 100 jobs, newest first.
  - Each job goes through the same guarded store method as its single-job endpoint (`RequestCancel`, `RetryJob`), audited with actor `operation:<id>`.
  - It then stores the cursor and counts in the same transaction: `succeeded`; `skipped` for jobs that are no longer eligible; `failed` if the batch hit an error.
- **Crash recovery:** a node that dies mid-batch rolls back that batch's cursor and counts. Its successor repeats the batch, and the actions already applied then count as `skipped`.
- **Errors:** an unexpected error ends the batch at the last job processed and is retried on the next tick. After 10 consecutive failed batches the operation becomes `FAILED`.
- **States:** `PENDING` → `RUNNING` → `SUCCEEDED` or `FAILED`.

### 13.4 Tests

- **Store:**
  - sessions expire and their attempts are lost, and those jobs are retried;
  - overdue attempts time out, but not within the grace period;
  - partitions are created and dropped, and expired rows are purged from every table;
  - bulk cancel and re-drive process batches, respect their filters and resume from the cursor.
- **Reaper:** warm-up delays expiry and restarts after a failed ping.
- **End to end:** a worker that stops heartbeating without deregistering has its job lost, retried and completed by another worker.

## 14. Platform administration

Implements FR-10 (trigger) and FR-21 ([ADR-017](decisions/ADR-017-platform-administration.md)).

### 14.1 Schema (phase 12 migration, part 1)

- `api_keys.role` also accepts `platform-admin`.
- `job_types.paused boolean` and `worker_sessions.draining boolean`, both defaulting to `false`.
- `pools (name PK, paused, updated_at)`. A row is created the first time a pool is paused.

### 14.2 Endpoints

| Endpoint | Role | Effect |
|---|---|---|
| `GET /v1/workers?pool=` | platform-admin | Active sessions with their running attempt counts |
| `POST /v1/workers/{id}/drain` | platform-admin | `draining = true`; `404` for a session that isn't active |
| `DELETE /v1/workers/{id}` | platform-admin | Closes the session; its held attempts become `LOST` and are retried |
| `GET /v1/pools` | platform-admin | Every pool known from job types, sessions or `pools`, with its state (see below) |
| `POST /v1/pools/{name}/pause`, `/resume` | platform-admin | Sets `pools.paused`; idempotent |
| `PUT /v1/pools/{name}/settings` | platform-admin | Sets the pool's backlog target (§18.5) |
| `POST /v1/job-types/{name}/pause`, `/resume` | operator | Sets `job_types.paused` for the caller's tenant; idempotent |
| `POST /v1/schedules/{id}/trigger` | operator | `201` with the created job |

**Pool state** returned by `GET /v1/pools`:

- whether it is paused;
- its owner, from the pool lease: node, address and epoch;
- its active workers and their total slots;
- its `READY` and `RUNNING` job counts;
- its effective backlog target, and its backlog age from the owner's latest sample (§18).

**Access and audit:**

- Only a platform-admin can create a `platform-admin` key. Other callers get `403`.
- Every mutation above writes an audit row: `pool.pause`, `pool.resume`, `pool.settings`, `worker.drain`, `worker.deregister`, `job_type.pause`, `job_type.resume` and `schedule.trigger`.

### 14.3 Behavior

- **Holds:** the claim statement and `ReadyPriorities` skip jobs of a paused pool or paused job type. A held job stays `READY`, and its start deadline still applies.
- **Drain:** `Heartbeat` and `Poll` responses carry `drain`.
  - A draining session's poll waits out its wait time and returns no assignments, so a worker that hasn't seen the flag yet can't spin.
  - On `drain`, the SDK stops polling, finishes running handlers within `DrainTimeout`, deregisters, and `Run` returns `ErrDrained`.
- **Trigger:**
  - Locks the schedule, whose job type must be enabled (else `409`), then inserts one fire at `now()` through the ledger, with `created_by` set to the caller.
  - The job is `SCHEDULED` and due, so the promoter applies the overlap policy.
  - A paused or completed schedule can be triggered. `fire_count` and the cursor are unchanged.

### 14.4 Tests

- **Store:** pool and job-type holds block claims until resumed; drain reaches heartbeats; draining sessions get no assignments; trigger creates a due job.
- **API:** role enforcement (tenant admin → `403`); a platform-admin mints a platform-admin key but a tenant admin cannot; pool and worker listings; job-type pause.
- **End to end:** a drained SDK worker finishes its running job, deregisters, and `Run` returns `ErrDrained`.

## 15. Quotas and load shedding

Implements FR-16 and FR-17 ([ADR-018](decisions/ADR-018-quotas-and-load-shedding.md)). It replaces the "phase 12" note in §9.6.

### 15.1 Schema (phase 12 migration, part 2)

- **New columns on `tenants`:** `rate_limit`, `max_pending`, `max_schedules`, `min_schedule_interval_ms` and `max_payload_bytes`. All are nullable, and `NULL` means the platform default.
- **Changed meaning of `max_running`:** it now applies to each pool separately, since dispatch runs per pool.

### 15.2 API

| Endpoint | Role | Notes |
|---|---|---|
| `GET /v1/quotas` | viewer | The caller's effective quotas; `null` means unlimited |
| `GET /v1/tenants` | platform-admin | Up to 1,000 tenants, by name |
| `GET /v1/tenants/{id}/quotas` | platform-admin | Configured values; `null` means the default |
| `PUT /v1/tenants/{id}/quotas` | platform-admin | Replaces every field; audited as `tenant.quotas` |

**Validation:**

- `rate_limit` is in (0, 1,000,000].
- `max_pending` is at most 1,000,000.
- The other counts are at least 1.
- `min_schedule_interval` is between 1 s and 24 h.
- `max_payload_bytes` is between 1 and 65,536.

### 15.3 Enforcement

Order on `POST /v1/jobs`:

1. Authentication and role.
2. The tenant's rate limit.
3. Payload size (`413`).
4. Validation (`422`).
5. Shedding by priority, against the pool's backlog target (`503`, §18.4).
6. The pending-jobs quota (`429`).

**Caching on each `api` node:**

- Quotas are cached per tenant for 30 s.
- The pending count is `SELECT count(*) FROM (SELECT 1 FROM jobs WHERE tenant_id = $1 LIMIT max_pending)`, cached for 5 s.
- The pools' owner-recorded backlogs are read at most every 2 s, by whichever request finds its copy stale.

### 15.4 Tests

- **Store:** quotas round-trip; the pending count is bounded; the schedule quota is enforced; running caps are per pool. Backlog tests are in §18.6.
- **API:**
  - per-tenant rate limit, payload limit, pending quota and schedule quota;
  - shedding rejects `LOW` then `NORMAL` but never `HIGH`;
  - `GET /v1/quotas`, and platform-admin quota management with its validation.

## 16. Payload schemas and listing details

### 16.1 Payload schemas (FR-1, [ADR-019](decisions/ADR-019-payload-json-schema.md))

- **Field:** `payload_schema` on job type create and `PATCH`. It is at most 64 KiB, and `null` in a `PATCH` removes it.
- **Compilation:** a compiler with a loader that rejects every external reference, so only `#`-relative `$ref`s resolve. A compile error is a `422` on `payload_schema`.
- **Validation:** at submission, and on schedule create and update. It uses the compiled schema cached for `(tenant, type, updated_at)`, holding up to 1,000 entries per node. Violations are a `422` on `payload`, with the error text truncated to 1 KiB.

### 16.2 Listing and attempts (FR-3, FR-8)

- `GET /v1/jobs` accepts `created_after` (inclusive) and `created_before` (exclusive), as RFC 3339 times.
- Attempts include `session_id` and the session's `worker_id`, which is `null` once the session row has been purged.

## 17. Observability

Implements FR-23 and HLD §17 ([ADR-020](decisions/ADR-020-telemetry.md)).

### 17.1 Setup

- `observability.SetupTelemetry` installs the global providers:
  - a meter provider with the Prometheus exporter, served at `/metrics` on the ops server;
  - when `JS_OTLP_ENDPOINT` is set, a tracer provider exporting OTLP over gRPC, with a parent-based sampler whose root ratio is `JS_TRACE_SAMPLE_RATIO` (`JS_OTLP_INSECURE` allows plaintext);
  - the W3C trace-context propagator;
  - an error handler that reports export failures through the structured logger.
- **Resource:** `service.name=jobscheduler`, `service.version` and `service.instance.id`.
- **Instruments** are declared in one file, `observability/metrics.go`, with the names, units and labels of HLD §17.3. They bind to the provider whenever it is installed. Gauge callbacks register on the same global meter, which the SDK's own meter would reject.
- **Shutdown** flushes spans within 5 s; an unreachable collector delays exit by no more than that.

### 17.2 Where each metric is recorded

| Metric | Recorded by |
|---|---|
| `jobs_submitted_total`, `jobs_scheduled_total{source=delayed}` | API, per created job (not replays or deduplicated submissions) |
| `jobs_rejected_total{reason}` | API, for `POST /v1/jobs` refused with 413, 429 or 503; `reason` is the error code |
| `jobs_scheduled_total{source=schedule}` | Materializer, per inserted fire |
| `scheduling_lag_seconds` | Promoter, `ready_at − run_at` returned by each promotion |
| `dispatch_latency_seconds` | Dispatcher, for each job delivered to a poll: from the latest of `ready_at`, the poll's arrival and the round that last saw its capped tenant regain room, to `attempt_started_at` ([ADR-022](decisions/ADR-022-dispatch-latency-from-a-free-worker.md)). It is the minimum of the three waits, so the database and engine clocks are never compared |
| `queue_wait_seconds` | Dispatcher, `attempt_started_at − ready_at` of each job delivered to a poll: the whole wait, for a free worker or behind a hold |
| `attempts_total`, `execution_duration_seconds`, `jobs_retried_total`, `jobs_dead_lettered_total` | `CompleteAttempt`: worker reports, the reaper and deregistration all pass through it |
| `jobs_completed_total` | Every move to history: completion, cancel, expire, skip, supersede, bulk cancel |
| `jobs_ready`, `jobs_held`, `jobs_oldest_ready_age_seconds`, `pool_backlog_target_seconds`, `jobs_running`, `worker_slots` | The pool's owner, sampled every 10 s. `jobs_ready` and the age cover dispatchable work only (§18.2) |
| `pools_unowned` | Every engine node: pools with active sessions or `READY` jobs but no unexpired lease (for the "no pool owner" alert) |
| `worker_sessions_expired_total` | Reaper |
| `stale_completions_rejected_total`, `pool_owner_changes_total` | Dispatcher |
| `db_transaction_duration_seconds{operation}` | `Store.inTx`, labeled with the calling exported method |
| `db_pool_in_use`, `db_pool_max` | Connection pool statistics, labeled with the process roles |
| `db_clock_offset_seconds` | Engine: database `clock_timestamp()` against the local clock, adjusted for round trip, every 10 s |

**Counting only committed work.** Metrics recorded inside a transaction are queued with `onCommit(tx, fn)` and run by `inTx` only after the commit succeeds.

**Pool counters start at zero.** An engine creates `worker_sessions_expired_total`, `stale_completions_rejected_total` and `pool_owner_changes_total` at 0 for every pool it wants. These events are rare, and a series that first appears at 1 hides that event from `increase()`.

**HTTP and RPC metrics:**

- `otelhttp` measures requests (`http_server_request_duration_seconds`), labeled with the route pattern, `server_address="api"` and the listen port. Without the fixed name and port, both labels would come from the client's `Host` header.
- `otelgrpc` measures worker RPCs, except `Poll` and `Heartbeat`.

### 17.3 Traces

**Spans:**

- API requests, named after the route pattern, e.g. `POST /v1/jobs`.
- Worker RPCs, through `otelgrpc` on both the server and the SDK, except `Poll` and `Heartbeat`.
- Materialize and promote batches that did work, recorded after the fact.
- Database transactions (`db.tx <operation>`), only as children of an existing span.

**One job across the queue:**

1. The submission stores its `traceparent` on the job (`jobs.trace_parent`).
2. The dispatcher sends it as `Assignment.trace_parent`.
3. The SDK starts `execute <type>` as a new root span, linked to the submission span, with the job ID, attempt number, type and tenant as attributes. Handlers receive it in their context, so their own spans join the attempt's trace.
4. The SDK sends `Complete` inside that span, so the engine's completion span and its transaction join the attempt's trace.

A manual retry keeps the original submission's `trace_parent`. Schedule-created jobs have none.

The API access log includes `trace_id` and `span_id` whenever the request has a trace context: its own span, or the caller's when trace export is off.

### 17.4 Alerts and local stack

- **Alert rules:** `deploy/prometheus/alerts.yml` implements HLD §17.5 for the signals exported here.
  - `BacklogAge` compares each pool's backlog age with its own `pool_backlog_target_seconds` (§18).
  - Held work doesn't count, so paused pools and capped tenants don't trigger `BacklogAge` or `DispatchStalled`. `DispatchStalled` still fires when the free workers can't run the waiting job types.
  - Infrastructure alerts belong to the deployment phase: database CPU, failover and dead tuples.
  - CI validates the Prometheus configuration and runs the rule unit tests (`alerts_test.yml`) with `promtool`.
- **`docker compose`:**
  - adds Prometheus, which scrapes the platform and loads the rules;
  - adds `grafana/otel-lgtm` for traces and Grafana, with the platform exporting traces to it.
- **Dashboards** (HLD §17.5), in `deploy/grafana/dashboards`, provisioned into the "Job scheduler" folder with a "Platform Prometheus" data source:

  | Dashboard | Shows |
  |---|---|
  | [Platform overview](images/dashboards/overview.png) | SLO stats (scheduling lag and dispatch latency p99, API 5xx ratio, unowned pools), throughput, rejections, retries, backlog against target, held work, lag |
  | [Pools](images/dashboards/pools.png) | Per pool: backlog and target, dispatchable and held work, queue wait by priority, slots, running work by tenant, dispatch latency and scheduling lag, expiries, stale reports, owner |
  | [Tenants](images/dashboards/tenants.png) | Per tenant: submissions by type and priority, future work, rejections, running jobs by pool |
  | [Database and engines](images/dashboards/database.png) | Transaction time and rate by operation, connections, clock offset, pools per engine, owner changes, worker calls, runtime |

  - Each dashboard picks its Prometheus through a data-source variable, so it works unchanged against the deployment's Prometheus.
  - CI parses every dashboard query with `promtool`.
  - Each dashboard's name links to a screenshot from a local run with an overloaded pool, a capped tenant, a paused job type, a killed worker and an owner crash, described in the [README](../README.md#observability).

### 17.5 Tests

- **Metrics:**
  - Store: every transition's counters and histograms, measured as changes between scrapes; hooks run only for committed transactions; pool gauge queries.
  - API: after submissions, `/metrics` has exact per-tenant counts; replays and validation errors are not counted.
- **Traces, end to end:**
  - a submission's span is linked from the SDK's execution span, which starts a new trace;
  - the handler's spans, the engine's `Complete` span and its transaction share the execution span's trace;
  - polls and heartbeats produce no spans or RPC metrics;
  - dispatch latency, attempts and pool gauges are recorded;
  - a job that waits for a busy worker, or for its tenant's cap to free up, shows the wait in `queue_wait_seconds`, but not in dispatch latency;
  - the API stores its server span as the job's `trace_parent`, and logs the caller's trace ID.
- **Configuration:** OTLP and sampling variables are parsed and validated.
- **Smoke test:** with the collector unreachable, the binary serves `/metrics` and exits within the flush timeout.
- **Dashboards:** provisioned into Grafana 13.2.2 (the version in `otel-lgtm` 0.34.0) against a Prometheus scraping a running platform and demo worker; every panel query ran through Grafana without error.

## 18. Pool backlog

Implements [ADR-021](decisions/ADR-021-pool-backlog.md): one definition of a pool's backlog for shedding, alerts and autoscaling.

### 18.1 Schema (phase 11 migration)

`pools` gains:

- `backlog_target_ms`, between 10 s and 24 h; `NULL` means `JS_BACKLOG_TARGET`;
- `oldest_due_at` and `sampled_at`, written by the owner.

### 18.2 The owner's sample

Every 10 s, the pool owner:

1. **Reads one read-only snapshot** (repeatable read), so a job just claimed can't count as both `READY` and `RUNNING`:
   - `READY` jobs not past their start deadline, grouped by priority, tenant and hold: `pool_paused`, then `job_type_paused` (the claim's `notHeldSQL`);
   - `RUNNING` jobs per tenant, the active sessions' slots, and the pool's target.
2. **Applies tenant caps**, with the dispatcher's caps (refreshed every 10 s):
   - A capped tenant may start `cap − running` more jobs, its allowance, as in the claim.
   - If more of its unpaused jobs are waiting than that, the rest are held for `tenant_cap`. Its allowance counts as dispatchable, taken from its most urgent priorities, and none of its jobs set the backlog age: their wait comes from the cap.
   - Otherwise all its unpaused jobs are dispatchable.
3. **Publishes the gauges.** `jobs_oldest_ready_age_seconds` is `now() − min(run_at)` over dispatchable jobs of tenants that their cap doesn't hold back.
4. **Records the backlog** on the pool row: `oldest_due_at` (the oldest such `run_at`, or `NULL`) and `sampled_at`. The upsert runs only while the owner's lease name, holder and epoch still match.

### 18.3 Ownership

- Engines want a pool that has an active session, or that is named by a job type or a `pools` row and has a `READY` job (one index probe per pool).
- As before, a pool no longer wanted is dropped after two session TTLs.
- A pool without workers is owned and sampled; its dispatch loop idles because nothing polls it.

### 18.4 Admission

- `api` nodes read the `pools` rows sampled within the last minute, at most every 2 s.
- A pool's backlog age is `now() − oldest_due_at`, or zero when that is `NULL`. Its target is the row's, or `JS_BACKLOG_TARGET`.
- `LOW` is shed when the age exceeds the target; `NORMAL` when it exceeds three times the target.
- A pool without a fresh row doesn't shed. Database saturation still sheds `LOW` (ADR-018).

### 18.5 Settings

- `PUT /v1/pools/{name}/settings` with `{"backlog_target": "15m"}`. `null` or an absent field resets to the default.
- The target is validated to 10 s–24 h, and the change is audited as `pool.settings`.
- The response is the pool, as in `GET /v1/pools`, which adds `backlog_target` (effective) and `backlog_age` (from a fresh sample, else `null`).

### 18.6 Tests

- **Store:**
  - held work is split by reason and left out of the oldest age;
  - a capped tenant's allowance is dispatchable and the rest held, and none of its jobs set the age; with room under its cap, all its jobs count;
  - a paused pool holds everything;
  - the fenced write ignores a deposed owner, and stale samples are ignored;
  - wanted and unowned pools follow sessions and `READY` jobs;
  - settings round-trip and are audited.
- **API:** shedding needs a fresh sample; it follows the pool's target (`LOW` past 1×, `NORMAL` past 3×) and ignores capped tenants and paused pools. Settings are validated and need platform-admin.
- **End to end:** the owner reports the target and held gauges, and records the backlog for admission.
- **Alerts:** a `promtool` test judges backlog age against each pool's own target.
- **Configuration:** `JS_BACKLOG_TARGET` is validated, and the removed shedding variables fail startup.

## 19. Load-test gate

Implements the gate of [HLD §15.1](architecture.md#151-capacity-model-tier-m) ([ADR-024](decisions/ADR-024-load-test-gate.md)): one PostgreSQL primary sustaining bursts of 5k jobs/s with p99 dispatch latency at most 1 s.

### 19.1 Harness (`loadtest/`)

| Part | What it does |
|---|---|
| `submit.js` (k6) | `ramping-arrival-rate`: `BASE_RATE` (500/s) for `WARMUP` (30 s), a 2 s ramp, `BURST_RATE` (5,000/s) for `BURST` (60 s), a 2 s ramp down, then `BASE_RATE` for `COOLDOWN` (30 s). Each iteration posts one `load.noop` job for the next tenant in turn, with a unique `Idempotency-Key` and the priority mix 2/10/68/20% (critical/high/normal/low). |
| `worker` (Go) | One process running `-workers` SDK workers (default 40) with `-slots` each (25). Worker *i* serves pool `load-(i mod pools)` and connects to address *i* mod the engine count. Each job waits `-work` (20 ms). |
| `gate` (Go) | Waits until the burst's steady part starts, scrapes every node's `/metrics`, waits `BURST − 10 s`, scrapes again, and judges the changes (§19.2). |
| `run.sh` | Builds the binaries and starts PostgreSQL (unless `JS_DATABASE_URL` is set), one `api` node and two `engine` nodes. It then creates the tenants, job types and quotas, starts the worker fleet, waits until every pool has an owner, and runs k6 and the gate side by side. Output goes to `loadtest/out/`. |

- **Tenants and pools:** `POOLS` tenants (4), each with job type `load.noop` in its own pool `load-<n>`. Their rate limit is raised to 1e6/s through the quotas API.
- **Nodes:** `JS_DB_MAX_CONNS=30` per node, so 90 connections in all, as HLD §15.1 budgets.
- **Throwaway PostgreSQL:** `initdb` from `PATH` or the test cache (`make test` fills it). Its settings are `max_connections=200`, `shared_buffers=1GB`, `max_wal_size=8GB` and `checkpoint_timeout=30min`, with `fsync` and `synchronous_commit` left on.

### 19.2 Verdict

The gate sums each series' change over all nodes, between the two scrapes:

| Check | Passes when |
|---|---|
| Accepted rate | `jobs_submitted_total` rose by at least 99% of `BURST_RATE` × the window |
| Dispatch latency (NFR-4) | At least 99% of `dispatch_latency_seconds` observations fall in the 1 s bucket |
| Keeping up | `dispatch_latency_seconds_count` rose by at least 95% of `jobs_submitted_total` |

- It also reports interpolated p50 and p99 for dispatch latency and queue wait, completions by outcome, and the p99 transaction time of each database operation.
- **CPU per job:** each node's `process_cpu_seconds_total`, and the database's CPU when `DB_CPU` names a command that prints it. Each is divided by the jobs accepted and projected to the offered rate. A run on one machine is CPU-bound well below 5,000 jobs/s, so these costs are what carries over to a deployment.
- It exits non-zero on failure. k6 fails the run too if more than 1% of requests fail, or if any iteration is dropped because it ran out of virtual users.

### 19.3 Commands

- `make loadtest`: the full gate on this machine, as a dry run. Rates and durations come from the environment variables above, and `POOLS=1` measures a single pool's ceiling.
- The `loadtest` workflow (`gh workflow run loadtest.yml`, or the Actions tab) runs the full gate on a clean GitHub-hosted runner (4 vCPUs, 16 GB). It uses PostgreSQL 17 in Docker with the same settings, and puts the verdict in the run's summary.
- `make loadtest-smoke`: 100 jobs/s with 10 s phases. CI runs it against a PostgreSQL service container, with the same checks.
- k6 is pinned in `.tools` and installed by `make tools`.

### 19.4 Results

The run that decides is in the deployment environment (phase 13). Until then, a clean runner gives a lower bound: PostgreSQL, the platform, the workers and k6 share its CPUs. A developer machine running desktop apps gives no valid result: an 8 GB laptop that was already swapping stalled every database operation for seconds.

| Date | Environment | Pools | Accepted/s | Dispatched/s | ≤ 1 s | Dispatch p99 | Verdict |
|---|---|---|---|---|---|---|---|
| 2026-10-05 | Apple M2 laptop, 8 GB, 13 GB of swap in use | 4 | 1,827 | 1,645 | 98.96% | 1.06 s | Invalid: memory pressure (a repeat accepted 843/s) |
| 2026-10-05 | GitHub runner, 4 vCPUs, 15 GB; PostgreSQL on the OS disk behind Docker's port proxy | 4 | 1,082 | 953 | 96.36% | 2.09 s | Fail: flushes queued on the IOPS-limited disk, and `docker-proxy` used up to 92% of a CPU |
| 2026-10-05 | The same runner; PostgreSQL on local SSD (about 10,000 fsyncs/s), host networking | 4 | 1,173 | 1,121 | 99.96% | 935 ms | Fail: CPU-bound. A 120-connection api pool accepted 2,601/s but starved dispatch (868/s). |
| 2026-10-05 | The same, offering 1,000 jobs/s | 4 | 1,004 | 1,003 | 100% | 96 ms | Pass |
| 2026-10-05 | The same, offering 800 jobs/s | 4 | 804 | 804 | 100% | 98 ms | Pass |

**Capacity per job.** The runner runs every component on 4 vCPUs. At the rates it sustains, each job cost this much CPU:

| Component | CPU per job | vCPUs at 5,000 jobs/s |
|---|---|---|
| PostgreSQL | 1.5–1.65 ms | 8 |
| `api` | 0.45–0.55 ms | 2.5 |
| `engine` (both) | 0.75 ms | 3.8 |

- The run saturated at about 1,200 jobs/s, as these costs predict for 4 shared vCPUs. Dispatch latency held under 1 s whenever the engines had CPU.
- The architecture therefore needs a primary with about 8 vCPUs free for bursts, for example an 8–16 vCPU class. `api` and `engine` nodes scale out.
- The database's cost is per transaction rather than per byte: a job writes about 4 KB of WAL and takes three transactions, at about 0.55 ms of CPU each. Batching those transactions, as HLD §15.1 anticipates, is the lever if a smaller class is wanted.
- The gate is decided in the deployment environment, where the database, nodes and load generator run on separate machines (phase 13).

### 19.5 Tests

- Gate: parsing the exposition format, summing changes across nodes, the exact share at a bucket bound, quantile interpolation, each check's verdict, and the CPU projection.
- CI runs the smoke scenario on every push.

## 20. Security hardening

Completes HLD §16 (phase 12) with worker credentials, API key rotation, TLS, least-privilege database roles, the row-level security decision and the security checks.

### 20.1 Per-pool worker tokens

[ADR-025](decisions/ADR-025-per-pool-worker-tokens.md).

- **Format:** `jsw_<prefix>_<secret>`, stored as API keys are: a lookup prefix and the SHA-256 of the whole token, shown once.
- **Table `worker_tokens`** (phase 12 migration, part 3): `id`, `pool`, `name`, `prefix` (unique), `secret_hash`, `created_at`, `expires_at`, `revoked_at`, `last_used_at`.
- **API**, for platform-admins:
  - `POST /v1/pools/{name}/worker-tokens` with `name` and optional `expires_at`;
  - `GET /v1/pools/{name}/worker-tokens`;
  - `DELETE /v1/pools/{name}/worker-tokens/{id}`.

  Creating and revoking are audited.
- **Authentication:** the engine checks every RPC's bearer token. It accepts:
  - a pool token, valid for its pool;
  - the cluster token `JS_WORKER_TOKEN`, now optional. It authorizes every pool and is meant for local development and migration.

  Successful checks are cached for 30 s, so a revoked token stops working within that time.
- **Authorization** by call:

  | Call | Check | On mismatch |
  |---|---|---|
  | `Register` | The token's pool is the requested pool | `PERMISSION_DENIED` |
  | `Poll` | The token's pool is the session's pool | `PERMISSION_DENIED` |
  | `Heartbeat`, `Deregister` | The update matches only sessions of the token's pool | `NOT_FOUND`: the SDK treats the session as lost |
  | `Complete` | The job is in the token's pool, checked under its row lock | `NOT_FOUND`: the SDK drops the report |
- **Rotation:** create a second token, roll the pool's workers onto it, then revoke the first. `last_used_at`, updated at most once a minute per token and node, shows when the old token has gone quiet.

### 20.2 API key rotation

HLD §16.1 asks for rotatable keys with last-used tracking.

- `GET /v1/api-keys` (admin): the tenant's keys with `last_used_at`, never their secrets.
- `POST /v1/api-keys/{id}/rotate` (admin), with optional `grace` and `expires_at`:
  - creates a key with the same name and role, and the optional new expiry;
  - makes the old key expire after `grace` (default 24 h, at most 30 days), unless it expires sooner;
  - requires the caller's role to include the key's, and is audited as `api_key.rotate`.
- `last_used_at` is written when a key is verified against the database, at most once a minute per key and node, so requests served from the 30 s cache cost nothing.

### 20.3 TLS

[ADR-026](decisions/ADR-026-tls-in-process.md).

- **Servers:** `JS_TLS_CERT_FILE` and `JS_TLS_KEY_FILE`, both or neither, turn on TLS 1.2+ for the HTTP API and the worker gRPC server.
- **Ops server:** it stays plaintext. It carries no tenant data, and orchestrator health checks and the metrics scraper reach it inside the network.
- **Reload:** the certificate is reloaded when either file changes, checked at most every 30 s, so a rotated certificate takes effect without a restart.
- **HSTS:** with TLS on, API responses carry `Strict-Transport-Security`.
- **Workers:** they verify the owner they are redirected to, so the certificate must cover every engine's `JS_WORKER_ADVERTISE_ADDR`. The SDK takes TLS through `DialOptions`, and `demo-worker` has `-tls-ca`.
- **Database:** connections use TLS through `JS_DATABASE_URL` (`sslmode=verify-full`, `sslrootcert`).

### 20.4 Database roles

[ADR-027](decisions/ADR-027-least-privilege-database-roles.md).

- **Roles:** a migration creates the group role `jobscheduler_runtime` (`NOLOGIN`). Deployments grant it to the login role the nodes use, and migrations run as the owner role.
- **Runtime privileges:**
  - `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the tables, including tables later migrations create, through the owner's default privileges;
  - on `audit_log`, only `SELECT` and `INSERT`;
  - no DDL, no `TRUNCATE`.
- **Partition maintenance** goes through two `SECURITY DEFINER` functions owned by the owner role. Each takes a parent, which must be `job_history` or `attempts`, and a day; it builds the partition's name and bounds itself, and pins `search_path`.
  - `jobscheduler_create_partition(parent, day)` creates the day's partition if it is missing.
  - `jobscheduler_drop_partition(parent, day)` drops it, waiting at most 1 s for its lock.
  - Only `jobscheduler_runtime` may execute them.

### 20.5 Row-level security

Not adopted in V1 ([ADR-028](decisions/ADR-028-row-level-security.md)). Tenant scoping stays in the repository, and §20.6's route-wide test checks every route.

### 20.6 Security checks

- **Tenant isolation:** a table-driven test covers every route.
  - Each tenant route is called with another tenant's resource IDs and must answer `404`.
  - Lists return only the caller's resources.
  - Platform routes refuse tenant admin keys with `403`.
  - The test fails when a route isn't in its table.
- **Response headers:** every API response carries `X-Content-Type-Options: nosniff`, `Cache-Control: no-store`, `Content-Security-Policy: default-src 'none'; frame-ancestors 'none'` and `Referrer-Policy: no-referrer`.
- **CI:**
  - `govulncheck` on every push;
  - the container image is built and scanned with Trivy, failing on fixable `HIGH` or `CRITICAL` findings;
  - Dependabot updates Go modules, GitHub Actions and the Docker base images weekly.

### 20.7 OWASP API Security Top 10 (2023)

| Risk | Controls | Checked by |
|---|---|---|
| API1 Broken object level authorization | The tenant comes from the credential alone. Every tenant query takes it, and other tenants' objects are not found. | §20.6 isolation test; store cross-tenant tests |
| API2 Broken authentication | Hashed keys and tokens with lookup prefixes, constant-time comparison, expiry, revocation and rotation; per-pool worker tokens | API key, rotation and worker-token tests |
| API3 Broken object property level authorization | Requests decode into structs that list their fields. Tenant, state and audit fields never come from bodies, and responses never include secrets or hashes. | API contract tests |
| API4 Unrestricted resource consumption | Body and payload limits, per-tenant rates and quotas, page-size limits, load shedding and server timeouts | Quota, shedding and limit tests |
| API5 Broken function level authorization | One role per route, checked before the handler; a key cannot grant more than its creator's role | Route role tests; §20.6 |
| API6 Unrestricted access to sensitive business flows | Rate limits and quotas; `CRITICAL` priority is limited to operators | Quota and priority tests |
| API7 Server-side request forgery | The platform makes no outbound requests built from request data. A future HTTP executor needs allow-lists (HLD §16.5). | Not applicable |
| API8 Security misconfiguration | TLS, security headers, a non-root distroless image, dependency and image scanning | Header test; CI |
| API9 Improper inventory management | One OpenAPI contract with a versioned `/v1` | Contract tests |
| API10 Unsafe consumption of APIs | No third-party APIs are consumed | Not applicable |

### 20.8 Tests

- **Worker tokens:** issue, list, revoke and rotate. A token for one pool cannot register, poll, heartbeat, complete or deregister in another. The cluster token still works, and revoked or expired tokens stop working.
- **API keys:** listing, rotation (the old key works until its grace ends) and last-used tracking.
- **TLS:** HTTPS and gRPC with a generated certificate, and reload after the files change.
- **Roles:** a login role holding only `jobscheduler_runtime` runs submissions, claims, completions and maintenance, but cannot create tables or change `audit_log`.
- **Isolation and headers:** §20.6.
