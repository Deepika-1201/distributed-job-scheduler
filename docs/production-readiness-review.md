# Production readiness review

- **System:** the distributed job scheduler, V1, as built through phase 14.
- **Date:** 2026-10-06.
- **Checklist:** the original brief's §39. Each question is answered with the mechanism, the evidence (tests, decision records, runbooks and measurements), and a verdict.
- **Verdict:** **passed with conditions.** V1 is ready for production once conditions C1–C3 (§9) are met. All three need the AWS account that phase 13's environment is built for.

## Summary

| Area | Question | Verdict |
|---|---|---|
| Correctness | Can jobs execute twice? | Only as at-least-once allows: after a worker fails mid-job. Never from the platform's own races. |
| | Can jobs disappear? | No |
| | Can jobs become permanently stuck? | No; holds are deliberate and visible |
| | Can two workers execute the same job? | Not within the protocol, provided handlers honor cancellation |
| | Can scheduler failover cause duplicates? | No |
| Reliability | What happens when each dependency fails? | Every failure has a tested or documented behavior (§2) |
| Scalability | What happens at 10x traffic? What becomes the bottleneck? | 10× the average is the burst design point. The bottleneck is the primary's CPU, then one pool's dispatcher. |
| Security | Can one tenant access another tenant's jobs? | No |
| | Can malicious jobs compromise workers? | A job is data, never code. Handlers are trusted code, isolated per pool. |
| Observability | Can we trace a job end to end? | Yes |
| Operability | Can we deploy? Roll back? Recover? Debug? | Built and documented. Deploy and rollback haven't run against AWS yet (C1). |
| Cost | Are we running unnecessary infrastructure? | No |
| Extensibility | New scheduler strategy, execution backend, queue, DAG workflows? | Yes, yes, partly, designed |

## 1. Correctness

### 1.1 Can jobs execute twice?

**Only in the failure cases that at-least-once execution allows** ([ADR-007](decisions/ADR-007-execution-semantics.md)).

- **When it happens.** A worker dies or is cut off after a handler's side effects but before its report is accepted. The job is then retried elsewhere.
- **The handler's part.** Handlers get the job ID as an idempotency key ([LLD §12.4](low-level-design.md#124-worker-sdk-pkgworkersdk)).
- **At-most-once job types.** They are never dispatched again after an assignment. A lost or timed-out attempt ends the job `FAILED` with `OUTCOME_UNKNOWN`, for a person to review.
- **The platform's own paths never double-run a job:**
  - Claims lock rows with `SKIP LOCKED`, in a statement fenced by the pool lease's epoch: `TestConcurrentClaimersNeverShareAJob`, `TestClaimIsFencedByThePoolLease`.
  - Each attempt carries a fencing token, and reports from stale attempts are rejected: `TestStaleAttemptIsFenced`, `TestDuplicateCompletionIsReplayed`.
  - A worker runs a duplicated assignment once: `TestDuplicateAssignmentRunsOnce`.
  - Assignments that were never delivered go back to `READY` without counting as an attempt ([ADR-015](decisions/ADR-015-releasing-undelivered-assignments.md)).
  - A database failover doesn't cancel running jobs, so it causes no retries. The job runs once: `TestWorkersRideOutADatabaseOutage` ([ADR-029](decisions/ADR-029-riding-out-database-outages.md)).

### 1.2 Can jobs disappear?

**No.**

- **Durability.** A submission is acknowledged only after its commit, and the standby is synchronous (NFR-6, RPO ≈ 0).
- **Guarded transitions.** Every state change is guarded by the expected state: `TestJobTransitionsMatchLLD`, `TestDatabaseEnforcesRunningInvariant`.
- **History.** A job that ends moves to history in the same statement that ends it: `TestSuccessMovesJobToHistory`.
- **Every way a job leaves `jobs` is visible:**
  - a terminal state, kept 30 days (NFR-9);
  - withdrawal of a schedule's provisional future jobs, which are re-materialized from the new definition ([ADR-016](decisions/ADR-016-withdrawing-provisional-schedule-jobs.md));
  - retention after 30 days.
- **At-most-once losses** end `FAILED` with a reason. They are never silent.

### 1.3 Can jobs become permanently stuck?

**No.** Every non-terminal state has an owner that moves it on, and a signal when that owner can't.

| State | Moved on by | If it can't, the signal |
|---|---|---|
| `SCHEDULED` | The promoter on every engine. A schedule waits only while another transaction holds one of its due runs ([ADR-033](decisions/ADR-033-a-schedule-is-decided-by-whoever-holds-all-its-due-runs.md)). | `SchedulingLagSLO` |
| `READY` | The pool's dispatcher; another engine takes the pool within about 13 s | `BacklogAge`, `NoPoolOwner`, `DispatchStalled` |
| `RUNNING` | The worker's report; session expiry and the reaper; the engine's timeout backstop (`TestOverdueAttemptsTimeOutAfterGrace`) | `SessionExpirySpike`, `ZombieReports` |
| `RETRY_PENDING` | The promoter | `SchedulingLagSLO` |

- **The state machines.** `TestNoTrapStates` shows every non-terminal state has an exit. `TestTerminalStatesExitOnlyThroughManualRetry` shows terminal states stay terminal.
- **Holds are deliberate.** Paused pools and job types hold work on purpose. The gauges count held work apart, so the backlog alerts don't fire for it: `TestPoolGaugesSeparateHeldWork`.
- **A stall found and fixed.** Phase 14 found a way the promoter could stall: a deferral design that re-selected the same blocked rows. It was replaced before release, and `TestPromoterProgressesWhenALaterFireSortsFirst` covers it.

### 1.4 Can two workers execute the same job?

**Not within the protocol's timing** ([HLD §14](architecture.md#14-failure-handling), LLD §21.4).

- **The ordering.**
  - A worker that can't renew its session self-fences at 25 s: it cancels its handlers and drops their results.
  - The session expires no earlier than 30 s, and only with evidence that the worker, not its engine, failed ([ADR-029](decisions/ADR-029-riding-out-database-outages.md)).
  - The job is re-dispatched only after that.
- **The tests:** `TestUnreachableEngineSelfFences`, `TestExpiryNeedsEvidence`, and `TestEnginePartitionedFromTheDatabase`, where the job runs exactly once.
- **The residual risk.** A handler that ignores its context keeps running after its worker has fenced. Its result is discarded by the fencing token, but its side effects continue.
  - The SDK contract says handlers must return promptly once their context is done ([`workersdk.Handler`](../pkg/workersdk/workersdk.go)).
  - Idempotency keys make the overlap harmless.

### 1.5 Can scheduler failover cause duplicates?

**No.**

- **One job per fire time.** The `schedule_fires` ledger is unique on `(schedule_id, fire_time)` and written with `ON CONFLICT DO NOTHING`. Materializers on any number of engines create one job per fire: `TestConcurrentMaterializersNeverFireTwice`, `TestMaterializeIsIdempotent`.
- **Dispatch failover is fenced.** Pool ownership moves with an epoch, and a node that lost its lease can't claim: `TestManagerSelfFencesBeforeTakeover`, `TestEngineCrashHandsPoolsOver`, `TestPoolOwnershipHandsOverWhenTheOwnerStops`.
- **Overlap decisions.** Concurrent promoters can't split a schedule's overlap decisions: `TestConcurrentPromotersKeepFireOrder`. Phase 14 found that they could; [ADR-033](decisions/ADR-033-a-schedule-is-decided-by-whoever-holds-all-its-due-runs.md) fixed it.

## 2. Reliability: what happens when each dependency fails?

PostgreSQL is the only stateful dependency. V1 has no broker and no cache ([ADR-003](decisions/ADR-003-message-broker.md), [ADR-011](decisions/ADR-011-caching-and-redis.md)). [LLD §21.3](low-level-design.md#213-scenario-coverage) maps HLD scenarios S1–S14 to tests.

| Dependency | Failure | Behavior | Evidence |
|---|---|---|---|
| PostgreSQL | Primary fails over (Multi-AZ, minutes) | The API answers `503` with `Retry-After`. Workers keep running their jobs and ride out up to 5 min. Reports land after recovery, and sessions survive. | `TestWorkersRideOutADatabaseOutage`, `TestDatabaseOutageAnswers503`, `TestReaperWarmsUpAfterConnectivityReturns`; [failover drill](runbooks/database-failover-drill.md) |
| PostgreSQL | Stalls or is blackholed | The same, because every call has a deadline: API 10 s, worker calls with headroom, reaper 5 s | `TestWorkersRideOutADatabaseOutage` (stall), `TestReaperStallRestartsWarmUp` |
| PostgreSQL | Data lost or corrupted | Point-in-time restore from 7 days of backups. RTO is hours (HLD §18). | [restore-database](runbooks/restore-database.md) |
| One engine | Partitioned from the database | It stops dispatching before another node takes its pools. Its workers finish their jobs. | `TestEnginePartitionedFromTheDatabase` |
| `engine` | A node crashes | Its pools move within the lease TTL plus one acquire round (about 13 s, NFR-8). Sessions survive. | `TestEngineCrashHandsPoolsOver`; `NoPoolOwner` |
| `api` | A node crashes | Stateless. The load balancer health-checks `/livez`, and clients retry with idempotency keys. | `TestSubmitIdempotency`, `TestConcurrentSubmissionsWithSameKeyCreateOneJob` |
| Worker | Crashes | Its session expires and its jobs are retried elsewhere | `TestCrashedWorkersJobIsRetriedElsewhere`; `SessionExpirySpike` |
| Worker | Partitioned | Self-fences, then re-registers | `TestUnreachableEngineSelfFences`, `TestAbandonedSessionTakesNoWork` |
| Load balancer or availability zone | Fails | Services and the database span availability zones, and ECS replaces tasks | [LLD §22](low-level-design.md#22-deployment) |
| Secrets Manager or KMS | Unavailable | Only new tasks need them, at start. Running tasks are unaffected. | LLD §22.4 |
| Telemetry pipeline | Collector or backends down | Telemetry is lost for the duration. Scheduling is unaffected. | ADR-020 |
| Clock | Host clocks drift | Correctness uses the database clock only | `TestClockOffset`; `ClockOffset` |

## 3. Scalability

### 3.1 What happens at 10x traffic?

[LLD §23](low-level-design.md#23-capacity) has the measurements: a 4-vCPU runner running every component, and per-job costs that carry over.

- **10× NFR-1's average.** That is about 5,800 jobs/s, the burst design point.
  - Measured costs per job: about 1.0–1.1 ms of database CPU, 0.4 ms on `api` and 0.4–0.5 ms on the engines.
  - That fits an 8-vCPU primary at about 75%, with `api` and engines scaled out.
  - Below saturation, submissions had a p99 of 75–85 ms at 1,000 jobs/s, inside NFR-5's 200 ms.
- **Beyond that.** Admission control sheds `LOW`, then `NORMAL`, when pool backlogs pass their targets (`TestOverloadedPoolShedsLowThenNormal`). Overload shows as `503`s for low priorities and visible lag, not collapse (A6).
- **10× the bursts** (50k jobs/s) is beyond one primary. [HLD §15.6](architecture.md#156-path-to-tier-l) is the path to tier L: partition pools, move payloads to object storage, shard by tenant.

### 3.2 What becomes the bottleneck?

In order:

1. **The primary's CPU and WAL.** A job takes three transactions.
2. **One pool's dispatcher.** It sustained 1,000 jobs/s with a p99 of 91 ms. Past one owner, pools split into leased partitions ([HLD §15.3](architecture.md#153-scaling-each-tier)).
3. **Promotion at a cron boundary.** 5,000 fires met NFR-3 on the runner. 10,000 took about 2.5 s; jitter windows or more engines handle that.
4. **Database connections,** as `api` scales out. Add a pooler then.

Measured and **not** bottlenecks:

- heartbeats, at 0.08 vCPU for 200 workers ([ADR-031](decisions/ADR-031-heartbeats-are-not-batched.md));
- 300,000 future-dated jobs;
- 200 workers with 10,000 jobs running.

## 4. Security

### 4.1 Can one tenant access another tenant's jobs?

**No** ([HLD §16](architecture.md#16-security), [LLD §20](low-level-design.md#20-security-hardening)).

- **Through the API.**
  - The tenant comes only from the credential, and every repository method takes it.
  - `TestTenantIsolationCoversEveryRoute` calls every route with another tenant's IDs. It fails when a new route is missing from its table.
  - `TestGetJobIsTenantScoped` and `TestPlatformEndpointsNeedPlatformAdmin` cover reads and platform operations.
- **Through workers.**
  - Worker tokens are scoped to a pool, so a worker can't take or report another pool's jobs: `TestPoolTokensAreScopedToTheirPool`, `TestPoolScopedCallsIgnoreOtherPools`.
  - Pools are platform resources. A worker in a shared pool sees the payloads of every tenant using that pool, by design. Tenants that need isolation get their own pools.
- **In the database.** The runtime role has no owner rights: `TestRuntimeRoleRunsThePlatformWithoutOwnerRights` ([ADR-027](decisions/ADR-027-least-privilege-database-roles.md)).
- **Accepted risk.** There is no row-level security ([ADR-028](decisions/ADR-028-row-level-security.md)). A future query that forgot the tenant would be caught by the route-wide test, not by the database.

### 4.2 Can malicious jobs compromise workers?

**A job can't carry code** (A3).

- **What a job is.** A payload is JSON data of at most 64 KB, validated against its job type's schema. External references in schemas are rejected: `TestPayloadSchemaIsEnforced`, `TestPayloadSchemaRejectsInvalidAndExternalSchemas`.
- **Handlers are trusted code**, deployed per pool ([HLD §16.5](architecture.md#165-execution-isolation)).
  - The SDK turns handler panics into retryable failures and enforces deadlines.
  - Workers hold no database credentials, only a pool token.
  - Every hop can use TLS: `TestWorkerProtocolOverTLS`.
- **The blast radius.** A payload that exploits a handler bug compromises at most a worker of one pool. Its token reaches only that pool's jobs.
- **Untrusted code** is out of scope for V1. It would need the sandboxed container executor that HLD §16.5 outlines.
- **Supply chain.** CI runs `govulncheck`, Trivy image scans and Dependabot.

## 5. Observability: can we trace a job end to end?

**Yes.**

- **Traces.** A job's execution trace links to the request that submitted it, across the queue: `TestJobIsTracedAcrossTheQueue`, `TestSubmissionIsTraced`.
- **Logs** carry `trace_id`, job, attempt and worker IDs.
- **The API** shows every attempt with its worker: `TestAttemptsNameTheirWorker`.
- **Metrics** are those of HLD §17.3.
- **Dashboards:** four, for the platform, pools, tenants, and the database and engines.
- **Alerts:** ten, each with a runbook URL and a `promtool` test.
- **Gap (low):** the ops port has no profiling endpoint. CPU investigations rely on metrics, traces and `pg_stat_statements`, which was enough for phase 14's.

## 6. Operability

| Question | Answer | Evidence |
|---|---|---|
| Deploy? | A manual `deploy` workflow: OIDC to AWS, build and push, migrate as a one-off task checked for exit 0, rolling update, then checks for stable services and healthy targets. **It has never run against AWS (C1).** | [LLD §22.7](low-level-design.md#227-pipelines); CI validates the Terraform and builds the images |
| Roll back? | Redeploy the previous image. Migrations are expand/contract, so the previous release runs on the current schema. Engines roll one task at a time. | [rollback](runbooks/rollback.md) |
| Recover? | Multi-AZ failover is automatic, and workers ride it out. Point-in-time restore from 7 days of backups. Workers drain gracefully, and credentials rotate without downtime. | [failover drill](runbooks/database-failover-drill.md), [restore-database](runbooks/restore-database.md), [rotate-credentials](runbooks/rotate-credentials.md); `TestDrainedWorkerFinishesAndDeregisters`, `TestRotateAPIKeyKeepsTheOldKeyUntilGraceEnds` |
| Debug? | Dashboards, linked traces, structured logs, the audit log, a job's attempt history, and a runbook per alert | [runbooks](runbooks/README.md) |

## 7. Cost: are we running unnecessary infrastructure?

**No.**

- **One stateful dependency.** No broker and no cache: ADR-003 and ADR-011 deferred both until a need is measured.
- **Environments on demand** (NFR-14). An idle environment costs about $0.35 an hour, and destroying it stops all of it but the state bucket ([LLD §22.9](low-level-design.md#229-cost)).
- **Autoscaling.** Workers scale on backlog age and `api` on CPU, with one NAT gateway unless `nat_per_az` is set.
- **The biggest item** is the database. [LLD §23.5](low-level-design.md#235-capacity-model) sizes it from the per-job cost, so a smaller burst target buys a smaller class.
- **Later savings:**
  - VPC endpoints instead of NAT for ECR and Secrets Manager traffic;
  - Fargate Spot for worker pools, which at-least-once execution tolerates.

## 8. Extensibility

| Extension | Answer | How |
|---|---|---|
| New scheduler strategy | **Yes** | A new `TriggerKind` whose `Compile` returns an `Evaluator` ([`domain/schedule.go`](../internal/domain/schedule.go)). Materialization, misfire and overlap handling are unchanged. |
| New execution backend | **Yes** | Executors are worker pools built on the SDK: HTTP, containers or functions ([HLD Appendix A](architecture.md#appendix-a--extension-points)). No engine change. |
| New queue | **Partly** | The scheduling, recovery and coordination loops depend on interfaces. The API and dispatcher take the PostgreSQL store directly. PostgreSQL as the queue is a decision ([ADR-004](decisions/ADR-004-database.md)). Another backend first needs those interfaces extracted, then an adapter. HLD Appendix A now says so (F5). |
| DAG workflows | **Designed, not built** (FR-24, Later) | A `WorkflowRun` entity and `job_dependencies`. The completion transaction releases children to `READY`, and dispatch is unchanged (HLD Appendix A). |

## 9. Findings and conditions

**Conditions for launch.** Each needs an AWS account.

| # | Condition | How to meet it |
|---|---|---|
| C1 | The AWS environment has never been created. Deploy, migration and rollback are validated only statically. | Bootstrap and deploy ([LLD §22](low-level-design.md#22-deployment)), then roll back once with the [rollback runbook](runbooks/rollback.md) |
| C2 | The 5k jobs/s burst gate is decided on production hardware ([ADR-024](decisions/ADR-024-load-test-gate.md)) | [deciding-load-test](runbooks/deciding-load-test.md): deploy with `db_instance_class=db.m7g.2xlarge`, then run the harness from a load-generator instance in the VPC |
| C3 | The database failover drill hasn't been run | [database-failover-drill](runbooks/database-failover-drill.md) under load |

**Findings.**

| # | Finding | Severity | Status |
|---|---|---|---|
| F1 | Concurrent promoters could split a schedule's overlap decisions. Of the first two fixes, one could stall and one was too slow. | High | Fixed ([ADR-033](decisions/ADR-033-a-schedule-is-decided-by-whoever-holds-all-its-due-runs.md)) |
| F2 | Dispatch rounds claimed once per typed waiter even when empty, so p99 dispatch was 2.4 s at 200 workers | High | Fixed (LLD §23.3) |
| F3 | The materializer ran at the cron boundary it prepared for | Medium | Fixed ([ADR-032](decisions/ADR-032-materialize-between-cron-boundaries.md)) |
| F4 | No row-level security | Accepted risk | [ADR-028](decisions/ADR-028-row-level-security.md); route-wide isolation test |
| F5 | The API and dispatcher depend on the concrete PostgreSQL store, which HLD Appendix A overstated as ports | Low | HLD corrected. Extract interfaces when a second backend is wanted. |
| F6 | A handler that ignores cancellation can keep running after its worker has fenced | Low | SDK contract; idempotency keys |
| F7 | 10,000 fires at one boundary exceed NFR-3 on the test runner | Low: twice tier M's bursts | Jitter windows (HLD §11.6), or more engines |
| F8 | No profiling endpoint on the ops port | Low | Add `pprof` behind the ops listener when needed |
| F9 | Schedule creation per tenant is serialized by the quota check | Low | A bulk-create endpoint if it ever becomes a hot path |

## 10. Sign-off

- **Review outcome:** passed with conditions C1–C3.
- **Re-review triggers:**
  - a failed condition;
  - an architecture change: a new stateful dependency, a broker, or partitioned pools;
  - an availability target above 99.9%.
