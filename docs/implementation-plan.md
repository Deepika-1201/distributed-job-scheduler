# Implementation plan

Builds the design in [architecture.md](architecture.md) incrementally. Each phase's LLD section is written in [low-level-design.md](low-level-design.md) **before** its code.

## Phases

| # | Phase | Scope | Exit criteria | Status |
|---|---|---|---|---|
| 0 | Technology selection | Language and core libraries | [ADR-012](decisions/ADR-012-language-and-core-libraries.md) accepted | Done |
| 1 | Scaffolding | Repository layout, configuration, logging, health and readiness, process lifecycle and graceful shutdown, container image, `docker compose`, Makefile, CI | `make lint test-race build` green; the binary serves `/livez` and `/readyz` and shuts down gracefully | Done |
| 2 | Domain core | Job and attempt state machines, retry decisions, priority selection | Exhaustive unit tests, including state-machine properties; LLD §3–§7 | Done |
| 3 | Persistence | Schema and migrations, repositories with guarded transitions, idempotency store, time partitioning | LLD schema section; integration tests against real PostgreSQL, including concurrent-transition races | Done |
| 4 | Job API | REST endpoints, error model, API-key auth and tenant scoping, idempotency, admission control, OpenAPI | Contract tests; cross-tenant access tests | Done |
| 5 | Walking skeleton + load-test gate | Minimal end-to-end path (submit → claim → complete) on real PostgreSQL; k6 scenario | 5k jobs/s bursts with p99 dispatch ≤ 1 s on one primary, or the architecture is revisited | Harness done ([ADR-024](decisions/ADR-024-load-test-gate.md), LLD §19): k6, an SDK worker fleet, a verdict from the platform's histograms, a CI smoke run and a manual workflow. On a 4-vCPU runner it passes at 1,000 jobs/s and measures about 2.8 ms of CPU per job, 1.5–1.65 ms of it on the database, so 5k jobs/s needs about 8 database vCPUs. The deciding run needs the phase 13 environment. |
| 6 | Scheduler | Materializer, promoter, cron with time zones, fixed-rate, fixed-delay, misfire and overlap policies | DST and misfire tests; no duplicate fires with several engines | Done |
| 7 | Coordination | Lease manager, epochs, fencing checks, singleton duties | Tests for split brain, lease expiry and handoff | Done |
| 8 | Worker system | gRPC worker protocol, dispatcher (weighted priority, tenant caps), sessions, heartbeats, cancellation, worker SDK, demo worker | Worker ↔ engine contract tests; end-to-end tests | Done |
| 9 | Retries and recovery | Retry integration, reaper (with warm-up), timeouts, dead-letter, re-drive, bulk operations | Recovery tests for worker crash, timeout and poison jobs | Done |
| 10 | Failure testing | Fault-injection suite covering HLD scenarios S1–S14 | Every scenario automated or documented as manual | |
| 11 | Observability | OpenTelemetry traces and metrics, dashboards, alerts, local telemetry stack | A job traceable end to end; metrics from HLD §17.3 exported | Done ([ADR-020](decisions/ADR-020-telemetry.md), LLD §17): every HLD §17.3 metric, traces linked across the queue, alert rules with `promtool` tests, Prometheus and `otel-lgtm` in `docker compose`. Pool backlog: dispatchable work only, per-pool targets that drive shedding and alerts ([ADR-021](decisions/ADR-021-pool-backlog.md), LLD §18). Grafana dashboards for the platform, pools, tenants, and database and engines, provisioned in `docker compose`. Dispatch latency measured from a free worker, with queue wait alongside ([ADR-022](decisions/ADR-022-dispatch-latency-from-a-free-worker.md)) |
| 12 | Security hardening | Complete RBAC, key rotation, TLS, worker credentials, row-level security decision, security tests | OWASP checklist; tenant-isolation tests | Done (LLD §20): platform-admin role ([ADR-017](decisions/ADR-017-platform-administration.md)), quotas and shedding ([ADR-018](decisions/ADR-018-quotas-and-load-shedding.md)), payload schemas ([ADR-019](decisions/ADR-019-payload-json-schema.md)), per-pool worker tokens ([ADR-025](decisions/ADR-025-per-pool-worker-tokens.md)), API key rotation, TLS with certificate reload ([ADR-026](decisions/ADR-026-tls-in-process.md)), a least-privilege runtime database role ([ADR-027](decisions/ADR-027-least-privilege-database-roles.md)), no row-level security in V1 ([ADR-028](decisions/ADR-028-row-level-security.md)), security headers, a route-wide tenant-isolation test, the OWASP API Top 10 table (LLD §20.7), and govulncheck, Trivy and Dependabot in CI |
| 13 | Deployment | Runtime decision (ADR-010), Terraform, CI/CD, runbooks | On-demand environment created and destroyed from CI | |
| 14 | Load testing and tuning | Tier M scenarios | Capacity report against NFR-1 to NFR-4 | |
| 15 | Production readiness review | Checklist from the original brief (§39) | Review passed | |
| Later | Extensions | Outbox and first consumer (webhooks), DAG workflows, HTTP and container executors, fair share | Per-feature ADRs | |

## Changes from the original phase list

- **Persistence comes before the API.** Endpoints need storage; building them against a temporary in-memory store would be thrown away.
- **"Queue/event infrastructure" is no longer a phase.** The queue is PostgreSQL (phases 3 and 8), and the outbox arrives with the first consumer ([ADR-003](decisions/ADR-003-message-broker.md)).
- **Coordination comes before the worker system**, because the dispatcher depends on pool leases.
- **Phase 5 adds an early load-test gate** to test the architecture's main risk, the PostgreSQL write ceiling, before building more on top of it.
- **Observability and security are cross-cutting.** Structured logging comes in phase 1, and authentication and tenant scoping in phase 4. Phases 11 and 12 complete and harden them.

## Definition of done (every phase)

- The phase's LLD section is written or updated before the code.
- Unit tests cover the logic; integration tests cover I/O; `make lint test-race` is green.
- Any decision that changed is reflected in the HLD and ADRs.

## Local prerequisites

- Go 1.26 or later.
- Docker, for `docker compose` only. Integration tests use an embedded PostgreSQL, downloaded on first run, so they don't need Docker. Docker (via Colima) is set up when the compose and failure-testing phases need it.
