# ADR-012: Language and core libraries

- **Status:** Accepted. Amended by [ADR-013](ADR-013-cron-evaluation.md) (cron) and [ADR-014](ADR-014-worker-protocol.md) (worker protocol).
- **Date:** 2026-09-26
- **Related:** [ADR-002](ADR-002-worker-pull-via-dispatcher.md), [ADR-004](ADR-004-database.md), [ADR-009](ADR-009-modular-monolith.md), [Implementation plan](../implementation-plan.md)

## Context

The architecture is settled: a PostgreSQL-centric modular monolith, workers pulling through a dispatcher, and correctness built from guarded SQL and leases. The implementation needs:

- **Concurrency:** cheap handling of many long-polls, background loops and timers.
- **Postgres driver:** batching, pipelining and precise transaction control.
- **Libraries:** first-class gRPC and OpenTelemetry.
- **Footprint:** small container images and fast startup, for cheap on-demand environments.
- **Explicit code:** the mechanics (claiming, leases, fencing) must stay visible and testable, not hidden behind framework magic, because they are what this project is meant to teach.

## Problem

Which language, and which core libraries, should the platform be built with?

## Options considered

| Criterion | Go | Java 21 + Spring Boot | Kotlin | Rust (tokio) | TypeScript (Node) |
|---|---|---|---|---|---|
| Concurrency | Goroutines and channels; simple and cheap | Virtual threads; good | Coroutines; good | Async; powerful but complex | Event loop; weak for CPU-bound work |
| PostgreSQL | `pgx`: native protocol, batching, `COPY`, pipelining | JDBC/jOOQ; mature | Same as Java | `sqlx`/`tokio-postgres`; good | `pg`; adequate |
| gRPC / OpenTelemetry | First-class / first-class | First-class / first-class | First-class | Good / good | Good / good |
| Footprint and startup | ~20 MB static binary; milliseconds | Larger heap; seconds | Same as Java | Smallest | Moderate |
| Explicitness | High; little magic | Framework conventions hide mechanics | Medium | High | Medium |
| Productivity for a small team | High | High, once Spring is known | High | Lower (borrow checker, async) | High |
| Prior art in this space | River, Temporal, Hatchet, Kubernetes, etcd | Quartz, JobRunr | — | — | BullMQ |

## Decision

**Go**: language version 1.26 or later in `go.mod` (the minimum required by `goose`), built with the current toolchain.

| Concern | Choice | Why |
|---|---|---|
| HTTP API | Standard library `net/http` with method and path patterns | Enough for a versioned REST API; no framework needed |
| PostgreSQL driver | `pgx` v5 with `pgxpool` | Fastest native driver; batching and pipelining for the write path |
| Data access | Hand-written SQL in repositories; no ORM | Guarded state transitions must be visible and reviewable |
| Migrations | `goose`, with embedded SQL files | Plain SQL, can run in-process or as a separate step |
| Worker protocol | gRPC with protobuf; generated code committed. *Amended by [ADR-014](ADR-014-worker-protocol.md): unary calls with a long-poll, not streaming.* | Streaming for heartbeats and cancellation; SDKs in other languages later; supported by AWS load balancers |
| Worker authentication | Per-pool bearer tokens over TLS (V1); mTLS optional later | Simple to rotate; enough inside the trust boundary |
| Logging | `log/slog` (JSON) | Standard library; structured |
| Telemetry | OpenTelemetry SDK (traces, metrics) → OTLP → OTel Collector | Vendor-neutral; the backend is chosen in the deployment phase |
| IDs | UUIDv7 | Time-ordered, so indexes stay compact |
| Cron parsing | Evaluated in the scheduler phase: an established parser plus our own DST-aware next-fire computation, verified by DST tests. *Amended by [ADR-013](ADR-013-cron-evaluation.md): parser and evaluator are both in-house.* | DST semantics (A7) must be exact |
| Tests | Standard `testing`; `testing/synctest` for time-dependent concurrency; integration tests against real PostgreSQL (embedded by default, or `JS_TEST_DATABASE_URL`) | No mocks of the database for correctness-critical code; tests need no Docker |
| Load tests | k6 | Scriptable; HTTP and gRPC |
| Container image | Static binary on a distroless, non-root base | Small, minimal attack surface |
| PostgreSQL version | 17 | Widely available on managed services |

## Trade-offs

- Go is more verbose than Kotlin or Java, and its generics are limited.
- There is no Spring-style ecosystem, so we write more infrastructure code ourselves. That is intended for the core, and deliberate elsewhere.
- Anyone new to Go has a learning curve. This is the cheapest decision to reverse, and the cost is lowest right now, while only scaffolding exists.

## Consequences

- One static binary serves the `api` and `engine` roles; workers are separate binaries that use the SDK.
- Fast CI, and small, cheap containers for on-demand environments.
- Concurrency code is explicit, so it must be tested (race detector, `synctest`, integration tests).
- Protobuf definitions make SDKs in other languages possible later without changing the engine.

## Revisit when

- Team skills or preferences change before substantial code exists.
- A required library (for example, cron or DST handling) turns out to be inadequate. That changes a library, not the language.
