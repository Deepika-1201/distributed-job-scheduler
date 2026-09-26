# ADR-009: Modular monolith vs microservices

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [HLD §9](../architecture.md#9-component-architecture)

## Context

The project is built solo or by a small team, for learning and as a portfolio piece, at tier M scale. It needs independent scaling of request handling, dispatch and execution, and it must stay cheap and simple to run and develop locally.

## Problem

Should V1 be split into services, or built as one deployable unit?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Microservices (API, scheduler, dispatcher, worker manager, event service) | Independent deploys and scaling; strong boundaries | Distributed transactions or sagas between services that share job state; many pipelines and deployments; network failure modes between our own components; heavy for a small team |
| Single-process monolith | Simplest | Request handling and dispatch compete for resources; can't scale them independently; a failure in one affects the other |
| **Modular monolith with process roles** | One codebase and pipeline; module boundaries enforced in code; roles (`api`, `engine`) scale and fail independently | A shared release cadence and database; boundaries need discipline |

## Decision

A **modular monolith**: one codebase, deployed as two server roles plus a worker runtime.

- **`api`:** REST, auth, validation, idempotency, admission control. Stateless.
- **`engine`:** materializer, promoter, dispatcher, reaper, maintenance. 2+ replicas.
- **Worker runtime:** the SDK plus handlers, deployed per pool.
- **All-in-one mode** (`api` + `engine` in one process) for local development.

Module boundaries (`domain`, `scheduling`, `dispatch`, `recovery`, `coordination`, `persistence`, `events`, `workerproto`, `worker-sdk`, `observability`) are enforced by package visibility, dependency rules (ports and adapters) and architecture tests in CI. Each module owns its tables, and no module reaches into another's tables directly.

## Trade-offs

- All server components release together.
- A shared database couples modules at the storage level, which table ownership and repository ports mitigate.
- Boundaries are only as strong as the enforcement tests.

## Consequences

- One build, one image for `api` and `engine` (selected by a role flag), plus worker images.
- Simple local development and debugging: one process can run everything.
- Future extraction candidates are identified in advance: `dispatch` (as a matching service), `scheduling`, and notifications once they exist.

## Revisit when

- Several teams need independent release cycles.
- One module needs a different reliability or scaling tier than roles can provide.
- A module's database load needs separate storage.
