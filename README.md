# Distributed Job Scheduler

A distributed job scheduling and execution platform: durable jobs that run now, later or on a recurring schedule, with at-least-once execution, retries, priorities and fair sharing across tenants, on a horizontally scalable worker fleet.

**Status:** phases 1–3 of the [implementation plan](docs/implementation-plan.md) are done (scaffolding, domain core, persistence). The job API is next.

## Documentation

| Document | Contents |
|---|---|
| [High-level design](docs/architecture.md) | Requirements, architecture, failure scenarios, deployment |
| [Low-level design](docs/low-level-design.md) | Code structure, state machines, algorithms (grows each phase) |
| [Decision records](docs/decisions/) | ADR-001 to ADR-012 |
| [Implementation plan](docs/implementation-plan.md) | Phases, exit criteria, status |

## Quick start

Requires Go 1.26+. Integration tests start an embedded PostgreSQL automatically (downloaded once); `go test -short ./...` skips them. Docker is only needed for `docker compose`.

```sh
make lint test-race   # formatting, vet, all tests with the race detector
make build            # bin/jobscheduler
docker compose up     # PostgreSQL, migrations, then the platform (api + engine roles)
curl localhost:9090/readyz
```

To run against an existing PostgreSQL instead: `JS_DATABASE_URL=postgres://... make migrate run`.

## Configuration

Environment variables, validated at startup (full reference in [LLD §2.5](docs/low-level-design.md#25-configuration)):

| Variable | Default |
|---|---|
| `JS_DATABASE_URL` | required |
| `JS_ROLES` | `api,engine` |
| `JS_OPS_ADDR` | `:9090` |
| `JS_DB_MAX_CONNS` | `10` |
| `JS_LOG_LEVEL` / `JS_LOG_FORMAT` | `info` / `json` |
| `JS_SHUTDOWN_DELAY` / `JS_SHUTDOWN_TIMEOUT` | `0s` / `30s` |

## Layout

```
cmd/jobscheduler/   server binary
internal/           application packages (see LLD §1)
docs/               designs, ADRs, plan
```
