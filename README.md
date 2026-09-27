# Distributed Job Scheduler

A distributed job scheduling and execution platform: durable jobs that run now, later or on a recurring schedule, with at-least-once execution, retries, priorities and fair sharing across tenants, on a horizontally scalable worker fleet.

**Status:** phases 1–4, 6–9 and 11 of the [implementation plan](docs/implementation-plan.md) are done: scaffolding, domain core, persistence, the REST API ([OpenAPI](api/openapi.yaml)), the scheduler (cron with time zones and DST, fixed-rate, fixed-delay, misfire and overlap policies), coordination (epoch-fenced leases with self-fencing) and the worker system (gRPC [protocol](proto/jobscheduler/worker/v1/worker.proto), dispatcher with weighted priorities and tenant caps, [Go SDK](pkg/workersdk), demo worker), recovery (reaper with warm-up, engine-side timeouts, retention and partition maintenance, bulk cancel and re-drive), and observability (Prometheus metrics, OpenTelemetry traces linked from submission to execution, alert rules). Every V1 functional requirement is implemented. That includes platform administration (worker drain, pool and job-type pause), tenant quotas with priority-aware load shedding, and payload JSON Schemas.

## Documentation

| Document | Contents |
|---|---|
| [High-level design](docs/architecture.md) | Requirements, architecture, failure scenarios, deployment |
| [Low-level design](docs/low-level-design.md) | Code structure, state machines, algorithms (grows each phase) |
| [Decision records](docs/decisions/) | ADR-001 to ADR-023, one decision per file |
| [Implementation plan](docs/implementation-plan.md) | Phases, exit criteria, status |

## Quick start

Requires Go 1.26+. Integration tests start an embedded PostgreSQL automatically (downloaded once); `go test -short ./...` skips them. Docker is only needed for `docker compose`.

```sh
make lint test-race   # formatting, vet, all tests with the race detector
make build            # bin/jobscheduler
docker compose up     # PostgreSQL, migrations, the platform (api + engine roles), Prometheus, Grafana
curl localhost:9090/readyz
curl localhost:9090/metrics
```

To run against an existing PostgreSQL instead: `JS_DATABASE_URL=postgres://... make migrate run`.

Create a tenant and its first admin key, then call the API:

```sh
./bin/jobscheduler bootstrap acme          # prints {"tenant_id": ..., "api_key": "jsk_..."}
export KEY=jsk_...
curl -X POST localhost:8080/v1/job-types -H "Authorization: Bearer $KEY" -d '{"name": "email.send"}'
curl -X POST localhost:8080/v1/jobs -H "Authorization: Bearer $KEY" -d '{"type": "email.send", "payload": {"to": "a@example.com"}}'
curl -X POST localhost:8080/v1/schedules -H "Authorization: Bearer $KEY" \
  -d '{"name": "nightly", "job_type": "email.send", "trigger": {"kind": "cron", "cron": "30 2 * * *", "time_zone": "Europe/Paris"}}'
```

## Configuration

Environment variables, validated at startup (full reference in [LLD §2.5](docs/low-level-design.md#25-configuration)):

| Variable | Default |
|---|---|
| `JS_DATABASE_URL` | required |
| `JS_ROLES` | `api,engine` |
| `JS_HTTP_ADDR` / `JS_OPS_ADDR` | `:8080` / `:9090` |
| `JS_TENANT_RATE_LIMIT` / `JS_API_REPLICAS` | `500` / `1` |
| `JS_MIN_SCHEDULE_INTERVAL` | `1m` |
| `JS_BACKLOG_TARGET` | `5m` (pool backlog target for pools without their own: `LOW` is shed past it, `NORMAL` past 3×) |
| `JS_WORKER_TOKEN` | required for `engine` (≥ 16 chars) |
| `JS_WORKER_ADDR` / `JS_WORKER_ADVERTISE_ADDR` | `:7070` / the listen address |
| `JS_NODE_ID` | hostname + random suffix |
| `JS_HISTORY_RETENTION` | `720h` (30 days) |
| `JS_DB_MAX_CONNS` | `10` |
| `JS_LOG_LEVEL` / `JS_LOG_FORMAT` | `info` / `json` |
| `JS_OTLP_ENDPOINT` / `JS_OTLP_INSECURE` | empty (trace export off) / `false` |
| `JS_TRACE_SAMPLE_RATIO` | `1` (share of root traces kept) |
| `JS_SHUTDOWN_DELAY` / `JS_SHUTDOWN_TIMEOUT` | `0s` / `30s` |

Run a worker against a local engine: `make demo-worker && JS_WORKER_TOKEN=... ./bin/demo-worker`.

## Observability

- **Metrics:** Prometheus format on the ops port (`/metrics`), with the names and labels of [HLD §17.3](docs/architecture.md#17-observability). Under `docker compose`, Prometheus runs on `localhost:9091` with the [alert rules](deploy/prometheus/alerts.yml) loaded.
- **Traces:** OTLP to `JS_OTLP_ENDPOINT`. A job's execution trace links back to the request that submitted it. Under `docker compose`, traces go to Grafana on `localhost:3000`.
- **Dashboards:** under `docker compose`, Grafana on `localhost:3000` has a "Job scheduler" folder with the [platform, pool, tenant, and database and engine views](deploy/grafana/dashboards).
- **Logs:** JSON, with `trace_id` on request logs.

## Layout

```
api/openapi.yaml    REST API contract
cmd/jobscheduler/   server binary (serve, migrate, bootstrap)
deploy/prometheus/  Prometheus configuration, alert rules and their tests
deploy/grafana/     Grafana dashboards and provisioning
internal/           application packages (see LLD §1)
docs/               designs, ADRs, plan
```
