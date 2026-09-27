# ADR-020: Telemetry: pulled metrics, linked attempt traces, owner-reported pool gauges

- **Status:** Accepted. Amends the telemetry row of [ADR-012](ADR-012-language-and-core-libraries.md) for metrics. Amended by [ADR-021](ADR-021-pool-backlog.md) (pool gauges).
- **Date:** 2026-09-27
- **Related:** [ADR-006](ADR-006-leader-election.md), [ADR-014](ADR-014-worker-protocol.md), [HLD §17](../architecture.md#17-observability), [LLD §17](../low-level-design.md#17-observability)

## Context

- **What the HLD fixes.** [HLD §17](../architecture.md#17-observability) sets the metric names and labels, one trace per attempt with a span link back to the submission, parent-based sampling with tail sampling in the collector, and the alert list.
- **The earlier plan.** [ADR-012](ADR-012-language-and-core-libraries.md) chose the OpenTelemetry SDK, exporting OTLP to a collector.
- **What remains open:**
  - how metrics leave each process;
  - who reports gauges derived from shared database state, such as a pool's `READY` count, when every engine node can read it;
  - how trace context survives the queue, where a job may run hours after it was submitted.
- **Checked against the current releases** (OpenTelemetry Go v1.46.0, contrib v0.71.0):
  - The Prometheus exporter is still pre-1.0 (v0.68.0); the OTLP exporters are stable.
  - The exporter produces the HLD's names exactly: counter `jobs_submitted` becomes `jobs_submitted_total`, and a histogram with unit `s` becomes `scheduling_lag_seconds`.
  - A W3C `traceparent` stored as text and extracted later works as a span link on a new root span.
  - Instruments created on the global meter before the SDK is installed do bind to it later. Gauge callbacks, however, must be registered on that same global meter: the SDK's own meter rejects them ("from different implementation").
  - `otelhttp` labels `server.address` and `server.port` from the client's `Host` header unless a server name with a port is given, which would make an unbounded label set.
  - The `otelgrpc` filter applies to metrics as well as spans.

## Problem

1. How do metrics leave the process?
2. Who reports gauges derived from shared database state?
3. How does trace context cross the queue?

## Options considered

**Metric export**

| Option | Pros | Cons |
|---|---|---|
| **A. Prometheus scrape endpoint (`/metrics` on the ops port)** | Works without a collector, locally and in CI; standard for Kubernetes and ECS scraping; the collector can still scrape it | The exporter is pre-1.0 |
| B. OTLP push to a collector | Stable exporter; matches ADR-012's pipeline | Every environment and test needs a collector before any metric is visible |
| C. Both | Flexible | Two paths to test, and double counting if both are ingested |

**Gauges from shared state**

| Option | Pros | Cons |
|---|---|---|
| Every node reports every pool | Simple | Summing over instances double counts; every query needs `max by` |
| A singleton reports everything | One reporter | A second ownership mechanism, and a single point for all pools |
| **The pool's owner reports that pool** | Ownership already exists ([ADR-006](ADR-006-leader-election.md)) and moves with failover | Series change instance label on failover; a handoff can overlap for one scrape |

**Trace context across the queue**

| Option | Pros | Cons |
|---|---|---|
| One trace from submission to completion | A single tree | Traces stay open for hours; tail sampling decides long before execution |
| **A new trace per attempt, linked to the submission span** | The HLD's choice; each attempt's trace is complete when it ends | Two traces to follow, joined by the link |
| No propagation | Nothing to store | Submission and execution can't be correlated |

## Decision

- **Metrics:** option A.
  - The OpenTelemetry metric API with the Prometheus exporter, served at `/metrics` on the ops server.
  - Our code uses only the stable metric API; the exporter is confined to one setup file.
  - The collector scrapes the endpoint, so ADR-012's collector pipeline still holds for everything downstream.
- **Traces:** OTLP over gRPC to the collector.
  - Enabled by `JS_OTLP_ENDPOINT`; without it the global provider stays a no-op, at zero cost.
  - Sampling is parent-based on a `JS_TRACE_SAMPLE_RATIO` root ratio (default 1), leaving error and latency retention to tail sampling in the collector.
- **Pool gauges:** reported by the pool's owner. These are `jobs_ready`, `jobs_oldest_ready_age_seconds`, `jobs_running` and `worker_slots`. *Amended by [ADR-021](ADR-021-pool-backlog.md): `jobs_ready` and the age count dispatchable work only; `jobs_held` and `pool_backlog_target_seconds` are added.*
  - The owner refreshes them every 10 s in the background, so a scrape never waits on the database.
  - Alerts use `max by (pool)`, which is safe during a handoff overlap.
- **Across the queue:**
  - The submission's `traceparent` is stored on the job (`jobs.trace_parent`) and carried in `Assignment.trace_parent`, an additive protocol field ([ADR-014](ADR-014-worker-protocol.md)).
  - The SDK starts each attempt as a new root span linked to it.
  - The SDK's `Complete` call carries the attempt's span context, so the engine's completion span belongs to the attempt's trace.
- **Cardinality:** labels come only from bounded sets: pool, priority, job type, state, outcome and reason, plus tenant, which the HLD treats as bounded (§17.1). Job, attempt and session IDs go on spans and logs, never on metrics.
  - HTTP metrics use a fixed server name and the listen port, and the route pattern rather than the path.
  - Worker long polls and heartbeats are neither traced nor measured: they are constant background traffic, and a poll's duration is only its wait.
- **What counts as a rejection:** `jobs_rejected_total` counts admission control only: rate limits, quotas, payload size and load shedding (413, 429, 503). Validation errors are client mistakes and show in the HTTP metrics by status.
- **Logs:** request logs carry `trace_id` and `span_id` whenever the request has a trace context. Telemetry SDK errors go through the structured logger.

## Trade-offs

- Pulled metrics need each process's ops port reachable by the scraper, just as the health checks already do.
- Schedule-created jobs have no submission span to link to, so their attempt traces are roots without links.
- A manual retry keeps the original submission's trace context, not the retry request's.
- Counters recorded inside a transaction run only after it commits, so a rolled-back transition is never counted.
- `tenant` and `type` labels multiply series. That is fine for a bounded set of internal tenants, but needs revisiting if tenants grow into the thousands.

## Consequences

- New dependencies: the OpenTelemetry API, SDK, Prometheus and OTLP exporters, and the gRPC and HTTP instrumentation.
- The SDK depends only on the OpenTelemetry API. A worker exports traces only if its process configures a tracer provider.
- Alert rules live in the repository, and CI validates them with `promtool`.

## Revisit when

- The Prometheus exporter reaches 1.0, or the platform standardizes on OTLP metrics.
- Tenant counts grow beyond what per-tenant series can afford.
