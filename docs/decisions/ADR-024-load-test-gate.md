# ADR-024: Load-test gate: k6 submissions, an SDK worker fleet and server-side histograms

- **Status:** Accepted
- **Date:** 2026-10-05
- **Related:** [ADR-004](ADR-004-database.md), [ADR-012](ADR-012-language-and-core-libraries.md), [ADR-022](ADR-022-dispatch-latency-from-a-free-worker.md), [HLD §15.1](../architecture.md#151-capacity-model-tier-m), [LLD §19](../low-level-design.md#19-load-test-gate)

## Context

- **The gate.** [HLD §15.1](../architecture.md#151-capacity-model-tier-m) and [ADR-004](ADR-004-database.md) require one PostgreSQL primary to sustain bursts of 5k jobs/s with p99 dispatch latency at most 1 s. If it can't, the architecture is revisited. The plan moved the gate after phase 9 so that it measures the real dispatch path.
- **The risk it tests.** The capacity model expects about 8 row writes per job, in 3 transactions, and says bursts need batching. Today claims are batched per poll, but every submission and every completion is a transaction of its own.
- **What NFR-4 means.** `dispatch_latency_seconds` counts from when a job could go to a free worker ([ADR-022](ADR-022-dispatch-latency-from-a-free-worker.md)).
- **What the gate needs:** an offered load, a measurement, a pass rule, and a way to repeat it cheaply as the code changes.

## Problem

1. What load does the gate offer, and what passes?
2. How is the load generated and measured?
3. Where does the run that decides happen?

## Options considered

**Load generation**

| Option | Pros | Cons |
|---|---|---|
| Insert jobs into the database directly | Highest rate | Skips authentication, admission and idempotency, so it overstates capacity |
| **k6 against `POST /v1/jobs`** (ADR-012's tool) | The real API path, idempotency keys included | k6 needs CPU of its own |

**Workers**

| Option | Pros | Cons |
|---|---|---|
| k6 gRPC clients | One tool | Reimplements sessions, redirects and reporting in JavaScript |
| **SDK workers, many per process** | The real protocol: long polls, redirects, heartbeats and reports | A Go program to maintain |

**Measurement**

| Option | Pros | Cons |
|---|---|---|
| Client side, end to end (submit, then poll until done) | Simple | Includes queueing and polling, so it can't see NFR-4 |
| **The platform's histograms, scraped when the burst starts and ends** | NFR-4 exactly as ADR-022 defines it, plus queue wait and transaction times | Only as precise as the bucket bounds, and 1 s is one of them |

**Pools**

| Option | Pros | Cons |
|---|---|---|
| One pool | The worst case for one owner | Measures one dispatch loop rather than the database. HLD §15.3 already scales a pool by partitioning it. |
| **Four pools** | Measures the database ceiling that the gate is about | One pool's ceiling is measured separately |

**Where it runs**

| Option | Pros | Cons |
|---|---|---|
| A developer machine | Immediate | Shares memory and CPU with desktop apps. A first run on an 8 GB laptop that was already swapping stalled every database operation for seconds, and two runs differed by 2×. |
| **A clean GitHub-hosted runner, through a manual workflow** | Repeatable, nothing else running, free for a public repository (4 vCPUs, 16 GB) | Shared cloud hardware with an unspecified disk, so it isn't the production database class |
| The deployment environment, on the proposed database class | Decides the gate | Needs phase 13 |

## Decision

- **Scenario.** k6 offers 500 jobs/s for 30 s, 5,000 jobs/s for 60 s, then 500 jobs/s for 30 s to one `api` node.
  - Four tenants, each with job type `load.noop` in a pool of its own.
  - Priorities are 2/10/68/20% critical, high, normal and low, and every request carries an `Idempotency-Key`.
- **Workers.** 40 SDK workers with 25 slots each, 10 per pool, alternating between two engines. Each job takes 20 ms, so about 100 of the 1,000 slots are busy at 5k jobs/s: dispatch is measured, not worker capacity.
- **Database.** One PostgreSQL primary with durable commits (`fsync` and `synchronous_commit` on).
- **Measurement.** A gate program scrapes every node's `/metrics` at the start and end of the burst's steady part, leaving out its first and last 5 s, and sums the changes.
- **Pass.** During the steady burst, all three hold:
  1. the API accepts at least 99% of the offered rate;
  2. at least 99% of dispatches have a dispatch latency of at most 1 s, read exactly at the 1 s bucket;
  3. dispatches reach at least 95% of accepted submissions, so the platform keeps up.

  k6 also fails the run if more than 1% of requests fail or are dropped.
- **Where it runs.** The `loadtest` workflow runs the gate on a clean runner, on demand, until the deployment environment exists. There, on the database class proposed for production, the run decides the gate (phase 13). `make loadtest` runs the same harness on a developer machine, as a dry run only.
- **CI.** A short smoke run at a low rate keeps the harness working, judged by the same criteria.

## Trade-offs

- A run on one machine shares its CPUs between PostgreSQL, the platform, the workers and k6, so even a clean runner's result is a lower bound.
- The short handler and spare slots make dispatch the subject, not execution. Queue wait under realistic handler times belongs to phase 14.
- These bursts come through the API. Cron-boundary bursts, where many timers fall due at once, also exercise the promoter; phase 14 adds them.

## Consequences

- New: `loadtest/` (the k6 scenario, the worker fleet and the gate), `make loadtest` and `make loadtest-smoke`, a pinned k6 in `.tools`, a CI smoke job, and the manual `loadtest` workflow.
- LLD §19 records each run's environment and result.

## Revisit when

- The deciding run fails. Batching, as HLD §15.1 anticipates, is the first lever before the architecture is revisited.

## Outcome (2026-10-05)

- **Laptop:** the first run was invalid because of memory pressure.
- **Runner, first attempts:** they exposed two artifacts of the test setup, an IOPS-limited OS disk and Docker's userland port proxy. The workflow now puts PostgreSQL on local SSD with host networking.
- **Runner, final setup:** it sustains about 1,200 jobs/s with every component sharing 4 vCPUs. At 800 and 1,000 jobs/s the gate passes, with dispatch p99 under 100 ms.
- **Per-job cost:** PostgreSQL 1.5–1.65 ms of CPU, `api` 0.45–0.55 ms and the engines together 0.75 ms. So 5,000 jobs/s needs about 8 vCPUs of database headroom; [LLD §19.4](../low-level-design.md#194-results) has the details.
- **Verdict:** the architecture stands, with the primary sized for that headroom. The gate itself is decided in the deployment environment.
