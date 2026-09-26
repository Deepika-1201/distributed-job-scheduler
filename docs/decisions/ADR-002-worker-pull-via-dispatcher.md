# ADR-002: Worker pull via dispatcher

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [HLD §12](../architecture.md#12-worker-architecture), [ADR-005](ADR-005-distributed-locking-and-fencing.md), [ADR-007](ADR-007-execution-semantics.md)

## Context

- Up to 200 workers run about 10k concurrent attempts, some lasting hours.
- The platform enforces weighted priority and per-tenant concurrency caps today, and will add fair share and per-key limits later.
- The roadmap adds HTTP and container executors and possibly workers in other languages.
- Workers should sit outside the database's trust boundary.

## Problem

How do jobs reach workers, and how does the platform know who is running what?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| A. Workers claim directly from the DB (`SKIP LOCKED`) | Fewest moving parts; proven (Oban, River, Solid Queue) | DB connections and credentials on every worker; priority and caps expressed in SQL (approximate or contended); polling load or `LISTEN/NOTIFY` commit contention; the claim protocol must be reimplemented per language |
| **B. Workers long-poll a dispatcher** (pull with central matching) | DB connections scale with engines; policy lives in code; workers hold worker credentials only; heartbeats per session; the protocol is the extension seam | An extra hop; the dispatcher must be HA; more code |
| C. The scheduler pushes jobs to workers | Low latency | Needs accurate, fresh capacity data; overloads slow workers; delivery is ambiguous when a push fails |
| D. A broker delivers jobs | Mature delivery mechanics | Two sources of truth; broker timeouts vs our leases; poor fit for priority × tenant (see [ADR-003](ADR-003-message-broker.md)) |
| E. One container per job | Strongest isolation; guaranteed kill | Seconds to minutes of startup; high cost per job. It is an executor, not a dispatch model |

## Decision

Adopt **option B**: workers pull from a dispatcher over a worker protocol.

- **Messages:** `Register`, `Poll` (long-poll for up to *free slots* assignments), `Heartbeat` (per session, every 5 s), `Complete` (carrying `attempt_id` and fencing token) and `Deregister`.
- **Pool ownership:** each pool is owned by one engine replica through a lease ([ADR-006](ADR-006-leader-election.md)). Workers connect to any engine node, and non-owners redirect them to the owner.
- **Demand-driven claiming:** the dispatcher claims only as many jobs as there are waiting slots.
- **Commit before send:** the attempt and the job's move to `RUNNING` are committed, guarded by the pool epoch, before the assignment is sent.
- **Worker sessions** have leases (TTL 30 s). The dispatcher renews them in batches, and the reaper expires dead ones.
- **Container-per-job** (option E) will arrive later as a worker pool whose handler launches containers. No new dispatch model is needed.
- The transport is gRPC with protobuf ([ADR-012](ADR-012-language-and-core-libraries.md)).

## Trade-offs

- An extra network hop and a component that must be highly available.
- More code than direct claiming: the protocol, sessions, redirects and matching.
- Dispatch pauses briefly (about one pool-lease TTL) when a pool owner fails.

## Consequences

- Database connections are independent of worker count, and workers never hold database credentials.
- The protocol needs explicit versioning. The SDK and engine get contract tests (a Worker ↔ Scheduler contract suite).
- Priority weights, tenant caps and capability matching are exact in steady state because one owner decides per pool.
- HTTP, container and serverless executors, and SDKs in other languages, plug in without core changes.

## Revisit when

- The principle is not expected to change.
- A single pool's dispatch rate outgrows one owner. The response is to split the pool into leased partitions, not to change this decision.
