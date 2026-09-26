# ADR-004: Database

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [HLD §15](../architecture.md#15-scalability), [ADR-003](ADR-003-message-broker.md), [ADR-005](ADR-005-distributed-locking-and-fencing.md)

## Context

The database is the source of truth, the timer store, the ready queue and the coordination store. It needs:

- multi-row transactions across jobs, attempts, leases, idempotency keys and audit records;
- row-level locking with skip semantics for queue-style claiming;
- partial indexes for hot subsets and time partitioning for 30-day retention;
- managed high availability with synchronous replication, for RPO ≈ 0;
- tier M write rates: ~1–5k row writes/s on average and up to ~40k/s in bursts.

## Problem

Which database should hold all of this?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| **PostgreSQL** (managed, Multi-AZ) | `FOR UPDATE SKIP LOCKED`; partial and expression indexes; declarative partitioning; transactional DDL; JSONB; mature managed offerings; widely used for job queues (Oban, River, Solid Queue) | A single-primary write ceiling; MVCC bloat under high churn needs tuning |
| MySQL 8 (InnoDB) | `SKIP LOCKED`; mature managed offerings | No partial indexes; partitioned tables can't have foreign keys; DDL isn't transactional; weaker JSON indexing |
| Distributed SQL (CockroachDB, YugabyteDB, Spanner-style) | Horizontal writes; multi-region | Higher per-transaction latency; queue-style hot rows under serializable isolation cause retries; cost and operational weight that tier M doesn't need |
| DynamoDB | Serverless scale; conditional writes | No ad-hoc queries or joins for history and filtering; queue and priority semantics must be hand-built; tenant and time queries need careful index design; AWS-specific local development |

## Decision

Use **PostgreSQL** (managed, Multi-AZ with a synchronous standby) for everything stateful in V1.

- **Isolation:** `READ COMMITTED`, plus explicit row locks (`SKIP LOCKED`) and state-guarded conditional updates ([ADR-005](ADR-005-distributed-locking-and-fencing.md)).
- **History tables** use declarative time partitioning, so retention drops partitions instead of deleting rows.
- **Hot subsets** (due timers, the ready set, active dedupe keys, dead-lettered jobs) use partial indexes.
- **Operational rules:**
  - no long-running transactions on the primary;
  - `statement_timeout`, `idle_in_transaction_session_timeout` and `lock_timeout` are set;
  - server-side TCP keepalives are enabled;
  - autovacuum is tuned for queue tables;
  - high-churn tables use a HOT-friendly fillfactor.
- **Load-test gate:** before building beyond the core, prove 5k jobs/s bursts with p99 dispatch ≤ 1 s on one primary.

## Trade-offs

- One primary caps write throughput. Scaling is vertical first.
- Queue workloads need deliberate bloat and vacuum management.
- The database is on the critical path for everything, but it is the source of truth in any design.

## Consequences

- One engine to learn, operate, back up and monitor.
- Leases live next to the data they fence, so fencing checks are atomic with writes ([ADR-006](ADR-006-leader-election.md)).
- List and dashboard reads can move to a read replica later.
- Evolution path: offload payloads to object storage, then shard by tenant (cells, or a distributed PostgreSQL) at tier L.

## Revisit when

- Primary write or WAL load stays above ~70% after batching and payload offload.
- A multi-region or data-residency requirement appears.
