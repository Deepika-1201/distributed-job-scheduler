# ADR-003: Message broker

- **Status:** Accepted
- **Date:** 2026-09-26
- **Related:** [HLD §13](../architecture.md#13-event-architecture), [ADR-002](ADR-002-worker-pull-via-dispatcher.md), [ADR-004](ADR-004-database.md)

## Context

Dispatch runs on PostgreSQL through the dispatcher ([ADR-002](ADR-002-worker-pull-via-dispatcher.md), [ADR-004](ADR-004-database.md)). The brief lists possible event consumers (notifications, analytics, monitoring, audit, billing), but in V1:

- audit records are written to the database in the same transaction as the action;
- metrics go directly to OpenTelemetry;
- billing does not apply to an internal platform;
- notifications and webhooks are on the roadmap, not in V1.

## Problem

Does V1 need a message broker, either for dispatch or for events? If so, which one?

## Options considered

| Option | As a dispatch queue | As an event bus | Operations |
|---|---|---|---|
| **A. None (PostgreSQL)** | Already provided by ADR-002/004, transactional with state | Consumers could read an outbox or event table, but none exist yet | Nothing new |
| B. RabbitMQ | Good delivery semantics and consumer flow control. But it is a second source of truth, the consumer timeout defaults to 30 min, and priority support is limited | Flexible routing and fan-out | A stateful cluster to run |
| C. AWS SQS + SNS | Visibility timeout ≤ 12 h; delays ≤ 15 min; no priorities; a second source of truth | Simple managed fan-out with dead-letter queues; no replay | Fully managed; AWS-specific (LocalStack for local development) |
| D. Kafka (e.g., MSK) | Poor fit: per-partition head-of-line blocking, no per-message acknowledgement, no priorities | Excellent for replay, ordering per key and stream processing | Costly and heavy to operate |
| E. Redis Streams | Workable, but durability depends on Redis persistence settings | Lightweight fan-out with consumer groups | Another stateful system; memory-bound |
| F. Google Pub/Sub | Not on AWS | n/a | n/a |

## Decision

**No broker in V1**, for dispatch or for events.

- The **event catalog and envelope** are defined now as a contract ([HLD §13.2](../architecture.md#132-event-catalog-contract)). Producers write through an `EventSink` port.
- The **transactional outbox** (the outbox row written in the same transaction as the state change) and a **relay** (one per stream, guarded by a singleton lease) are built together with the **first out-of-process consumer**, most likely completion webhooks or notifications.
- The broker is chosen at that point. The current leaning is **SNS + SQS** for managed fan-out without replay, or **Kafka** if replay or stream processing becomes a requirement.
- Outbox rows are written only for event types that have a subscriber. At tier M, writing about 4 events per job would add 40–200M rows a day with no reader.

## Trade-offs

- No event fan-out in V1. Any consumer added later needs the outbox and relay first.
- Hands-on broker experience is deferred to the consumer phase.

## Consequences

- The implementation plan's "queue/event infrastructure" phase becomes "PostgreSQL queue plus outbox". The broker phase moves to the first-consumer milestone.
- `docker compose` has no broker service in V1.
- Adding a broker later means adding a relay adapter; domain and dispatch code don't change.
- Consumers must deduplicate by `event_id` and rely on per-aggregate `sequence` ordering, and the event contract says so from day one.

## Revisit when

- The first out-of-process consumer is planned (webhooks, notifications, analytics, audit export).
- Integration with other services is required.
- Event replay or stream processing becomes a requirement, which would favor Kafka.
