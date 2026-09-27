# ADR-017: Platform administration: platform-admin role, dispatch holds and worker drain

- **Status:** Accepted
- **Date:** 2026-09-27
- **Related:** [HLD §15.2](../architecture.md#152-backpressure-and-admission-control), [HLD §16.2](../architecture.md#162-authorization-rbac), [ADR-014](ADR-014-worker-protocol.md), [LLD §14](../low-level-design.md#14-platform-administration)

## Context

- **FR-21.** Operators must be able to list workers, drain or deregister them, and pause or resume a pool or a job type.
- **HLD §16.2** defines a `platform-admin` role for pools, quotas and worker management. Pools are platform resources shared by every tenant.
- **HLD §15.2** lists pausing a pool or job type as execution protection (layer 8).
- **The gap.** So far the code has only the four tenant roles (`viewer` to `admin`). Endpoints for shared resources guarded by `admin` would let any tenant pause a pool that every tenant uses.

## Problem

1. How is platform authority represented, when every API key belongs to a tenant?
2. What does pausing a pool or a job type do?
3. How does a drain request reach a worker?

## Options considered

**Platform authority**

| Option | Pros | Cons |
|---|---|---|
| A. A separate credential type without a tenant | Clean separation | A second authentication path, and a second kind of key to store, rotate and audit |
| **B. A `platform-admin` role, above `admin`, on keys of a dedicated platform tenant** | One authentication path; audit rows attribute actions like any other; tenant endpoints act only on the platform tenant, so they are harmless | Platform keys belong to a tenant that exists only to hold them |
| C. A static admin token in configuration | Trivial | No rotation, expiry or per-person attribution |

**Pause semantics**

| Option | Pros | Cons |
|---|---|---|
| Reuse "disable" (`enabled = false`) | Already exists | Disabling rejects submissions and holds schedules, which is a different intent from "stop running for now" |
| Move the jobs to `PAUSED` in bulk | Uses an existing state | `PAUSED` is a per-job, user-controlled state; bulk moves touch millions of rows, and resuming can't tell apart jobs a user paused themselves |
| **A dispatch hold: jobs stay `READY` and claims skip them** | Submissions and schedules keep flowing; resuming is instant; the claim statement enforces the hold, so it is exact even if a dispatcher's view is stale | Claims must skip held rows (see Trade-offs) |

**Drain delivery**

| Option | Pros | Cons |
|---|---|---|
| **A `drain` flag on `Heartbeat` and `Poll` responses** | Additive to the protocol ([ADR-014](ADR-014-worker-protocol.md)); reaches the worker within one heartbeat (≤ 5 s), or at once on its next poll | The worker must honor it |
| Deregister the session | Immediate | Running attempts are lost and retried, and a live worker simply registers again |

## Decision

- **Role:** `platform-admin` ranks above `admin`.
  - Only a platform-admin can create platform-admin keys.
  - `jobscheduler bootstrap <tenant> platform-admin` creates the first one.
- **Platform endpoints** (platform-admin):
  - `GET /v1/workers`, `POST /v1/workers/{id}/drain`, `DELETE /v1/workers/{id}`;
  - `GET /v1/pools`, `POST /v1/pools/{name}/pause`, `POST /v1/pools/{name}/resume`.
- **Tenant endpoints** (operator):
  - `POST /v1/job-types/{name}/pause` and `/resume`;
  - `POST /v1/schedules/{id}/trigger` (FR-10).
- **Pause** is a dispatch hold, stored in `pools.paused` and `job_types.paused`. The claim statement and the ready-class check exclude held pools and job types.
- **Drain:**
  - A drained session keeps `draining = true`.
  - `Heartbeat` and `Poll` responses carry `drain`, and a draining session receives no assignments.
  - The SDK stops polling, lets running handlers finish within its drain timeout, deregisters, and returns `workersdk.ErrDrained`.
- **Deregister** (`DELETE`) closes the session at once. Its held attempts are recorded as lost and retried. A live SDK registers again on its next call, so drain is the way to take a worker out of rotation.
- **Trigger:**
  - Creates one job now, with `fire_time = now()`, recorded through the `schedule_fires` ledger.
  - It follows the schedule's overlap policy.
  - It doesn't move the cursor or count toward `max_runs`.

## Trade-offs

- A paused job type's `READY` jobs stay in the pool's ready index, so claims in that pool scan past them. High-volume job types belong in their own pool.
- Platform keys need a tenant to live in. Tenant-scoped endpoints called with them act on that platform tenant only.

## Consequences

- Pool pause and resume, worker drain and deregistration, and job type pause and resume are audited. Platform actions are recorded under the platform tenant.
- A pool gets a row in `pools` the first time it is paused. `GET /v1/pools` also lists pools known only from job types and sessions.
- Protocol additions: `HeartbeatResponse.drain` and `PollResponse.drain`.

## Revisit when

- SSO or OIDC replaces API keys for people.
- Platform-admins need to act inside other tenants: cross-tenant operations such as re-driving another tenant's jobs.
