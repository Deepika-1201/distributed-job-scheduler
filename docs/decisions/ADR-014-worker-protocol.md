# ADR-014: Worker protocol: unary calls, long-poll and owner redirects

- **Status:** Accepted. Amends the worker-protocol row of [ADR-012](ADR-012-language-and-core-libraries.md). Resolves HLD open questions 7 and 8. Amended by [ADR-023](ADR-023-session-calls-fall-back-to-the-owner.md) (session calls fall back to the owner) and [ADR-025](ADR-025-per-pool-worker-tokens.md) (per-pool worker tokens).
- **Date:** 2026-09-26
- **Related:** [ADR-002](ADR-002-worker-pull-via-dispatcher.md), [ADR-006](ADR-006-leader-election.md), [HLD §12](../architecture.md#12-worker-architecture), [LLD §12.1](../low-level-design.md#121-protocol), [protocol definition](../../proto/jobscheduler/worker/v1/worker.proto)

## Context

- **Topology.** Workers pull from a dispatcher ([ADR-002](ADR-002-worker-pull-via-dispatcher.md)). Each pool has one owner, the holder of its lease ([ADR-006](ADR-006-leader-election.md)), and workers may connect to any engine node.
- **Earlier plan.** [ADR-012](ADR-012-language-and-core-libraries.md) chose gRPC and expected to use streaming for heartbeats and cancellation.
- **Needs:**
  - dispatch within the 1 s p99 budget;
  - cancellation within about 5 s ([HLD §10.3](../architecture.md#103-cancellation-pause-and-run-now));
  - workers behind ordinary HTTP/2 load balancers;
  - clean handling of engine restarts and ownership changes;
  - simple SDKs in other languages later.

## Problem

What shape of RPCs carries the protocol, and how does a worker reach its pool's owner?

## Options considered

**RPC shape**

| Option | Pros | Cons |
|---|---|---|
| A. One bidirectional stream per worker | Lowest latency; cancels pushed at once; one connection | See below |
| B. Server stream for assignments, unary calls for the rest | Assignments are pushed | The most important message still has option A's fragility, and assignments lost with a stream still need reconciling |
| **C. Unary calls, with `Poll` as a long-poll** | Every call has a deadline and a definite outcome; retries are per call; works through any HTTP/2 load balancer; easy in any gRPC language | One request per batch of assignments. Cancels wait for the next heartbeat (≤ 5 s, within budget), and idle workers repeat a poll every 20 s. |

Option A's drawbacks:

- A stream dies with its engine node, so every restart or ownership change breaks all of that node's streams at once.
- A message in flight when a stream breaks has an unknown fate, so every message type needs its own acknowledgement.
- Long-lived streams need load-balancer support and idle-timeout tuning.
- It is the hardest shape to implement in other languages.

**Reaching the pool owner**

| Option | Pros | Cons |
|---|---|---|
| Proxy: any node forwards polls to the owner | Workers need one address | An extra hop and extra load on non-owners, and a second failure point |
| **Redirect: non-owners return the owner's address** | Direct path. The lease row already stores the owner's address, and engines never talk to each other. | Workers must be able to reach engine nodes' advertised addresses |

## Decision

- **Calls:** all RPCs are unary.
  - `Register`
  - `Poll`: waits up to 30 s.
  - `Heartbeat`: every 5 s; the response carries cancels and stale attempts.
  - `Complete`
  - `Deregister`
- **Routing:** any node serves every call except `Poll`. For `Poll`, a non-owner answers at once with `redirect_address`, or with `retry_after` if the pool has no owner yet.
- **Worker side:**
  - The worker polls the owner over a second connection.
  - If that fails, it goes back to its configured address with exponential backoff and jitter (0.5 s doubling to 10 s).
  - A redirect that arrives while the worker is already redirected also backs off, which stops two nodes with different views of the lease from bouncing a worker between them.
  - *Amended by [ADR-023](ADR-023-session-calls-fall-back-to-the-owner.md): calls other than `Poll` go to the configured address, and are retried on the owner's connection when that address is unreachable.*
- **Versioning:** package `jobscheduler.worker.v1`. Fields are only ever added; a breaking change becomes a `v2` package served alongside `v1`.
- **Authentication:** a bearer token in call metadata, compared in constant time. Until phase 12 there is one token per cluster. Phase 12 adds per-pool tokens and TLS, as HLD §16 requires.

## Trade-offs

- We trade push latency for simplicity.
  - An assignment takes one poll round trip plus at most one dispatch round (100 ms), well inside the 1 s budget.
  - A cancel takes up to one heartbeat interval (5 s).
- Polling is overhead: about one request per idle worker every 20 s.

## Consequences

- Engine nodes must be reachable at `JS_WORKER_ADVERTISE_ADDR`. This is a requirement for the runtime choice still pending in [ADR-010](ADR-010-deployment-strategy.md).
- ADR-012's "streaming for heartbeats and cancellation" no longer applies. gRPC stays for its schemas, code generation and HTTP/2 transport.
- When an engine node is lost, its workers lose only their in-flight calls, which they retry.

## Revisit when

- Load tests show that poll traffic or assignment latency is a bottleneck.
- Sub-second cancellation becomes a requirement.
