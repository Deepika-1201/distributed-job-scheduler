# ADR-023: Worker session calls fall back to the pool owner

- **Status:** Accepted. Amends [ADR-014](ADR-014-worker-protocol.md) (worker-side routing).
- **Date:** 2026-09-27
- **Related:** [ADR-007](ADR-007-execution-semantics.md), [ADR-015](ADR-015-releasing-undelivered-assignments.md), [HLD §12.1](../architecture.md#121-worker-runtime-sdk), [LLD §12](../low-level-design.md#12-worker-system)

## Context

- **Routing today ([ADR-014](ADR-014-worker-protocol.md)):**
  - Every engine node serves every call except `Poll`, which only the pool owner serves.
  - The SDK sends `Register`, `Heartbeat`, `Complete` and `Deregister` to its configured address.
  - After a redirect, it polls the owner over a second connection.
- **What happened.** In a local run with two engines, one worker's configured node stopped while the owner kept running.
  - The worker's polls went to the owner and kept succeeding. Its heartbeats and reports went to the stopped node and all failed.
  - For 33 s, until the reaper expired its session, the worker took jobs, ran them and couldn't report them: 79 attempts were lost, and jobs allowed only one lost attempt were dead-lettered.
  - Self-fencing, after 25 s without a heartbeat, didn't stop it. While re-registering, the worker kept polling on the session it had abandoned and started whatever it received.
- **Not only a local problem.** Behind a load balancer, the configured address usually reaches another node within a retry. But a worker can lose the balancer, or the node behind it, while still reaching the owner's advertised address. ADR-014's consequence that "workers lose only their in-flight calls" doesn't hold then.

## Problem

1. Where do a redirected worker's session calls go when its configured address is unreachable?
2. What may a worker do while it is replacing its session?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Keep session calls on the configured address | Simple; a load balancer spreads them | A worker that the owner still feeds loses its session and its results |
| Send every call to the owner while redirected | One connection in use | A partitioned owner takes the heartbeats down with it, although the configured address is healthy |
| **The configured address first, then the owner** | Survives losing either node; the configured address still carries the calls normally | A call to a blackholed address costs its timeout before the fallback |
| Stop polling whenever heartbeats fail | Takes no work that can't be reported | Pauses work on every brief heartbeat failure, and doesn't save work already running |

## Decision

- **Fallback.** Calls other than `Poll` go to the configured address. If one fails with `UNAVAILABLE` or `DEADLINE_EXCEEDED` while the worker is polling an owner, the SDK retries it once on the owner's connection.
  - Every node serves these calls.
  - `Complete` replays are no-ops, so retrying on another node is safe.
- **No work on an abandoned session.** Once the worker gives up its session (by self-fencing, or because the engine reports it gone), it doesn't poll again until it has registered again. It drops assignments that arrive for the old session, which the engine recovers as lost attempts.

## Trade-offs

- A blackholed configured address delays each call by its timeout before the fallback. For heartbeats that is at most one interval (5 s), well within the 30 s session lease.
- The owner carries the session calls of workers whose configured node is down.
- A worker that can reach neither its configured address nor the owner still loses its session, as before.

## Consequences

- Only the Go SDK changes; the protocol and the engine don't. SDKs in other languages need the same behavior.
- ADR-014's consequence holds whenever a worker can reach either its configured node or the pool's owner.

## Revisit when

- Workers need several configured addresses, for example without a load balancer.
