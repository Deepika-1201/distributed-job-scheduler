# ADR-025: Per-pool worker tokens, issued through the API

- **Status:** Accepted. Amends [ADR-014](ADR-014-worker-protocol.md) (authentication).
- **Date:** 2026-10-05
- **Related:** [ADR-017](ADR-017-platform-administration.md), [HLD §16.1](../architecture.md#161-trust-boundaries), [LLD §20.1](../low-level-design.md#201-per-pool-worker-tokens)

## Context

- **Today:** one bearer token per cluster, `JS_WORKER_TOKEN`, authenticates every worker for every pool ([ADR-014](ADR-014-worker-protocol.md)).
- **What the HLD requires:** per-pool tokens, and "a worker can register only for pools it is authorized for" ([HLD §16.1](../architecture.md#161-trust-boundaries)).
- **Why it matters.** Pools isolate execution: separate deployments, IAM roles and network policies (HLD §16.5). With one shared token, a compromised worker in any pool can register in every pool, take its jobs and read their payloads.
- **Rotation.** Changing the shared token means restarting every engine and every worker at once.

## Problem

1. Where do worker credentials live, and who issues them?
2. How does the engine scope each call to the token's pool?

## Options considered

**Where tokens live**

| Option | Pros | Cons |
|---|---|---|
| Per-pool tokens in engine configuration | No schema | Rotation restarts every engine; configuration grows with pools |
| **Hashed tokens in the database, issued by platform-admins through the API** | Rotation without restarts; audited; the same scheme as API keys | A lookup per token, cached |
| mTLS with a certificate per pool | Strong identity | Needs a private CA and certificate distribution; ADR-012 left mTLS for later |

**Scoping each call**

| Option | Pros | Cons |
|---|---|---|
| Check only `Register` | Simplest | A session ID or attempt ID from another pool would still be accepted |
| **Check every call: `Register` and `Poll` in Go, the rest inside the statement that acts** | No extra round trip; covers every path | A `pool` condition in a few store methods |

## Decision

- **Tokens.**
  - Platform-admins issue, list and revoke per-pool tokens through `/v1/pools/{name}/worker-tokens`, with an optional expiry.
  - The `worker_tokens` table stores them as API keys are stored: a lookup prefix and a SHA-256 hash, the plaintext shown once.
  - Issuing and revoking are audited, and `last_used_at` shows when a token was last seen.
- **The cluster token stays, optional.** `JS_WORKER_TOKEN` authorizes every pool, for local development and for migrating a running cluster. Production leaves it unset.
- **Every call is scoped to the token's pool.**
  - `Register` and `Poll` compare it with the requested or session's pool.
  - `Heartbeat`, `Deregister` and `Complete` add the pool to the statement that acts, so another pool's session or job is not found.
- **Caching.** The engine caches successful checks for 30 s. Revocation takes effect within that time.

## Trade-offs

- A revoked token keeps working for up to 30 s on each engine.
- A pool's workers share one token. A token per worker would need an identity per worker, which is mTLS's job.

## Consequences

- A migration adds `worker_tokens`; the engine's authentication reads it.
- The SDK doesn't change: a pool token goes in `Config.Token`, as before.
- Rotation: issue a new token, roll the pool's workers onto it, then revoke the old one once its `last_used_at` stops moving.

## Revisit when

- Workers need individual identities, for example to audit which worker ran what. Then mTLS.
