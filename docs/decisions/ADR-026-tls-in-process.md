# ADR-026: TLS terminated in the process, with certificate reload

- **Status:** Accepted
- **Date:** 2026-10-05
- **Related:** [ADR-012](ADR-012-language-and-core-libraries.md), [ADR-014](ADR-014-worker-protocol.md), [ADR-025](ADR-025-per-pool-worker-tokens.md), [HLD §16.4](../architecture.md#164-data-protection), [LLD §20.3](../low-level-design.md#203-tls)

## Context

- **Today:** the HTTP API and the worker gRPC server listen in plaintext. API keys and worker tokens are bearer secrets, so anyone on the path can replay them.
- **What the HLD requires:** TLS on every external hop, and on the worker protocol, whose payloads carry tenant data.
- **Where traffic goes.** Clients usually reach the API through a load balancer, which can terminate TLS. Workers are different: the engine redirects them to the pool owner's `JS_WORKER_ADVERTISE_ADDR`, so they connect straight to engine nodes, past any load balancer.
- **Certificates rotate.** Short-lived certificates from cert-manager or ACM Private CA are replaced on disk while the process runs.

## Problem

1. Where is TLS terminated?
2. How does a rotated certificate take effect?

## Options considered

**Where TLS is terminated**

| Option | Pros | Cons |
|---|---|---|
| Only at the load balancer or a service mesh | No code | Worker redirects bypass the load balancer; a mesh is a large dependency for one hop |
| **In the process, optional, for the API and worker servers** | Covers redirects; works with or without a load balancer | Certificate files to provision |
| In the process for every listener, including ops | Uniform | Health checks and metrics scrapers would need the CA too; ops carries no tenant data |

**Rotation**

| Option | Pros | Cons |
|---|---|---|
| Restart to pick up a new certificate | Simplest | A rolling restart per rotation, and a missed one serves an expired certificate |
| **Reload when the files change, checked at most every 30 s during handshakes** | No restart; no watcher goroutine | Up to 30 s before the new certificate is served |
| `fsnotify` watcher | Immediate | Symlink swaps (Kubernetes secrets) need care; another dependency |

## Decision

- **Configuration.** `JS_TLS_CERT_FILE` and `JS_TLS_KEY_FILE`, both or neither, turn on TLS 1.2+ for the API and the worker server. Startup fails if they don't load.
- **Ops stays plaintext.** It serves health and metrics inside the network.
- **Reload.** During a handshake, if 30 s have passed since the last check, the process compares the files' modification times and reloads them on change. A file that fails to load is logged, and the previous certificate stays in use.
- **HSTS.** With TLS on, API responses carry `Strict-Transport-Security: max-age=31536000`.
- **Workers.** The SDK takes TLS through `DialOptions` and uses the same options for redirect targets, so the certificate must name every engine's advertise address. `demo-worker` has `-tls-ca`.
- **Database.** TLS to PostgreSQL is set in `JS_DATABASE_URL` (`sslmode=verify-full`), as pgx supports.

## Trade-offs

- A rotated certificate takes up to 30 s to be served. Certificates are renewed well before they expire, so this doesn't matter.
- Client certificates (mTLS) are not checked. Workers authenticate with tokens ([ADR-025](ADR-025-per-pool-worker-tokens.md)).

## Consequences

- Deployments mount a certificate covering the API host name and every engine's advertise address, or terminate API TLS at the load balancer and use the files for the worker port only.
- Tests generate a certificate and check HTTPS, gRPC with a job end to end, and reload.

## Revisit when

- Workers need individual identities: then mTLS with client certificates.
