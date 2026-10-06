# ADR-028: Row-level security is not adopted in V1

- **Status:** Accepted. Resolves HLD open question 5.
- **Date:** 2026-10-06
- **Related:** [ADR-024](ADR-024-load-test-gate.md), [ADR-027](ADR-027-least-privilege-database-roles.md), [HLD §16.3](../architecture.md#163-tenant-isolation-and-noisy-neighbours), [LLD §20.5](../low-level-design.md#205-row-level-security)

## Context

- **Tenant isolation today.** The tenant comes from the API key alone. Every tenant-facing repository method takes it as a required argument and puts it in the query, so another tenant's rows are not found. Cross-tenant tests cover the store and the API.
- **The question.** PostgreSQL row-level security (RLS) would add a second line: policies comparing `tenant_id` with a session setting, so a query that forgets its tenant condition returns nothing.
- **Much of the engine is cross-tenant by design.**
  - The dispatcher claims across tenants within a pool, weighted by priority and capped per tenant.
  - The promoter, materializer, reaper, maintenance and bulk operations scan all tenants.
  - Platform-admin endpoints list tenants, pools and workers.
- **Database CPU per job is the binding constraint.** The phase 5 load test found the primary CPU-bound well before disk or locks ([ADR-024](ADR-024-load-test-gate.md)).
- **Trust model.** Tenants are internal teams that make mistakes but aren't malicious (HLD A2). The nodes are the only database clients, and they connect with the least-privilege role of [ADR-027](ADR-027-least-privilege-database-roles.md).

## Problem

Should tenant isolation also be enforced by the database, with RLS?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| RLS on every tenant table, with a bypass role for engine paths | A missed condition fails closed | Two runtime roles and routing between them; `SET LOCAL` per transaction; policy checks on the hot path; engine bugs stay uncovered because they bypass it |
| RLS on API paths only: API nodes connect with a role subject to policies | Covers the code most exposed to tenant input | The API's own cross-tenant reads (platform-admin, admission's backlog and quota checks) still need a bypass; a second set of grants to keep aligned |
| **No RLS; a route-wide isolation test that fails when a route isn't covered** | No cost on the hot path; one runtime role; the test checks the HTTP behaviour tenants see | Isolation rests on code review and tests, not on the database |

## Decision

- **No RLS in V1.** Tenant scoping stays in the repository.
- **The second line is a test** ([LLD §20.6](../low-level-design.md#206-security-checks)). It calls every tenant route with another tenant's resource IDs and expects `404`, checks that lists return only the caller's resources, and checks that platform routes refuse tenant keys. It enumerates the registered routes and fails when one is missing from its table, so a new endpoint can't skip it.

## Trade-offs

- A query that forgets its tenant condition is caught only by tests and review. Required tenant arguments make that mistake hard to write, and the route-wide test catches it at the API.
- An attacker running SQL through the node's connection is not stopped by tenant boundaries. Least privilege still stops schema changes and audit tampering.

## Consequences

- No policies or session settings in the schema; queries stay as they are.
- New tenant-facing routes must be added to the isolation test's table, or the test fails.

## Revisit when

- Tenants become external or untrusted, or a compliance regime requires database-enforced isolation.
- A component outside the platform needs direct database access, such as an analytics reader.
- The primary gets CPU headroom to spare and a measured RLS overhead is acceptable.
