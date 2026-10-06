# ADR-027: Least-privilege database roles

- **Status:** Accepted
- **Date:** 2026-10-05
- **Related:** [ADR-004](ADR-004-database.md), [HLD §16.4](../architecture.md#164-data-protection), [HLD §16.6](../architecture.md#166-audit), [LLD §20.4](../low-level-design.md#204-database-roles)

## Context

- **Today:** API and engine nodes connect as the role that ran the migrations. It owns every table, so a SQL injection or a compromised node could drop tables, change the schema or rewrite the audit log.
- **What the nodes actually need:** row reads and writes, plus one kind of DDL. Maintenance creates daily `job_history` and `attempts` partitions ahead of time and drops them past retention ([LLD §13.2](../low-level-design.md#132-maintenance)).
- **The audit log** is append-only: "the runtime role can only insert" ([HLD §16.6](../architecture.md#166-audit)). Nothing in the code updates or deletes it.
- **Tests share one PostgreSQL server.** Roles belong to the server, not a database, and several test processes migrate at once.

## Problem

1. Which privileges do the nodes get?
2. How does partition maintenance work without DDL rights?

## Options considered

**Partition maintenance**

| Option | Pros | Cons |
|---|---|---|
| Grant the runtime role ownership of the partitioned tables | No functions | Ownership brings `DROP` and `ALTER` of the parents: no gain |
| Run maintenance as the owner, in a separate job | No DDL at runtime at all | A second credential and deployment for one daily task; partitions could run out if the job stops |
| **`SECURITY DEFINER` functions, owned by the owner, that create or drop one day's partition** | Nodes keep doing maintenance; the functions accept only a known parent and a date | Functions to keep correct: fixed `search_path`, names built inside |
| `pg_partman` | Mature | An extension many managed services restrict; more than daily ranges need |

**Who gets the role**

| Option | Pros | Cons |
|---|---|---|
| Migrations create a login role with a password | One step | Passwords in migrations, or a second secret path |
| **Migrations create a `NOLOGIN` group role; deployments grant it to their login role** | No secrets in migrations; works with IAM authentication on RDS | One manual `GRANT` per environment |

## Decision

- **Group role.** Migration 00012 creates `jobscheduler_runtime` (`NOLOGIN`) if it doesn't exist. A creation that races with another database's migration is tolerated.
- **Privileges.**
  - `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the application tables, and through default privileges on tables later migrations or the partition function create.
  - `audit_log`: `SELECT` and `INSERT` only.
  - `goose_db_version`: `SELECT` only.
  - No `CREATE` on the schema, no `TRUNCATE`, no ownership.
- **Partition functions,** `SECURITY DEFINER` with `search_path = public, pg_temp`:
  - `jobscheduler_create_partition(parent, day)` creates the day's partition if missing and reports whether it did;
  - `jobscheduler_drop_partition(parent, day)` drops it with a 1 s lock timeout, so history inserts never queue long behind it;
  - each rejects any parent other than `job_history` and `attempts` and builds the name and bounds itself;
  - `EXECUTE` is revoked from `PUBLIC` and granted to `jobscheduler_runtime`.
- **Deployments** run migrations as the owner role and the nodes as a login role that is a member of `jobscheduler_runtime` only.

## Trade-offs

- The runtime role can still read and change every tenant's rows. Tenant isolation stays in the application ([ADR-028](ADR-028-row-level-security.md)).
- Migrations need `CREATEROLE`, or the role created beforehand. The RDS master user has it.

## Consequences

- Maintenance calls the functions instead of issuing DDL.
- A test connects as a login role holding only `jobscheduler_runtime` and runs submission, dispatch, completion and maintenance. It also checks that the role cannot create tables or change `audit_log`.

## Revisit when

- Separate API and engine roles would narrow privileges further, for example API nodes never touching `leases`. That needs a list of tables per role, which is worth it once the schema settles.
