# ADR-019: Payload validation with JSON Schema

- **Status:** Accepted
- **Date:** 2026-09-27
- **Related:** [ADR-012](ADR-012-language-and-core-libraries.md), [HLD §15.2](../architecture.md#152-backpressure-and-admission-control), [HLD §16.1](../architecture.md#161-trust-boundaries), [LLD §16](../low-level-design.md#16-payload-schemas-and-listing-details)

## Context

- **FR-1.** A job type can declare a JSON Schema for its payloads.
- **HLD §15.2.** Schema validation is layer 1 of admission control and answers `422`.
- **What exists.** The `job_types.payload_schema` column has existed since the first migration, but nothing reads it.
- **Trust.** Schemas are written by tenant admins, so the server must treat them as untrusted input.

## Problem

Which validator, and how are untrusted schemas kept from reaching outside themselves?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| A. No validation; handlers validate | Nothing to build | FR-1 isn't met; bad payloads are caught only at run time, after retries |
| B. `xeipuuv/gojsonschema` | Widely used | No release since 2019 (v1.2.0); drafts up to 7 only |
| C. `google/jsonschema-go` | Maintained | Pre-1.0 (v0.4.3), so the API may still change |
| **D. `santhosh-tekuri/jsonschema` v6** | Maintained (v6.0.3, 2026); drafts 4 through 2020-12; detailed error locations | Its default loader reads local files (see below) |

**Verified against v6.0.3:**

- A schema with `{"$ref": "file:///etc/hosts"}` made the default loader read the file. It failed only because the content isn't JSON, and a JSON file would have loaded.
- `https` references aren't fetched by default.
- Invalid schemas, such as `{"type": "strng"}`, are rejected at compile time against the metaschema.

## Decision

**Option D, with a loader that refuses every external reference.**

- **References:** only same-document `$ref`s (`#/$defs/...`) resolve. Any `file:`, `http:` or other URL fails compilation with `422` on `payload_schema`.
- **Drafts:** the one the schema's `$schema` declares, defaulting to 2020-12.
- **Formats** such as `email` are annotations, not assertions, as in the 2020-12 default.
- **Limits:** a schema is at most 64 KiB and must compile when the job type is created or updated.
- **Caching:** each `api` node caches compiled schemas by `(tenant, job type, updated_at)`, so an update takes effect at once.
- **Where payloads are validated:** job submission, and schedule create and update. Failures return `422` with the first violations under `payload`.

## Trade-offs

- Validation costs CPU on `api` nodes, proportional to payload size (≤ 64 KiB). Tenants that don't declare schemas pay nothing.
- Jobs a schedule creates later aren't re-validated, so changing a type's schema doesn't re-check existing schedules.

## Consequences

- One new dependency, `santhosh-tekuri/jsonschema/v6`.
- The deny-all loader is a security control, covered by a test: a `file:` reference must be rejected without reading the file.

## Revisit when

- Schemas need shared definitions across types, which means references to other stored schemas, resolved from the database rather than from URLs.
- Tenants need format assertions.
