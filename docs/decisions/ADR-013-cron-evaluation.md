# ADR-013: Cron evaluation with explicit DST rules

- **Status:** Accepted. Amends the cron row of [ADR-012](ADR-012-language-and-core-libraries.md).
- **Date:** 2026-09-26
- **Related:** [ADR-001](ADR-001-scheduler-architecture.md), [HLD §11.1](../architecture.md#111-triggers), [LLD §10.2](../low-level-design.md#102-triggers)

## Context

- **Required rules.** Cron schedules run in an IANA time zone (FR-9). HLD §11.1 fixes their behavior at DST changes:
  - a local time skipped by a DST gap fires at the next valid instant;
  - a local time repeated by an overlap fires once.
- **Earlier plans.** ADR-012 planned an established parser plus our own DST-aware next-fire computation. HLD constraint C8 counts cron parsing among the routine concerns left to libraries.
- **The obvious library fails these rules.** `robfig/cron` v3 is the de facto Go library. Checked against v3.0.1 in `America/New_York`, 2026:

| Expression | DST change | `robfig/cron` v3.0.1 | Required |
|---|---|---|---|
| `30 2 * * *` | Spring forward, Mar 8 | Next fire Mar 9, 02:30: the Mar 8 run is lost | Mar 8, 03:00 EDT |
| `30 1 * * *` | Fall back, Nov 1 | 01:30 EDT, then 01:30 EST: two runs | 01:30 EDT only |
| `*/30 * * * *` | Fall back, Nov 1 | 01:00 and 01:30 EDT, then 01:00 and 01:30 EST | Each local time once |

- **Why the double fire matters.** The two runs have different instants, so the `schedule_fires` ledger (I2) would accept both.

## Problem

How is a cron schedule's next fire time computed so that DST behavior is exactly the documented behavior?

## Options considered

| Option | Pros | Cons |
|---|---|---|
| A. `robfig/cron` v3 as is | Widely used; parser and evaluator in one | Breaks both DST rules (table above): loses runs in gaps and duplicates them in overlaps |
| B. `robfig/cron`'s parser plus our own evaluator | Reuses a tested parser; `SpecSchedule` exports the field bit sets | Couples us to its internals: whether a day field was `*`, which day matching depends on, is an unexported marker bit. The evaluator, the hard part, would still be ours. |
| **C. Our own parser and evaluator in `domain`** | The rules are implemented and tested exactly; the domain package stays standard-library-only; about 300 lines | We own a parser and its syntax surface |

## Decision

**Option C.**

- **Syntax:**
  - five fields, or six with seconds first;
  - `*`, `?`, values, ranges, steps, lists, and month and weekday names;
  - the macros `@yearly` to `@hourly`.
- **Rejected at creation:** `L`, `W` and `#`, and expressions that can never fire, such as `0 0 30 2 *`.
- **Evaluation:**
  1. Search wall-clock time field by field. Wall time is represented in UTC, which has no DST, so the arithmetic is exact.
  2. Map each matching wall time to an instant: a time in a gap maps to the gap's end, and a repeated time to its first occurrence.
- **Tests:**
  - the cases in the table above;
  - `Europe/London`, and `Australia/Lord_Howe` with its 30-minute shift;
  - a property test over a year of hourly fires in four zones.

## Trade-offs

- We maintain a parser. Syntax outside the documented subset is rejected rather than guessed.
- Firing a repeated time once means an every-15-minutes cron pauses for the repeated hour. Fixed-rate triggers are the choice for real-time intervals (LLD §10.2).

## Consequences

- The engine has no cron dependency, and `domain` stays standard-library-only, which a test enforces.
- HLD constraint C8 no longer counts cron parsing as a library concern.
- DST behavior is part of the API contract. `GET /v1/schedules/{id}` returns the next five fire times, so users can check them.

## Revisit when

- Users need Quartz extensions (`L`, `W`, `#`) or calendar exclusions.
- A maintained library documents and tests these exact DST rules.
