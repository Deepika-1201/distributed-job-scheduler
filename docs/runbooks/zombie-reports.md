# ZombieReports

**Meaning:** in a pool, workers report outcomes for attempts that are no longer theirs, more than 5 in 10 minutes. The engine rejected the reports safely: completions are fenced by attempt ID and number.

**Impact:** none on platform state. It means some work ran on a worker that had lost its session, possibly at the same time as the retry. Duplicate side effects are possible downstream if handlers aren't idempotent.

## Diagnose

1. **Long pauses:** workers that stall past their fence, from garbage collection, CPU starvation or a frozen host, lose their session and report late. Check the pool's CPU against its task limits.
2. **Partitions:** worker logs show `heartbeat failed` followed by `session lost`.
3. **Riding out an outage:** workers ride out a database outage their engine reports (ADR-029). If the database was out longer than the outage tolerance (5 min), their sessions expired.

## Fix

- Give the pool more CPU, or fewer slots per worker.
- Check that the affected handlers deduplicate on the job ID.
