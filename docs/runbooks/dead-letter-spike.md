# DeadLetterSpike

**Meaning:** a job type dead-letters at more than three times its daily rate, with at least 10 in 10 minutes. Jobs dead-letter when their retries are exhausted, their overall deadline passes, or they were lost three times (poison).

**Impact:** those jobs won't run again until someone re-drives them.

## Diagnose

1. **Last errors:** `GET /v1/jobs?state=DEAD_LETTERED`, then `GET /v1/jobs/<id>/attempts` for a few of them. Group them by error.
2. **The same downstream failing** (timeouts, `5xx` from a dependency) points at that dependency.
3. **`LOST` attempts** point at crashing workers. See [session-expiry-spike.md](session-expiry-spike.md).
4. **A recent release of the worker image** or of the job type's schema.

## Fix

1. Fix the cause: the dependency, the handler, or the payloads.
2. Re-drive the jobs:

   ```sh
   curl -X POST "$API/v1/operations" -H "Authorization: Bearer $KEY" \
     -d '{"kind": "redrive", "filter": {"state": "DEAD_LETTERED", "type": "<type>"}}'
   ```

   Then follow `GET /v1/operations/<id>` until it finishes.
