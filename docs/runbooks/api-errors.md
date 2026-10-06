# APIErrors

**Meaning:** more than 1% of API responses are 5xx, for 5 minutes. Three codes count:

- `500 internal`, a bug or an unexpected failure;
- `503 unavailable`, when the database can't be reached or doesn't answer within 10 s;
- `503 overloaded`, when admission sheds load by priority.

**Impact:** clients see errors. `503`s carry `Retry-After`, and clients retry submissions safely with `Idempotency-Key`.

## Diagnose

1. **Which code?** Break the ratio down: `sum by (http_response_status_code) (rate(http_server_request_duration_seconds_count{server_address="api"}[5m]))`.
2. **`503 overloaded`:** this is shedding, not a fault. See [backlog-age.md](backlog-age.md).
3. **`503 unavailable`:** a database problem. Check the RDS events and metrics, and [database-connections-saturated.md](database-connections-saturated.md). During a failover these last one to a few minutes.
4. **`500 internal`:** search the API logs for `request failed`, which carries the `request_id`. The access-log line with the same `request_id` has the `trace_id`; follow the trace in X-Ray.

## Fix

- **Overload:** scale the pool's workers, or raise the shedding target if it is too tight.
- **Database:** wait out a failover, or scale the instance.
- **A bad release:** roll back ([rollback.md](rollback.md)).
