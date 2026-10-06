# DatabaseConnectionsSaturated

**Meaning:** a node uses more than 90% of its database connection pool (`JS_DB_MAX_CONNS`), for 5 minutes.

**Impact:** requests and engine loops queue for connections, so latency rises. Phase 5 found that an oversized API pool starved dispatch (LLD §19.4).

## Diagnose

1. **Slow statements hold connections:** compare `db_transaction_duration_seconds` by `operation` with normal. Check RDS Performance Insights for the top statements and wait events.
2. **Lock waits:** look for `lock_timeout` errors in the logs. Statements wait at most 5 s for a lock.
3. **The pool is too small** for the node's load. Check `db_pool_in_use` against the request rate.

## Fix

- Fix the slow statement, or scale the database if its CPU is saturated.
- Raise `JS_DB_MAX_CONNS` for the role, within the database's `max_connections` across every task. Prefer more API tasks over bigger pools.
