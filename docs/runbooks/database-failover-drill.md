# Database failover drill

Checks that the platform rides out a managed failover (HLD S6, ADR-029). Run it every release cycle on a non-production environment.

## Before

- Keep a steady load running, for example the load-test harness at a low rate, and a few long jobs (`demo.sleep` for 10 minutes).
- Note the running jobs and their attempt numbers.

## Run

```sh
aws rds reboot-db-instance --db-instance-identifier jobscheduler-$ENV --force-failover
```

## Expect

1. For one to a few minutes:
   - the API answers `503 unavailable` with `Retry-After`;
   - the engines answer workers `DATABASE_UNAVAILABLE`;
   - lag metrics rise.
2. Running handlers keep running: worker logs show `heartbeat failed` with `riding_out_outage=true`, and no `session lost`.
3. After the failover:
   - heartbeats succeed within a heartbeat interval;
   - outcomes reported meanwhile land;
   - pools have owners again within about 13 s.
4. Long jobs finish with a single attempt. `SessionExpirySpike` and `ZombieReports` stay quiet.

## If not

- Sessions expired anyway: check the outage lasted less than the outage tolerance (5 min), and the engines' `engine_nodes.beat_at`.
- Handlers were cancelled: check that worker SDKs are recent enough to read `outage_tolerance` (ADR-029).
