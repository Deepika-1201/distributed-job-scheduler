# Runbooks

One runbook per alert in [`deploy/prometheus/alerts.yml`](../../deploy/prometheus/alerts.yml), linked from each alert's `runbook_url`, plus the operational procedures (LLD §22.8).

## Alerts

| Alert | Severity | Runbook |
|---|---|---|
| `SchedulingLagSLO` | page | [scheduling-lag-slo.md](scheduling-lag-slo.md) |
| `BacklogAge` | page | [backlog-age.md](backlog-age.md) |
| `DispatchStalled` | page | [dispatch-stalled.md](dispatch-stalled.md) |
| `NoPoolOwner` | page | [no-pool-owner.md](no-pool-owner.md) |
| `SessionExpirySpike` | page | [session-expiry-spike.md](session-expiry-spike.md) |
| `APIErrors` | page | [api-errors.md](api-errors.md) |
| `DeadLetterSpike` | ticket | [dead-letter-spike.md](dead-letter-spike.md) |
| `ZombieReports` | ticket | [zombie-reports.md](zombie-reports.md) |
| `DatabaseConnectionsSaturated` | ticket | [database-connections-saturated.md](database-connections-saturated.md) |
| `ClockOffset` | ticket | [clock-offset.md](clock-offset.md) |

## Procedures

- [Rolling back a release](rollback.md)
- [Database failover drill](database-failover-drill.md)
- [Restoring the database](restore-database.md)
- [Rotating credentials](rotate-credentials.md)

## Conventions

- `$ENV` is the environment name. Log groups are `/jobscheduler/jobscheduler-$ENV-<service>`, for example `aws logs tail /jobscheduler/jobscheduler-$ENV-engine --since 30m`.
- API calls need a key: `-H "Authorization: Bearer $KEY"`. Pool and worker endpoints need `platform-admin`.
- Logs are JSON. Filter by `component` (`dispatcher`, `reaper`, `pool-leases`, `singleton-leases`), `pool`, `request_id` and `trace_id`.
- Database queries need a session as the owner role, through a bastion or a one-off ECS task.
