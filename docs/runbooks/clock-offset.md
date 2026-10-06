# ClockOffset

**Meaning:** a node's clock is more than 0.5 s off the database's, for 5 minutes.

**Impact:** correctness doesn't depend on node clocks. Leases, due times and deadlines use the database clock (HLD S13). Node clocks time local deadlines and timestamp logs and metrics, so those drift.

## Diagnose

- Fargate tasks sync their clocks through the Amazon Time Sync Service. A large offset on one task points at that host. An offset on every node points at the database host.

## Fix

- **One task:** stop it, and ECS replaces it on another host.
- **Every node:** open an AWS support case for the database instance, or fail it over ([database-failover-drill.md](database-failover-drill.md)).
