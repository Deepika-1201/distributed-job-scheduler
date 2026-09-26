# ADR-010: Deployment strategy

- **Status:** Accepted in part. The container runtime is pending Technology Selection.
- **Date:** 2026-09-26
- **Related:** [HLD §18](../architecture.md#18-deployment-architecture), [ADR-004](ADR-004-database.md), [ADR-009](ADR-009-modular-monolith.md)

## Context

- **Targets:** AWS, one region, 99.9% availability, RPO ≈ 0.
- **Operations:** rolling deploys without losing jobs; environments created on demand to control cost; `docker compose up` for local development.
- **Workloads:** a stateless `api`, a small `engine` fleet with lease-based ownership, and worker pools that autoscale on backlog.

## Problem

Where and how does the platform run in production?

## Decided now

| Aspect | Decision |
|---|---|
| Cloud and topology | AWS, one region, at least 2 availability zones |
| Database | Managed PostgreSQL, Multi-AZ with a synchronous standby, point-in-time recovery, encrypted |
| Packaging | Containers: one image for `api` and `engine` (role flag), one image per worker pool |
| Infrastructure as code | Terraform for all infrastructure; environments created and destroyed on demand |
| Traffic | Internal load balancer for the API; internal load balancer with long-lived HTTP/2 support for the worker protocol |
| Secrets | AWS Secrets Manager and KMS, injected at runtime |
| Releases | Rolling deploys; `engine` releases leases on shutdown; workers drain; expand/contract migrations as a separate step |
| Health | Liveness checks don't touch dependencies; readiness checks do |
| Disaster recovery | In-region failover to the synchronous standby; cross-region snapshot restore as a manual runbook |

## Pending: container runtime

| Option | Pros | Cons |
|---|---|---|
| EKS (Kubernetes) | Industry-standard skills; KEDA for backlog-based autoscaling; local parity with kind or k3d; rich ecosystem | Control-plane fee (about $0.10/h per cluster); more to operate (node groups, add-ons, upgrades) |
| ECS on Fargate | No cluster to manage; per-task pricing; simpler IAM and networking | AWS-specific; autoscaling on custom metrics goes through CloudWatch; weaker local parity |
| EC2 VMs with systemd | Cheapest and most transparent | Manual rollout, health and scaling tooling; poor fit for autoscaling worker pools |

**Criteria for Technology Selection:** cost of on-demand environments, learning value, autoscaling on backlog metrics, local parity and operational burden. The architecture works on either EKS or ECS: leases live in PostgreSQL, not in the orchestrator ([ADR-006](ADR-006-leader-election.md)).

## Trade-offs

- A single region means a regional outage is downtime, which is accepted for 99.9%.
- On-demand environments mean nothing runs continuously for demos without a deliberate spin-up.

## Consequences

- Terraform modules are written from the start (network, database, services, telemetry, secrets).
- Deployment runbooks cover rollback, database failover drills and restoring from snapshots.

## Revisit when

- Technology Selection completes; this ADR is then updated with the runtime decision.
- An availability target above 99.95% or data-residency rules call for multi-region.
