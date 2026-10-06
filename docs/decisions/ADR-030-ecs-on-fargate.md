# ADR-030: Container runtime: ECS on Fargate

- **Status:** Accepted. Settles the runtime [ADR-010](ADR-010-deployment-strategy.md) left pending.
- **Date:** 2026-10-06
- **Related:** [ADR-006](ADR-006-leader-election.md), [ADR-026](ADR-026-tls-in-process.md), [ADR-027](ADR-027-least-privilege-database-roles.md), [HLD §18](../architecture.md#18-deployment-architecture), [LLD §22](../low-level-design.md#22-deployment)

## Context

- **ADR-010's criteria:** the cost of on-demand environments, learning value, autoscaling on backlog, local parity and operational burden.
- **The workloads are simple to schedule.** One stateless image runs as `api` and `engine`, plus one image per worker pool. Leases live in PostgreSQL, not in the orchestrator ([ADR-006](ADR-006-leader-election.md)), so nothing needs Kubernetes primitives.
- **Environments are created and destroyed on demand** (NFR-14), from CI.
- **Phase 5 showed database CPU is the binding constraint,** not compute orchestration.

## Options considered

| Option | Cost per environment | Time to create and destroy | Autoscaling on backlog | Operating burden |
|---|---|---|---|---|
| EKS | About $73 a month for the control plane, plus nodes or Fargate | 15\u201320 minutes each way | KEDA with a Prometheus scaler | Cluster upgrades, add-ons, node groups, a second IaC layer (manifests or Helm) |
| **ECS on Fargate** | Nothing for the cluster; per-task pricing | A few minutes | Target tracking on a CloudWatch metric, published by the telemetry collector | Task definitions only, all in Terraform |
| EC2 with systemd | Cheapest compute | Fast | Hand-built | Patching, rollout and health tooling |

## Decision

- **ECS on Fargate,** with every resource in Terraform: no cluster to upgrade, nothing to pay while nothing runs, and one IaC layer.
- **Services:** `api` (2 tasks), `engine` (2 tasks) and one service per worker pool, spread over two availability zones.
- **Traffic:** an internal Network Load Balancer passes TCP through, and the processes terminate TLS themselves ([ADR-026](ADR-026-tls-in-process.md)). The API listens on port 443 and the worker protocol on port 7070. Health checks hit `/readyz` on the ops port.
- **Telemetry:** an AWS Distro for OpenTelemetry collector runs as a sidecar in each task:
  - traces go to X-Ray;
  - metrics go to Amazon Managed Service for Prometheus, which evaluates `deploy/prometheus/alerts.yml` unchanged;
  - backlog metrics also go to CloudWatch, for worker autoscaling.
- **Local parity:** `docker compose` stays the local environment. The same image and environment variables run in both.

## Trade-offs

- AWS-specific: a move to Kubernetes would mean rewriting the service layer of the Terraform. The image, configuration and probes would carry over.
- Autoscaling goes through CloudWatch, one hop more than KEDA reading Prometheus.
- No ECS-native rolling-update hooks: the engine's graceful lease handover and the workers' drain rely on `SIGTERM` and the task's stop timeout, which the task definitions set.

## Consequences

- Terraform in `deploy/terraform`: a bootstrap root (state bucket, GitHub OIDC role) and an environment root built from network, database and service modules.
- A manual workflow creates and destroys an environment from CI.
- The image gains the RDS CA bundle, so database connections verify the server (`sslmode=verify-full`).

## Revisit when

- Pools need autoscaling faster than CloudWatch's one-minute resolution.
- The organization standardizes on Kubernetes.
