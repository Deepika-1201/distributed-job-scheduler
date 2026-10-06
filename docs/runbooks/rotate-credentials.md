# Rotating credentials

| Credential | How |
|---|---|
| API keys | `POST /v1/api-keys/<id>/rotate` with a grace period, and move clients to the new key before it ends (LLD §20.2) |
| Per-pool worker tokens | Issue a new token (`POST /v1/pools/<pool>/worker-tokens`), roll the pool's workers onto it, then revoke the old one once its `last_used_at` stops moving (ADR-025) |
| Cluster worker token | Taint `random_password.worker_token` and run the `deploy` workflow: engines and workers restart with the new value |
| Runtime database password | Taint `random_password.runtime_db` and run the `deploy` workflow. Migrate sets the new password, then the services restart with it. Connections already open keep working until their tasks are replaced. |
| Master database password | RDS rotates it in its own secret; only the migrate task uses it |
| TLS certificate | Renewed by Terraform within 30 days of expiry on the next `deploy` run; the services restart with it (LLD §22.3) |

To taint a resource, run the following before the workflow, with the same backend settings:

```sh
terraform -chdir=deploy/terraform/env taint random_password.worker_token
```
