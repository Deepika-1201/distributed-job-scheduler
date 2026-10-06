# Rolling back a release

Releases are rolled back by redeploying the previous image. Migrations are expand/contract, so the previous release runs on the current schema (HLD §18.4).

1. Find the commit that was running before. It is the previous successful run of the `deploy` workflow for the environment.
2. Run the `deploy` workflow from that commit:

   ```sh
   gh workflow run deploy.yml --ref <commit> -f name=$ENV -f action=apply
   ```

   Its migrate step finds nothing to apply, since the newer migrations stay. The services roll back one task at a time for engines, and with connection draining for the API.
3. Watch the rollout: `aws ecs describe-services --cluster jobscheduler-$ENV --services jobscheduler-$ENV-api jobscheduler-$ENV-engine`.

**When a deployment fails by itself,** ECS's deployment circuit breaker rolls the service back to its last steady task definition. The workflow then fails at `services-stable`.

**Never roll a migration back by hand** while the new release may still be running. Write a forward migration instead.
