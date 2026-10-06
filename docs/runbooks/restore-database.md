# Restoring the database

The database keeps 7 days of point-in-time recovery (HLD §18.6). A restore creates a new instance; the platform then moves to it.

1. **Stop new work.** Scale the API and engine services to 0, so nothing writes to the old instance:

   ```sh
   for s in api engine; do aws ecs update-service --cluster jobscheduler-$ENV --service jobscheduler-$ENV-$s --desired-count 0; done
   ```

2. **Restore** to a time before the problem:

   ```sh
   aws rds restore-db-instance-to-point-in-time --source-db-instance-identifier jobscheduler-$ENV \
     --target-db-instance-identifier jobscheduler-$ENV-restored --restore-time <UTC time> \
     --db-subnet-group-name jobscheduler-$ENV --multi-az
   ```

   Attach the original security group and parameter group once the instance is available.
3. **Point the platform at it.** Rename the old instance away and the restored one to `jobscheduler-$ENV`, so endpoints stay the same. Then run the `deploy` workflow, which migrates and starts the services.
4. **Reconcile:**
   - jobs that ran after the restore point are lost from history;
   - schedules fire again under their misfire policy;
   - clients retry submissions the API acknowledged after the restore point, with their idempotency keys.

**Regional disaster:** copy snapshots to another region and restore there with a new environment (`region` variable). In V1 this is manual, with an RTO of hours (HLD §18.6).
