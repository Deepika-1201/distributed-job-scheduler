# Deciding load test

Decides the load-test gate on the proposed production database class: bursts of 5,000 jobs/s with p99 dispatch latency of at most 1 s (ADR-024, LLD §23.5). It meets condition C2 of the [production readiness review](../production-readiness-review.md).

The harness runs on one large load-generator instance inside the environment's VPC. It runs the `api` node, two engines, the worker fleet and k6, against the environment's RDS instance. It uses the `postgres` database there, so the environment's own data in `jobs` is untouched.

## Before

1. Deploy, or redeploy, with the proposed class:

   ```sh
   gh workflow run deploy.yml -f name=$ENV -f action=apply -f db_instance_class=db.m7g.2xlarge
   ```

   Pass the same class on every later apply. The default moves the database back to `db.t4g.medium`.
2. Locally, with AWS CLI v2, the Session Manager plugin and the environment's state initialized as the deploy workflow does (`terraform init -backend-config=bucket=<state bucket> -backend-config=key=env/$ENV.tfstate -backend-config=region=<region>`):

   ```sh
   cd deploy/terraform/env
   DB=$(terraform output -raw database_address)
   SG=$(terraform output -json migrate | jq -r .security_group)   # admitted by the database
   AZ=$(aws rds describe-db-instances --db-instance-identifier jobscheduler-$ENV --query 'DBInstances[0].AvailabilityZone' --output text)
   SECRET=$(aws rds describe-db-instances --db-instance-identifier jobscheduler-$ENV --query 'DBInstances[0].MasterUserSecret.SecretArn' --output text)
   SUBNET=$(aws ec2 describe-subnets --subnet-ids $(terraform output -json migrate | jq -r '.subnets | join(" ")') \
     --query "Subnets[?AvailabilityZone=='$AZ'].SubnetId | [0]" --output text)
   KEY=$(aws secretsmanager describe-secret --secret-id "$SECRET" --query KmsKeyId --output text)
   ```

## Launch the load generator

Use an x86 instance with 32 vCPUs in the primary's availability zone. At 5,000 jobs/s the platform and k6 need about 12 vCPUs.

```sh
aws iam create-role --role-name jobscheduler-$ENV-loadgen --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
aws iam attach-role-policy --role-name jobscheduler-$ENV-loadgen --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam put-role-policy --role-name jobscheduler-$ENV-loadgen --policy-name db-secret --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"secretsmanager:GetSecretValue\",\"Resource\":\"$SECRET\"},{\"Effect\":\"Allow\",\"Action\":\"kms:Decrypt\",\"Resource\":\"$KEY\"}]}"
aws iam create-instance-profile --instance-profile-name jobscheduler-$ENV-loadgen
aws iam add-role-to-instance-profile --instance-profile-name jobscheduler-$ENV-loadgen --role-name jobscheduler-$ENV-loadgen

ID=$(aws ec2 run-instances --instance-type c7i.8xlarge \
  --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --subnet-id "$SUBNET" --security-group-ids "$SG" --iam-instance-profile Name=jobscheduler-$ENV-loadgen \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=jobscheduler-$ENV-loadgen}]" \
  --query 'Instances[0].InstanceId' --output text)
aws ec2 wait instance-status-ok --instance-ids "$ID"
echo "DB=$DB SECRET=$SECRET"   # needed on the instance
aws ssm start-session --target "$ID"
```

The private subnets reach the internet through the NAT gateway, for Go, k6 and the repository.

## Run

On the instance:

```sh
sudo dnf install -y git jq make
curl -fsSL https://go.dev/dl/go1.27.1.linux-amd64.tar.gz | sudo tar -C /usr/local -xz
export PATH=/usr/local/go/bin:$PATH
git clone https://github.com/Deepika-1201/distributed-job-scheduler.git && cd distributed-job-scheduler
curl -fsSLO https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

export DB=<from above> SECRET=<from above>
export PGPASSWORD=$(aws secretsmanager get-secret-value --secret-id "$SECRET" --query SecretString --output text | jq -r .password)
export JS_DATABASE_URL="postgres://jobscheduler_owner@$DB:5432/postgres?sslmode=verify-full&sslrootcert=$PWD/global-bundle.pem"
make loadtest                                     # the gate: 500/s, 5,000/s for 60 s, 500/s
SCENARIO=cron SCHEDULES=5000 make loadtest        # NFR-3 at a cron boundary
```

## Read the result

- **The verdict** is the gate's output (LLD §19.2). It passes when:
  - at least 99% of the offered rate was accepted;
  - at least 99% of dispatch latencies are at most 1 s;
  - dispatches kept up.
- **Database CPU.** The gate prints CPU per job for the nodes. For the database, read the burst minute from CloudWatch:

  ```sh
  aws cloudwatch get-metric-statistics --namespace AWS/RDS --metric-name CPUUtilization \
    --dimensions Name=DBInstanceIdentifier,Value=jobscheduler-$ENV --period 60 --statistics Average Maximum \
    --start-time <burst start, UTC> --end-time <burst end, UTC>
  ```

  Then vCPUs used = average % × 8 / 100, and database CPU per job = vCPUs used ÷ jobs accepted per second.
- **Record the run:**
  - add it to LLD §19.4 and §23.4;
  - mark C2 met in the review;
  - if it fails, follow ADR-024's "Revisit when".

## After

```sh
aws ec2 terminate-instances --instance-ids "$ID" && aws ec2 wait instance-terminated --instance-ids "$ID"
aws iam remove-role-from-instance-profile --instance-profile-name jobscheduler-$ENV-loadgen --role-name jobscheduler-$ENV-loadgen
aws iam delete-instance-profile --instance-profile-name jobscheduler-$ENV-loadgen
aws iam delete-role-policy --role-name jobscheduler-$ENV-loadgen --policy-name db-secret
aws iam detach-role-policy --role-name jobscheduler-$ENV-loadgen --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam delete-role --role-name jobscheduler-$ENV-loadgen
```

- Destroy the environment when done (`-f action=destroy`), or redeploy it with the default class.
- The harness's tenants and tables stay in the `postgres` database until the environment is destroyed.
