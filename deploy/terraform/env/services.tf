# The platform's services and the one-off migrate task (LLD §22.2, §22.5).

locals {
  platform_repository = aws_ecr_repository.this["platform"].repository_url
  runtime_role        = "jobscheduler_app"
  # pgx takes the password from PGPASSWORD, so the URL carries none (LLD §22.4).
  database_url = "postgres://%s@${module.database.address}:${module.database.port}/${module.database.database}?sslmode=verify-full&sslrootcert=/etc/ssl/certs/rds-global-bundle.pem"
  secret       = { for k, s in aws_secretsmanager_secret.this : k => s.arn }

  platform_environment = {
    JS_LOG_FORMAT    = "json"
    JS_DATABASE_URL  = format(local.database_url, local.runtime_role)
    JS_OTLP_ENDPOINT = "127.0.0.1:4317"
    JS_OTLP_INSECURE = "true" # the collector sidecar, on the task's loopback
  }
  platform_secrets = {
    PGPASSWORD  = local.secret["runtime-db-password"]
    JS_TLS_CERT = local.secret["tls-cert"]
    JS_TLS_KEY  = local.secret["tls-key"]
  }
}

module "api" {
  source      = "../modules/service"
  name        = "jobscheduler-${var.name}-api"
  cluster_arn = aws_ecs_cluster.this.arn
  image       = "${local.platform_repository}:${var.image_tag}"
  command     = ["serve"]
  environment = merge(local.platform_environment, {
    JS_ROLES          = "api"
    JS_API_REPLICAS   = tostring(var.api_count.min)
    JS_DB_MAX_CONNS   = "20"
    JS_SHUTDOWN_DELAY = "5s" # keep serving while the load balancer deregisters the task
  })
  secrets            = local.platform_secrets
  ports              = [8080, 9090]
  desired_count      = var.api_count.min
  stop_timeout       = 45
  subnet_ids         = module.network.private_subnet_ids
  security_group_ids = [aws_security_group.api.id]
  execution_role_arn = aws_iam_role.execution.arn
  task_role_arn      = aws_iam_role.telemetry.arn
  target_groups      = [{ arn = aws_lb_target_group.this["api"].arn, port = 8080 }]
  collector          = { image = var.collector_image, config = local.collector_config["api"] }
}

module "engine" {
  source      = "../modules/service"
  name        = "jobscheduler-${var.name}-engine"
  cluster_arn = aws_ecs_cluster.this.arn
  image       = "${local.platform_repository}:${var.image_tag}"
  command     = ["serve"]
  environment = merge(local.platform_environment, {
    JS_ROLES        = "engine"
    JS_DB_MAX_CONNS = "30"
  })
  secrets       = merge(local.platform_secrets, { JS_WORKER_TOKEN = local.secret["worker-token"] })
  ports         = [7070, 9090]
  desired_count = var.engine_count
  # One task at a time; on SIGTERM an engine hands its leases over (HLD §18.4).
  deployment_minimum_healthy_percent = 50
  deployment_maximum_percent         = 100
  stop_timeout                       = 60
  subnet_ids                         = module.network.private_subnet_ids
  security_group_ids                 = [aws_security_group.engine.id]
  execution_role_arn                 = aws_iam_role.execution.arn
  task_role_arn                      = aws_iam_role.telemetry.arn
  target_groups                      = [{ arn = aws_lb_target_group.this["engine"].arn, port = 7070 }]
  collector                          = { image = var.collector_image, config = local.collector_config["engine"] }
}

# Worker pools. They use the cluster worker token here; pools run by other teams get per-pool
# tokens through the API instead (ADR-025).
module "worker" {
  for_each    = var.worker_pools
  source      = "../modules/service"
  name        = "jobscheduler-${var.name}-worker-${each.key}"
  cluster_arn = aws_ecs_cluster.this.arn
  image       = "${coalesce(each.value.image_repository, aws_ecr_repository.this["demo-worker"].repository_url)}:${var.image_tag}"
  command     = each.value.command
  environment = {
    JS_WORKER_POOL            = each.key
    JS_ENGINE_ADDR            = "${aws_lb.this.dns_name}:7070"
    JS_WORKER_TLS_CA          = tls_self_signed_cert.ca.cert_pem
    JS_WORKER_TLS_SERVER_NAME = local.engine_server_name
  }
  secrets            = { JS_WORKER_TOKEN = local.secret["worker-token"] }
  cpu                = each.value.cpu
  memory             = each.value.memory
  desired_count      = each.value.min
  stop_timeout       = each.value.stop_timeout
  subnet_ids         = module.network.private_subnet_ids
  security_group_ids = [aws_security_group.worker.id]
  execution_role_arn = aws_iam_role.execution.arn
}

module "migrate" {
  source  = "../modules/service"
  service = false
  name    = "jobscheduler-${var.name}-migrate"
  image   = "${local.platform_repository}:${var.migrate_image_tag}"
  command = ["migrate"]
  environment = {
    JS_LOG_FORMAT      = "json"
    JS_DATABASE_URL    = format(local.database_url, module.database.owner)
    JS_RUNTIME_DB_ROLE = local.runtime_role
  }
  secrets = {
    PGPASSWORD             = "${module.database.master_secret_arn}:password::"
    JS_RUNTIME_DB_PASSWORD = local.secret["runtime-db-password"]
  }
  cpu                = 256
  memory             = 512
  execution_role_arn = aws_iam_role.execution.arn
}
