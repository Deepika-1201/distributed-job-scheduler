locals {
  engine_server_name = "engine.${var.name}.internal"
  api_server_name    = "api.${var.name}.internal"
  client_cidrs       = concat([module.network.vpc_cidr], var.allowed_cidrs)
}

data "aws_caller_identity" "current" {}

module "network" {
  source     = "../modules/network"
  name       = "jobscheduler-${var.name}"
  cidr       = var.vpc_cidr
  nat_per_az = var.nat_per_az
}

resource "aws_kms_key" "env" {
  description             = "jobscheduler ${var.name}: database, secrets"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "env" {
  name          = "alias/jobscheduler-${var.name}"
  target_key_id = aws_kms_key.env.key_id
}

resource "aws_ecs_cluster" "this" {
  name = "jobscheduler-${var.name}"
}

# One security group per role (LLD §22.2).
resource "aws_security_group" "lb" {
  name        = "jobscheduler-${var.name}-lb"
  description = "Load balancer: the API on 443, the worker protocol on 7070"
  vpc_id      = module.network.vpc_id
}

resource "aws_security_group" "api" {
  name        = "jobscheduler-${var.name}-api"
  description = "API tasks"
  vpc_id      = module.network.vpc_id
}

resource "aws_security_group" "engine" {
  name        = "jobscheduler-${var.name}-engine"
  description = "Engine tasks"
  vpc_id      = module.network.vpc_id
}

resource "aws_security_group" "worker" {
  name        = "jobscheduler-${var.name}-worker"
  description = "Worker tasks"
  vpc_id      = module.network.vpc_id
}

resource "aws_security_group" "migrate" {
  name        = "jobscheduler-${var.name}-migrate"
  description = "The one-off migrate task"
  vpc_id      = module.network.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "lb" {
  for_each          = { for pair in setproduct(local.client_cidrs, [443, 7070]) : "${pair[0]}-${pair[1]}" => pair }
  security_group_id = aws_security_group.lb.id
  cidr_ipv4         = each.value[0]
  ip_protocol       = "tcp"
  from_port         = each.value[1]
  to_port           = each.value[1]
}

locals {
  # Who may reach each task port: the load balancer forwards and health-checks; workers
  # follow redirects straight to the pool owner's engine.
  task_ingress = {
    "api-8080-lb"        = { sg = aws_security_group.api.id, port = 8080, from = aws_security_group.lb.id }
    "api-9090-lb"        = { sg = aws_security_group.api.id, port = 9090, from = aws_security_group.lb.id }
    "engine-7070-lb"     = { sg = aws_security_group.engine.id, port = 7070, from = aws_security_group.lb.id }
    "engine-9090-lb"     = { sg = aws_security_group.engine.id, port = 9090, from = aws_security_group.lb.id }
    "engine-7070-worker" = { sg = aws_security_group.engine.id, port = 7070, from = aws_security_group.worker.id }
  }
}

resource "aws_vpc_security_group_ingress_rule" "tasks" {
  for_each                     = local.task_ingress
  security_group_id            = each.value.sg
  referenced_security_group_id = each.value.from
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
}

resource "aws_vpc_security_group_egress_rule" "all" {
  for_each = {
    lb      = aws_security_group.lb.id
    api     = aws_security_group.api.id
    engine  = aws_security_group.engine.id
    worker  = aws_security_group.worker.id
    migrate = aws_security_group.migrate.id
  }
  security_group_id = each.value
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

module "database" {
  source                    = "../modules/database"
  name                      = "jobscheduler-${var.name}"
  vpc_id                    = module.network.vpc_id
  subnet_ids                = module.network.private_subnet_ids
  client_security_group_ids = [aws_security_group.api.id, aws_security_group.engine.id, aws_security_group.migrate.id]
  instance_class            = var.db_instance_class
  kms_key_arn               = aws_kms_key.env.arn
  ephemeral                 = var.ephemeral
}

# Execution role: pulls images, reads this environment's secrets, writes logs.
resource "aws_iam_role" "execution" {
  name = "jobscheduler-${var.name}-execution"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ecs-tasks.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "execution_secrets" {
  name = "secrets"
  role = aws_iam_role.execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = concat([for s in aws_secretsmanager_secret.this : s.arn], [module.database.master_secret_arn])
      },
      { Effect = "Allow", Action = "kms:Decrypt", Resource = aws_kms_key.env.arn },
    ]
  })
}

# Task role for api and engine: what their collector sidecar sends to AWS (LLD §22.6).
resource "aws_iam_role" "telemetry" {
  name = "jobscheduler-${var.name}-telemetry"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ecs-tasks.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy" "telemetry" {
  name = "telemetry"
  role = aws_iam_role.telemetry.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = "aps:RemoteWrite", Resource = aws_prometheus_workspace.this.arn },
      {
        Effect   = "Allow"
        Action   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
        Resource = "${aws_cloudwatch_log_group.metrics.arn}:*"
      },
    ]
  })
}
