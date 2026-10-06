# One ECS service on Fargate: its task definition, log group, optional collector sidecar and
# load balancer targets (LLD §22.2). With service = false it defines only the task, for
# one-off runs such as migrate.

data "aws_region" "current" {}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/jobscheduler/${var.name}"
  retention_in_days = var.log_retention_days
}

locals {
  log_options = {
    awslogs-group         = aws_cloudwatch_log_group.this.name
    awslogs-region        = data.aws_region.current.region
    awslogs-stream-prefix = "ecs"
  }
  app = {
    name         = "app"
    image        = var.image
    essential    = true
    command      = var.command
    environment  = [for k, v in var.environment : { name = k, value = v }]
    secrets      = [for k, v in var.secrets : { name = k, valueFrom = v }]
    portMappings = [for p in var.ports : { containerPort = p, protocol = "tcp" }]
    stopTimeout  = var.stop_timeout
    logConfiguration = {
      logDriver = "awslogs"
      options   = local.log_options
    }
  }
  # Non-essential: losing telemetry must not stop the platform.
  collector = var.collector == null ? [] : [{
    name        = "collector"
    image       = var.collector.image
    essential   = false
    environment = [{ name = "AOT_CONFIG_CONTENT", value = var.collector.config }]
    logConfiguration = {
      logDriver = "awslogs"
      options   = merge(local.log_options, { awslogs-stream-prefix = "collector" })
    }
  }]
}

resource "aws_ecs_task_definition" "this" {
  family                   = var.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn
  container_definitions    = jsonencode(concat([local.app], local.collector))

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
}

resource "aws_ecs_service" "this" {
  count                              = var.service ? 1 : 0
  name                               = var.name
  cluster                            = var.cluster_arn
  task_definition                    = aws_ecs_task_definition.this.arn
  desired_count                      = var.desired_count
  launch_type                        = "FARGATE"
  deployment_minimum_healthy_percent = var.deployment_minimum_healthy_percent
  deployment_maximum_percent         = var.deployment_maximum_percent
  health_check_grace_period_seconds  = length(var.target_groups) > 0 ? 60 : null
  propagate_tags                     = "SERVICE"
  wait_for_steady_state              = false

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = var.security_group_ids
    assign_public_ip = false
  }

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  dynamic "load_balancer" {
    for_each = var.target_groups
    content {
      target_group_arn = load_balancer.value.arn
      container_name   = "app"
      container_port   = load_balancer.value.port
    }
  }

  lifecycle {
    # Autoscaling owns the count once the service exists.
    ignore_changes = [desired_count]
  }
}
