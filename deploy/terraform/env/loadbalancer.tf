# One Network Load Balancer passes TCP through to the tasks, which terminate TLS (ADR-030):
# 443 to the API, 7070 to the engines.

resource "aws_lb" "this" {
  name               = "js-${var.name}" # load balancer and target group names stop at 32 characters
  load_balancer_type = "network"
  internal           = !var.public_endpoint
  subnets            = var.public_endpoint ? module.network.public_subnet_ids : module.network.private_subnet_ids
  security_groups    = [aws_security_group.lb.id]
}

locals {
  listeners = {
    api    = { listen = 443, port = 8080, deregistration = 30 }
    engine = { listen = 7070, port = 7070, deregistration = 60 }
  }
}

resource "aws_lb_target_group" "this" {
  for_each             = local.listeners
  name                 = "js-${var.name}-${each.key}"
  port                 = each.value.port
  protocol             = "TCP"
  target_type          = "ip"
  vpc_id               = module.network.vpc_id
  deregistration_delay = each.value.deregistration

  # Liveness, not readiness: ECS replaces tasks that fail this check, and replacing every task
  # during a database outage would only add a restart storm (HLD §18.3).
  health_check {
    protocol            = "HTTP"
    port                = "9090"
    path                = "/livez"
    interval            = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "this" {
  for_each          = local.listeners
  load_balancer_arn = aws_lb.this.arn
  port              = each.value.listen
  protocol          = "TCP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this[each.key].arn
  }
}
