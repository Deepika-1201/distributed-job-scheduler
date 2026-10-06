# API on CPU; worker pools on how long their oldest dispatchable job has waited (HLD §18.5).

resource "aws_appautoscaling_target" "api" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.this.name}/${module.api.service_name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = var.api_count.min
  max_capacity       = var.api_count.max
}

resource "aws_appautoscaling_policy" "api_cpu" {
  name               = "cpu"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.api.service_namespace
  resource_id        = aws_appautoscaling_target.api.resource_id
  scalable_dimension = aws_appautoscaling_target.api.scalable_dimension
  target_tracking_scaling_policy_configuration {
    target_value       = 60
    scale_out_cooldown = 60
    scale_in_cooldown  = 120
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}

resource "aws_appautoscaling_target" "worker" {
  for_each           = var.worker_pools
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.this.name}/${module.worker[each.key].service_name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = each.value.min
  max_capacity       = each.value.max
}

resource "aws_appautoscaling_policy" "worker_backlog" {
  for_each           = var.worker_pools
  name               = "backlog-age"
  policy_type        = "TargetTrackingScaling"
  service_namespace  = aws_appautoscaling_target.worker[each.key].service_namespace
  resource_id        = aws_appautoscaling_target.worker[each.key].resource_id
  scalable_dimension = aws_appautoscaling_target.worker[each.key].scalable_dimension
  target_tracking_scaling_policy_configuration {
    target_value       = each.value.backlog_age_target
    scale_out_cooldown = 60
    scale_in_cooldown  = 300
    customized_metric_specification {
      namespace   = "JobScheduler"
      metric_name = "jobs_oldest_ready_age_seconds"
      statistic   = "Maximum"
      dimensions {
        name  = "pool"
        value = each.key
      }
    }
  }
}
