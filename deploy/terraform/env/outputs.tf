output "api_url" {
  value = "https://${aws_lb.this.dns_name}"
}

output "worker_endpoint" {
  description = "Where workers connect; they verify engine_server_name in the certificate."
  value       = "${aws_lb.this.dns_name}:7070"
}

output "engine_server_name" {
  value = local.engine_server_name
}

output "ca_certificate" {
  description = "The CA clients and workers trust for the API and the worker protocol."
  value       = tls_self_signed_cert.ca.cert_pem
}

output "image_tag" {
  description = "The tag the services run; the deploy workflow keeps it while migrating (LLD §22.7)."
  value       = var.image_tag
}

output "cluster" {
  value = aws_ecs_cluster.this.name
}

output "services" {
  value = concat([module.api.service_name, module.engine.service_name], [for w in module.worker : w.service_name])
}

output "target_group_arns" {
  value = [for tg in aws_lb_target_group.this : tg.arn]
}

output "migrate" {
  description = "What run-task needs to migrate."
  value = {
    task_definition = module.migrate.task_definition_arn
    subnets         = module.network.private_subnet_ids
    security_group  = aws_security_group.migrate.id
  }
}

output "repositories" {
  value = { for k, r in aws_ecr_repository.this : k => r.repository_url }
}

output "prometheus_endpoint" {
  value = aws_prometheus_workspace.this.prometheus_endpoint
}

output "alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "database_address" {
  value = module.database.address
}
