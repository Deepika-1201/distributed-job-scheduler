# Telemetry on AWS (LLD §22.6): a managed Prometheus workspace that evaluates the platform's
# alert rules unchanged, alerts to SNS, and a CloudWatch log group for the backlog metrics that
# drive worker autoscaling.

resource "aws_prometheus_workspace" "this" {
  alias = "jobscheduler-${var.name}"
}

resource "aws_prometheus_rule_group_namespace" "alerts" {
  name         = "jobscheduler"
  workspace_id = aws_prometheus_workspace.this.id
  data         = file("${path.module}/../../prometheus/alerts.yml")
}

resource "aws_sns_topic" "alerts" {
  name              = "jobscheduler-${var.name}-alerts"
  kms_master_key_id = "alias/aws/sns"
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "aps.amazonaws.com" }
      Action    = ["sns:Publish", "sns:GetTopicAttributes"]
      Resource  = aws_sns_topic.alerts.arn
      Condition = {
        ArnEquals    = { "aws:SourceArn" = aws_prometheus_workspace.this.arn }
        StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
      }
    }]
  })
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_prometheus_alert_manager_definition" "this" {
  workspace_id = aws_prometheus_workspace.this.id
  definition = yamlencode({
    alertmanager_config = yamlencode({
      route = { receiver = "sns", group_by = ["alertname", "pool"] }
      receivers = [{
        name        = "sns"
        sns_configs = [{ topic_arn = aws_sns_topic.alerts.arn, sigv4 = { region = var.region } }]
      }]
    })
  })
}

resource "aws_cloudwatch_log_group" "metrics" {
  name              = "/jobscheduler/${var.name}/metrics"
  retention_in_days = 7
}

locals {
  collector_config = { for role in ["api", "engine"] : role => templatefile("${path.module}/collector.yaml.tftpl", {
    environment   = var.name
    role          = role
    region        = var.region
    remote_write  = "${aws_prometheus_workspace.this.prometheus_endpoint}api/v1/remote_write"
    emf_log_group = aws_cloudwatch_log_group.metrics.name
  }) }
}
