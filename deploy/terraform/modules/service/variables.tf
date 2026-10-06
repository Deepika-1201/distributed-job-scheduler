variable "name" {
  description = "Service and task family name."
  type        = string
}

variable "service" {
  description = "Create the ECS service; false defines only the task, for one-off runs."
  type        = bool
  default     = true
}

variable "cluster_arn" {
  type    = string
  default = null
}

variable "image" {
  type = string
}

variable "command" {
  type    = list(string)
  default = []
}

variable "environment" {
  description = "Plain environment variables."
  type        = map(string)
  default     = {}
}

variable "secrets" {
  description = "Environment variables from Secrets Manager: name to secret ARN, or ARN:json-key::."
  type        = map(string)
  default     = {}
}

variable "ports" {
  type    = list(number)
  default = []
}

variable "cpu" {
  type    = number
  default = 512
}

variable "memory" {
  type    = number
  default = 1024
}

variable "desired_count" {
  type    = number
  default = 1
}

variable "deployment_minimum_healthy_percent" {
  type    = number
  default = 100
}

variable "deployment_maximum_percent" {
  type    = number
  default = 200
}

variable "stop_timeout" {
  description = "Seconds between SIGTERM and SIGKILL, at most 120 on Fargate: the drain time."
  type        = number
  default     = 30
}

variable "subnet_ids" {
  type    = list(string)
  default = []
}

variable "security_group_ids" {
  type    = list(string)
  default = []
}

variable "execution_role_arn" {
  type = string
}

variable "task_role_arn" {
  type    = string
  default = null
}

variable "target_groups" {
  description = "Load balancer target groups and the container port each forwards to."
  type        = list(object({ arn = string, port = number }))
  default     = []
}

variable "collector" {
  description = "An OpenTelemetry collector sidecar: its image and configuration."
  type        = object({ image = string, config = string })
  default     = null
}

variable "log_retention_days" {
  type    = number
  default = 30
}
