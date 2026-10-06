variable "name" {
  description = "Environment name, e.g. dev or perf; prefixes every resource."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.name))
    error_message = "name must be 2-21 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "image_tag" {
  description = "Tag of the images the services run."
  type        = string
}

variable "migrate_image_tag" {
  description = "Tag of the image the migrate task runs; the deploy workflow migrates with the new tag before the services move to it."
  type        = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "nat_per_az" {
  type    = bool
  default = false
}

variable "ephemeral" {
  description = "Allow destroying everything, the database included, without a final snapshot."
  type        = bool
  default     = true
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.medium"
}

variable "public_endpoint" {
  description = "Put the load balancer in public subnets, reachable from allowed_cidrs. Default is internal."
  type        = bool
  default     = false
}

variable "allowed_cidrs" {
  description = "Extra client ranges allowed on ports 443 and 7070, besides the VPC."
  type        = list(string)
  default     = []
}

variable "api_count" {
  type    = object({ min = number, max = number })
  default = { min = 2, max = 6 }
}

variable "engine_count" {
  type    = number
  default = 2
}

variable "worker_pools" {
  description = "Worker services by pool name, which they receive as JS_WORKER_POOL. image_repository defaults to the demo worker's."
  type = map(object({
    image_repository   = optional(string)
    command            = list(string)
    cpu                = optional(number, 256)
    memory             = optional(number, 512)
    min                = optional(number, 1)
    max                = optional(number, 4)
    backlog_age_target = optional(number, 60)
    stop_timeout       = optional(number, 120)
  }))
  default = {
    default = { command = ["-slots", "8"] }
  }
}

variable "collector_image" {
  type    = string
  default = "public.ecr.aws/aws-observability/aws-otel-collector:v0.50.0"
}

variable "alert_email" {
  description = "Subscribes this address to the alert topic; empty for none."
  type        = string
  default     = ""
}
