variable "region" {
  type    = string
  default = "us-east-1"
}

variable "state_bucket" {
  description = "Globally unique name of the S3 bucket for environment state."
  type        = string
}

variable "github_repository" {
  description = "owner/name of the repository whose deploy workflow may assume the role."
  type        = string
}
