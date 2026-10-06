variable "name" {
  description = "Identifier of the database instance and its companions."
  type        = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  description = "Private subnets in at least two availability zones."
  type        = list(string)
}

variable "client_security_group_ids" {
  description = "Security groups allowed to connect on 5432."
  type        = list(string)
}

variable "instance_class" {
  type = string
}

variable "kms_key_arn" {
  description = "Encrypts the storage and the master password secret."
  type        = string
}

variable "ephemeral" {
  description = "No deletion protection or final snapshot, so the environment can be destroyed at once."
  type        = bool
}
