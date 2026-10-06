variable "name" {
  description = "Prefix for resource names."
  type        = string
}

variable "cidr" {
  description = "The VPC's address range: public subnets take /24s, private subnets /20s."
  type        = string
}

variable "nat_per_az" {
  description = "One NAT gateway per availability zone instead of one in total."
  type        = bool
  default     = false
}
