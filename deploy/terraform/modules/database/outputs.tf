output "address" {
  value = aws_db_instance.this.address
}

output "port" {
  value = aws_db_instance.this.port
}

output "database" {
  value = aws_db_instance.this.db_name
}

output "owner" {
  description = "The master user, which owns the schema and runs migrations."
  value       = aws_db_instance.this.username
}

output "master_secret_arn" {
  description = "RDS's secret, a JSON object with username and password."
  value       = aws_db_instance.this.master_user_secret[0].secret_arn
}
