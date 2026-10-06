output "deploy_role_arn" {
  description = "Set as the repository variable AWS_DEPLOY_ROLE_ARN."
  value       = aws_iam_role.deploy.arn
}

output "state_bucket" {
  description = "Set as the repository variable TF_STATE_BUCKET."
  value       = aws_s3_bucket.state.id
}
