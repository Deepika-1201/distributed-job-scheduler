# Secrets the tasks receive as environment variables (LLD §22.4). RDS keeps the master
# password in its own secret (module.database.master_secret_arn).

resource "random_password" "runtime_db" {
  length  = 40
  special = false
}

resource "random_password" "worker_token" {
  length  = 48
  special = false
}

locals {
  secret_values = {
    runtime-db-password = random_password.runtime_db.result
    worker-token        = random_password.worker_token.result
    tls-cert            = tls_locally_signed_cert.server.cert_pem
    tls-key             = tls_private_key.server.private_key_pem
  }
}

resource "aws_secretsmanager_secret" "this" {
  for_each                = local.secret_values
  name                    = "jobscheduler/${var.name}/${each.key}"
  kms_key_id              = aws_kms_key.env.arn
  recovery_window_in_days = var.ephemeral ? 0 : 7
}

resource "aws_secretsmanager_secret_version" "this" {
  for_each      = local.secret_values
  secret_id     = aws_secretsmanager_secret.this[each.key].id
  secret_string = each.value
}
