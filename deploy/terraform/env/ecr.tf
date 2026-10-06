resource "aws_ecr_repository" "this" {
  for_each             = toset(["platform", "demo-worker"])
  name                 = "jobscheduler-${var.name}/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = var.ephemeral
  image_scanning_configuration {
    scan_on_push = true
  }
  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.env.arn
  }
}

resource "aws_ecr_lifecycle_policy" "this" {
  for_each   = aws_ecr_repository.this
  repository = each.value.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last 20 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 20 }
      action       = { type = "expire" }
    }]
  })
}
