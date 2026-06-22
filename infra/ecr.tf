# ECR repository for the serving-Lambda image, plus the build+push step that
# lands an image there before the function is created.
#
# Lambda container images must already exist in ECR at apply time, so the build
# runs first (terraform_data below) and the function depends_on it. The image is
# tagged with a content hash of lambda/, so any source edit produces a new tag,
# a new image_uri, and a clean function update — no mutable :latest churn.

resource "aws_ecr_repository" "lambda" {
  name                 = "${local.name_prefix}-serving"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true # demo: let `terraform destroy` clean up pushed images

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Keep only the few most recent images so the repo doesn't accrete forever.
resource "aws_ecr_lifecycle_policy" "lambda" {
  repository = aws_ecr_repository.lambda.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

locals {
  # Hash every file under lambda/ so the tag (and thus the image_uri) changes
  # whenever the handler, Dockerfile, requirements, or schema change.
  lambda_files      = fileset("${path.module}/lambda", "**")
  lambda_dir_hash   = sha1(join("", [for f in local.lambda_files : filesha1("${path.module}/lambda/${f}")]))
  image_tag         = substr(local.lambda_dir_hash, 0, 12)
  serving_image_uri = "${aws_ecr_repository.lambda.repository_url}:${local.image_tag}"
}

# Build + push the image. Re-runs only when image_tag changes (triggers_replace).
resource "terraform_data" "build_push" {
  triggers_replace = [local.image_tag]

  provisioner "local-exec" {
    command = "${path.module}/build_and_push.sh"
    environment = {
      REPO_URL = aws_ecr_repository.lambda.repository_url
      TAG      = local.image_tag
      REGION   = var.region
      CONTEXT  = "${path.module}/lambda"
    }
  }

  depends_on = [aws_ecr_repository.lambda]
}
