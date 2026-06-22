# ECR repository for the always-on live-tap consumer image (build-order step 9),
# plus the build+push step that lands an image there.
#
# Mirrors ecr.tf (the serving-Lambda image), but the build context is ../consumer
# and the artifact runs as a long-lived Fargate service, not a Lambda. The ECS
# task definition — added with the rest of the step-9 runtime in a later substep —
# references this image by its content-hash URI, so, like the Lambda image, it
# must already exist in ECR before the service is created: build_push runs first
# and the service will depend_on it.

resource "aws_ecr_repository" "consumer" {
  name                 = "${local.name_prefix}-live"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true # demo: let `terraform destroy` clean up pushed images

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Keep only the few most recent images so the repo doesn't accrete forever.
resource "aws_ecr_lifecycle_policy" "consumer" {
  repository = aws_ecr_repository.consumer.name
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
  # Content-address the image: hash the build context so the tag — and thus the
  # image_uri — changes on any source edit, the same scheme as the Lambda image.
  # Skip .venv/__pycache__/.env — they're excluded from the image (see
  # consumer/.dockerignore) and would only churn or bloat the hash. Anything left
  # that the .dockerignore also drops (README, Dockerfile) merely over-triggers a
  # rebuild — never under-triggers, so the running image is never stale.
  consumer_ctx_files = [
    for f in fileset("${path.module}/../consumer", "**") :
    f if !startswith(f, ".venv/") && !startswith(f, "__pycache__/") && !startswith(f, ".env")
  ]
  consumer_dir_hash  = sha1(join("", [for f in local.consumer_ctx_files : filesha1("${path.module}/../consumer/${f}")]))
  consumer_image_tag = substr(local.consumer_dir_hash, 0, 12)
  consumer_image_uri = "${aws_ecr_repository.consumer.repository_url}:${local.consumer_image_tag}"
}

# Build + push the image. Re-runs only when consumer_image_tag changes. Reuses the
# generic build_and_push.sh (shared with the Lambda image) via env-var inputs.
resource "terraform_data" "consumer_build_push" {
  triggers_replace = [local.consumer_image_tag]

  provisioner "local-exec" {
    command = "${path.module}/build_and_push.sh"
    environment = {
      REPO_URL = aws_ecr_repository.consumer.repository_url
      TAG      = local.consumer_image_tag
      REGION   = var.region
      CONTEXT  = "${path.module}/../consumer"
    }
  }

  depends_on = [aws_ecr_repository.consumer]
}
