# Serving Lambda (container image): POST /produce -> Kafka, GET /latest &
# GET /query -> Snowflake. Reads its two secrets from Secrets Manager.

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.name_prefix}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# CloudWatch Logs write access.
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Read only the two secrets this function needs — least privilege.
data "aws_iam_policy_document" "lambda_secret" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      data.aws_secretsmanager_secret.snowflake.arn,
      data.aws_secretsmanager_secret.confluent.arn,
    ]
  }
}

resource "aws_iam_role_policy" "lambda_secret" {
  name   = "read-serving-secrets"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_secret.json
}

# Container image (built + pushed by ecr.tf). No runtime/handler/filename here —
# those come from the image's Dockerfile CMD. The function lives outside any VPC,
# so it keeps default internet egress to Confluent Cloud (9092) and Snowflake.
resource "aws_lambda_function" "api" {
  function_name = "${local.name_prefix}-serving"
  role          = aws_iam_role.lambda.arn
  package_type  = "Image"
  image_uri     = local.serving_image_uri
  architectures = ["x86_64"]
  timeout       = 30
  memory_size   = 512 # headroom for librdkafka + the Snowflake connector

  environment {
    variables = {
      SNOWFLAKE_SECRET_NAME = var.snowflake_secret_name
      CONFLUENT_SECRET_NAME = var.confluent_secret_name
      NUM_ENGINES           = tostring(var.num_engines)
    }
  }

  depends_on = [terraform_data.build_push]
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${aws_lambda_function.api.function_name}"
  retention_in_days = 14
}
