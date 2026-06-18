# Lambda that queries Snowflake and returns the latest telemetry as JSON.

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

# Read only the one Snowflake secret — least privilege (CDK's sfSecret.grantRead).
data "aws_iam_policy_document" "lambda_secret" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [data.aws_secretsmanager_secret.snowflake.arn]
  }
}

resource "aws_iam_role_policy" "lambda_secret" {
  name   = "read-snowflake-secret"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_secret.json
}

# Zip up lambda/. boto3 is in the runtime; see the TODO below for the
# Snowflake connector, which is not.
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/lambda"
  output_path = "${path.module}/build/lambda.zip"
}

resource "aws_lambda_function" "api" {
  function_name    = "${local.name_prefix}-latest"
  role             = aws_iam_role.lambda.arn
  runtime          = var.lambda_runtime
  handler          = "handler.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      SNOWFLAKE_SECRET_NAME = var.snowflake_secret_name
    }
  }

  # TODO: snowflake-connector-python isn't in the base runtime. Ship it via a
  # Lambda layer or a container image, then implement the query in
  # lambda/handler.py.
  # layers = [aws_lambda_layer_version.snowflake.arn]
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${aws_lambda_function.api.function_name}"
  retention_in_days = 14
}
