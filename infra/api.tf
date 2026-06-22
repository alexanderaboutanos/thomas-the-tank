# HTTP API (API Gateway v2): three routes -> one Lambda, with CORS for the site.
#   POST /produce   one tick of telemetry to Kafka
#   GET  /latest    live view from Snowflake
#   GET  /query     canned analytics from Snowflake

resource "aws_apigatewayv2_api" "api" {
  name          = "${local.name_prefix}-api"
  protocol_type = "HTTP"

  # API Gateway owns the CORS response headers — the Lambda must NOT also set
  # them, or browsers reject the duplicate. Tighten allow_origins below.
  cors_configuration {
    allow_origins = var.cors_allow_origins
    allow_methods = ["GET", "POST"]
    allow_headers = ["*"]
  }
}

resource "aws_apigatewayv2_integration" "lambda" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

# All three routes share the one Lambda integration; the handler dispatches on
# method + path.
resource "aws_apigatewayv2_route" "produce" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "POST /produce"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

resource "aws_apigatewayv2_route" "latest" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "GET /latest"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

resource "aws_apigatewayv2_route" "query" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "GET /query"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true
}

# Let API Gateway invoke the function.
resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}
