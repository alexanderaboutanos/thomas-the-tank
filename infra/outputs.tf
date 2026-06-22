output "api_endpoint" {
  description = "Invoke URL; the web UI calls <api_endpoint>/{produce,latest,query}."
  value       = aws_apigatewayv2_stage.default.invoke_url
}

output "web_url" {
  description = "CloudFront URL for the static site."
  value       = "https://${aws_cloudfront_distribution.site.domain_name}"
}

output "site_bucket" {
  description = "Name of the static-site S3 bucket."
  value       = aws_s3_bucket.site.bucket
}

output "ecr_repository_url" {
  description = "ECR repo holding the serving-Lambda image."
  value       = aws_ecr_repository.lambda.repository_url
}
