output "api_endpoint" {
  description = "Invoke URL; the web UI calls <api_endpoint>/latest."
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
