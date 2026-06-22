output "web_url" {
  description = "CloudFront URL — the whole app: static site (default) + /stream, /produce, /latest, /query via the ALB origin."
  value       = "https://${aws_cloudfront_distribution.site.domain_name}"
}

output "alb_dns_name" {
  description = "Live-tap ALB DNS name. CloudFront's app-routes origin — locked to CloudFront, not for direct browser use."
  value       = aws_lb.live.dns_name
}

output "site_bucket" {
  description = "Name of the static-site S3 bucket."
  value       = aws_s3_bucket.site.bucket
}

output "consumer_ecr_repository_url" {
  description = "ECR repo holding the step-9 live-tap consumer image."
  value       = aws_ecr_repository.consumer.repository_url
}

output "consumer_image_uri" {
  description = "Content-hash-tagged URI of the live-tap consumer image; the ECS task definition references this."
  value       = local.consumer_image_uri
}
