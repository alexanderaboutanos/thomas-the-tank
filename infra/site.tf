# Private S3 bucket + CloudFront distribution for the /web static site.

resource "aws_s3_bucket" "site" {
  bucket_prefix = "${local.name_prefix}-site-"
}

resource "aws_s3_bucket_public_access_block" "site" {
  bucket                  = aws_s3_bucket.site.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload the static UI. The page is now same-origin — it calls /stream, /produce,
# /latest, /query on this same CloudFront domain (no API_BASE to wire). Substep 4
# swaps its /latest polling for an EventSource on /stream.
resource "aws_s3_object" "index" {
  bucket       = aws_s3_bucket.site.id
  key          = "index.html"
  source       = "${path.module}/../web/index.html"
  etag         = filemd5("${path.module}/../web/index.html")
  content_type = "text/html"
  # Let CloudFront revalidate the page each load so a re-uploaded index.html shows
  # up without a manual invalidation.
  cache_control = "no-cache"
}

# Hero image for the web UI. index.html references it as "thomas.png".
resource "aws_s3_object" "thomas" {
  bucket       = aws_s3_bucket.site.id
  key          = "thomas.png"
  source       = "${path.module}/../web/thomas.png"
  etag         = filemd5("${path.module}/../web/thomas.png")
  content_type = "image/png"
}

# Origin Access Control: CloudFront reads the bucket; the bucket stays private.
resource "aws_cloudfront_origin_access_control" "site" {
  name                              = "${local.name_prefix}-site"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

data "aws_cloudfront_cache_policy" "optimized" {
  name = "Managed-CachingOptimized"
}

# Dynamic origin (the live-tap ALB): never cache, and forward the whole viewer
# request to the origin. AllViewerExceptHostHeader drops only the Host header so
# the ALB sees its own hostname (CachingDisabled forwards nothing-cacheable).
data "aws_cloudfront_cache_policy" "caching_disabled" {
  name = "Managed-CachingDisabled"
}

data "aws_cloudfront_origin_request_policy" "all_viewer_except_host" {
  name = "Managed-AllViewerExceptHostHeader"
}

locals {
  # The app routes CloudFront forwards to the ALB origin. All identical except
  # /produce, which is a POST (so it needs the full method set).
  alb_behaviors = [
    { path = "/stream*", methods = ["GET", "HEAD", "OPTIONS"] },
    { path = "/latest", methods = ["GET", "HEAD", "OPTIONS"] },
    { path = "/query", methods = ["GET", "HEAD", "OPTIONS"] },
    { path = "/healthz", methods = ["GET", "HEAD", "OPTIONS"] },
    { path = "/produce", methods = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"] },
  ]
}

resource "aws_cloudfront_distribution" "site" {
  enabled             = true
  default_root_object = "index.html"
  price_class         = "PriceClass_100"

  origin {
    origin_id                = "s3-site"
    domain_name              = aws_s3_bucket.site.bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.site.id
  }

  # The live-tap ALB as a second origin. CloudFront→ALB is plain HTTP (the ALB has
  # no domain/cert); the viewer→CloudFront hop stays HTTPS on CloudFront's cert —
  # no mixed content, no separate cert, no CORS (one origin to the browser).
  origin {
    origin_id   = "alb-live"
    domain_name = aws_lb.live.dns_name

    custom_origin_config {
      http_port                = 80
      https_port               = 443
      origin_protocol_policy   = "http-only"
      origin_ssl_protocols     = ["TLSv1.2"]
      origin_read_timeout      = 60 # SSE: must exceed the app's 20s heartbeat
      origin_keepalive_timeout = 60
    }
  }

  # Default: the static site from S3.
  default_cache_behavior {
    target_origin_id       = "s3-site"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = data.aws_cloudfront_cache_policy.optimized.id
  }

  # App routes → the ALB origin: SSE live view (/stream) + the folded serving
  # routes (/produce, /latest, /query) + the health check. Caching disabled,
  # whole request forwarded, no compression (SSE must stream, not buffer).
  dynamic "ordered_cache_behavior" {
    for_each = local.alb_behaviors
    content {
      path_pattern             = ordered_cache_behavior.value.path
      target_origin_id         = "alb-live"
      viewer_protocol_policy   = "redirect-to-https"
      allowed_methods          = ordered_cache_behavior.value.methods
      cached_methods           = ["GET", "HEAD"]
      compress                 = false
      cache_policy_id          = data.aws_cloudfront_cache_policy.caching_disabled.id
      origin_request_policy_id = data.aws_cloudfront_origin_request_policy.all_viewer_except_host.id
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

# Allow only this distribution to read the private bucket (OAC + SourceArn).
data "aws_iam_policy_document" "site_bucket" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.site.arn}/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.site.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "site" {
  bucket = aws_s3_bucket.site.id
  policy = data.aws_iam_policy_document.site_bucket.json
}
