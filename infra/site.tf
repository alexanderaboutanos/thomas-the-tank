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

# Upload the static UI (CDK's BucketDeployment). Point API_BASE in
# web/index.html at the api_endpoint output before/after apply.
resource "aws_s3_object" "index" {
  bucket       = aws_s3_bucket.site.id
  key          = "index.html"
  source       = "${path.module}/../web/index.html"
  etag         = filemd5("${path.module}/../web/index.html")
  content_type = "text/html"
  # Let CloudFront revalidate the page each load so a re-uploaded index.html (e.g.
  # after wiring API_BASE) shows up without a manual invalidation.
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

resource "aws_cloudfront_distribution" "site" {
  enabled             = true
  default_root_object = "index.html"
  price_class         = "PriceClass_100"

  origin {
    origin_id                = "s3-site"
    domain_name              = aws_s3_bucket.site.bucket_regional_domain_name
    origin_access_control_id = aws_cloudfront_origin_access_control.site.id
  }

  default_cache_behavior {
    target_origin_id       = "s3-site"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = data.aws_cloudfront_cache_policy.optimized.id
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
