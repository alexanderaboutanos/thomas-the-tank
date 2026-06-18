# AWS serving layer for the Sodor engine telemetry exercise — Terraform.
#
# Replaces the earlier CDK sketch. Mirrors the same four pieces:
#   S3 (static site) + CloudFront  <-  API Gateway  <-  Lambda  ->  Snowflake
#
# Snowflake read creds live in AWS Secrets Manager (created out-of-band, e.g.
# from the key-pair user in ../snowflake) and are looked up below — never
# hardcoded here. Pin the provider versions and verify attributes against:
# https://registry.terraform.io/providers/hashicorp/aws/latest/docs

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" # TODO: pin to the current release
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  name_prefix = var.name_prefix
}

# Snowflake read creds the Lambda needs. Create the secret out-of-band
# (console/CLI or a separate workspace); this only reads its ARN so the
# Lambda role can be granted least-privilege access to it.
data "aws_secretsmanager_secret" "snowflake" {
  name = var.snowflake_secret_name
}
