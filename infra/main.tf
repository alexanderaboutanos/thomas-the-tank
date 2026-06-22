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
  }
}

provider "aws" {
  region = var.region
}

locals {
  name_prefix = var.name_prefix
}

# Secrets the Lambda needs, both created out-of-band (console/CLI or a separate
# workspace) — we only look up their ARNs so the role can be granted
# least-privilege GetSecretValue on exactly these two.
#
#   snowflake  {account, user, private_key (full PEM), warehouse, database,
#               schema, role}                          -> /latest, /query reads
#   confluent  {bootstrap_servers, kafka_api_key, kafka_api_secret,
#               schema_registry_url, schema_registry_api_key,
#               schema_registry_api_secret, topic}     -> /produce
data "aws_secretsmanager_secret" "snowflake" {
  name = var.snowflake_secret_name
}

data "aws_secretsmanager_secret" "confluent" {
  name = var.confluent_secret_name
}
