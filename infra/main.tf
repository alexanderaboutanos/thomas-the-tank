# AWS runtime for the Sodor engine telemetry exercise — Terraform.
#
# Step 9 folded the step-7 Lambda + API Gateway into one always-on container:
#   S3 (static site) ─┐
#                     ├─ CloudFront ─> ALB ─> Fargate task ─┬─> Kafka (live + produce)
#   ALB (app routes) ─┘                                     └─> Snowflake (reads)
# See ecs.tf / alb.tf / site.tf. (api.tf + lambda.tf + ecr.tf were retired here.)
#
# All creds live in AWS Secrets Manager (created out-of-band) and are looked up
# below — never hardcoded. The ECS task injects them as env vars (see ecs.tf).
# Pin the provider versions and verify attributes against:
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
  # Pinned to the personal account (319120789796) after the corpa->personal
  # migration. Note: build_and_push.sh's `aws ecr` call ignores this block and
  # uses ambient creds, so apply with AWS_PROFILE=personal too (belt & suspenders).
  profile = "personal"
}

locals {
  name_prefix = var.name_prefix
}

# Secrets the task needs, all created out-of-band (console/CLI or a separate
# workspace) — we only look up their ARNs so the execution role can be granted
# least-privilege GetSecretValue on exactly these three, and the task definition
# can inject specific JSON keys as env vars (see ecs.tf).
#
#   snowflake       {account, user, private_key (full PEM), warehouse, database,
#                    schema, role}                          -> /latest, /query reads
#   confluent       {bootstrap_servers, kafka_api_key, kafka_api_secret,
#                    schema_registry_url, schema_registry_api_key,
#                    schema_registry_api_secret, topic}     -> /produce (WRITE)
#   confluent-live  same shape as confluent, but the read-only app-live service
#                   account's keys                          -> /stream live consumer
data "aws_secretsmanager_secret" "snowflake" {
  name = var.snowflake_secret_name
}

data "aws_secretsmanager_secret" "confluent" {
  name = var.confluent_secret_name
}

data "aws_secretsmanager_secret" "confluent_live" {
  name = var.live_confluent_secret_name
}
