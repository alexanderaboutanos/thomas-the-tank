# Confluent Cloud infrastructure (SKELETON).
#
# Workflow: build these in the Confluent Cloud console once to learn them,
# THEN codify here. Pin the provider version and verify every attribute against:
# https://registry.terraform.io/providers/confluentinc/confluent/latest/docs

terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0" # TODO: pin to the current release
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

resource "confluent_environment" "sodor" {
  display_name = "sodor-railway"
}

resource "confluent_kafka_cluster" "basic" {
  display_name = "sodor-telemetry"
  availability = "SINGLE_ZONE"
  cloud        = "AWS"
  region       = var.region
  basic {}

  environment {
    id = confluent_environment.sodor.id
  }
}

resource "confluent_kafka_topic" "telemetry" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  topic_name       = "sodor-telemetry"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.basic.rest_endpoint
  # credentials { key = ... secret = ... }  # TODO: cluster API key for topic mgmt
}

# TODO: confluent_service_account "producer"  -> identity for producer.py
# TODO: confluent_api_key for that service account (feeds producer/.env)
# TODO: confluent_schema (Avro) once you move off plain JSON
# TODO: confluent_connector "snowflake_sink"  -> the managed Snowflake Sink Connector.
#       Its config needs the Snowflake account URL, the key-pair user, the private key,
#       and the target database/schema/table created in ../snowflake.
