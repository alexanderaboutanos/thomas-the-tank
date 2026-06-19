# Confluent Cloud stack for the Sodor pipeline — see README.md.

terraform {
  required_version = ">= 1.5"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.76.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

# ─── Environment + cluster + topic ───────────────────────────────────────────

resource "confluent_environment" "sodor" {
  display_name = "sodor-railway"

  # Stream Governance (Schema Registry) — enabled when we add Avro. To turn on:
  # stream_governance {
  #   package = "ESSENTIALS"
  # }
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
  topic_name = "sodor-telemetry"

  # Deliberately 3 (1 would handle this throughput). Partitions can only INCREASE,
  # and increasing them reshuffles hash(key) % N and breaks per-engine ordering.
  partitions_count = 3

  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# ─── app-manager: admin identity Terraform uses to manage the topic + ACLs ───

resource "confluent_service_account" "app-manager" {
  display_name = "sodor-app-manager"
  description  = "Terraform-managed admin for the sodor-telemetry cluster"
}

resource "confluent_role_binding" "app-manager-kafka-cluster-admin" {
  principal   = "User:${confluent_service_account.app-manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.basic.rbac_crn
}

resource "confluent_api_key" "app-manager-kafka-api-key" {
  display_name = "sodor-app-manager-kafka-api-key"
  description  = "Kafka API key owned by the sodor-app-manager service account"
  owner {
    id          = confluent_service_account.app-manager.id
    api_version = confluent_service_account.app-manager.api_version
    kind        = confluent_service_account.app-manager.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.basic.id
    api_version = confluent_kafka_cluster.basic.api_version
    kind        = confluent_kafka_cluster.basic.kind
    environment {
      id = confluent_environment.sodor.id
    }
  }
  # Role binding must land before this key can manage anything.
  depends_on = [confluent_role_binding.app-manager-kafka-cluster-admin]
}

# ─── app-producer: the producer.py identity — WRITE on the topic only ────────

resource "confluent_service_account" "app-producer" {
  display_name = "sodor-app-producer"
  description  = "Identity for producer.py — writes engine telemetry"
}

resource "confluent_api_key" "app-producer-kafka-api-key" {
  display_name = "sodor-app-producer-kafka-api-key"
  description  = "Kafka API key owned by the sodor-app-producer service account"
  owner {
    id          = confluent_service_account.app-producer.id
    api_version = confluent_service_account.app-producer.api_version
    kind        = confluent_service_account.app-producer.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.basic.id
    api_version = confluent_kafka_cluster.basic.api_version
    kind        = confluent_kafka_cluster.basic.kind
    environment {
      id = confluent_environment.sodor.id
    }
  }
}

resource "confluent_kafka_acl" "app-producer-write-on-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.telemetry.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-producer.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# TODO: confluent_schema for the Avro value schema (Avro step).
# TODO: confluent_connector.snowflake_sink — needs the Snowflake account URL,
#   key-pair user, private key, and target DB/schema/table from ../snowflake.
