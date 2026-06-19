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

  # Stream Governance provisions the environment's Schema Registry cluster.
  # ESSENTIALS is the free tier — enough for the Avro schema + evolution work.
  stream_governance {
    package = "ESSENTIALS"
  }
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

# ─── Schema Registry: the Avro value schema, owned by Terraform ──────────────

# The SR cluster is provisioned by the environment's stream_governance block.
# depends_on defers this read until apply (after governance is enabled), which
# avoids a first-run failure when the SR cluster doesn't exist at plan time.
data "confluent_schema_registry_cluster" "sr" {
  environment {
    id = confluent_environment.sodor.id
  }
  depends_on = [confluent_environment.sodor]
}

# SR API key for app-manager — Terraform uses it to register/evolve schemas.
resource "confluent_api_key" "app-manager-sr-api-key" {
  display_name = "sodor-app-manager-sr-api-key"
  description  = "Schema Registry API key used by Terraform to manage schemas"
  owner {
    id          = confluent_service_account.app-manager.id
    api_version = confluent_service_account.app-manager.api_version
    kind        = confluent_service_account.app-manager.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = confluent_environment.sodor.id
    }
  }
}

# Lets app-manager register/manage schemas under any subject in this SR cluster.
resource "confluent_role_binding" "app-manager-sr-resource-owner" {
  principal   = "User:${confluent_service_account.app-manager.id}"
  role_name   = "ResourceOwner"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=*"
}

# The registered Avro value schema. The .avsc file is the single source of
# truth, shared with the producer. Evolving the schema = edit the file + apply;
# the compatibility check runs at apply time.
resource "confluent_schema" "telemetry-value" {
  schema_registry_cluster {
    id = data.confluent_schema_registry_cluster.sr.id
  }
  rest_endpoint = data.confluent_schema_registry_cluster.sr.rest_endpoint
  subject_name  = "${confluent_kafka_topic.telemetry.topic_name}-value"
  format        = "AVRO"
  schema        = file("${path.module}/../producer/telemetry.avsc")

  credentials {
    key    = confluent_api_key.app-manager-sr-api-key.id
    secret = confluent_api_key.app-manager-sr-api-key.secret
  }

  depends_on = [confluent_role_binding.app-manager-sr-resource-owner]
}

# ─── app-producer: read-only access to its subject + its own SR key ──────────

# SR API key the producer uses to look up the registered schema.
resource "confluent_api_key" "app-producer-sr-api-key" {
  display_name = "sodor-app-producer-sr-api-key"
  description  = "Schema Registry API key for producer.py — reads the value schema"
  owner {
    id          = confluent_service_account.app-producer.id
    api_version = confluent_service_account.app-producer.api_version
    kind        = confluent_service_account.app-producer.kind
  }
  managed_resource {
    id          = data.confluent_schema_registry_cluster.sr.id
    api_version = data.confluent_schema_registry_cluster.sr.api_version
    kind        = data.confluent_schema_registry_cluster.sr.kind
    environment {
      id = confluent_environment.sodor.id
    }
  }
}

# The producer never registers schemas (auto.register is off); it only reads the
# latest version of its own subject, so DeveloperRead on that subject suffices.
resource "confluent_role_binding" "app-producer-sr-read" {
  principal   = "User:${confluent_service_account.app-producer.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=${confluent_kafka_topic.telemetry.topic_name}-value"
}

# TODO: confluent_connector.snowflake_sink — needs the Snowflake account URL,
#   key-pair user, private key, and target DB/schema/table from ../snowflake.
