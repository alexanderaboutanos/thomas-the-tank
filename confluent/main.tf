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

# ─── app-connector: the managed Snowflake Sink connector's Kafka identity ─────

# The connector is a Kafka consumer, so it gets its own service account with the
# minimal ACLs a fully-managed sink needs: DESCRIBE the cluster, READ the topic,
# READ its consumer group, and CREATE/WRITE its dead-letter-queue topic. Confluent
# names managed-connector groups "connect-lcc-<id>" and DLQ topics "dlq-lcc-<id>",
# so those two are PREFIXED ACLs (the id isn't known until the connector exists).
resource "confluent_service_account" "app-connector" {
  display_name = "sodor-app-connector"
  description  = "Identity for the managed Snowflake Sink connector — reads telemetry"
}

resource "confluent_kafka_acl" "app-connector-describe-cluster" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-connector-read-on-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.telemetry.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-connector-read-on-group" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "GROUP"
  resource_name = "connect-lcc-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-connector-dlq-create" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "TOPIC"
  resource_name = "dlq-lcc-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "CREATE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-connector-dlq-write" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "TOPIC"
  resource_name = "dlq-lcc-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.app-connector.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# Input format is AVRO, so the connector reads the registered value schema from
# Schema Registry — DeveloperRead on its subject, same as the producer.
resource "confluent_role_binding" "app-connector-sr-read" {
  principal   = "User:${confluent_service_account.app-connector.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=${confluent_kafka_topic.telemetry.topic_name}-value"
}

# ─── The managed Snowflake Sink connector ────────────────────────────────────

# Reads sodor-telemetry (Avro) and lands it in Snowflake via Snowpipe Streaming
# (step 6) — rows are written straight into the table via the streaming API, no
# file/stage/pipe COPY, so the warehouse view is near-real-time (measured ~3–30s;
# the managed connector caps the flush at ~30s — see the buffer.flush.time note).
# Snowflake-side values come from ../snowflake outputs; the private key is the
# connector half of the key pair (body only, one line). See ../SNOWFLAKE.md.
resource "confluent_connector" "snowflake_sink" {
  environment {
    id = confluent_environment.sodor.id
  }
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }

  config_sensitive = {
    "snowflake.private.key" = var.snowflake_private_key
  }

  config_nonsensitive = {
    "connector.class"          = "SnowflakeSink"
    "name"                     = "sodor-snowflake-sink"
    "kafka.auth.mode"          = "SERVICE_ACCOUNT"
    "kafka.service.account.id" = confluent_service_account.app-connector.id
    "topics"                   = confluent_kafka_topic.telemetry.topic_name
    "input.data.format"        = "AVRO"
    # Step 6: streaming ingest (was "SNOWPIPE"). Schematization is left OFF (default
    # FALSE), so data still lands as the two VARIANT columns RECORD_CONTENT +
    # RECORD_METADATA — TELEMETRY_LATEST in ../snowflake is unchanged.
    "snowflake.ingestion.method" = "SNOWPIPE_STREAMING"
    # Freshness lever — and a documented ceiling. buffer.flush.time is the connector's
    # internal-buffer flush interval (min 1s for streaming, 10s for classic); we ask
    # for 5s. BUT measured cadence stays ~30s: the managed connector runs Snowpipe
    # Streaming in single-buffer mode (the connector buffer is bypassed, so this knob
    # has no observed effect) and pins the streaming client-lag at its 30s default.
    # The two knobs that WOULD lower it — "snowflake.streaming.max.client.lag" and
    # "snowflake.streaming.enable.single.buffer" — are silently dropped by the managed
    # connector (they never land in state; setting them only causes a perpetual diff).
    # Net: freshness oscillates ~3s (just after a flush) to ~30s (just before the next).
    # True single-digit needs a self-managed connector or the step-9 direct-Kafka tap.
    "buffer.flush.time"         = "5"
    "snowflake.url.name"        = var.snowflake_url
    "snowflake.user.name"       = var.snowflake_connector_user
    "snowflake.role.name"       = var.snowflake_connector_role
    "snowflake.database.name"   = var.snowflake_database
    "snowflake.schema.name"     = var.snowflake_schema
    "snowflake.topic2table.map" = "${confluent_kafka_topic.telemetry.topic_name}:${var.snowflake_table}"
    "tasks.max"                 = "1"
  }

  # ACLs + the SR binding must land before the connector starts consuming.
  depends_on = [
    confluent_kafka_acl.app-connector-describe-cluster,
    confluent_kafka_acl.app-connector-read-on-topic,
    confluent_kafka_acl.app-connector-read-on-group,
    confluent_kafka_acl.app-connector-dlq-create,
    confluent_kafka_acl.app-connector-dlq-write,
    confluent_role_binding.app-connector-sr-read,
  ]
}

# ─── app-live: the step-9 live-tap consumer's Kafka identity ──────────────────

# The always-on Fargate consumer (../consumer) that serves the live view straight
# from Kafka — the production-faithful split that closes step 6's freshness gap.
# It joins sodor-telemetry under its OWN consumer group, independent of the
# Snowflake sink's group: the same topic fanned out to two consumer groups, each
# reading every message at its own offset. That independence is the whole point —
# the live tap and the warehouse sink don't compete for the topic.
#
# Read-only and minimal, mirroring app-connector: DESCRIBE the cluster, READ the
# topic, READ its consumer group, and DeveloperRead on the value subject (to
# deserialize Avro). No WRITE anywhere, no DLQ. The group is a PREFIXED ACL on
# "sodor-live-" so a stable single-task group.id ("sodor-live-consumer") works
# today AND a per-instance unique group ("sodor-live-<task>") works later without
# an ACL change — that's the documented HA fan-out path (one task per partition,
# each its own group, every instance sees every message).
resource "confluent_service_account" "app-live" {
  display_name = "sodor-app-live"
  description  = "Identity for the step-9 live-tap Fargate consumer — reads telemetry"
}

resource "confluent_api_key" "app-live-kafka-api-key" {
  display_name = "sodor-app-live-kafka-api-key"
  description  = "Kafka API key owned by the sodor-app-live service account"
  owner {
    id          = confluent_service_account.app-live.id
    api_version = confluent_service_account.app-live.api_version
    kind        = confluent_service_account.app-live.kind
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

resource "confluent_kafka_acl" "app-live-describe-cluster" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "CLUSTER"
  resource_name = "kafka-cluster"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-live.id}"
  host          = "*"
  operation     = "DESCRIBE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-live-read-on-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.telemetry.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app-live.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-live-read-on-group" {
  kafka_cluster {
    id = confluent_kafka_cluster.basic.id
  }
  resource_type = "GROUP"
  resource_name = "sodor-live-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.app-live.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.basic.rest_endpoint
  credentials {
    key    = confluent_api_key.app-manager-kafka-api-key.id
    secret = confluent_api_key.app-manager-kafka-api-key.secret
  }
}

# SR API key + DeveloperRead on the value subject — the consumer deserializes
# Avro, so it reads the registered schema, same as the producer and the connector.
resource "confluent_api_key" "app-live-sr-api-key" {
  display_name = "sodor-app-live-sr-api-key"
  description  = "Schema Registry API key for the live-tap consumer — reads the value schema"
  owner {
    id          = confluent_service_account.app-live.id
    api_version = confluent_service_account.app-live.api_version
    kind        = confluent_service_account.app-live.kind
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

resource "confluent_role_binding" "app-live-sr-read" {
  principal   = "User:${confluent_service_account.app-live.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_schema_registry_cluster.sr.resource_name}/subject=${confluent_kafka_topic.telemetry.topic_name}-value"
}
