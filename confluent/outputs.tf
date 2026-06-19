# Outputs that feed producer/.env.

output "kafka_bootstrap_servers" {
  description = "BOOTSTRAP_SERVERS for producer/.env (SASL_SSL:// scheme stripped)."
  value       = replace(confluent_kafka_cluster.basic.bootstrap_endpoint, "SASL_SSL://", "")
}

output "producer_kafka_api_key" {
  description = "KAFKA_API_KEY for producer/.env."
  value       = confluent_api_key.app-producer-kafka-api-key.id
}

output "producer_kafka_api_secret" {
  description = "KAFKA_API_SECRET for producer/.env."
  value       = confluent_api_key.app-producer-kafka-api-key.secret
  sensitive   = true
}

output "schema_registry_url" {
  description = "SCHEMA_REGISTRY_URL for producer/.env."
  value       = data.confluent_schema_registry_cluster.sr.rest_endpoint
}

output "producer_schema_registry_api_key" {
  description = "SCHEMA_REGISTRY_API_KEY for producer/.env."
  value       = confluent_api_key.app-producer-sr-api-key.id
}

output "producer_schema_registry_api_secret" {
  description = "SCHEMA_REGISTRY_API_SECRET for producer/.env."
  value       = confluent_api_key.app-producer-sr-api-key.secret
  sensitive   = true
}
