variable "confluent_cloud_api_key" {
  description = "Cloud (account-level) API key for Terraform — not the producer's cluster key."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Cloud API secret paired with confluent_cloud_api_key."
  type        = string
  sensitive   = true
}

variable "region" {
  description = "AWS region for the Basic cluster. MUST equal the Snowflake account's region (SELECT CURRENT_REGION()) — the managed Snowflake Sink connector requires Kafka and Snowflake in the same region."
  type        = string
  default     = "us-east-2"
}

# ─── Snowflake Sink connector — values from `terraform output` in ../snowflake ─

variable "snowflake_url" {
  description = "snowflake.url.name, e.g. https://<org>-<account>.snowflakecomputing.com (../snowflake output snowflake_url)."
  type        = string
}

variable "snowflake_private_key" {
  description = "Connector's RSA PRIVATE key, body only (no PEM header/footer, one line). Pairs with the public key on the Snowflake user."
  type        = string
  sensitive   = true
}

variable "snowflake_connector_user" {
  description = "Snowflake user the connector logs in as (../snowflake output connector_user)."
  type        = string
  default     = "SODOR_CONNECTOR"
}

variable "snowflake_connector_role" {
  description = "Snowflake role the connector uses (../snowflake output connector_role)."
  type        = string
  default     = "SODOR_CONNECTOR_ROLE"
}

variable "snowflake_database" {
  description = "Target Snowflake database (../snowflake output database)."
  type        = string
  default     = "SODOR_RAILWAY"
}

variable "snowflake_schema" {
  description = "Target Snowflake schema (../snowflake output schema)."
  type        = string
  default     = "TELEMETRY"
}

variable "snowflake_table" {
  description = "Target table for snowflake.topic2table.map (../snowflake output table)."
  type        = string
  default     = "TELEMETRY_RAW"
}
