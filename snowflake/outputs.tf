# Outputs that feed /confluent/terraform.tfvars (the connector config).

output "snowflake_url" {
  description = "snowflake.url.name for the connector. Verify against Snowsight's 'Copy account URL'."
  value       = "https://${var.snowflake_organization_name}-${var.snowflake_account_name}.snowflakecomputing.com"
}

output "connector_user" {
  description = "snowflake.user.name for the connector."
  value       = snowflake_user.connector.name
}

output "connector_role" {
  description = "snowflake.role.name for the connector."
  value       = snowflake_account_role.connector.name
}

output "database" {
  description = "snowflake.database.name for the connector."
  value       = snowflake_database.sodor.name
}

output "schema" {
  description = "snowflake.schema.name for the connector."
  value       = snowflake_schema.telemetry.name
}

output "table" {
  description = "Target table — used in the connector's snowflake.topic2table.map."
  value       = snowflake_table.telemetry_raw.name
}

output "warehouse" {
  description = "Warehouse name (for the serving Lambda in step 6)."
  value       = snowflake_warehouse.sodor.name
}
