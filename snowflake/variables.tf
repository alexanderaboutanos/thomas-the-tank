# ─── Admin connection: how Terraform authenticates to Snowflake ──────────────

variable "snowflake_organization_name" {
  description = "Snowflake organization name (Snowsight: Admin > Account, or the account URL's first segment)."
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name within the org (the account URL's second segment)."
  type        = string
}

variable "snowflake_admin_user" {
  description = "Admin user Terraform connects as (needs ACCOUNTADMIN, or SYSADMIN+SECURITYADMIN)."
  type        = string
}

variable "snowflake_admin_role" {
  description = "Role Terraform uses. ACCOUNTADMIN covers creating the db/warehouse/role/user in one go."
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_admin_private_key_path" {
  description = "Path to the admin user's RSA private key (full PEM, e.g. rsa_key_admin.p8). Gitignored."
  type        = string
}

# ─── Connector identity ──────────────────────────────────────────────────────

variable "connector_user_name" {
  description = "Service user the Snowflake Sink connector logs in as."
  type        = string
  default     = "SODOR_CONNECTOR"
}

variable "connector_role_name" {
  description = "Least-privilege role granted to the connector user."
  type        = string
  default     = "SODOR_CONNECTOR_ROLE"
}

variable "connector_rsa_public_key" {
  description = "Connector's RSA PUBLIC key, body only (no PEM header/footer, one line). Pairs with the private key in /confluent."
  type        = string
}

# ─── Warehouse + namespace names ─────────────────────────────────────────────
# Defaults match the names hard-coded in infra/lambda/handler.py — change in both
# places if you change them here.

variable "warehouse_name" {
  description = "Warehouse (compute) name."
  type        = string
  default     = "SODOR_WH"
}

variable "database_name" {
  description = "Database name."
  type        = string
  default     = "SODOR_RAILWAY"
}

variable "schema_name" {
  description = "Schema name."
  type        = string
  default     = "TELEMETRY"
}

variable "table_name" {
  description = "Classic-Snowpipe landing table name."
  type        = string
  default     = "TELEMETRY_RAW"
}
