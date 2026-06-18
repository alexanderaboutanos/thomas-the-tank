# Snowflake infrastructure (SKELETON).
#
# Verify resource names against your provider version: some were renamed across
# major versions (e.g. snowflake_role -> snowflake_account_role). Pin the version.
# https://registry.terraform.io/providers/snowflakedb/snowflake/latest/docs

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0" # TODO: pin to the current release
    }
  }
}

provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = var.admin_user
  # Authenticate via key-pair or env; do NOT hardcode a password here.
}

resource "snowflake_database" "sodor" {
  name = "SODOR_RAILWAY"
}

resource "snowflake_schema" "telemetry" {
  database = snowflake_database.sodor.name
  name     = "TELEMETRY"
}

resource "snowflake_warehouse" "sodor" {
  name           = "SODOR_RAILWAY_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
}

# TODO: snowflake_table "telemetry_raw" (or let the sink connector create it)
# TODO: a key-pair user + role for the Snowflake Sink Connector (write access)
# TODO: a read-only user/role for the Lambda API
