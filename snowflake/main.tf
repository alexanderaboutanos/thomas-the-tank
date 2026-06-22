# Snowflake warehouse for the Sodor pipeline — see README.md.
#
# This layer stands up everything the managed Snowflake Sink connector needs to
# land telemetry: the warehouse (compute), the SODOR_RAILWAY.TELEMETRY namespace,
# the two-VARIANT landing table, a flattening view, and a least-privilege key-pair
# service user/role for the connector to authenticate as. (Step 6 switched ingest
# from classic Snowpipe to Snowpipe Streaming; with schematization off the table
# shape is identical, so only the connector's grants changed — see below.)
#
# It is a SEPARATE root module from /confluent (separate state). Values cross the
# boundary by hand, the same way producer/.env is filled from /confluent outputs:
# `terraform output` here -> /confluent/terraform.tfvars (see both READMEs).

terraform {
  required_version = ">= 1.5"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.8"
    }
  }
}

# Terraform authenticates as an admin user via key-pair (SNOWFLAKE_JWT). Snowflake
# is phasing out single-factor passwords for programmatic logins, so we register an
# RSA public key on the admin user once by hand, then sign in with the private key.
# See README.md "Stand it up" for the one-time bootstrap.
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_admin_user
  role              = var.snowflake_admin_role
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(var.snowflake_admin_private_key_path)

  # snowflake_table is still a "preview" resource in the v2 provider (it predates
  # the v1 resource redesign and hasn't been re-stabilized), so it must be opted
  # into explicitly. snowflake_view has already graduated and needs no flag.
  preview_features_enabled = ["snowflake_table_resource"]
}

# ─── Compute + namespace ─────────────────────────────────────────────────────

# The warehouse is COMPUTE, not storage — the engine that runs queries. Storage
# (the tables) is billed separately; decoupling the two is the core OLAP trick.
# XSMALL + a short auto-suspend keeps the trial cheap. (Snowpipe ingest — classic
# or streaming — uses Snowflake-managed serverless compute, not this warehouse, but
# the Lambda and any ad-hoc queries will use it.)
resource "snowflake_warehouse" "sodor" {
  name           = var.warehouse_name
  warehouse_size = "XSMALL"
  auto_suspend   = 60 # seconds idle before it parks itself
  comment        = "Compute for the Sodor telemetry pipeline (queries + the serving Lambda)."
}

resource "snowflake_database" "sodor" {
  name    = var.database_name
  comment = "North Western Railway engine-telemetry warehouse."
}

resource "snowflake_schema" "telemetry" {
  database = snowflake_database.sodor.name
  name     = var.schema_name
  comment  = "Engine telemetry landing schema."
}

# ─── Landing table + flattening view ─────────────────────────────────────────

# With schematization off, the connector lands every Kafka message as two VARIANT
# columns — the parsed value in RECORD_CONTENT, Kafka bookkeeping
# (topic/partition/offset/ts) in RECORD_METADATA — under BOTH classic Snowpipe and
# Snowpipe Streaming. The connector would auto-create this table, but we codify it
# so the shape is explicit and the view below has something to depend on. The typed
# fields live INSIDE RECORD_CONTENT — see telemetry_latest.
resource "snowflake_table" "telemetry_raw" {
  database = snowflake_database.sodor.name
  schema   = snowflake_schema.telemetry.name
  name     = var.table_name
  comment  = "Two-VARIANT landing table (Snowpipe Streaming, schematization off); typed fields live inside RECORD_CONTENT."

  column {
    name = "RECORD_METADATA"
    type = "VARIANT"
  }
  column {
    name = "RECORD_CONTENT"
    type = "VARIANT"
  }
}

# A view that pulls the typed fields out of the RECORD_CONTENT blob and keeps the
# newest reading per engine — this is what the serving Lambda (step 7) queries.
# The timestamp is ISO-8601, which sorts lexicographically = chronologically, so
# ordering on the string is correct.
resource "snowflake_view" "telemetry_latest" {
  database = snowflake_database.sodor.name
  schema   = snowflake_schema.telemetry.name
  name     = "TELEMETRY_LATEST"
  comment  = "Latest reading per engine, flattened from RECORD_CONTENT into typed columns."

  statement = <<-SQL
    SELECT
      RECORD_CONTENT:engine_id::string        AS engine_id,
      RECORD_CONTENT:event_id::string         AS event_id,
      RECORD_CONTENT:timestamp::string        AS event_timestamp,
      RECORD_CONTENT:latitude::float          AS latitude,
      RECORD_CONTENT:longitude::float         AS longitude,
      RECORD_CONTENT:speed_kmh::float         AS speed_kmh,
      RECORD_CONTENT:heading::float           AS heading,
      RECORD_CONTENT:door_status::string      AS door_status,
      RECORD_CONTENT:car_temperature_c::float AS car_temperature_c
    FROM ${snowflake_database.sodor.name}.${snowflake_schema.telemetry.name}.${snowflake_table.telemetry_raw.name}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY RECORD_CONTENT:engine_id::string
      ORDER BY RECORD_CONTENT:timestamp::string DESC
    ) = 1
  SQL

  depends_on = [snowflake_table.telemetry_raw]
}

# ─── Connector identity: least-privilege role + key-pair service user ─────────

resource "snowflake_account_role" "connector" {
  name    = var.connector_role_name
  comment = "Least-privilege role for the Confluent Snowflake Sink connector."
}

# USAGE lets the role "see into" the database/warehouse; it grants nothing on the
# objects within on its own.
resource "snowflake_grant_privileges_to_account_role" "connector_db_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.connector.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.sodor.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "connector_wh_usage" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.connector.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.sodor.name
  }
}

# Schema-level: USAGE to enter the schema, plus the object-creation rights the
# connector needs. Step 6 (Snowpipe Streaming) writes rows straight to the table
# via the streaming API — no files, no stage — so CREATE STAGE was dropped. We keep
# CREATE PIPE: the classic/managed Kafka connector's streaming mode still provisions
# a per-table pipe (it tracks committed offsets via the pipe's offset token), and
# the docs don't confirm it's droppable, so least-privilege stops here. CREATE TABLE
# lets the connector recreate the landing table if it's ever dropped.
resource "snowflake_grant_privileges_to_account_role" "connector_schema" {
  privileges        = ["USAGE", "CREATE TABLE", "CREATE PIPE"]
  account_role_name = snowflake_account_role.connector.name
  on_schema {
    schema_name = "\"${snowflake_database.sodor.name}\".\"${snowflake_schema.telemetry.name}\""
  }
}

# Table-level: the connector inserts rows into the table, so it needs INSERT;
# SELECT lets it read back what it has already committed (kept conservatively —
# streaming tracks offsets via channel/pipe tokens rather than table reads).
resource "snowflake_grant_privileges_to_account_role" "connector_table" {
  privileges        = ["INSERT", "SELECT"]
  account_role_name = snowflake_account_role.connector.name
  on_schema_object {
    object_type = "TABLE"
    object_name = "\"${snowflake_database.sodor.name}\".\"${snowflake_schema.telemetry.name}\".\"${snowflake_table.telemetry_raw.name}\""
  }
}

# The service user the connector logs in as. No password — it authenticates with
# the RSA private key (stored in /confluent's connector config); Snowflake holds
# the matching public key here. Provide the key BODY only (no PEM header/footer,
# one line) — see README.md.
resource "snowflake_user" "connector" {
  name              = var.connector_user_name
  default_role      = snowflake_account_role.connector.name
  default_warehouse = snowflake_warehouse.sodor.name
  rsa_public_key    = var.connector_rsa_public_key
  comment           = "Service user for the Confluent Snowflake Sink connector (key-pair auth)."
}

resource "snowflake_grant_account_role" "connector_to_user" {
  role_name = snowflake_account_role.connector.name
  user_name = snowflake_user.connector.name
}
