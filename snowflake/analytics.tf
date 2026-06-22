# Serving-layer views — the query logic behind the infra/ Lambda's GET /latest
# and GET /query routes (build-order step 7). The Lambda holds NO SQL; it only
# runs `SELECT * FROM <one of these views>`, so all query logic is versioned here
# next to TELEMETRY_LATEST, the view it already follows.
#
# This is a SEPARATE file from main.tf on purpose: step 6 (Snowpipe Streaming) is
# being built in main.tf in parallel, so keeping the serving views here avoids
# colliding on that file. They reference main.tf's resources (same module/state).
#
# Classic-vs-streaming seam: TELEMETRY_FLAT below does the one RECORD_CONTENT
# extraction these analytics share. If step 6 switches the sink to Snowpipe
# Streaming WITH schematization, RECORD_CONTENT becomes typed top-level columns —
# update TELEMETRY_FLAT (and TELEMETRY_LATEST in main.tf) and every downstream
# view + the Lambda keep working unchanged.

# All rows, flattened to typed columns. The base the analytics views build on.
resource "snowflake_view" "telemetry_flat" {
  database = snowflake_database.sodor.name
  schema   = snowflake_schema.telemetry.name
  name     = "TELEMETRY_FLAT"
  comment  = "Every reading, flattened from RECORD_CONTENT into typed columns."

  statement = <<-SQL
    SELECT
      RECORD_CONTENT:engine_id::string                      AS engine_id,
      RECORD_CONTENT:event_id::string                       AS event_id,
      RECORD_CONTENT:timestamp::string                      AS event_timestamp,
      TRY_TO_TIMESTAMP_TZ(RECORD_CONTENT:timestamp::string) AS event_ts,
      RECORD_CONTENT:latitude::float                        AS latitude,
      RECORD_CONTENT:longitude::float                       AS longitude,
      RECORD_CONTENT:speed_kmh::float                       AS speed_kmh,
      RECORD_CONTENT:heading::float                         AS heading,
      RECORD_CONTENT:door_status::string                    AS door_status,
      RECORD_CONTENT:car_temperature_c::float               AS car_temperature_c
    FROM ${snowflake_database.sodor.name}.${snowflake_schema.telemetry.name}.${snowflake_table.telemetry_raw.name}
  SQL

  depends_on = [snowflake_table.telemetry_raw]
}

# (1) Speed leaderboard — current speed per engine, fastest first. Builds on the
# existing latest-per-engine view.
resource "snowflake_view" "speed_leaderboard" {
  database = snowflake_database.sodor.name
  schema   = snowflake_schema.telemetry.name
  name     = "SPEED_LEADERBOARD"
  comment  = "Latest speed per engine, ranked. Serves /query?name=speed_leaderboard."

  statement = <<-SQL
    SELECT engine_id, ROUND(speed_kmh, 1) AS speed_kmh
    FROM ${snowflake_database.sodor.name}.${snowflake_schema.telemetry.name}.${snowflake_view.telemetry_latest.name}
    ORDER BY speed_kmh DESC
  SQL

  depends_on = [snowflake_view.telemetry_latest]
}

# (2) Fleet throughput — messages landed per minute, most recent 15 minutes.
resource "snowflake_view" "throughput_per_minute" {
  database = snowflake_database.sodor.name
  schema   = snowflake_schema.telemetry.name
  name     = "THROUGHPUT_PER_MINUTE"
  comment  = "Event count per minute across the fleet. Serves /query?name=throughput_per_minute."

  statement = <<-SQL
    SELECT TO_CHAR(DATE_TRUNC('minute', event_ts), 'YYYY-MM-DD HH24:MI') AS minute,
           COUNT(*)                                                       AS events
    FROM ${snowflake_database.sodor.name}.${snowflake_schema.telemetry.name}.${snowflake_view.telemetry_flat.name}
    WHERE event_ts IS NOT NULL
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 15
  SQL

  depends_on = [snowflake_view.telemetry_flat]
}

# (3) Safety — doors reported open while the engine was moving.
resource "snowflake_view" "doors_open_moving" {
  database = snowflake_database.sodor.name
  schema   = snowflake_schema.telemetry.name
  name     = "DOORS_OPEN_MOVING"
  comment  = "Open-door-while-moving events per engine. Serves /query?name=doors_open_moving."

  statement = <<-SQL
    SELECT engine_id,
           COUNT(*)                 AS unsafe_events,
           ROUND(MAX(speed_kmh), 1) AS max_speed_kmh
    FROM ${snowflake_database.sodor.name}.${snowflake_schema.telemetry.name}.${snowflake_view.telemetry_flat.name}
    WHERE door_status = 'open' AND speed_kmh > 5
    GROUP BY engine_id
    ORDER BY unsafe_events DESC
  SQL

  depends_on = [snowflake_view.telemetry_flat]
}

# ─── Grant the serving Lambda (the connector role) read access ────────────────
#
# The connector role has SELECT on TELEMETRY_RAW but not on the views, so grant
# it SELECT on exactly the four views the Lambda queries directly. TELEMETRY_FLAT
# needs no grant — it's reached only through the analytics views via the view
# owner's rights (ownership chaining).
locals {
  serving_views = {
    latest      = snowflake_view.telemetry_latest.name
    leaderboard = snowflake_view.speed_leaderboard.name
    throughput  = snowflake_view.throughput_per_minute.name
    doors       = snowflake_view.doors_open_moving.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "connector_serving_views" {
  for_each = local.serving_views

  privileges        = ["SELECT"]
  account_role_name = snowflake_account_role.connector.name
  on_schema_object {
    object_type = "VIEW"
    object_name = "\"${snowflake_database.sodor.name}\".\"${snowflake_schema.telemetry.name}\".\"${each.value}\""
  }
}
