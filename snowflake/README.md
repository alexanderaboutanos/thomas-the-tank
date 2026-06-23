# snowflake

Terraform for the Snowflake side: warehouse (compute), the
`SODOR_RAILWAY.TELEMETRY` namespace, the landing table (`TELEMETRY_RAW`), analytics
**views** (`TELEMETRY_LATEST` + the canned queries), and a least-privilege key-pair
user/role (`SODOR_CONNECTOR` / `SODOR_CONNECTOR_ROLE`) for the Sink connector.

Separate root module from `/confluent` (separate state) — the connector itself lives
there; this just prepares the warehouse and emits the values that configure it.

## Ingestion: Snowpipe Streaming

The connector appends rows **directly** via the Snowpipe Streaming API (no
files/stage/COPY), so the view is near-real-time (~3–30s; the managed connector caps
the flush at ~30s). Schematization off, so every message lands as **two VARIANT
columns** — `RECORD_CONTENT` (the Avro value) + `RECORD_METADATA` (Kafka
topic/partition/offset/timestamp). `TELEMETRY_LATEST` flattens `RECORD_CONTENT` into
typed columns, newest row per engine. The connector role needs no `CREATE STAGE`
(streaming uses none) — just `USAGE`, `CREATE TABLE`/`PIPE`, and `INSERT`/`SELECT`.

## Key pairs

Two RSA pairs, both gitignored (`rsa_key*`, `*.p8`): **admin** (Terraform signs in —
Snowflake is retiring password logins) and **connector** (the Sink signs in as
`SODOR_CONNECTOR`).

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key_admin.p8 -nocrypt
openssl rsa -in rsa_key_admin.p8 -pubout -out rsa_key_admin.pub
# repeat for rsa_key_connector
```

**Three shapes of the same key — don't mix them up:**

| Where | Wants | How to get it |
|-------|-------|---------------|
| Terraform provider here (`private_key`) | admin **private**, full PEM | `file("rsa_key_admin.p8")` |
| Snowflake user `rsa_public_key` (here) | connector **public**, body only, one line | `grep -v '^-----' rsa_key_connector.pub \| tr -d '\n'` |
| Connector `snowflake.private.key` (`/confluent`) | connector **private**, body only, one line | `grep -v '^-----' rsa_key_connector.p8 \| tr -d '\n'` |

## Stand it up

```bash
cp terraform.tfvars.example terraform.tfvars   # org/account/admin + connector public key

# One-time: register the admin PUBLIC key so Terraform can authenticate.
# In a Snowsight worksheet (body-only key):
#   ALTER USER <ADMIN_USER> SET RSA_PUBLIC_KEY='MIIBIjAN...';

terraform init && terraform apply
```

Feed the outputs into `/confluent` (`snowflake_url`, `connector_user`,
`connector_role`, `database`, `schema`, `table`) plus the connector PRIVATE key body.

## Verify rows land

Run the producer, then query. The `snow` CLI connection `sodor` is preconfigured and
default (key-pair/JWT, `ACCOUNTADMIN`, `SODOR_WH`, default db/schema):

```bash
snow sql -q "SELECT COUNT(*) FROM SODOR_RAILWAY.TELEMETRY.TELEMETRY_RAW;"
snow sql -q "SELECT engine_id, event_timestamp, speed_kmh FROM SODOR_RAILWAY.TELEMETRY.TELEMETRY_LATEST ORDER BY engine_id;"
```

Rows land in ~3–30s — re-query a few seconds after producing, then again ~30s later.

> The other `snow` connection `ML23422` uses interactive OAuth and fails
> non-interactively (*"Password is empty"*) — use `sodor` for scripts/agents.

## Gotchas

- **Provider attribute churn.** Written for the Snowflake provider **v2.x** (lock
  file pins the build); majors rename things (`snowflake_role` →
  `snowflake_account_role`, account split into `organization_name` + `account_name`).
  Perpetual churn on `snowflake_user` → set `default_secondary_roles_option = "ALL"`.
- **`snowflake_table` is a preview resource** in v2 — the provider block needs
  `preview_features_enabled = ["snowflake_table_resource"]`. (`snowflake_view` has
  graduated.)
- **`snowflake.url.name` format.** The output guesses
  `https://<org>-<account>.snowflakecomputing.com`; if the connector can't reach
  Snowflake, compare against Snowsight's **Copy account URL**.
- **JWT auth fails on first apply.** The admin public key hasn't propagated, or the
  `ALTER USER … SET RSA_PUBLIC_KEY` body still had the PEM header/footer. Re-check,
  re-run.

Concepts (warehouse vs storage, VARIANT, ingestion methods) live in `../notes/SNOWFLAKE.md`.
