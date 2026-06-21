# snowflake

Terraform for the Snowflake side of the pipeline: the warehouse (compute), the
`SODOR_RAILWAY.TELEMETRY` namespace, the classic-Snowpipe landing table
(`TELEMETRY_RAW`), a flattening view (`TELEMETRY_LATEST`), and a least-privilege
key-pair user/role (`SODOR_CONNECTOR` / `SODOR_CONNECTOR_ROLE`) for the managed
Snowflake Sink connector to authenticate as.

This is a **separate root module** from `/confluent` (separate state). The
connector itself lives in `/confluent`; this layer just prepares the warehouse
and emits the values that configure it. See `../SNOWFLAKE.md` for the concepts
(warehouse vs storage, VARIANT, the two ingestion methods).

## Ingestion mode: classic Snowpipe

The connector batches messages into files and a Snowpipe pipe loads them, landing
every message as **two VARIANT columns** — `RECORD_CONTENT` (the Avro value) and
`RECORD_METADATA` (Kafka topic/partition/offset/timestamp). The typed fields live
*inside* `RECORD_CONTENT`; `TELEMETRY_LATEST` flattens them into tidy columns and
keeps the newest row per engine. (Snowpipe Streaming + schematization — typed
columns, lower latency — is the step-7 upgrade. See `../SNOWFLAKE.md`.)

## Key pairs

Two RSA key pairs, both gitignored (`rsa_key*`, `*.p8`):

1. **Admin** — so Terraform can sign in (Snowflake is retiring single-factor
   passwords for programmatic logins).
2. **Connector** — so the Sink connector can sign in as `SODOR_CONNECTOR`.

```bash
# Admin key pair (PKCS#8, unencrypted)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key_admin.p8 -nocrypt
openssl rsa -in rsa_key_admin.p8 -pubout -out rsa_key_admin.pub

# Connector key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key_connector.p8 -nocrypt
openssl rsa -in rsa_key_connector.p8 -pubout -out rsa_key_connector.pub
```

**Three shapes of the same key — don't mix them up:**

| Where | Wants | How to get it |
|-------|-------|---------------|
| Terraform provider here (`private_key`) | admin **private**, full PEM | `file("rsa_key_admin.p8")` (used as-is) |
| Snowflake user `rsa_public_key` (this module) | connector **public**, body only, one line | `grep -v '^-----' rsa_key_connector.pub \| tr -d '\n'` |
| Connector `snowflake.private.key` (`/confluent`) | connector **private**, body only, one line | `grep -v '^-----' rsa_key_connector.p8 \| tr -d '\n'` |

## Stand it up

```bash
cp terraform.tfvars.example terraform.tfvars   # org/account/admin + connector public key

# One-time bootstrap: register the admin PUBLIC key so Terraform can authenticate.
# Run in a Snowsight worksheet (paste the body-only public key):
#   ALTER USER <YOUR_ADMIN_USER> SET RSA_PUBLIC_KEY='MIIBIjANBgkqhk...';

terraform init
terraform apply
```

Then feed the outputs into the connector in `/confluent`:

```bash
terraform output snowflake_url      # -> /confluent snowflake_url
terraform output connector_user     # -> /confluent snowflake_connector_user
terraform output connector_role     # -> /confluent snowflake_connector_role
terraform output database           # -> /confluent snowflake_database
terraform output schema             # -> /confluent snowflake_schema
terraform output table              # -> /confluent snowflake_table
# plus the connector PRIVATE key body -> /confluent snowflake_private_key
```

## Verify rows land

Run the producer (`/producer`), then in a Snowsight worksheet:

```sql
SELECT COUNT(*) FROM SODOR_RAILWAY.TELEMETRY.TELEMETRY_RAW;          -- raw VARIANT rows
SELECT * FROM SODOR_RAILWAY.TELEMETRY.TELEMETRY_LATEST;              -- typed, latest per engine
```

Give it up to a minute — classic Snowpipe batches, so it's near-real-time, not
instant.

## Gotchas

- **Provider attribute churn.** The Snowflake provider has renamed things across
  majors (`snowflake_role` -> `snowflake_account_role`, account split into
  `organization_name` + `account_name`). This is written for **v2.x**; the lock
  file pins the exact build. If `terraform plan` shows perpetual churn on
  `snowflake_user`, set `default_secondary_roles_option = "ALL"` on it.
- **`snowflake_table` is a preview resource.** In the v2 provider the table
  predates the v1 resource redesign, so the provider block must opt in with
  `preview_features_enabled = ["snowflake_table_resource"]` — otherwise apply
  fails with *"snowflake_table_resource is currently a preview feature"*.
  `snowflake_view` has already graduated and needs no flag.
- **`snowflake.url.name` format.** The output guesses
  `https://<org>-<account>.snowflakecomputing.com`. If the connector can't reach
  Snowflake, compare it against Snowsight's account-menu **Copy account URL**.
- **JWT auth fails on first apply.** The admin public key hasn't propagated, or
  the `ALTER USER ... SET RSA_PUBLIC_KEY` body still had the PEM header/footer.
  Re-check the bootstrap, then re-run.
