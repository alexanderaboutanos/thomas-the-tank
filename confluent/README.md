# confluent

Terraform for the Confluent Cloud side: environment (with Schema Registry), Basic
Kafka cluster, the `sodor-telemetry` topic (3 partitions), producer/admin service
accounts + API keys, the registered Avro value schema, and the managed **Snowflake
Sink connector** that drains the topic into Snowflake.

Everything is **created** by Terraform except the **environment, which is imported** —
Confluent won't let you delete your last environment.

## Schema Registry

Stream Governance (`ESSENTIALS`) provisions the Schema Registry. Terraform reads
`../producer/telemetry.avsc` and registers it as the `sodor-telemetry-value`
subject — it **owns** the schema, so editing that file and re-applying evolves it
(with the compatibility check). Admin SA gets `ResourceOwner` on all subjects;
the producer SA gets `DeveloperRead` on its own subject only.

## Snowflake Sink connector

The fully-managed `SnowflakeSink` consumes `sodor-telemetry` (Avro) and lands it via
**Snowpipe Streaming** (`snowflake.ingestion.method = SNOWPIPE_STREAMING`),
near-real-time at ~3–30s (the managed connector caps the flush at ~30s). Schematization
off, so rows land as two VARIANT columns. It authenticates to Kafka as its own SA
(`sodor-app-connector`) and to Snowflake with the key-pair user from `../snowflake`,
with minimal sink ACLs (cluster `DESCRIBE`, topic `READ`, group `READ`, DLQ `CREATE`/`WRITE`).

> Stand up `../snowflake` **first** — the connector needs its outputs
> (`snowflake_url/connector_user/connector_role/database/schema/table`) plus the
> connector private-key body (one line) in `terraform.tfvars`.

## Stand it up

```bash
cp terraform.tfvars.example terraform.tfvars   # cloud API key/secret + region + Snowflake values
terraform init
terraform import confluent_environment.sodor env-XXXXX   # env id from the console
terraform apply
```

Then feed the outputs into `../producer/.env` (`kafka_bootstrap_servers`,
`producer_kafka_api_*`, `schema_registry_url`, `producer_schema_registry_api_*`;
use `-raw` for secrets).

## Gotchas

- **Region must match Snowflake.** The managed Sink only connects to a data system
  in the **same region + cloud** as the Kafka cluster. Mismatch → connector
  provisions then fails. Fix the region (recreates the cluster), not the URL.
- **`terraform destroy` fails** on the environment (org's last one). Target the
  cluster: `terraform destroy -target=confluent_kafka_cluster.basic` (cascades
  topic/ACLs), or `terraform state rm confluent_environment.sodor` first.
- **First apply 403s on topic/ACL?** The `app-manager` role binding hasn't
  propagated — re-run `apply`.
- **Connector stuck `PROVISIONING`/`FAILED`?** Usual causes: wrong
  `snowflake.url.name`, private-key body still wrapped in PEM header/footer, or the
  public key not yet on the Snowflake user. Avro failures = SR binding not
  propagated; re-run.

Concepts (Snowpipe, VARIANT, ingestion methods) live in `../notes/SNOWFLAKE.md`.
