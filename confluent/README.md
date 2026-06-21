# confluent

Terraform for the Confluent Cloud side of the pipeline: environment (with Stream
Governance / Schema Registry), Basic Kafka cluster, the `sodor-telemetry` topic
(3 partitions), the producer/admin service accounts + API keys, the registered
Avro value schema, and the managed **Snowflake Sink connector** that drains the
topic into Snowflake.

## Schema Registry

Stream Governance is enabled on the environment (`ESSENTIALS` tier), which
provisions the Schema Registry cluster. The Avro value schema is read from
`../producer/telemetry.avsc` and registered as the `sodor-telemetry-value`
subject — Terraform owns it, so editing that file and re-applying evolves the
schema (and runs the compatibility check). The admin SA gets `ResourceOwner` on
all subjects to manage schemas; the producer SA gets only `DeveloperRead` on its
own subject, since it reads the schema rather than registering it.

Everything here is **created** by Terraform except the **environment, which is
imported** — Confluent won't let you delete your last environment, so we adopted
the existing one instead of recreating it.

## Snowflake Sink connector

The fully-managed `SnowflakeSink` connector consumes `sodor-telemetry` (Avro) and
lands it in Snowflake via **classic Snowpipe** (`snowflake.ingestion.method =
SNOWPIPE`). It authenticates to Kafka as its own service account
(`sodor-app-connector`) and to Snowflake with the key-pair user created in
`../snowflake`. Being a consumer, it gets the minimal sink ACLs: cluster
`DESCRIBE`, topic `READ`, consumer-group `READ` (`connect-lcc-` prefix), and
DLQ-topic `CREATE`/`WRITE` (`dlq-lcc-` prefix).

Stand up `../snowflake` **first**, then fill these in `terraform.tfvars` from its
outputs (plus the connector private-key body):

```bash
snowflake_url / snowflake_connector_user / snowflake_connector_role /
snowflake_database / snowflake_schema / snowflake_table   # ../snowflake outputs
snowflake_private_key   # body of ../snowflake/rsa_key_connector.p8, one line
```

The concepts (Snowpipe, VARIANT, the two ingestion methods) live in
`../SNOWFLAKE.md`.

## Stand it up

```bash
cp terraform.tfvars.example terraform.tfvars   # cloud API key/secret + region + Snowflake values
terraform init
terraform import confluent_environment.sodor env-XXXXX   # env id from the console
terraform apply
```

Then point the producer at the outputs:

```bash
terraform output kafka_bootstrap_servers              # -> producer/.env BOOTSTRAP_SERVERS
terraform output producer_kafka_api_key               # -> producer/.env KAFKA_API_KEY
terraform output -raw producer_kafka_api_secret       # -> producer/.env KAFKA_API_SECRET
terraform output schema_registry_url                  # -> producer/.env SCHEMA_REGISTRY_URL
terraform output producer_schema_registry_api_key     # -> producer/.env SCHEMA_REGISTRY_API_KEY
terraform output -raw producer_schema_registry_api_secret  # -> producer/.env SCHEMA_REGISTRY_API_SECRET
```

## Gotchas

- **Region must match Snowflake.** The managed Snowflake Sink connector only
  connects to a data system in the **same region + cloud** as the Kafka cluster.
  If `region` here ≠ your Snowflake account's `CURRENT_REGION()`, the connector
  provisions then fails with *"make sure the cloud region is supported … and
  Snowflake database are in the same region."* Fix the region (recreates the
  cluster), not the URL.
- **`terraform destroy` fails** on the environment (org's last one). Target the
  cluster instead — `terraform destroy -target=confluent_kafka_cluster.basic`
  cascades the topic/ACL — or `terraform state rm confluent_environment.sodor` first.
- **First apply 403s on the topic/ACL?** The `app-manager` role binding hasn't
  propagated yet — just re-run `apply`.
- **Connector stuck in `PROVISIONING`/`FAILED`?** Check it in the console's
  Connectors view. Usual causes: a wrong `snowflake.url.name`, the private-key
  body still wrapped in `-----BEGIN/END-----` lines, or the public key not yet on
  the Snowflake user (re-run `../snowflake`). Avro failures mean the SR binding
  hasn't propagated — re-run `apply`.
