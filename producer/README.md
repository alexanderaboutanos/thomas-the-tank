# producer

Simulated Sodor engine telemetry → Confluent Cloud, serialized as **Avro**
against a Schema Registry subject.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in your cluster + Schema Registry details
python producer.py
```

## Schema

`telemetry.avsc` is the single source of truth for the value schema. Terraform
(in `../confluent`) registers it as the `sodor-telemetry-value` subject and owns
its evolution. This producer is **read-only**: it runs with
`auto.register.schemas=false` + `use.latest.version=true`, so it fetches the
registered schema and serializes against it — it never registers anything. To
change the shape, edit `telemetry.avsc`, `terraform apply` in `../confluent`
(which runs the compatibility check), then restart the producer.

## Config

`.env` needs the Kafka credentials plus a **separate** Schema Registry
credential pair. Pull them from `../confluent`:

```bash
terraform output kafka_bootstrap_servers              # -> BOOTSTRAP_SERVERS
terraform output producer_kafka_api_key               # -> KAFKA_API_KEY
terraform output -raw producer_kafka_api_secret       # -> KAFKA_API_SECRET
terraform output schema_registry_url                  # -> SCHEMA_REGISTRY_URL
terraform output producer_schema_registry_api_key     # -> SCHEMA_REGISTRY_API_KEY
terraform output -raw producer_schema_registry_api_secret  # -> SCHEMA_REGISTRY_API_SECRET
```

Sanity-check before this: use Confluent's built-in **Datagen Source Connector**
as a 5-minute smoke test to confirm the cluster works, then run your own producer.
