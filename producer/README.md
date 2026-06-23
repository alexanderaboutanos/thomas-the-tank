# producer

Simulated Sodor engine telemetry → Confluent Cloud, serialized as **Avro** against
the Schema Registry. Emits one reading per engine every `INTERVAL_SECONDS` (default 2).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # fill from `terraform output` in ../confluent
python producer.py
```

`.env` needs Kafka creds **and a separate Schema Registry credential pair** —
both from `../confluent` outputs (`kafka_bootstrap_servers`, `producer_kafka_api_*`,
`schema_registry_url`, `producer_schema_registry_api_*`; use `-raw` for secrets).

## Schema

`telemetry.avsc` is the value schema's source of truth, but **Terraform owns it** —
`../confluent` registers it as the `sodor-telemetry-value` subject and runs the
compatibility check. This producer is **read-only** (`auto.register.schemas=false`,
`use.latest.version=true`): it serializes against the registered schema, never
registers. To change shape: edit `telemetry.avsc`, `terraform apply` in
`../confluent`, restart.
