# producer

Simulated Sodor engine telemetry → Confluent Cloud.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in your cluster details
python producer.py
```

Sanity-check before this: use Confluent's built-in **Datagen Source Connector**
as a 5-minute smoke test to confirm the cluster works, then run your own producer.
Once the JSON loop is solid, upgrade to Avro + Schema Registry.
