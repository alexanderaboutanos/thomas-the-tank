# consumer — the live Kafka tap (step 9)

The production-faithful live path: an always-on FastAPI app that consumes
`sodor-telemetry` straight from Kafka and pushes the latest reading per engine to
the browser over **Server-Sent Events** — no Snowflake in the live loop. It closes
step 6's freshness gap (the managed Snowflake Sink pins streaming lag at ~30s), so
the warehouse now serves **analytics only** while the UI's **live view** comes from
here.

```
producer  ─▶  Kafka (sodor-telemetry)  ─▶  this consumer  ─▶  SSE  ─▶  browser
                       │
                       └▶  Snowflake Sink (separate consumer group)  ─▶  analytics
```

The live tap and the Snowflake sink read the **same topic under different consumer
groups** — Kafka's fan-out made physical. Each group tracks its own offsets and
sees every message; neither starves the other.

## How it works

- **`live.py`** — `confluent-kafka` is librdkafka (blocking), so the poll loop runs
  in its **own daemon thread**, never on the event loop. It deserializes Avro and
  keeps an in-memory **latest-per-engine** map (`LiveState`), fanning each new
  reading out to every SSE subscriber via `loop.call_soon_threadsafe`.
- **`app.py`** — FastAPI. The lifespan hook captures the event loop, then starts the
  consumer thread. `GET /stream` is the SSE endpoint: a **snapshot-on-connect**
  (the full current map), then one `update` event per reading, with a ~20s
  **heartbeat** comment to survive idle timeouts and quiet engines.

Offsets are **not committed** (`enable.auto.commit=false`, `auto.offset.reset=latest`):
state lives in memory and we never replay history, so every (re)start jumps to the
live edge. The cold-start gap — an engine is invisible until it next reports — is
the owned trade-off (a log-compacted `…-latest` topic would close it).

### Routes

| Route           | Purpose                                                          | Source       |
|-----------------|------------------------------------------------------------------|--------------|
| `GET /stream`   | SSE live feed (snapshot + per-engine updates + heartbeat)        | `app.py`     |
| `POST /produce` | produce one tick of Avro telemetry to Kafka (WRITE)              | `serving.py` |
| `GET /latest`   | live view: latest row per engine, from Snowflake                 | `serving.py` |
| `GET /query`    | one of three canned analytics queries, from Snowflake            | `serving.py` |
| `GET /healthz`  | liveness probe for the ALB target group                          | `app.py`     |
| `GET /`         | localhost test page (same-origin `EventSource`)                  | `app.py`     |

The step-7 serving Lambda's routes — `POST /produce`, `GET /latest`, `GET /query` —
are now **folded into this same app** (`serving.py`) as the Lambda + API Gateway are
retired (see the build order in the root README). `confluent-kafka` and
`snowflake-connector-python` are blocking, so those routes hop off the event loop
with `asyncio.to_thread` — an in-flight Snowflake query never stalls the SSE stream.
`/produce` authenticates as the **producer** service account (WRITE); the live
consumer above stays on the read-only **app-live** identity.

## Run it locally

The live-tap path is validated **locally first** — a local producer + a localhost
page, so there's no mixed-content (the browser talks to `http://localhost`, not a
remote `http://` from an `https://` page).

```bash
cd consumer
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env        # fill from `terraform output` in ../confluent:
#   live_kafka_api_key / -raw live_kafka_api_secret
#   live_schema_registry_api_key / -raw live_schema_registry_api_secret
#   kafka_bootstrap_servers, schema_registry_url
# The folded routes are optional locally (see the second half of .env.example):
#   /produce needs the PRODUCER keys (WRITE); /latest + /query need SNOWFLAKE_*.
# Leave them blank to exercise just the live SSE path.

uvicorn app:app --reload --port 8000
```

Then, in another terminal, wake the engines with the existing producer and open
the page:

```bash
( cd ../producer && source .venv/bin/activate && python producer.py )
open http://localhost:8000
```

You should see the table fill on connect (snapshot) and each engine row flash as
new readings stream in. `curl -N http://localhost:8000/stream` shows the raw SSE
frames.

## Deploy

Containerized and run as a single always-on **Fargate** task behind an **ALB**,
with **CloudFront** routing all the app routes (`/stream`, `/produce`, `/latest`,
`/query`, `/healthz`, caching disabled) to the ALB origin while the static site
stays on S3 — see `../infra`. That's the first piece of this project that **doesn't
scale to zero** (~a few $/mo always-on); scale the service to 0 between demos and
the live view goes dark (analytics keep working — they read Snowflake). The cloud
task gets all its creds from Secrets Manager, injected as env vars by the ECS task
definition — the image carries no AWS SDK.
