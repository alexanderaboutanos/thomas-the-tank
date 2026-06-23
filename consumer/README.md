# consumer — live Kafka tap

An always-on FastAPI app that consumes `sodor-telemetry` straight from Kafka and
pushes the latest reading per engine to the browser over **Server-Sent Events** —
no Snowflake in the live path. The live tap and the Snowflake sink read the **same
topic under different consumer groups** (Kafka fan-out): the warehouse serves
analytics, this serves the live view.

```
producer ─▶ Kafka (sodor-telemetry) ─┬─▶ this consumer ─▶ SSE ─▶ browser
                                      └─▶ Snowflake Sink (own group) ─▶ analytics
```

## How it works

- **`live.py`** — `confluent-kafka` is blocking (librdkafka), so the poll loop runs
  in its **own daemon thread**. It deserializes Avro into an in-memory
  **latest-per-engine** map (`LiveState`) and fans each reading out to SSE
  subscribers via `loop.call_soon_threadsafe`.
- **`app.py`** — FastAPI. `GET /stream` sends a **snapshot-on-connect**, then one
  `update` per reading, with a ~20s **heartbeat** to survive idle timeouts.
- Offsets are **not committed** (`auto.offset.reset=latest`): state is in-memory and
  history is never replayed, so every restart jumps to the live edge. Cold-start
  trade-off — an engine is invisible until it next reports.
- **`serving.py`** folds in `/produce`, `/latest`, `/query`. Being blocking clients,
  they hop off the event loop with `asyncio.to_thread` so a slow Snowflake query
  never stalls the stream. `/produce` authenticates as the **producer** SA (WRITE);
  the live consumer stays on the read-only **app-live** identity.

| Route           | Purpose                                                   | Source       |
|-----------------|-----------------------------------------------------------|--------------|
| `GET /stream`   | SSE live feed (snapshot + per-engine updates + heartbeat) | `app.py`     |
| `POST /produce` | produce one tick of Avro telemetry to Kafka (WRITE)       | `serving.py` |
| `GET /latest`   | latest row per engine, from Snowflake                     | `serving.py` |
| `GET /query`    | one of three canned analytics queries, from Snowflake     | `serving.py` |
| `GET /healthz`  | ALB target-group liveness probe                           | `app.py`     |
| `GET /`         | localhost test page (same-origin `EventSource`)           | `app.py`     |

## Run locally

Validated locally first — local producer + localhost page, so no mixed-content
(browser talks to `http://localhost`, not a remote `http://` from an `https://` page).

```bash
cd consumer
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
cp .env.example .env          # live_* keys + kafka_bootstrap_servers + schema_registry_url from ../confluent
uvicorn app:app --reload --port 8000
```

```bash
( cd ../producer && source .venv/bin/activate && python producer.py )   # wake the engines
open http://localhost:8000                                              # or: curl -N localhost:8000/stream
```

The folded routes are optional locally: `/produce` needs the PRODUCER keys,
`/latest` + `/query` need `SNOWFLAKE_*`. Leave them blank to exercise just the SSE path.

## Deploy

A single always-on **Fargate** task behind an **ALB**, with **CloudFront** routing
the app routes to it and the static site to S3 — see `../infra`. The first piece
that **doesn't scale to zero** (~a few $/mo); scale to 0 between demos and the live
view goes dark (analytics keep working — they read Snowflake). Creds come from
Secrets Manager as env vars — the image carries no AWS SDK.
