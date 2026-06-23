Hi, I'm Alexander and I love building software for people.

This is a train-telemetry streaming pipeline built on the Kafka → Confluent → Snowflake
stack. Simulated engines emit telemetry (speed, location, door status, …); the messages
stream through Kafka on Confluent Cloud, fan out to both a Snowflake warehouse for analytics
and an always-on live tap, and surface on an interactive webpage. Everything runs in the cloud.

I loved Thomas the Tank Engine as a kid, so the theme was a given — the North Western Railway
on the Island of Sodor.

**Live demo:** https://d3seigtcpso2da.cloudfront.net — deployed in AWS `us-east-2`.

## Architecture

![North Western Railway streaming data pipeline — engine telemetry into a three-partition Kafka topic on Confluent Cloud, fanned out to two independent consumer groups: a Snowflake Sink Connector (analytics) and an always-on ECS Fargate live tap that streams to the web view over SSE; analytics queries round-trip through that task to Snowflake.](web/architecture.svg)

### Producer

Pull the lever on the Master Regulator and the trains start moving. Each tick fires
`POST /produce`, which emits one telemetry reading **per engine** to Kafka; the browser
repeats it on a timer until you throw the lever back. 

Messages are **Avro**, not JSON — it's more compact on the wire and, via the Schema Registry,
gives the pipeline a firm data contract.

### Stream

Telemetry lands on a single Apache Kafka topic (`sodor-telemetry`) on Confluent Cloud. The
topic has **3 partitions** and is keyed by `engine_id` (the train's name — Thomas, Henry, …),
so every reading for a given engine keeps its order on one partition. Two independent consumer
groups read the topic — Kafka's fan-out made physical.

### Consumer group A — Snowflake sink (analytics)

The managed **Snowflake Sink Connector** lands every event in **Snowflake** via
**Snowpipe Streaming** (~3–30s end to end). Analytics are pre-defined Snowflake **views**
(speed leaderboard, throughput per minute, doors-open-while-moving), queried on demand
through `GET /query`.

### Consumer group B — Fargate live tap (live view)

A small **always-on ECS Fargate container** taps the topic under its own consumer group,
holds a latest-per-engine map in memory, and pushes each fresh reading to the browser over
**Server-Sent Events** (`GET /stream`) — straight off Kafka, no warehouse in the path. New
clients get a snapshot-on-connect, then live updates, with a heartbeat to keep idle
connections open.

### Web

One **CloudFront** distribution fronts everything: the static site from S3, plus same-origin
app routes proxied to the live-tap container behind an ALB. No CORS, no separate API host.

## Endpoints

All served same-origin from the CloudFront domain above.

| Method & path        | Source            | Purpose                                                        |
|----------------------|-------------------|---------------------------------------------------------------|
| `GET /stream`        | Kafka (live tap)  | SSE — snapshot on connect, then one event per new reading      |
| `POST /produce`      | Kafka (producer)  | Emit one tick of Avro telemetry for every engine               |
| `GET /query?name=…`  | Snowflake         | Run one of three canned analytics views                        |
| `GET /latest`        | Snowflake         | Latest row per engine (warehouse-backed; UI uses `/stream`)    |
| `GET /healthz`       | container         | ALB liveness probe                                             |
| `GET /`              | container/S3      | The web UI                                                     |

## Stack

| Layer             | Tool                          | Managed in   |
|-------------------|-------------------------------|--------------|
| Producer          | Python + `confluent-kafka` (local script + container `POST /produce`) | `/producer`, `/consumer` (+ `/infra`) |
| Stream            | Confluent Cloud (Kafka)       | `/confluent` (Terraform) |
| Live tap          | Always-on Fargate consumer (FastAPI + `confluent-kafka`) → SSE | `/consumer` (+ `/infra`) |
| Sink              | Confluent Snowflake Sink Connector (Snowpipe Streaming) | `/confluent` (Terraform) |
| Warehouse         | Snowflake (analytics views)   | `/snowflake` (Terraform) |
| Serving + hosting | ECS/Fargate + ALB, S3, CloudFront | `/infra` (Terraform) |
| UI                | Static HTML/JS                | `/web`       |

## Repo layout

Each module has its own README with setup and deploy steps.

- `producer/` — local Avro telemetry generator
- `consumer/` — the live-tap FastAPI container (SSE + folded produce/query routes)
- `confluent/` — Terraform: Kafka cluster, topic, Schema Registry, Snowflake sink connector
- `snowflake/` — Terraform: warehouse, database, analytics views
- `infra/` — Terraform: ECS/Fargate, ALB, S3, CloudFront, secrets
- `web/` — the static site
- `notes/` — design notes

## Future improvements

- **Schematization — typed columns.** Turn the sink's schematization on so it reads the Avro schema and auto-creates real typed columns instead of the single `RECORD_CONTENT` VARIANT.
- **Dead-letter queue.** Arm the sink's `errors.*` knobs so poison records dead-letter instead of stopping the task, plus a redrive path to replay fixed records.
