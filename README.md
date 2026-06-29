Hi, I'm Alexander and I love building software for people.

This is a train-telemetry streaming pipeline built on the Kafka → Confluent → Snowflake
stack. Simulated engines emit telemetry (speed, location, door status, …); the messages
stream through Kafka on Confluent Cloud, fan out to both a Snowflake warehouse for analytics
and an always-on live tap, and surface on an interactive webpage. Everything runs in the cloud.

A lever in the live view reroutes the same telemetry through **AWS Kinesis** instead of
Confluent Kafka — the identical pipeline over a swappable transport, side by side.

I loved Thomas the Tank Engine as a kid, so the theme was a given — the North Western Railway
on the Island of Sodor.

**Live demo:** https://d3seigtcpso2da.cloudfront.net — deployed in AWS `us-east-2`.

## Architecture

![North Western Railway streaming data pipeline — engine telemetry produced to two swappable stream backends side by side: a three-partition Kafka topic on Confluent Cloud (Avro) and a parallel three-shard AWS Kinesis Data Stream (JSON). A Snowflake Sink Connector reads the Kafka topic into Snowflake for analytics; an always-on ECS Fargate task taps both backends and streams to the web view over SSE, where a lever selects which backend is shown; analytics queries round-trip through that task to Snowflake.](web/architecture.svg)

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

### Swappable stream backend — Confluent Kafka ↔ AWS Kinesis

A lever in the live view flips the same telemetry between **two stream backends**: the
Confluent Kafka path above and a parallel **AWS Kinesis Data Stream**. Both run from boot —
one in-memory state and one consumer thread per backend — so the toggle is instant and
bidirectional; `/produce` and `/stream` simply carry `?source=kafka|kinesis`. Everything
downstream (the live state, the SSE layer, the table) is transport-agnostic, so only the
wire format changes: **Avro + Schema Registry on Kafka, plain JSON on Kinesis** (JSON for
practice; **AWS Glue Schema Registry** is the productionizing step).

Two things surface the Kinesis internals rather than hiding them behind the abstraction:

- a **Stream Internals** panel reads out the active backend's machinery in its own
  vocabulary — Kinesis: shard, sequence number, iterator age; Kafka: partition, offset,
  group — so each backend is visible on its own terms, not just identical-looking rows.
- `notes/KINESIS.md` is a full Kafka↔Kinesis concept map.

One deliberate departure rides along: unlike the rest of the app — which makes **no AWS
calls at runtime** and receives its secrets as injected env vars — the Kinesis path uses
**boto3 authenticating via the ECS task role**, an inline IAM policy scoped to exactly this
stream's ARN with only the six actions it needs. Real, keyless, least-privilege access.

## Endpoints

All served same-origin from the CloudFront domain above.

| Method & path             | Source            | Purpose                                                        |
|---------------------------|-------------------|---------------------------------------------------------------|
| `GET /stream?source=`     | Kafka / Kinesis   | SSE — snapshot on connect, then one event per new reading, from the selected backend |
| `POST /produce?source=`   | Kafka / Kinesis   | Emit one tick of telemetry for every engine to the selected backend |
| `GET /internals?source=`  | Kafka / Kinesis   | The active backend's live machinery (shard/sequence/iterator age · partition/offset) for the Stream Internals panel |
| `GET /query?name=…`       | Snowflake         | Run one of three canned analytics views                        |
| `GET /latest`             | Snowflake         | Latest row per engine (warehouse-backed; UI uses `/stream`)    |
| `GET /healthz`            | container         | ALB liveness probe                                             |
| `GET /`                   | container/S3      | The web UI                                                     |

## Stack

| Layer             | Tool                          | Managed in   |
|-------------------|-------------------------------|--------------|
| Producer          | Python + `confluent-kafka` (local script + container `POST /produce`) | `/producer`, `/consumer` (+ `/infra`) |
| Stream            | Confluent Cloud (Kafka) **or** AWS Kinesis Data Streams — swappable via the live-view lever | `/confluent`, `/infra` (Terraform) |
| Live tap          | Always-on Fargate consumer (FastAPI + `confluent-kafka` + `boto3`) → SSE, tapping either backend | `/consumer` (+ `/infra`) |
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
