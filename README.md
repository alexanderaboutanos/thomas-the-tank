# Thomas The Tank

![Thomas the Tank Engine emerging from a tunnel](web/thomas.png)

Hello, I'm Alexander and I love building software for people. 

I built a train IoT data pipeline using the Kafka, Confluent, and Snowflake stack.
Trains emit telemetry data (speed, location, etc.), those messages stream through Kafka (Confluent), they land in 
a Snowflake database for analytics, and finally everything is visibile on an interactive webpage. Everything is hosted in the cloud. 

I loved Thomas the Tank Engine as a kid, so the theme was a given... the North Western Railway on the Island of Sodor...

The website is found here: https://d3seigtcpso2da.cloudfront.net

## Architecture

![North Western Railway streaming data pipeline — engine telemetry into a three-partition Kafka topic on Confluent Cloud, fanned out to two independent consumer groups: a Snowflake Sink Connector (analytics) and an always-on ECS Fargate live tap that streams to the web view over SSE; analytics queries round-trip through that task to Snowflake.](web/architecture.svg)


### Producer
Navigate to the webpage and pull the lever on the Master Regulator. Now the trains are moving! Behind the scenes, a python script is now running and emiting dummy telemetry data every 2 seconds until it's told to stop. Rather than using JSON, it emits Avro... for 2 reasons: it's faster and it gives a firm data contract. The telemetry data will continue to flow until you pul the lever and tell the trains to stop.

### Stream
The telemetry data is emitted to an Apache Kafka topic, managed by Confluent. There are three partitions in this topic and the message key used is the `engine_id`, which is the name of the train (Thomas, Henry, etc.). Messages land in the appropriate partition and are consumed by 2 consumer groups...

### Consumer Groups
There are 2 groups consuming this data....
#### A - Snowflake Sink - Analytics 

the managed **Snowflake Sink Connector** lands the same events in **Snowflake**
  via **Snowpipe Streaming** (~3–30s), where canned aggregate queries (`/query`) roll
  them up on demand.

#### B - Fargate Container - Live View



- **Live view** — a small **always-on Fargate container** taps Kafka under its own consumer
  group, holds a latest-per-engine map in memory, and streams each fresh reading to the browser
  over **Server-Sent Events** (`/stream`) — straight off the topic, no warehouse in the path.
- **Analytics** — 


### Webpage
**Web UI + API** — https://d3seigtcpso2da.cloudfront.net — one CloudFront origin: the static site plus same-origin routes `POST /produce`, `GET /latest`, `GET /query?name=…`, and the live SSE stream `GET /stream`. 


~~~~~~~~~~~~

- need a short list of endpoints
- deployed on us-east-2
- 


~~~~~~~~~~~


## Stack

| Layer             | Tool                          | Managed in   |
|-------------------|-------------------------------|--------------|
| Producer          | Python + `confluent-kafka` (local script + container `POST /produce`) | `/producer`, `/consumer` (+ `/infra`) |
| Stream            | Confluent Cloud (Kafka)       | `/confluent` (Terraform) |
| Live tap          | Always-on Fargate consumer (FastAPI + `confluent-kafka`) → SSE | `/consumer` (+ `/infra`) |
| Sink              | Confluent Snowflake Sink Connector (Snowpipe Streaming) | `/confluent` (Terraform) |
| Warehouse         | Snowflake (analytics)         | `/snowflake` (Terraform) |
| Serving + hosting | ECS/Fargate + ALB, S3, CloudFront | `/infra` (Terraform) |
| UI                | Static HTML/JS                | `/web`       |


## Theoretical Considerations

- Why partition by `engine_id` (per-engine ordering) and how partition count affects parallelism.
- Schema Registry + a schema-evolution change you made and what broke / didn't.
- Snowflake Sink Connector config: batching, and why the warehouse view is near-real-time, not sub-second (and that Snowpipe Streaming / a direct Kafka tap is the lower-latency option).
- Consumer lag: what you saw and how you monitor it in production.
- **Live view taps Kafka; analytics reads Snowflake — the production-faithful split (step 9).** Step 7 deliberately read *both* views from Snowflake — one backend, one query shape, far less to break in front of an interviewer — but that runs against the grain: Snowflake is a columnar analytical warehouse, so using it as a low-latency operational store for "latest row per engine" is the wrong tool. Snowpipe Streaming pushed that Snowflake-backed view to near-real-time (~3–30s), but the fully-managed connector's ~30s flush cap kept it short of truly "live." Step 9 closed the gap by **architecture, not by the connector**: the live path now reads straight from Kafka (own consumer group → in-memory latest-per-engine → SSE), sub-second and off the warehouse entirely, while Snowflake keeps the analytics it's actually good at.
- **Snowflake *Stream* vs Snowpipe *Streaming*.** A *Stream* is a CDC change-feed object (drained by a TASK); *Snowpipe Streaming* is a low-latency row-based ingestion method. Easy to conflate — only the latter reduces ingest lag (it's what step 6 turns on).
- **Consuming live from Kafka: stateless Lambda vs. persistent consumer.** Two ways to serve a live Kafka view — a stateless Lambda that `assign()`s partitions + `seek()`s to the high watermark and dedupes by key (scales to zero, but per-invoke connection overhead), or a persistent consumer holding state + pushing over WebSocket/SSE (always-on cost, but lower latency). **Step 9 took the latter** — the always-on Fargate consumer + SSE — trading scale-to-zero for sub-second freshness.
